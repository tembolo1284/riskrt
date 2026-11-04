//! The scheduling policy, with no threads of its own.

use riskrt_core::{MarketVersion, PricedTrade, TradeId};
use riskrt_curves::CurveSet;
use riskrt_deps::{CurveIndex, DirtySet};
use riskrt_pricing::{Pricer, PricingError, price_batch};
use std::sync::Arc;

/// One round of repricing: every trade the scheduler decided needed it at
/// `version`, priced against that one curve set.
#[derive(Clone, Debug, PartialEq)]
pub struct PricedBatch {
    /// Set version every result in `trades` was priced against.
    pub version: MarketVersion,
    /// In ascending `TradeId` order.
    pub trades: Vec<PricedTrade>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchedStats {
    /// Curve sets handed to the scheduler (versions skipped are not counted).
    pub sets_seen: u64,
    /// Batches produced (rounds with at least one priced trade).
    pub batches: u64,
    /// Trades priced successfully, summed over batches.
    pub trades_priced: u64,
    /// Largest single batch.
    pub max_batch: usize,
    /// Trades put back because their curve was not built yet.
    pub requeued: u64,
    /// Trades dropped for any other pricing error.
    pub failures: u64,
    pub last_failure: Option<(TradeId, PricingError)>,
}

/// Owns the dirty set and decides, for each curve set, which trades to price.
///
/// Single-owner, so the policy is testable without threads; `spawn_scheduler`
/// runs it on a thread.
pub struct Scheduler {
    pricer: Arc<dyn Pricer>,
    index: CurveIndex,
    dirty: DirtySet,
    /// Last set version whose changes have been marked.
    seen: MarketVersion,
    batch: Vec<TradeId>,
    stats: SchedStats,
}

impl Scheduler {
    pub fn new(pricer: Arc<dyn Pricer>, index: CurveIndex) -> Self {
        let trade_count = pricer.trade_count();
        assert_eq!(
            index.trade_count(),
            trade_count,
            "index and pricer are for different portfolios"
        );
        Self {
            pricer,
            index,
            dirty: DirtySet::new(trade_count),
            seen: MarketVersion::ZERO,
            batch: Vec::with_capacity(trade_count),
            stats: SchedStats::default(),
        }
    }

    pub fn stats(&self) -> &SchedStats {
        &self.stats
    }

    /// Number of trades waiting to be priced.
    pub fn pending(&self) -> usize {
        self.dirty.len()
    }

    /// Marks the trades affected by `set`: those on curves rebuilt since the
    /// last set handled. Returns how many were newly marked.
    pub fn mark(&mut self, set: &CurveSet) -> usize {
        self.stats.sets_seen += 1;
        let newly = self
            .dirty
            .mark_curves(&self.index, set.changed_since(self.seen));
        self.seen = self.seen.max(set.version());
        newly
    }

    /// Prices everything currently dirty against `set`, on the current rayon
    /// pool. Returns `None` if nothing was priced.
    ///
    /// Trades whose curve `set` has not built stay dirty for the next round;
    /// other failures are recorded in `stats` and the trade is dropped until
    /// its curve next moves.
    pub fn price_pending(&mut self, set: &CurveSet) -> Option<PricedBatch> {
        self.batch.clear();
        self.dirty.drain_into(&mut self.batch);
        if self.batch.is_empty() {
            return None;
        }
        let results = price_batch(self.pricer.as_ref(), &self.batch, set);

        let mut trades = Vec::with_capacity(results.len());
        for (trade, result) in self.batch.iter().zip(results) {
            match result {
                Ok(priced) => trades.push(priced),
                Err(PricingError::CurveNotBuilt { .. }) => {
                    self.stats.requeued += 1;
                    self.dirty.mark(*trade);
                }
                Err(e) => {
                    self.stats.failures += 1;
                    self.stats.last_failure = Some((*trade, e));
                }
            }
        }
        if trades.is_empty() {
            return None;
        }
        self.stats.batches += 1;
        self.stats.trades_priced += trades.len() as u64;
        self.stats.max_batch = self.stats.max_batch.max(trades.len());
        Some(PricedBatch {
            version: set.version(),
            trades,
        })
    }

    /// `mark` then `price_pending`: one full round for one curve set.
    pub fn round(&mut self, set: &CurveSet) -> Option<PricedBatch> {
        self.mark(set);
        self.price_pending(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_core::time::Date;
    use fq_rates::index::OvernightIndex;
    use riskrt_core::{BookId, CurveId, Day, PayRec, Portfolio, Position, Product, QuoteId, Tick};
    use riskrt_curves::{BuildConfig, CurveBuilder, CurveSetSpec, CurveSpec};
    use riskrt_market::QuoteSnapshot;
    use riskrt_pricing::FqPricer;

    const TODAY: Day = Day(20_719);
    const QUOTES: [(&str, f64); 4] = [
        ("1Y", 0.040),
        ("2Y", 0.038),
        ("5Y", 0.0378),
        ("10Y", 0.0395),
    ];

    fn swap(curve: u32) -> Position {
        let d = |y, m, dd| Day(Date::from_ymd(y, m, dd).unwrap().unix_days());
        Position {
            book: BookId(0),
            discount: CurveId(curve),
            forward: Some(CurveId(curve)),
            notional: 1e6,
            product: Product::Swap {
                start: d(2026, 9, 25),
                maturity: d(2030, 9, 25),
                fixed_rate: 0.04,
                fixed_leg: PayRec::Pay,
            },
        }
    }

    /// trades 0,2 on curve 0; trades 1,3 on curve 1. Quotes 0..4 and 4..8.
    fn fixture() -> (Scheduler, CurveBuilder) {
        let mut b = Portfolio::builder();
        for c in [0, 1, 0, 1] {
            b.add(swap(c)).unwrap();
        }
        let portfolio = b.build();
        let valuation = Date::from_unix_days(TODAY.0).unwrap();
        let pricer: Arc<dyn Pricer> = Arc::new(FqPricer::build(&portfolio, valuation).unwrap());
        let index = CurveIndex::build(&portfolio, 2).unwrap();
        let spec = |c: u32| {
            let pillars: Vec<_> = QUOTES
                .iter()
                .enumerate()
                .map(|(k, (t, _))| (*t, QuoteId(c * 4 + k as u32)))
                .collect();
            CurveSpec::sofr_ois(CurveId(c), &format!("C{c}"), &pillars).unwrap()
        };
        let set_spec = CurveSetSpec::new(TODAY, vec![spec(0), spec(1)]).unwrap();
        let builder = CurveBuilder::new(
            set_spec,
            Arc::new(OvernightIndex::sofr()),
            BuildConfig::default(),
        );
        (Scheduler::new(pricer, index), builder)
    }

    fn ticks(curve: u32, seq: u64, shift: f64) -> Vec<Tick> {
        QUOTES
            .iter()
            .enumerate()
            .map(|(k, (_, r))| Tick::new(QuoteId(curve * 4 + k as u32), seq, 0, r + shift))
            .collect()
    }

    fn ids(batch: &PricedBatch) -> Vec<u32> {
        batch.trades.iter().map(|p| p.risk.trade.0).collect()
    }

    #[test]
    fn first_set_prices_only_trades_on_built_curves() {
        let (mut sched, mut builder) = fixture();
        let v1 = QuoteSnapshot::empty(8).next(&ticks(0, 1, 0.0)).unwrap();
        let set = builder.on_snapshot(&v1).unwrap();
        assert_eq!(sched.mark(&set), 2);
        let batch = sched.price_pending(&set).unwrap();
        assert_eq!(batch.version, set.version());
        assert_eq!(ids(&batch), vec![0, 2]);
        assert!(batch.trades.iter().all(|p| p.risk.version == set.version()));
        assert_eq!(sched.pending(), 0);
        assert_eq!(sched.stats().trades_priced, 2);
    }

    #[test]
    fn later_sets_reprice_only_the_curve_that_moved() {
        let (mut sched, mut builder) = fixture();
        let v1 = QuoteSnapshot::empty(8)
            .next(&[ticks(0, 1, 0.0), ticks(1, 1, 0.0)].concat())
            .unwrap();
        let first = builder.on_snapshot(&v1).unwrap();
        assert_eq!(ids(&sched.round(&first).unwrap()), vec![0, 1, 2, 3]);

        let v2 = v1.next(&[Tick::new(QuoteId(5), 2, 0, 0.039)]).unwrap();
        let second = builder.on_snapshot(&v2).unwrap();
        assert_eq!(ids(&sched.round(&second).unwrap()), vec![1, 3]);
        assert!(
            sched.round(&second).is_none(),
            "same set again: nothing dirty"
        );
        assert_eq!(sched.stats().batches, 2);
    }

    #[test]
    fn skipped_sets_are_conflated_into_one_batch() {
        let (mut sched, mut builder) = fixture();
        let v1 = QuoteSnapshot::empty(8)
            .next(&[ticks(0, 1, 0.0), ticks(1, 1, 0.0)].concat())
            .unwrap();
        let first = builder.on_snapshot(&v1).unwrap();
        sched.round(&first).unwrap();
        // Three curve sets go by; the scheduler only sees the last.
        let v2 = v1.next(&[Tick::new(QuoteId(1), 2, 0, 0.0381)]).unwrap();
        let _ = builder.on_snapshot(&v2).unwrap();
        let v3 = v2.next(&[Tick::new(QuoteId(6), 2, 0, 0.0379)]).unwrap();
        let _ = builder.on_snapshot(&v3).unwrap();
        let v4 = v3.next(&[Tick::new(QuoteId(1), 3, 0, 0.0382)]).unwrap();
        let last = builder.on_snapshot(&v4).unwrap();
        let batch = sched.round(&last).unwrap();
        assert_eq!(ids(&batch), vec![0, 1, 2, 3]);
        assert_eq!(batch.version, last.version());
        assert_eq!(sched.stats().sets_seen, 2);
    }

    #[test]
    fn trades_on_an_unbuilt_curve_wait_without_erroring() {
        let (mut sched, mut builder) = fixture();
        // Mark everything by hand, as a start-up might, before curve 1 exists.
        let v1 = QuoteSnapshot::empty(8).next(&ticks(0, 1, 0.0)).unwrap();
        let set = builder.on_snapshot(&v1).unwrap();
        sched.mark(&set);
        sched.dirty.mark_all();
        let batch = sched.price_pending(&set).unwrap();
        assert_eq!(ids(&batch), vec![0, 2]);
        assert_eq!(sched.stats().requeued, 2);
        assert_eq!(sched.stats().failures, 0);
        assert_eq!(sched.pending(), 2);
        // Curve 1 arrives: the waiting trades go out, marked by the set change.
        let v2 = v1.next(&ticks(1, 1, 0.0)).unwrap();
        let set = builder.on_snapshot(&v2).unwrap();
        assert_eq!(ids(&sched.round(&set).unwrap()), vec![1, 3]);
        assert_eq!(sched.pending(), 0);
    }

    #[test]
    fn results_do_not_depend_on_thread_count() {
        let run = |threads: usize| {
            let (mut sched, mut builder) = fixture();
            let v1 = QuoteSnapshot::empty(8)
                .next(&[ticks(0, 1, 0.0), ticks(1, 1, 0.0)].concat())
                .unwrap();
            let set = builder.on_snapshot(&v1).unwrap();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| sched.round(&set).unwrap())
        };
        assert_eq!(run(1), run(4));
    }
}
