//! The per-trade table, and the thread that turns batches into snapshots.

use crate::snapshot::{RiskSnapshot, TradeStatus};
use crossbeam::channel::Receiver;
use riskrt_core::{BookId, MarketVersion, Portfolio, TradeId};
use riskrt_curves::{Subscriber, publish};
use riskrt_sched::PricedBatch;
use std::io;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AggStats {
    pub batches: u64,
    pub trades_applied: u64,
    /// Results ignored because the trade already had a newer version.
    pub stale_dropped: u64,
    /// Results ignored because the trade id was outside the portfolio.
    pub unknown_dropped: u64,
    /// Failures applied (a trade marked failed, or kept failed).
    pub failures_applied: u64,
}

/// Owns the latest-result-per-trade table. Single owner, no threads;
/// `spawn_aggregator` runs it.
pub struct Aggregator {
    books: Vec<BookId>,
    book_count: usize,
    curve_count: usize,
    current: Arc<RiskSnapshot>,
    stats: AggStats,
}

impl Aggregator {
    pub fn new(portfolio: &Portfolio, curve_count: usize) -> Self {
        let books: Vec<BookId> = portfolio.positions().iter().map(|p| p.book).collect();
        let book_count = portfolio.book_count();
        let current = Arc::new(RiskSnapshot::empty(books.len(), book_count, curve_count));
        Self {
            books,
            book_count,
            curve_count,
            current,
            stats: AggStats::default(),
        }
    }

    /// The latest snapshot (version zero before the first batch).
    pub fn current(&self) -> Arc<RiskSnapshot> {
        Arc::clone(&self.current)
    }

    pub fn stats(&self) -> &AggStats {
        &self.stats
    }

    /// Applies one batch and returns the new snapshot.
    ///
    /// A result for a trade that already holds a newer version is dropped;
    /// batches arrive in version order from the scheduler, so that only
    /// happens if someone replays old batches.
    pub fn apply(&mut self, batch: &PricedBatch) -> Arc<RiskSnapshot> {
        self.stats.batches += 1;
        let mut table = self.current.table().to_vec();
        let mut status = self.current.statuses().to_vec();
        for priced in &batch.trades {
            let trade: TradeId = priced.risk.trade;
            let Some(slot) = table.get_mut(trade.index()) else {
                self.stats.unknown_dropped += 1;
                continue;
            };
            if slot
                .as_ref()
                .is_some_and(|old| old.risk.version > priced.risk.version)
            {
                self.stats.stale_dropped += 1;
                continue;
            }
            *slot = Some(Arc::new(priced.clone()));
            status[trade.index()] = TradeStatus::Priced;
            self.stats.trades_applied += 1;
        }
        for failure in &batch.failed {
            let Some(slot) = status.get_mut(failure.trade.index()) else {
                self.stats.unknown_dropped += 1;
                continue;
            };
            if table[failure.trade.index()]
                .as_ref()
                .is_some_and(|old| old.risk.version > batch.version)
            {
                self.stats.stale_dropped += 1;
                continue;
            }
            let since = match slot {
                TradeStatus::Failed { since, .. } => *since,
                _ => batch.version,
            };
            *slot = TradeStatus::Failed {
                since,
                error: failure.error.clone(),
            };
            self.stats.failures_applied += 1;
        }
        let version: MarketVersion = batch.version.max(self.current.version());
        self.current = Arc::new(RiskSnapshot::from_table(
            version,
            table,
            status,
            &self.books,
            self.book_count,
            self.curve_count,
        ));
        self.current()
    }
}

/// Runs `aggregator` on its own thread, publishing a snapshot per batch.
///
/// Exits when the batch channel closes (the scheduler is gone), which closes
/// the returned subscriber after its final snapshot.
pub fn spawn_aggregator(
    batches: Receiver<PricedBatch>,
    mut aggregator: Aggregator,
) -> io::Result<(JoinHandle<AggStats>, Subscriber<RiskSnapshot>)> {
    let (mut publisher, subscriber) = publish::channel(aggregator.current());
    let handle = thread::Builder::new()
        .name("aggregator".into())
        .spawn(move || {
            for batch in batches.iter() {
                let before = aggregator.current().version();
                let snapshot = aggregator.apply(&batch);
                // A replayed or empty batch gives no new version: nothing to publish.
                if snapshot.version() > before {
                    publisher.publish(snapshot);
                }
            }
            aggregator.stats().clone()
        })?;
    Ok((handle, subscriber))
}

#[cfg(test)]
mod tests {
    use super::*;
    use riskrt_core::{CurveId, Day, Ladder, PricedTrade, Product, Risk, TradeRisk};
    use riskrt_core::{PayRec, Position};

    fn position(book: u32, curve: u32) -> Position {
        Position {
            book: BookId(book),
            discount: CurveId(curve),
            forward: Some(CurveId(curve)),
            notional: 1e6,
            product: Product::Swap {
                start: Day(20_721),
                maturity: Day(22_547),
                fixed_rate: 0.04,
                fixed_leg: PayRec::Pay,
            },
        }
    }

    /// trade: 0 -> book 0 curve 0, 1 -> book 1 curve 0, 2 -> book 0 curve 1
    fn aggregator() -> Aggregator {
        let mut b = Portfolio::builder();
        for (book, curve) in [(0, 0), (1, 0), (0, 1)] {
            b.add(position(book, curve)).unwrap();
        }
        Aggregator::new(&b.build(), 2)
    }

    fn priced(trade: u32, curve: u32, version: u64, pv: f64, dv01: f64) -> PricedTrade {
        PricedTrade {
            risk: TradeRisk::new(TradeId(trade), MarketVersion(version), Risk { pv, dv01 }),
            ladder: Ladder {
                curve: CurveId(curve),
                per_bp: vec![dv01 * 0.25, dv01 * 0.75],
            },
        }
    }

    fn batch(version: u64, trades: Vec<PricedTrade>) -> PricedBatch {
        PricedBatch {
            version: MarketVersion(version),
            trades,
            failed: vec![],
        }
    }

    fn failure(trade: u32, reason: &str) -> riskrt_sched::TradeFailure {
        riskrt_sched::TradeFailure {
            trade: TradeId(trade),
            error: riskrt_sched::PricingError::Invalid {
                trade: TradeId(trade),
                reason: reason.into(),
            },
        }
    }

    #[test]
    fn a_failed_trade_keeps_its_last_numbers_and_is_flagged_until_it_prices_again() {
        let mut agg = aggregator();
        agg.apply(&batch(
            1,
            vec![
                priced(0, 0, 1, 100.0, 10.0),
                priced(1, 0, 1, 50.0, -4.0),
                priced(2, 1, 1, -20.0, 2.0),
            ],
        ));
        let before = agg.current();
        assert!(before.is_healthy());

        // Round 2: trade 1 fails, the others reprice.
        let mut b = batch(
            2,
            vec![priced(0, 0, 2, 101.0, 10.0), priced(2, 1, 2, -21.0, 2.0)],
        );
        b.failed = vec![failure(1, "non-finite risk")];
        let s = agg.apply(&b);
        assert_eq!(s.version(), MarketVersion(2));
        assert_eq!(s.failed_count(), 1);
        assert!(!s.is_healthy());
        assert!(s.is_complete(), "it still has a result");
        assert_eq!(
            s.status(TradeId(1)),
            Some(&TradeStatus::Failed {
                since: MarketVersion(2),
                error: riskrt_sched::PricingError::Invalid {
                    trade: TradeId(1),
                    reason: "non-finite risk".into()
                }
            })
        );
        // Last good numbers stay in the table and in the totals.
        assert_eq!(s.trade(TradeId(1)).unwrap().risk.risk.pv, 50.0);
        assert_eq!(s.total().pv, 101.0 + 50.0 - 21.0);
        assert_eq!(
            s.failed().map(|(t, _)| t).collect::<Vec<_>>(),
            vec![TradeId(1)]
        );

        // Round 3: still failing. `since` is kept, the error is the newest.
        let mut b = batch(3, vec![]);
        b.failed = vec![failure(1, "still bad")];
        let s = agg.apply(&b);
        assert!(matches!(
            s.status(TradeId(1)),
            Some(TradeStatus::Failed { since: MarketVersion(2), error })
                if error.to_string().contains("still bad")
        ));
        assert_eq!(agg.stats().failures_applied, 2);

        // Round 4: it prices again.
        let s = agg.apply(&batch(4, vec![priced(1, 0, 4, 52.0, -4.0)]));
        assert_eq!(s.status(TradeId(1)), Some(&TradeStatus::Priced));
        assert_eq!(s.failed_count(), 0);
        assert!(s.is_healthy());
        assert_eq!(s.total().pv, 101.0 + 52.0 - 21.0);
    }

    #[test]
    fn a_trade_that_never_priced_and_fails_is_stale_but_not_in_the_totals() {
        use fq_core::time::Date;
        use fq_rates::index::OvernightIndex;
        use riskrt_curves::CurveSet;
        let mut agg = aggregator();
        let mut b = batch(1, vec![priced(0, 0, 1, 100.0, 10.0)]);
        b.failed = vec![failure(2, "boom")];
        let s = agg.apply(&b);
        assert_eq!(s.priced_count(), 1);
        assert_eq!(s.failed_count(), 1);
        assert_eq!(s.total().pv, 100.0);
        let today = Date::from_ymd(2026, 9, 23).unwrap();
        let set = CurveSet::empty(2, today, Arc::new(OvernightIndex::sofr()));
        // Trade 0 is stale because the set has nothing built; trade 2 because it failed.
        assert_eq!(s.stale_against(&set), 2);
    }

    #[test]
    fn empty_snapshot_has_zero_everything() {
        let agg = aggregator();
        let s = agg.current();
        assert_eq!(s.version(), MarketVersion::ZERO);
        assert_eq!(s.total(), Risk::ZERO);
        assert_eq!((s.trade_count(), s.priced_count()), (3, 0));
        assert!(s.ladder(CurveId(0)).is_none());
        assert!(s.trade(TradeId(0)).is_none());
    }

    #[test]
    fn totals_partition_by_book_and_curve() {
        let mut agg = aggregator();
        let s = agg.apply(&batch(
            1,
            vec![
                priced(0, 0, 1, 100.0, 10.0),
                priced(1, 0, 1, 50.0, -4.0),
                priced(2, 1, 1, -20.0, 2.0),
            ],
        ));
        assert_eq!(s.version(), MarketVersion(1));
        assert!(s.is_complete());
        assert_eq!(
            s.total(),
            Risk {
                pv: 130.0,
                dv01: 8.0
            }
        );
        assert_eq!(
            s.book(BookId(0)),
            Some(Risk {
                pv: 80.0,
                dv01: 12.0
            })
        );
        assert_eq!(
            s.book(BookId(1)),
            Some(Risk {
                pv: 50.0,
                dv01: -4.0
            })
        );
        assert_eq!(
            s.curve(CurveId(0)),
            Some(Risk {
                pv: 150.0,
                dv01: 6.0
            })
        );
        assert_eq!(
            s.curve(CurveId(1)),
            Some(Risk {
                pv: -20.0,
                dv01: 2.0
            })
        );
        // Book and curve totals each sum to the portfolio total.
        let books: f64 = s.books().iter().map(|r| r.pv).sum();
        let curves: f64 = s.curves().iter().map(|r| r.pv).sum();
        assert_eq!(books, s.total().pv);
        assert_eq!(curves, s.total().pv);
        // Ladders sum per curve.
        assert_eq!(s.ladder(CurveId(0)).unwrap().per_bp, vec![1.5, 4.5]);
        assert_eq!(s.ladder(CurveId(1)).unwrap().per_bp, vec![0.5, 1.5]);
        assert_eq!(s.book(BookId(9)), None);
    }

    #[test]
    fn repricing_replaces_only_the_trades_in_the_batch() {
        let mut agg = aggregator();
        let first = agg.apply(&batch(
            1,
            vec![priced(0, 0, 1, 100.0, 10.0), priced(2, 1, 1, -20.0, 2.0)],
        ));
        let second = agg.apply(&batch(2, vec![priced(0, 0, 2, 110.0, 11.0)]));
        assert_eq!(second.version(), MarketVersion(2));
        assert_eq!(
            second.total(),
            Risk {
                pv: 90.0,
                dv01: 13.0
            }
        );
        assert_eq!(
            second.trade(TradeId(0)).unwrap().risk.version,
            MarketVersion(2)
        );
        assert_eq!(
            second.trade(TradeId(2)).unwrap().risk.version,
            MarketVersion(1)
        );
        assert_eq!(second.older_than(MarketVersion(2)), 1);
        // The earlier snapshot is untouched.
        assert_eq!(
            first.total(),
            Risk {
                pv: 80.0,
                dv01: 12.0
            }
        );
        assert_eq!(first.trade(TradeId(0)).unwrap().risk.risk.pv, 100.0);
        assert_eq!(agg.stats().trades_applied, 3);
    }

    #[test]
    fn stale_and_unknown_results_are_dropped() {
        let mut agg = aggregator();
        agg.apply(&batch(3, vec![priced(0, 0, 3, 100.0, 10.0)]));
        let s = agg.apply(&batch(
            2,
            vec![priced(0, 0, 2, 999.0, 99.0), priced(7, 0, 2, 1.0, 1.0)],
        ));
        assert_eq!(s.trade(TradeId(0)).unwrap().risk.risk.pv, 100.0);
        assert_eq!(
            s.version(),
            MarketVersion(3),
            "version never goes backwards"
        );
        assert_eq!(agg.stats().stale_dropped, 1);
        assert_eq!(agg.stats().unknown_dropped, 1);
    }

    #[test]
    fn totals_do_not_depend_on_batch_history() {
        let mut a = aggregator();
        a.apply(&batch(
            1,
            vec![
                priced(0, 0, 1, 1.0, 1.0),
                priced(1, 0, 1, 2.0, 2.0),
                priced(2, 1, 1, 3.0, 3.0),
            ],
        ));
        a.apply(&batch(2, vec![priced(0, 0, 2, 5.0, 5.0)]));
        a.apply(&batch(3, vec![priced(1, 0, 3, 7.0, 7.0)]));
        let mut b = aggregator();
        b.apply(&batch(
            3,
            vec![
                priced(0, 0, 2, 5.0, 5.0),
                priced(1, 0, 3, 7.0, 7.0),
                priced(2, 1, 1, 3.0, 3.0),
            ],
        ));
        assert_eq!(a.current().total(), b.current().total());
        assert_eq!(a.current().books(), b.current().books());
        assert_eq!(
            a.current().ladder(CurveId(0)),
            b.current().ladder(CurveId(0))
        );
    }

    #[test]
    fn trades_on_a_degraded_curve_count_as_stale() {
        use fq_core::time::Date;
        use fq_rates::index::OvernightIndex;
        use riskrt_curves::{CurveBuildError, CurveSet};
        let mut agg = aggregator();
        agg.apply(&batch(
            1,
            vec![
                priced(0, 0, 1, 1.0, 1.0),
                priced(1, 0, 1, 1.0, 1.0),
                priced(2, 1, 1, 1.0, 1.0),
            ],
        ));
        let snap = agg.current();
        let today = Date::from_ymd(2026, 9, 23).unwrap();
        let healthy = CurveSet::empty(2, today, Arc::new(OvernightIndex::sofr()));
        // Nothing built: every curve is Unbuilt, so everything is stale.
        assert_eq!(snap.stale_against(&healthy), 3);
        // A set whose curve 0 failed (and curve 1 unbuilt) still flags all three.
        let failed = healthy.next(
            MarketVersion(1),
            healthy.index().clone(),
            vec![],
            vec![(CurveId(0), CurveBuildError::Spec("x".into()))],
        );
        assert_eq!(snap.stale_against(&failed), 3);
    }
}
