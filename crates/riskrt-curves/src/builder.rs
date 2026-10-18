//! The curve-building stage: decides what to rebuild, and the thread that runs it.

use crate::build::{BuildConfig, BuiltCurve, build_curve};
use crate::error::CurveBuildError;
use crate::publish::{self, Subscriber};
use crate::set::CurveSet;
use crate::spec::{CurveSetSpec, CurveSpec};
use fq_rates::index::OvernightIndex;
use rayon::prelude::*;
use riskrt_core::{CurveId, MarketVersion};
use riskrt_market::{MarketReader, QuoteSnapshot, Wait};
use std::io;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BuildStats {
    /// Quote snapshots examined.
    pub snapshots: u64,
    /// Curve sets produced.
    pub sets: u64,
    /// Individual curve rebuilds that succeeded.
    pub curves_built: u64,
    /// Rebuilds skipped because a pillar had no quote yet.
    pub not_ready: u64,
    /// Rebuilds that failed; the previous curve stayed in place.
    pub failures: u64,
    pub last_failure: Option<(CurveId, CurveBuildError)>,
}

/// Decides which curves a new quote snapshot dirties, rebuilds those, and
/// produces the next `CurveSet`. Plain single-owner state with no threads of
/// its own, so the policy is testable directly; `spawn_curve_builder` runs it.
pub struct CurveBuilder {
    spec: CurveSetSpec,
    cfg: BuildConfig,
    index: Arc<OvernightIndex>,
    current: Arc<CurveSet>,
    /// Latest quote version already accounted for.
    seen_quotes: MarketVersion,
    /// Set by an index change: every curve must be rebuilt.
    rebuild_all: bool,
    stats: BuildStats,
}

impl CurveBuilder {
    pub fn new(spec: CurveSetSpec, index: Arc<OvernightIndex>, cfg: BuildConfig) -> Self {
        let current = Arc::new(CurveSet::empty(
            spec.curve_count(),
            spec.valuation_date(),
            Arc::clone(&index),
        ));
        Self {
            spec,
            cfg,
            index,
            current,
            seen_quotes: MarketVersion::ZERO,
            rebuild_all: false,
            stats: BuildStats::default(),
        }
    }

    /// The latest set this builder produced (version zero before the first).
    pub fn current(&self) -> Arc<CurveSet> {
        Arc::clone(&self.current)
    }

    pub fn spec(&self) -> &CurveSetSpec {
        &self.spec
    }

    pub fn stats(&self) -> &BuildStats {
        &self.stats
    }

    /// Replaces the fixing index (the daily SOFR publication). Every curve is
    /// rebuilt on the next `on_snapshot`, since fixings can move any of them.
    pub fn set_index(&mut self, index: Arc<OvernightIndex>) {
        self.index = index;
        self.rebuild_all = true;
    }

    /// Brings the curves up to date with `quotes`.
    ///
    /// Rebuilds exactly the curves that have a pillar quote newer than the
    /// last snapshot handled (all of them after `set_index`), in parallel.
    /// Returns the next set, or `None` if no curve was rebuilt. A curve whose
    /// rebuild fails keeps its previous version and is retried when one of
    /// its quotes next moves.
    pub fn on_snapshot(&mut self, quotes: &QuoteSnapshot) -> Option<Arc<CurveSet>> {
        self.stats.snapshots += 1;
        let seen = self.seen_quotes;
        let all = self.rebuild_all;
        let dirty: Vec<&CurveSpec> = self
            .spec
            .curves()
            .iter()
            .filter(|c| {
                all || c
                    .pillars
                    .iter()
                    .any(|p| quotes.changed_at(p.quote).is_some_and(|at| at > seen))
            })
            .collect();
        self.seen_quotes = self.seen_quotes.max(quotes.version());
        self.rebuild_all = false;

        let valuation = self.spec.valuation_date();
        let (index, cfg) = (&self.index, &self.cfg);
        let results: Vec<(CurveId, Result<BuiltCurve, CurveBuildError>)> = dirty
            .par_iter()
            .map(|spec| (spec.id, build_curve(spec, valuation, quotes, index, cfg)))
            .collect();

        let mut rebuilt = Vec::with_capacity(results.len());
        for (id, result) in results {
            match result {
                Ok(curve) => rebuilt.push(curve),
                Err(CurveBuildError::NotReady { .. }) => self.stats.not_ready += 1,
                Err(e) => {
                    self.stats.failures += 1;
                    self.stats.last_failure = Some((id, e));
                }
            }
        }
        if rebuilt.is_empty() {
            return None;
        }
        self.stats.curves_built += rebuilt.len() as u64;
        self.stats.sets += 1;
        self.current = Arc::new(self.current.next(
            quotes.version(),
            Arc::clone(&self.index),
            rebuilt,
        ));
        Some(self.current())
    }
}

/// Out-of-band instructions to a running curve builder.
#[derive(Clone, Debug)]
pub enum Control {
    /// New fixings; rebuild every curve against them.
    SetIndex(Arc<OvernightIndex>),
}

/// How long the builder sleeps on the market before checking `control`.
const CONTROL_POLL: Duration = Duration::from_millis(25);

/// Runs `builder` on its own thread.
///
/// The thread sleeps until the quote store has a newer version, rebuilds the
/// dirty curves and publishes the new set. If it falls behind it skips
/// straight to the latest quotes; intermediate versions are never built. It
/// exits when the quote store closes, which closes the returned subscriber.
///
/// Bootstraps fan out on `pool`, or on rayon's global pool if `None`. Pass a
/// dedicated pool to keep curve building from competing with pricing workers.
pub fn spawn_curve_builder(
    market: MarketReader,
    mut builder: CurveBuilder,
    control: Receiver<Control>,
    pool: Option<Arc<rayon::ThreadPool>>,
) -> io::Result<(JoinHandle<BuildStats>, Subscriber<CurveSet>)> {
    let (mut publisher, subscriber) = publish::channel(builder.current());
    let handle = thread::Builder::new()
        .name("curve-builder".into())
        .spawn(move || {
            let mut seen = MarketVersion::ZERO;
            loop {
                let mut index_changed = false;
                while let Ok(Control::SetIndex(index)) = control.try_recv() {
                    builder.set_index(index);
                    index_changed = true;
                }
                let quotes = match market.wait_newer(seen, CONTROL_POLL) {
                    Wait::Updated(quotes) => quotes,
                    // No new quotes, but new fixings still need a rebuild.
                    Wait::TimedOut if index_changed => market.load(),
                    Wait::TimedOut => continue,
                    Wait::Closed => break,
                };
                seen = quotes.version();
                let set = match &pool {
                    Some(pool) => pool.install(|| builder.on_snapshot(&quotes)),
                    None => builder.on_snapshot(&quotes),
                };
                if let Some(set) = set {
                    publisher.publish(set);
                }
            }
            builder.stats().clone()
        })?;
    Ok((handle, subscriber))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::testkit::*;
    use crate::publish::Wait as SetWait;
    use riskrt_core::{Day, QuoteId, Tick};
    use riskrt_market::market_store;
    use std::sync::mpsc;

    const TODAY: Day = Day(20_719); // 2026-09-23
    const LONG: Duration = Duration::from_secs(60);

    /// Two ten-pillar curves: quotes 0..10 and 10..20.
    fn builder() -> CurveBuilder {
        let spec = CurveSetSpec::new(TODAY, vec![spec(0, 0), spec(1, 10)]).unwrap();
        CurveBuilder::new(
            spec,
            Arc::new(OvernightIndex::sofr()),
            BuildConfig::default(),
        )
    }

    #[test]
    fn nothing_is_built_until_a_curve_has_all_its_quotes() {
        let mut b = builder();
        let partial: Vec<Tick> = ticks(0, 1, 0.0).into_iter().take(9).collect();
        let v1 = snapshot(20, &[partial]);
        assert!(b.on_snapshot(&v1).is_none());
        assert_eq!(b.stats().not_ready, 1);

        let v2 = v1.next(&[Tick::new(QuoteId(9), 1, 0, 0.041)]).unwrap();
        let set = b.on_snapshot(&v2).unwrap();
        assert_eq!(
            (set.version(), set.quotes_version()),
            (MarketVersion(1), MarketVersion(2))
        );
        assert!(set.curve(CurveId(0)).is_some());
        assert!(set.curve(CurveId(1)).is_none());
        assert!(!set.is_complete());
    }

    #[test]
    fn only_the_curve_whose_quotes_moved_is_rebuilt() {
        let mut b = builder();
        let v1 = snapshot(20, &[[ticks(0, 1, 0.0), ticks(10, 1, 0.0)].concat()]);
        let first = b.on_snapshot(&v1).unwrap();
        assert!(first.is_complete());

        let v2 = v1.next(&[Tick::new(QuoteId(13), 2, 0, 0.0405)]).unwrap();
        let second = b.on_snapshot(&v2).unwrap();
        assert!(Arc::ptr_eq(
            first.curve(CurveId(0)).unwrap(),
            second.curve(CurveId(0)).unwrap()
        ));
        assert!(!Arc::ptr_eq(
            first.curve(CurveId(1)).unwrap(),
            second.curve(CurveId(1)).unwrap()
        ));
        assert_eq!(
            second.changed_since(first.version()).collect::<Vec<_>>(),
            vec![CurveId(1)]
        );
        assert_eq!(b.stats().curves_built, 3);
    }

    #[test]
    fn same_snapshot_twice_builds_nothing_new() {
        let mut b = builder();
        let v1 = snapshot(20, &[ticks(0, 1, 0.0)]);
        assert!(b.on_snapshot(&v1).is_some());
        assert!(b.on_snapshot(&v1).is_none());
        assert_eq!(b.stats().sets, 1);
    }

    #[test]
    fn skipped_quote_versions_still_dirty_the_right_curves() {
        let mut b = builder();
        let v1 = snapshot(20, &[[ticks(0, 1, 0.0), ticks(10, 1, 0.0)].concat()]);
        b.on_snapshot(&v1).unwrap();
        // Three more quote versions; the builder only ever sees the last.
        let v2 = v1.next(&[Tick::new(QuoteId(2), 2, 0, 0.0416)]).unwrap();
        let v3 = v2.next(&[Tick::new(QuoteId(15), 2, 0, 0.0376)]).unwrap();
        let v4 = v3.next(&[Tick::new(QuoteId(3), 2, 0, 0.0401)]).unwrap();
        let set = b.on_snapshot(&v4).unwrap();
        assert_eq!(
            set.changed_since(MarketVersion(1)).collect::<Vec<_>>(),
            vec![CurveId(0), CurveId(1)]
        );
        // And the result is what a fresh build from v4 gives.
        let fresh = build_curve(
            &spec(1, 10),
            today(),
            &v4,
            &OvernightIndex::sofr(),
            &BuildConfig::default(),
        )
        .unwrap();
        assert_eq!(set.curve(CurveId(1)).unwrap().curve(), fresh.curve());
    }

    #[test]
    fn failed_rebuild_keeps_the_previous_curve() {
        let mut b = builder();
        let v1 = snapshot(20, &[ticks(0, 1, 0.0)]);
        let good = b.on_snapshot(&v1).unwrap();

        let v2 = v1.next(&[Tick::new(QuoteId(9), 2, 0, 50.0)]).unwrap(); // 5000%
        assert!(b.on_snapshot(&v2).is_none());
        assert_eq!(b.stats().failures, 1);
        assert!(matches!(
            b.stats().last_failure,
            Some((CurveId(0), CurveBuildError::Bootstrap(_)))
        ));
        assert!(Arc::ptr_eq(&b.current(), &good));

        // It recovers when the quote does.
        let v3 = v2.next(&[Tick::new(QuoteId(9), 3, 0, 0.0411)]).unwrap();
        let recovered = b.on_snapshot(&v3).unwrap();
        assert_eq!(recovered.version(), MarketVersion(2));
    }

    #[test]
    fn set_index_rebuilds_every_curve_without_new_quotes() {
        let mut b = builder();
        let v1 = snapshot(20, &[[ticks(0, 1, 0.0), ticks(10, 1, 0.0)].concat()]);
        let before = b.on_snapshot(&v1).unwrap();

        let mut fixed = OvernightIndex::sofr();
        fixed.add_fixing(today(), 0.0431);
        b.set_index(Arc::new(fixed));
        let after = b.on_snapshot(&v1).unwrap();

        assert_eq!(after.version(), MarketVersion(2));
        assert_eq!(after.quotes_version(), MarketVersion(1));
        assert_eq!(after.changed_since(before.version()).count(), 2);
        assert_eq!(after.index().fixing(today()), Some(0.0431));
        assert_eq!(before.index().fixing(today()), None);
    }

    #[test]
    fn thread_publishes_sets_and_closes_with_the_market() {
        let (mut writer, reader) = market_store(20);
        let (_control_tx, control_rx) = mpsc::channel();
        let (handle, sets) = spawn_curve_builder(reader, builder(), control_rx, None).unwrap();

        writer.publish(&[ticks(0, 1, 0.0), ticks(10, 1, 0.0)].concat());
        let SetWait::Updated(first) = sets.wait_newer(MarketVersion::ZERO, LONG) else {
            panic!("no curve set");
        };
        assert!(first.is_complete());

        writer.publish(&[Tick::new(QuoteId(4), 2, 0, 0.0381)]);
        let SetWait::Updated(second) = sets.wait_newer(first.version(), LONG) else {
            panic!("no second curve set");
        };
        assert_eq!(
            second.changed_since(first.version()).collect::<Vec<_>>(),
            vec![CurveId(0)]
        );

        drop(writer);
        let stats = handle.join().unwrap();
        assert_eq!(stats.sets, 2);
        assert!(sets.is_closed());
        assert!(matches!(
            sets.wait_newer(second.version(), LONG),
            SetWait::Closed
        ));
    }

    #[test]
    fn thread_applies_an_index_change_while_the_market_is_quiet() {
        let (mut writer, reader) = market_store(20);
        let (control_tx, control_rx) = mpsc::channel();
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let (handle, sets) =
            spawn_curve_builder(reader, builder(), control_rx, Some(pool)).unwrap();

        writer.publish(&[ticks(0, 1, 0.0), ticks(10, 1, 0.0)].concat());
        let SetWait::Updated(first) = sets.wait_newer(MarketVersion::ZERO, LONG) else {
            panic!("no curve set");
        };

        let mut fixed = OvernightIndex::sofr();
        fixed.add_fixing(today(), 0.0431);
        control_tx.send(Control::SetIndex(Arc::new(fixed))).unwrap();
        let SetWait::Updated(second) = sets.wait_newer(first.version(), LONG) else {
            panic!("index change was not applied");
        };
        assert_eq!(second.index().fixing(today()), Some(0.0431));
        assert_eq!(second.quotes_version(), first.quotes_version());

        drop(writer);
        handle.join().unwrap();
    }
}
