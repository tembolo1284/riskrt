//! The published bundle: every curve, plus the fixing index, at one version.

use crate::build::BuiltCurve;
use crate::error::CurveBuildError;
use crate::publish::Versioned;
use fq_core::time::Date;
use fq_rates::index::OvernightIndex;
use riskrt_core::{CurveId, MarketVersion};
use std::sync::Arc;

/// Where one curve stands relative to the market and the fixings.
///
/// "Priced", "current" and "healthy" are different states. A curve can be
/// built and fully priced while its last rebuild failed, in which case the
/// numbers downstream are consistent with the curve but stale against the
/// quotes. This is how a reader tells the two apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CurveStatus {
    /// No build has succeeded yet (quotes still warming up, or every attempt failed).
    Unbuilt,
    /// The current curve reflects the latest quotes it was asked to reflect.
    Current,
    /// The current curve is from `built_from`; a rebuild at a newer quote
    /// version failed, so it does not reflect those quotes.
    Failed,
    /// The curve was built against an older fixing index than the set's.
    /// Its prices are consistent with that older index, not the new one.
    IndexStale,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CurveHealth {
    /// Quote version the current curve was built from; zero if unbuilt.
    pub built_from: MarketVersion,
    /// Latest quote version a rebuild was attempted at.
    pub attempted: MarketVersion,
    /// Quote version at which the current run of failures began.
    pub failed_since: Option<MarketVersion>,
    pub last_error: Option<CurveBuildError>,
    /// False once the set's index has moved on and this curve has not.
    pub index_current: bool,
}

impl CurveHealth {
    const fn unbuilt() -> Self {
        Self {
            built_from: MarketVersion::ZERO,
            attempted: MarketVersion::ZERO,
            failed_since: None,
            last_error: None,
            index_current: true,
        }
    }

    pub fn status(&self) -> CurveStatus {
        if self.built_from == MarketVersion::ZERO {
            CurveStatus::Unbuilt
        } else if self.failed_since.is_some() {
            CurveStatus::Failed
        } else if !self.index_current {
            CurveStatus::IndexStale
        } else {
            CurveStatus::Current
        }
    }

    /// True unless the curve is current. Trades on a degraded curve are
    /// counted as stale by the aggregator even if they were priced against it.
    pub fn is_degraded(&self) -> bool {
        self.status() != CurveStatus::Current
    }
}

/// Everything a pricing thread needs for one consistent revaluation.
///
/// Immutable. Each new set shares the curves that did not change with the
/// previous one, so publishing costs one small `Vec` of pointers.
#[derive(Clone, Debug)]
pub struct CurveSet {
    version: MarketVersion,
    quotes_version: MarketVersion,
    valuation: Date,
    /// The fixings every curve should be built against. A curve whose own
    /// `index()` is not this one is reported `IndexStale`.
    index: Arc<OvernightIndex>,
    curves: Vec<Option<Arc<BuiltCurve>>>,
    /// Set version at which each curve was last rebuilt.
    changed_at: Vec<MarketVersion>,
    health: Vec<CurveHealth>,
}

impl CurveSet {
    /// Version zero: no curve built yet.
    pub fn empty(curve_count: usize, valuation: Date, index: Arc<OvernightIndex>) -> Self {
        Self {
            version: MarketVersion::ZERO,
            quotes_version: MarketVersion::ZERO,
            valuation,
            index,
            curves: vec![None; curve_count],
            changed_at: vec![MarketVersion::ZERO; curve_count],
            health: vec![CurveHealth::unbuilt(); curve_count],
        }
    }

    /// The next version. `rebuilt` curves replace their predecessors;
    /// `failed` curves keep their predecessors and have the failure recorded
    /// in their health; all other curves are shared with `self` unchanged.
    ///
    /// `quotes_version` is the quote snapshot the attempt was made against.
    /// A set is worth publishing when either list is non-empty: a failure
    /// changes what a reader should believe about the numbers even though
    /// no curve changed.
    pub fn next(
        &self,
        quotes_version: MarketVersion,
        index: Arc<OvernightIndex>,
        rebuilt: Vec<BuiltCurve>,
        failed: Vec<(CurveId, CurveBuildError)>,
    ) -> Self {
        let mut next = Self {
            version: self.version.next(),
            quotes_version,
            index,
            ..self.clone()
        };
        for curve in rebuilt {
            let i = curve.id().index();
            next.changed_at[i] = next.version;
            let health = &mut next.health[i];
            health.built_from = curve.quotes_version();
            health.attempted = quotes_version;
            health.failed_since = None;
            health.last_error = None;
            next.curves[i] = Some(Arc::new(curve));
        }
        for (id, error) in failed {
            let health = &mut next.health[id.index()];
            health.attempted = quotes_version;
            health.failed_since.get_or_insert(quotes_version);
            health.last_error = Some(error);
        }
        for (i, slot) in next.curves.iter().enumerate() {
            next.health[i].index_current = match slot {
                Some(curve) => Arc::ptr_eq(curve.index(), &next.index),
                None => true,
            };
        }
        next
    }

    /// Version of this set. Risk numbers are tagged with it.
    pub fn version(&self) -> MarketVersion {
        self.version
    }

    /// Version of the quote snapshot this set reflects.
    pub fn quotes_version(&self) -> MarketVersion {
        self.quotes_version
    }

    pub fn valuation_date(&self) -> Date {
        self.valuation
    }

    /// The fixings every curve should be built against. For pricing use the
    /// curve's own `BuiltCurve::index()`, which is what it was calibrated with.
    pub fn index(&self) -> &Arc<OvernightIndex> {
        &self.index
    }

    /// `None` if the id is unknown or the curve has not been built yet.
    pub fn curve(&self, id: CurveId) -> Option<&Arc<BuiltCurve>> {
        self.curves.get(id.index())?.as_ref()
    }

    pub fn health(&self, id: CurveId) -> Option<&CurveHealth> {
        self.health.get(id.index())
    }

    /// Health of every curve, indexed by `CurveId`.
    pub fn healths(&self) -> &[CurveHealth] {
        &self.health
    }

    pub fn curve_count(&self) -> usize {
        self.curves.len()
    }

    pub fn built_count(&self) -> usize {
        self.curves.iter().flatten().count()
    }

    /// True once every curve has been built at least once.
    pub fn is_complete(&self) -> bool {
        self.built_count() == self.curves.len()
    }

    /// True when every curve is `Current`: built, reflecting the quotes it
    /// was last asked to reflect, and on the set's fixing index.
    pub fn is_healthy(&self) -> bool {
        self.health
            .iter()
            .all(|h| h.status() == CurveStatus::Current)
    }

    /// Curves that are not `Current`, in id order.
    pub fn degraded(&self) -> impl Iterator<Item = CurveId> + '_ {
        self.health
            .iter()
            .enumerate()
            .filter(|(_, h)| h.is_degraded())
            .map(|(i, _)| CurveId(i as u32))
    }

    /// Curves rebuilt after set version `seen`, in id order. Correct across
    /// skipped versions, so a slow consumer still finds every dirty curve.
    /// The set version at which `id` was last rebuilt; `ZERO` if never.
    /// A trade priced against any set at or after this version reflects
    /// the curve's current build.
    pub fn rebuilt_at(&self, id: CurveId) -> MarketVersion {
        self.changed_at
            .get(id.index())
            .copied()
            .unwrap_or(MarketVersion::ZERO)
    }

    pub fn changed_since(&self, seen: MarketVersion) -> impl Iterator<Item = CurveId> + '_ {
        self.changed_at
            .iter()
            .enumerate()
            .filter(move |(_, at)| **at > seen)
            .map(|(i, _)| CurveId(i as u32))
    }
}

impl Versioned for CurveSet {
    fn version(&self) -> MarketVersion {
        self.version
    }
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<CurveSet>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::testkit::*;
    use crate::build::{BuildConfig, build_curve};

    fn curve_with(id: u32, shift: f64, index: &Arc<OvernightIndex>) -> BuiltCurve {
        let snap = snapshot(10, &[ticks(0, 1, shift)]);
        build_curve(&spec(id, 0), today(), &snap, index, &BuildConfig::default()).unwrap()
    }

    fn curve(id: u32, shift: f64) -> BuiltCurve {
        curve_with(id, shift, &Arc::new(OvernightIndex::sofr()))
    }

    fn empty(curves: usize) -> CurveSet {
        CurveSet::empty(curves, today(), Arc::new(OvernightIndex::sofr()))
    }

    fn failure() -> CurveBuildError {
        CurveBuildError::Spec("injected".into())
    }

    #[test]
    fn empty_set_has_no_curves_and_is_unbuilt() {
        let set = empty(2);
        assert_eq!(set.version(), MarketVersion::ZERO);
        assert_eq!((set.curve_count(), set.built_count()), (2, 0));
        assert!(set.curve(CurveId(0)).is_none());
        assert!(set.curve(CurveId(9)).is_none());
        assert_eq!(set.changed_since(MarketVersion::ZERO).count(), 0);
        assert_eq!(
            set.health(CurveId(0)).unwrap().status(),
            CurveStatus::Unbuilt
        );
        assert!(!set.is_healthy());
        assert_eq!(set.degraded().count(), 2);
    }

    #[test]
    fn next_replaces_rebuilt_curves_and_shares_the_rest() {
        let v0 = empty(2);
        let index = v0.index().clone();
        let v1 = v0.next(
            MarketVersion(5),
            index.clone(),
            vec![curve_with(0, 0.0, &index), curve_with(1, 0.0, &index)],
            vec![],
        );
        let v2 = v1.next(
            MarketVersion(9),
            index.clone(),
            vec![curve_with(1, 0.001, &index)],
            vec![],
        );

        assert_eq!(
            (v2.version(), v2.quotes_version()),
            (MarketVersion(2), MarketVersion(9))
        );
        assert!(v2.is_complete());
        assert!(v2.is_healthy());
        // Curve 0 is the same allocation; curve 1 is new.
        assert!(Arc::ptr_eq(
            v1.curve(CurveId(0)).unwrap(),
            v2.curve(CurveId(0)).unwrap()
        ));
        assert!(!Arc::ptr_eq(
            v1.curve(CurveId(1)).unwrap(),
            v2.curve(CurveId(1)).unwrap()
        ));
        assert_eq!(v2.health(CurveId(0)).unwrap().built_from, MarketVersion(1));
        // The older set is untouched.
        assert_eq!(v1.version(), MarketVersion(1));
        assert_eq!(v0.built_count(), 0);
    }

    #[test]
    fn a_failed_rebuild_keeps_the_old_curve_and_reports_it() {
        let v0 = empty(2);
        let index = v0.index().clone();
        let v1 = v0.next(
            MarketVersion(1),
            index.clone(),
            vec![curve_with(0, 0.0, &index), curve_with(1, 0.0, &index)],
            vec![],
        );
        let v2 = v1.next(
            MarketVersion(2),
            index.clone(),
            vec![],
            vec![(CurveId(1), failure())],
        );
        let v3 = v2.next(
            MarketVersion(3),
            index.clone(),
            vec![],
            vec![(CurveId(1), failure())],
        );

        // Same curve object, flagged.
        assert!(Arc::ptr_eq(
            v1.curve(CurveId(1)).unwrap(),
            v3.curve(CurveId(1)).unwrap()
        ));
        let h = v3.health(CurveId(1)).unwrap();
        assert_eq!(h.status(), CurveStatus::Failed);
        assert_eq!(h.built_from, MarketVersion(1));
        assert_eq!(h.attempted, MarketVersion(3));
        assert_eq!(
            h.failed_since,
            Some(MarketVersion(2)),
            "first failure, not the latest"
        );
        assert!(h.last_error.is_some());
        assert_eq!(
            v3.health(CurveId(0)).unwrap().status(),
            CurveStatus::Current
        );
        assert!(!v3.is_healthy());
        assert_eq!(v3.degraded().collect::<Vec<_>>(), vec![CurveId(1)]);
        // A failure publishes a new version but does not count as a change
        // for repricing: the numbers downstream are still right for this curve.
        assert_eq!(v3.version(), MarketVersion(3));
        assert_eq!(v3.changed_since(v1.version()).count(), 0);

        // Recovery clears the failure.
        let v4 = v3.next(
            MarketVersion(4),
            index.clone(),
            vec![curve_with(1, 0.001, &index)],
            vec![],
        );
        let h = v4.health(CurveId(1)).unwrap();
        assert_eq!(h.status(), CurveStatus::Current);
        assert_eq!((h.built_from, h.failed_since), (MarketVersion(1), None));
        assert!(v4.is_healthy());
    }

    #[test]
    fn a_curve_left_on_an_old_index_is_index_stale() {
        let v0 = empty(2);
        let old = v0.index().clone();
        let v1 = v0.next(
            MarketVersion(1),
            old.clone(),
            vec![curve_with(0, 0.0, &old), curve_with(1, 0.0, &old)],
            vec![],
        );

        let mut fixed = OvernightIndex::sofr();
        fixed.add_fixing(today(), 0.0431);
        let new = Arc::new(fixed);
        // Curve 0 rebuilt on the new index, curve 1 failed and kept the old one.
        let v2 = v1.next(
            MarketVersion(2),
            new.clone(),
            vec![curve_with(0, 0.0, &new)],
            vec![(CurveId(1), failure())],
        );

        assert!(Arc::ptr_eq(v2.curve(CurveId(0)).unwrap().index(), &new));
        assert!(Arc::ptr_eq(v2.curve(CurveId(1)).unwrap().index(), &old));
        assert_eq!(
            v2.health(CurveId(0)).unwrap().status(),
            CurveStatus::Current
        );
        // Failed takes precedence over IndexStale while the failure persists...
        assert_eq!(v2.health(CurveId(1)).unwrap().status(), CurveStatus::Failed);
        assert!(!v2.health(CurveId(1)).unwrap().index_current);
        // ...and once a later attempt is not made (quotes quiet), a set built
        // with no failures still shows the index mismatch.
        let v3 = v2.next(MarketVersion(2), new.clone(), vec![], vec![]);
        let v3 = CurveSet {
            health: v3
                .health
                .iter()
                .cloned()
                .map(|mut h| {
                    h.failed_since = None;
                    h
                })
                .collect(),
            ..v3
        };
        assert_eq!(
            v3.health(CurveId(1)).unwrap().status(),
            CurveStatus::IndexStale
        );
        assert!(!v3.is_healthy());
    }

    #[test]
    fn changed_since_survives_skipped_versions() {
        let v0 = empty(3);
        let v1 = v0.next(
            MarketVersion(1),
            v0.index().clone(),
            vec![curve(0, 0.0)],
            vec![],
        );
        let v2 = v1.next(
            MarketVersion(2),
            v1.index().clone(),
            vec![curve(2, 0.0)],
            vec![],
        );
        let v3 = v2.next(
            MarketVersion(3),
            v2.index().clone(),
            vec![curve(0, 0.001)],
            vec![],
        );
        let since = |seen: u64| -> Vec<u32> {
            v3.changed_since(MarketVersion(seen)).map(|c| c.0).collect()
        };
        assert_eq!(since(0), vec![0, 2]);
        assert_eq!(since(1), vec![0, 2]);
        assert_eq!(since(2), vec![0]);
        assert_eq!(since(3), Vec::<u32>::new());
    }
}
