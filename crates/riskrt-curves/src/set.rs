//! The published bundle: every curve, plus the fixing index, at one version.

use crate::build::BuiltCurve;
use crate::publish::Versioned;
use fq_core::time::Date;
use fq_rates::index::OvernightIndex;
use riskrt_core::{CurveId, MarketVersion};
use std::sync::Arc;

/// Everything a pricing thread needs for one consistent revaluation.
///
/// Immutable. Each new set shares the curves that did not change with the
/// previous one, so publishing costs one small `Vec` of pointers.
#[derive(Clone, Debug)]
pub struct CurveSet {
    version: MarketVersion,
    quotes_version: MarketVersion,
    valuation: Date,
    index: Arc<OvernightIndex>,
    curves: Vec<Option<Arc<BuiltCurve>>>,
    /// Set version at which each curve was last rebuilt.
    changed_at: Vec<MarketVersion>,
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
        }
    }

    /// The next version: `rebuilt` curves replace their predecessors, all
    /// other curves are shared with `self`.
    pub fn next(
        &self,
        quotes_version: MarketVersion,
        index: Arc<OvernightIndex>,
        rebuilt: Vec<BuiltCurve>,
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
            next.curves[i] = Some(Arc::new(curve));
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

    /// Fixings to price with. Travels with the curves so the two never mix.
    pub fn index(&self) -> &Arc<OvernightIndex> {
        &self.index
    }

    /// `None` if the id is unknown or the curve has not been built yet.
    pub fn curve(&self, id: CurveId) -> Option<&Arc<BuiltCurve>> {
        self.curves.get(id.index())?.as_ref()
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

    /// Curves rebuilt after set version `seen`, in id order. Correct across
    /// skipped versions, so a slow consumer still finds every dirty curve.
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

    fn curve(id: u32, shift: f64) -> BuiltCurve {
        let snap = snapshot(10, &[ticks(0, 1, shift)]);
        build_curve(
            &spec(id, 0),
            today(),
            &snap,
            &OvernightIndex::sofr(),
            &BuildConfig::default(),
        )
        .unwrap()
    }

    fn empty(curves: usize) -> CurveSet {
        CurveSet::empty(curves, today(), Arc::new(OvernightIndex::sofr()))
    }

    #[test]
    fn empty_set_has_no_curves() {
        let set = empty(2);
        assert_eq!(set.version(), MarketVersion::ZERO);
        assert_eq!((set.curve_count(), set.built_count()), (2, 0));
        assert!(set.curve(CurveId(0)).is_none());
        assert!(set.curve(CurveId(9)).is_none());
        assert_eq!(set.changed_since(MarketVersion::ZERO).count(), 0);
    }

    #[test]
    fn next_replaces_rebuilt_curves_and_shares_the_rest() {
        let v0 = empty(2);
        let v1 = v0.next(
            MarketVersion(5),
            v0.index().clone(),
            vec![curve(0, 0.0), curve(1, 0.0)],
        );
        let v2 = v1.next(MarketVersion(9), v1.index().clone(), vec![curve(1, 0.001)]);

        assert_eq!(
            (v2.version(), v2.quotes_version()),
            (MarketVersion(2), MarketVersion(9))
        );
        assert!(v2.is_complete());
        // Curve 0 is the same allocation; curve 1 is new.
        assert!(Arc::ptr_eq(
            v1.curve(CurveId(0)).unwrap(),
            v2.curve(CurveId(0)).unwrap()
        ));
        assert!(!Arc::ptr_eq(
            v1.curve(CurveId(1)).unwrap(),
            v2.curve(CurveId(1)).unwrap()
        ));
        // The older set is untouched.
        assert_eq!(v1.version(), MarketVersion(1));
        assert_eq!(v0.built_count(), 0);
    }

    #[test]
    fn changed_since_survives_skipped_versions() {
        let v0 = empty(3);
        let v1 = v0.next(MarketVersion(1), v0.index().clone(), vec![curve(0, 0.0)]);
        let v2 = v1.next(MarketVersion(2), v1.index().clone(), vec![curve(2, 0.0)]);
        let v3 = v2.next(MarketVersion(3), v2.index().clone(), vec![curve(0, 0.001)]);
        let since = |seen: u64| -> Vec<u32> {
            v3.changed_since(MarketVersion(seen)).map(|c| c.0).collect()
        };
        assert_eq!(since(0), vec![0, 2]);
        assert_eq!(since(1), vec![0, 2]);
        assert_eq!(since(2), vec![0]);
        assert_eq!(since(3), Vec::<u32>::new());
    }

    #[test]
    fn index_travels_with_the_set() {
        let v0 = empty(1);
        let mut with_fixing = OvernightIndex::sofr();
        with_fixing.add_fixing(today(), 0.0431);
        let v1 = v0.next(MarketVersion(1), Arc::new(with_fixing), vec![curve(0, 0.0)]);
        assert_eq!(v0.index().fixing(today()), None);
        assert_eq!(v1.index().fixing(today()), Some(0.0431));
    }
}
