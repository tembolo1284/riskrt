//! Reference data: which quotes build which curve.

use crate::error::CurveBuildError;
use fq_core::time::Date;
use fq_rates::swap::Tenor;
use riskrt_core::{CurveId, Day, QuoteId};

/// One curve input: the quote that carries the par rate for a tenor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pillar {
    pub quote: QuoteId,
    pub tenor: Tenor,
}

/// One curve and its pillars. All pillars are USD SOFR OIS par rates for now,
/// matching `fq_rates::curve::BootstrapInstrument`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurveSpec {
    pub id: CurveId,
    pub name: String,
    pub pillars: Vec<Pillar>,
}

impl CurveSpec {
    /// Convenience for tests and config loaders: `("5Y", QuoteId)` pairs.
    pub fn sofr_ois(
        id: CurveId,
        name: &str,
        pillars: &[(&str, QuoteId)],
    ) -> Result<Self, CurveBuildError> {
        let pillars = pillars
            .iter()
            .map(|(tenor, quote)| {
                let tenor = tenor
                    .parse::<Tenor>()
                    .map_err(|e| CurveBuildError::Spec(format!("curve '{name}': {e}")))?;
                Ok(Pillar {
                    quote: *quote,
                    tenor,
                })
            })
            .collect::<Result<Vec<_>, CurveBuildError>>()?;
        Ok(Self {
            id,
            name: name.to_owned(),
            pillars,
        })
    }
}

/// Every curve the engine builds, validated once at start-up.
#[derive(Clone, Debug, PartialEq)]
pub struct CurveSetSpec {
    valuation: Date,
    curves: Vec<CurveSpec>,
    /// Indexed by `QuoteId`: the curve that quote feeds, if any.
    owner: Vec<Option<CurveId>>,
}

impl CurveSetSpec {
    /// `valuation_date` is fixed for the session. Curve ids must be dense
    /// (`curves[i].id == CurveId(i)`), and a quote may feed only one curve.
    pub fn new(valuation_date: Day, curves: Vec<CurveSpec>) -> Result<Self, CurveBuildError> {
        let bad = |msg: String| Err(CurveBuildError::Spec(msg));
        let valuation = Date::from_unix_days(valuation_date.0)?;

        let quote_count = curves
            .iter()
            .flat_map(|c| &c.pillars)
            .map(|p| p.quote.index() + 1)
            .max()
            .unwrap_or(0);
        let mut owner: Vec<Option<CurveId>> = vec![None; quote_count];

        for (i, curve) in curves.iter().enumerate() {
            if curve.id.index() != i {
                return bad(format!(
                    "curve '{}' has id {} at position {i}; ids must be dense",
                    curve.name, curve.id
                ));
            }
            if curve.pillars.is_empty() {
                return bad(format!("curve '{}' has no pillars", curve.name));
            }
            for (k, pillar) in curve.pillars.iter().enumerate() {
                if curve.pillars[..k].iter().any(|p| p.tenor == pillar.tenor) {
                    return bad(format!(
                        "curve '{}' has two pillars at {}",
                        curve.name, pillar.tenor
                    ));
                }
                let slot = &mut owner[pillar.quote.index()];
                if let Some(other) = slot {
                    return bad(format!(
                        "{} feeds both {other} and {}",
                        pillar.quote, curve.id
                    ));
                }
                *slot = Some(curve.id);
            }
        }
        Ok(Self {
            valuation,
            curves,
            owner,
        })
    }

    pub fn valuation_date(&self) -> Date {
        self.valuation
    }

    pub fn curves(&self) -> &[CurveSpec] {
        &self.curves
    }

    pub fn curve_count(&self) -> usize {
        self.curves.len()
    }

    /// One past the highest quote id any curve uses. Sizes the quote store.
    pub fn quote_count(&self) -> usize {
        self.owner.len()
    }

    /// The curve a quote feeds, or `None` if no curve uses it.
    pub fn curve_of(&self, quote: QuoteId) -> Option<CurveId> {
        self.owner.get(quote.index()).copied().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TODAY: Day = Day(20_719); // 2026-09-23

    fn curve(id: u32, pillars: &[(&str, u32)]) -> CurveSpec {
        let pillars: Vec<_> = pillars.iter().map(|(t, q)| (*t, QuoteId(*q))).collect();
        CurveSpec::sofr_ois(CurveId(id), &format!("C{id}"), &pillars).unwrap()
    }

    #[test]
    fn valid_spec_maps_quotes_to_curves() {
        let spec = CurveSetSpec::new(
            TODAY,
            vec![curve(0, &[("1Y", 0), ("2Y", 1)]), curve(1, &[("5Y", 3)])],
        )
        .unwrap();
        assert_eq!(spec.valuation_date().to_string(), "2026-09-23");
        assert_eq!(spec.curve_count(), 2);
        assert_eq!(spec.quote_count(), 4);
        assert_eq!(spec.curve_of(QuoteId(1)), Some(CurveId(0)));
        assert_eq!(spec.curve_of(QuoteId(2)), None); // a gap in the quote ids
        assert_eq!(spec.curve_of(QuoteId(3)), Some(CurveId(1)));
        assert_eq!(spec.curve_of(QuoteId(99)), None);
    }

    #[test]
    fn non_dense_curve_ids_are_rejected() {
        let err = CurveSetSpec::new(TODAY, vec![curve(1, &[("1Y", 0)])]).unwrap_err();
        assert!(matches!(err, CurveBuildError::Spec(_)));
    }

    #[test]
    fn shared_quote_is_rejected() {
        let err = CurveSetSpec::new(TODAY, vec![curve(0, &[("1Y", 0)]), curve(1, &[("2Y", 0)])])
            .unwrap_err();
        assert!(matches!(err, CurveBuildError::Spec(_)));
    }

    #[test]
    fn duplicate_tenor_and_empty_curve_are_rejected() {
        let dup = CurveSetSpec::new(TODAY, vec![curve(0, &[("1Y", 0), ("12M", 1)])]);
        assert!(matches!(dup, Err(CurveBuildError::Spec(_))));
        let empty = CurveSetSpec::new(TODAY, vec![curve(0, &[])]);
        assert!(matches!(empty, Err(CurveBuildError::Spec(_))));
    }

    #[test]
    fn bad_tenor_and_bad_date_are_rejected() {
        let bad = CurveSpec::sofr_ois(CurveId(0), "X", &[("1W", QuoteId(0))]);
        assert!(matches!(bad, Err(CurveBuildError::Spec(_))));
        let ancient = CurveSetSpec::new(Day(-100_000), vec![curve(0, &[("1Y", 0)])]);
        assert!(matches!(ancient, Err(CurveBuildError::Date(_))));
    }
}
