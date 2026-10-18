//! Building one curve: bootstrap, par-quote Jacobian, and pre-shifted copies.

use crate::error::CurveBuildError;
use crate::spec::CurveSpec;
use fq_core::time::Date;
use fq_rates::curve::{
    BootstrapConfig, BootstrapInstrument, CurveError, CurveJacobian, DiscountCurve, InstrumentKey,
    bootstrap_sofr,
};
use fq_rates::index::OvernightIndex;
use rayon::prelude::*;
use riskrt_core::{CurveId, MarketVersion, QuoteId};
use riskrt_market::QuoteSnapshot;

/// One basis point, in rate units. Every risk number this crate hands out is
/// per basis point.
pub const BP: f64 = 1e-4;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BuildConfig {
    pub bootstrap: BootstrapConfig,
    /// Par-quote bump used for the Jacobian (central difference).
    pub quote_bump: f64,
    /// Zero-rate bump used for the shifted curves (central difference).
    pub zero_bump: f64,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            bootstrap: BootstrapConfig::default(),
            quote_bump: BP,
            zero_bump: BP,
        }
    }
}

/// A bootstrapped curve with everything needed to risk a position against it.
///
/// Immutable and shared as `Arc<BuiltCurve>`. The shifted curves are the same
/// for every position, so they are built here once instead of per position.
#[derive(Clone, Debug, PartialEq)]
pub struct BuiltCurve {
    id: CurveId,
    quotes_version: MarketVersion,
    base: DiscountCurve,
    up: DiscountCurve,
    down: DiscountCurve,
    bucket_up: Vec<DiscountCurve>,
    bucket_down: Vec<DiscountCurve>,
    jacobian: CurveJacobian,
    /// Input quote of each node, in node (maturity) order.
    pillar_quotes: Vec<QuoteId>,
    zero_bump: f64,
}

/// Bootstraps `spec` from `quotes` and derives its risk scaffolding.
///
/// Runs 2n + 1 bootstraps for n pillars. The 2n bumped ones are independent
/// and run on the current rayon pool; the result does not depend on the
/// number of threads.
pub fn build_curve(
    spec: &CurveSpec,
    valuation: Date,
    quotes: &QuoteSnapshot,
    index: &OvernightIndex,
    cfg: &BuildConfig,
) -> Result<BuiltCurve, CurveBuildError> {
    let rate_of = |quote: QuoteId| {
        quotes.value(quote).ok_or(CurveBuildError::NotReady {
            curve: spec.id,
            missing: quote,
        })
    };
    let instruments = spec
        .pillars
        .iter()
        .map(|p| Ok(p.instrument(rate_of(p.quote)?)))
        .collect::<Result<Vec<_>, CurveBuildError>>()?;

    let base = bootstrap_sofr(valuation, index, &instruments, cfg.bootstrap)?;
    let n = base.nodes.len();

    // Re-order pillars into node order so Jacobian column j, ladder bucket j
    // and pillar_quotes[j] all refer to the same instrument.
    let keys: Vec<InstrumentKey> = base.nodes.iter().map(|(key, _, _)| *key).collect();
    let mut ordered = Vec::with_capacity(n);
    let mut pillar_quotes = Vec::with_capacity(n);
    for key in &keys {
        let k = spec
            .pillars
            .iter()
            .position(|p| p.key == *key)
            .ok_or_else(|| {
                CurveBuildError::Spec(format!(
                    "bootstrap returned a node for {key} that is not a pillar"
                ))
            })?;
        ordered.push(instruments[k]);
        pillar_quotes.push(spec.pillars[k].quote);
    }

    // Column j of the Jacobian: bump quote j both ways and re-bootstrap.
    let bump = cfg.quote_bump;
    let columns = (0..n)
        .into_par_iter()
        .map(|j| {
            let up = bootstrap_sofr(valuation, index, &bumped(&ordered, j, bump), cfg.bootstrap)?;
            let down =
                bootstrap_sofr(valuation, index, &bumped(&ordered, j, -bump), cfg.bootstrap)?;
            (0..n)
                .map(|i| {
                    Ok(
                        (up.curve.node_zero_rate(i)? - down.curve.node_zero_rate(i)?)
                            / (2.0 * bump),
                    )
                })
                .collect::<Result<Vec<f64>, CurveBuildError>>()
        })
        .collect::<Result<Vec<Vec<f64>>, CurveBuildError>>()?;
    let matrix = (0..n)
        .map(|i| columns.iter().map(|col| col[i]).collect())
        .collect();

    let curve = base.curve;
    let shift = cfg.zero_bump;
    let bucket = |s: f64| {
        (0..n)
            .map(|i| curve.with_node_shift(i, s))
            .collect::<Result<Vec<_>, CurveError>>()
    };
    Ok(BuiltCurve {
        id: spec.id,
        quotes_version: quotes.version(),
        up: curve.shifted(shift),
        down: curve.shifted(-shift),
        bucket_up: bucket(shift)?,
        bucket_down: bucket(-shift)?,
        base: curve,
        jacobian: CurveJacobian { keys, matrix, bump },
        pillar_quotes,
        zero_bump: shift,
    })
}

fn bumped(instruments: &[BootstrapInstrument], j: usize, shift: f64) -> Vec<BootstrapInstrument> {
    instruments
        .iter()
        .enumerate()
        .map(|(k, inst)| if k == j { inst.bumped(shift) } else { *inst })
        .collect()
}

impl BuiltCurve {
    pub fn id(&self) -> CurveId {
        self.id
    }

    /// Version of the quote snapshot this curve was built from.
    pub fn quotes_version(&self) -> MarketVersion {
        self.quotes_version
    }

    /// The curve to price against.
    pub fn curve(&self) -> &DiscountCurve {
        &self.base
    }

    pub fn node_count(&self) -> usize {
        self.pillar_quotes.len()
    }

    /// Input quote behind each node and each ladder bucket, in node order.
    pub fn pillar_quotes(&self) -> &[QuoteId] {
        &self.pillar_quotes
    }

    /// Instrument behind each node and each ladder bucket, in node order.
    pub fn keys(&self) -> &[InstrumentKey] {
        &self.jacobian.keys
    }

    /// `matrix[i][j]` = d(zero rate of node i) / d(par quote j).
    pub fn jacobian(&self) -> &CurveJacobian {
        &self.jacobian
    }

    /// The whole zero curve shifted up / down by the zero bump.
    pub fn parallel_shifted(&self) -> (&DiscountCurve, &DiscountCurve) {
        (&self.up, &self.down)
    }

    /// Node `i`'s zero rate shifted up / down by the zero bump.
    pub fn node_shifted(&self, i: usize) -> Option<(&DiscountCurve, &DiscountCurve)> {
        Some((self.bucket_up.get(i)?, self.bucket_down.get(i)?))
    }

    /// PV change for a +1bp parallel shift of the zero curve.
    ///
    /// `price` maps a curve to a PV; it is called twice.
    pub fn dv01<E>(&self, price: impl Fn(&DiscountCurve) -> Result<f64, E>) -> Result<f64, E> {
        Ok(self.per_bp(price(&self.up)?, price(&self.down)?))
    }

    /// PV change for +1bp on each node's zero rate, in node order.
    /// Sums to `dv01`. `price` is called 2n times.
    pub fn zero_ladder<E>(
        &self,
        price: impl Fn(&DiscountCurve) -> Result<f64, E>,
    ) -> Result<Vec<f64>, E> {
        self.bucket_up
            .iter()
            .zip(&self.bucket_down)
            .map(|(up, down)| Ok(self.per_bp(price(up)?, price(down)?)))
            .collect()
    }

    /// PV change for +1bp on each input par quote, in `pillar_quotes` order.
    /// This is the ladder a desk hedges with. `price` is called 2n times and
    /// no curve is rebuilt.
    pub fn par_ladder<E: From<CurveError>>(
        &self,
        price: impl Fn(&DiscountCurve) -> Result<f64, E>,
    ) -> Result<Vec<f64>, E> {
        let zero = self.zero_ladder(price)?;
        Ok(self.jacobian.to_par_ladder(&zero)?)
    }

    fn per_bp(&self, up: f64, down: f64) -> f64 {
        (up - down) / (2.0 * self.zero_bump) * BP
    }
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<BuiltCurve>();
};

#[cfg(test)]
pub(crate) mod testkit {
    use super::*;
    use riskrt_core::Tick;

    pub const QUOTES: [(&str, f64); 10] = [
        ("1M", 0.0430),
        ("3M", 0.0425),
        ("6M", 0.0415),
        ("1Y", 0.0400),
        ("2Y", 0.0380),
        ("3Y", 0.0375),
        ("5Y", 0.0378),
        ("7Y", 0.0385),
        ("10Y", 0.0395),
        ("30Y", 0.0410),
    ];

    pub fn today() -> Date {
        Date::from_ymd(2026, 9, 23).unwrap()
    }

    /// A ten-pillar curve whose quote ids start at `first_quote`.
    pub fn spec(id: u32, first_quote: u32) -> CurveSpec {
        let pillars: Vec<_> = QUOTES
            .iter()
            .enumerate()
            .map(|(k, (t, _))| (*t, QuoteId(first_quote + k as u32)))
            .collect();
        CurveSpec::sofr_ois(CurveId(id), &format!("SOFR{id}"), &pillars).unwrap()
    }

    /// Ticks setting every quote of a `spec(_, first_quote)` curve, shifted by `shift`.
    pub fn ticks(first_quote: u32, seq: u64, shift: f64) -> Vec<Tick> {
        QUOTES
            .iter()
            .enumerate()
            .map(|(k, (_, rate))| {
                Tick::new(
                    QuoteId(first_quote + k as u32),
                    seq,
                    seq * 1_000,
                    rate + shift,
                )
            })
            .collect()
    }

    pub fn snapshot(quote_count: usize, batches: &[Vec<Tick>]) -> QuoteSnapshot {
        batches
            .iter()
            .fold(QuoteSnapshot::empty(quote_count), |snap, batch| {
                snap.next(batch).unwrap()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::testkit::*;
    use super::*;
    use fq_rates::bond::FixedRateBond;
    use fq_rates::curve::{bootstrap_sofr_with_jacobian, zero_bucket_ladder};
    use fq_rates::swap::{OisSwap, SwapError, SwapSide, Tenor};
    use riskrt_core::Tick;

    fn built() -> (BuiltCurve, OvernightIndex) {
        let index = OvernightIndex::sofr();
        let snap = snapshot(10, &[ticks(0, 1, 0.0)]);
        let curve =
            build_curve(&spec(0, 0), today(), &snap, &index, &BuildConfig::default()).unwrap();
        (curve, index)
    }

    fn close(a: f64, b: f64, rel: f64) -> bool {
        (a - b).abs() <= rel * a.abs().max(b.abs()).max(1.0)
    }

    #[test]
    fn every_pillar_reprices_at_par() {
        let (curve, index) = built();
        for (tenor, rate) in QUOTES {
            let swap = OisSwap::usd_sofr_spot(
                today(),
                tenor.parse().unwrap(),
                SwapSide::PayFixed,
                1.0,
                rate,
            )
            .unwrap();
            assert!(
                swap.npv(&index, curve.curve()).unwrap().abs() < 1e-11,
                "{tenor}"
            );
        }
    }

    #[test]
    fn parallel_jacobian_matches_ferrumquant_serial_exactly() {
        let (curve, index) = built();
        let instruments: Vec<_> = QUOTES
            .iter()
            .map(|(t, r)| BootstrapInstrument::ois(t.parse().unwrap(), *r))
            .collect();
        let (serial, jacobian) = bootstrap_sofr_with_jacobian(
            today(),
            &index,
            &instruments,
            BootstrapConfig::default(),
            BP,
        )
        .unwrap();
        assert_eq!(curve.curve(), &serial.curve);
        assert_eq!(curve.jacobian(), &jacobian);
    }

    #[test]
    fn result_does_not_depend_on_thread_count() {
        let index = OvernightIndex::sofr();
        let snap = snapshot(10, &[ticks(0, 1, 0.0)]);
        let build_on = |threads: usize| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| {
                build_curve(&spec(0, 0), today(), &snap, &index, &BuildConfig::default()).unwrap()
            })
        };
        assert_eq!(build_on(1), build_on(8));
    }

    #[test]
    fn pillar_quotes_follow_node_order_not_spec_order() {
        let index = OvernightIndex::sofr();
        // Pillars listed longest first; quote ids follow that listing.
        let reversed: Vec<_> = QUOTES
            .iter()
            .rev()
            .enumerate()
            .map(|(k, (t, _))| (*t, QuoteId(k as u32)))
            .collect();
        let spec = CurveSpec::sofr_ois(CurveId(0), "REV", &reversed).unwrap();
        let batch: Vec<_> = QUOTES
            .iter()
            .rev()
            .enumerate()
            .map(|(k, (_, r))| Tick::new(QuoteId(k as u32), 1, 0, *r))
            .collect();
        let curve = build_curve(
            &spec,
            today(),
            &snapshot(10, &[batch]),
            &index,
            &BuildConfig::default(),
        )
        .unwrap();

        assert_eq!(curve.keys()[0].to_string(), "OIS 1M");
        assert_eq!(curve.pillar_quotes()[0], QuoteId(9)); // 1M was listed last
        assert_eq!(curve.pillar_quotes()[9], QuoteId(0)); // 30Y was listed first
        assert_eq!(curve.curve(), built().0.curve());
    }

    #[test]
    fn swap_risk_matches_ferrumquant() {
        let (curve, index) = built();
        let swap = OisSwap::usd_sofr_spot(today(), Tenor::years(4), SwapSide::PayFixed, 1e7, 0.05)
            .unwrap();
        let price = |c: &DiscountCurve| swap.npv(&index, c);

        let dv01 = curve.dv01(price).unwrap();
        assert!(close(
            dv01,
            swap.dv01(&index, curve.curve()).unwrap(),
            1e-12
        ));

        let zero = curve.zero_ladder(price).unwrap();
        let reference: Vec<f64> = zero_bucket_ladder(curve.curve(), BP, price).unwrap();
        assert_eq!(zero.len(), 10);
        for (ours, theirs) in zero.iter().zip(&reference) {
            assert!(close(*ours, theirs * BP, 1e-12));
        }
        // Bucket shifts add up to the parallel shift.
        assert!(close(zero.iter().sum::<f64>(), dv01, 1e-6));
    }

    #[test]
    fn par_ladder_matches_bump_and_rebootstrap() {
        let (curve, index) = built();
        // A 4Y swap sits between the 3Y and 5Y pillars.
        let swap = OisSwap::usd_sofr_spot(today(), Tenor::years(4), SwapSide::PayFixed, 1e7, 0.05)
            .unwrap();
        let par = curve.par_ladder(|c| swap.npv(&index, c)).unwrap();

        for (j, quote) in curve.pillar_quotes().iter().enumerate() {
            let rebuilt = |shift: f64| -> f64 {
                let mut batch = ticks(0, 1, 0.0);
                batch[quote.index()].value += shift;
                let c = build_curve(
                    &spec(0, 0),
                    today(),
                    &snapshot(10, &[batch]),
                    &index,
                    &BuildConfig::default(),
                )
                .unwrap();
                swap.npv(&index, c.curve()).unwrap()
            };
            let direct = (rebuilt(BP) - rebuilt(-BP)) / 2.0;
            assert!(
                (par[j] - direct).abs() < 0.01,
                "{}: {} vs {direct}",
                curve.keys()[j],
                par[j]
            );
        }
        // Almost all the risk is in the two surrounding pillars.
        let (three, five) = (par[5], par[6]);
        assert!((three + five) / par.iter().sum::<f64>() > 0.98);
        assert!(par[7].abs() < 1e-6 && par[9].abs() < 1e-6); // nothing beyond 5Y
    }

    #[test]
    fn works_for_bonds_too() {
        let (curve, _) = built();
        let d = |y, m, dd| Date::from_ymd(y, m, dd).unwrap();
        let bond =
            FixedRateBond::us_treasury(d(2026, 8, 15), d(2036, 8, 15), 0.0425, None).unwrap();
        let dv01 = curve.dv01(|c| bond.npv(c)).unwrap();
        assert!(dv01 < 0.0);
        assert!(close(dv01, bond.dv01(curve.curve()).unwrap(), 1e-12));
        let par = curve.par_ladder(|c| bond.npv(c)).unwrap();
        assert_eq!(par.len(), 10);
        assert!(par[8] < 0.0); // long a 10Y bond: short the 10Y bucket
    }

    #[test]
    fn missing_quote_is_not_ready() {
        let index = OvernightIndex::sofr();
        let mut batch = ticks(0, 1, 0.0);
        batch.remove(4);
        let err = build_curve(
            &spec(0, 0),
            today(),
            &snapshot(10, &[batch]),
            &index,
            &BuildConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            CurveBuildError::NotReady {
                curve: CurveId(0),
                missing: QuoteId(4)
            }
        );
    }

    #[test]
    fn absurd_quote_is_a_bootstrap_error_not_a_panic() {
        let index = OvernightIndex::sofr();
        let mut batch = ticks(0, 1, 0.0);
        batch[9].value = 50.0; // 5000% for 30 years
        let err = build_curve(
            &spec(0, 0),
            today(),
            &snapshot(10, &[batch]),
            &index,
            &BuildConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CurveBuildError::Bootstrap(_)));
    }

    #[test]
    fn records_the_quote_version_it_was_built_from() {
        let index = OvernightIndex::sofr();
        let snap = snapshot(10, &[ticks(0, 1, 0.0), ticks(0, 2, 0.0001)]);
        let curve =
            build_curve(&spec(0, 0), today(), &snap, &index, &BuildConfig::default()).unwrap();
        assert_eq!(curve.quotes_version(), MarketVersion(2));
        let _: Result<f64, SwapError> = curve.dv01(|_| Ok(0.0));
    }
}
