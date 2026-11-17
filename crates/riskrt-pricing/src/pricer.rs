//! The pricing contract and the parallel fan-out over it.

use crate::error::PricingError;
use rayon::prelude::*;
use riskrt_core::{PricedTrade, TradeId};
use riskrt_curves::CurveSet;

/// Prices one trade against one consistent curve set.
///
/// Implementations are shared between all worker threads, so they must be
/// `Send + Sync` and price through `&self`. All risk is per basis point and
/// tagged with `curves.version()`.
pub trait Pricer: Send + Sync {
    fn price(&self, trade: TradeId, curves: &CurveSet) -> Result<PricedTrade, PricingError>;

    /// Number of trades this pricer knows; ids are `0..trade_count()`.
    fn trade_count(&self) -> usize;
}

/// Checks that a result is one the aggregator can trust: it is for `trade`,
/// tagged with `curves.version()`, on a curve the set has, with a ladder of
/// that curve's bucket count, and every number finite.
///
/// This is the contract every `Pricer` is held to. `price_batch` applies it,
/// so an adapter that returns NaN or a short ladder shows up as a failed
/// trade instead of a corrupted total.
pub fn validate(
    priced: &PricedTrade,
    trade: TradeId,
    curves: &CurveSet,
) -> Result<(), PricingError> {
    let invalid = |reason: String| PricingError::Invalid { trade, reason };
    if priced.risk.trade != trade {
        return Err(invalid(format!("result is for {}", priced.risk.trade)));
    }
    if priced.risk.version != curves.version() {
        return Err(invalid(format!(
            "priced against set v{} not v{}",
            priced.risk.version.0,
            curves.version().0
        )));
    }
    let curve_id = priced.ladder.curve;
    let Some(curve) = curves.curve(curve_id) else {
        return Err(invalid(format!("{curve_id} is not built in this set")));
    };
    let buckets = curve.keys().len();
    if priced.ladder.per_bp.len() != buckets {
        return Err(invalid(format!(
            "ladder has {} buckets, {curve_id} has {buckets}",
            priced.ladder.per_bp.len()
        )));
    }
    if !priced.risk.risk.pv.is_finite() || !priced.risk.risk.dv01.is_finite() {
        return Err(invalid(format!(
            "non-finite risk (pv {}, dv01 {})",
            priced.risk.risk.pv, priced.risk.risk.dv01
        )));
    }
    if let Some(k) = priced.ladder.per_bp.iter().position(|x| !x.is_finite()) {
        return Err(invalid(format!("non-finite ladder bucket {k}")));
    }
    Ok(())
}

/// Prices `trades` on the current rayon pool, one result per input in the
/// same order, each checked with `validate`. A failure for one trade does
/// not affect the others.
pub fn price_batch<P: Pricer + ?Sized>(
    pricer: &P,
    trades: &[TradeId],
    curves: &CurveSet,
) -> Vec<Result<PricedTrade, PricingError>> {
    trades
        .par_iter()
        .map(|t| {
            let priced = pricer.price(*t, curves)?;
            validate(&priced, *t, curves)?;
            Ok(priced)
        })
        .collect()
}
