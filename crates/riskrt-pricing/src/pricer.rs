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

/// Prices `trades` on the current rayon pool, one result per input in the
/// same order. A failure for one trade does not affect the others.
pub fn price_batch<P: Pricer + ?Sized>(
    pricer: &P,
    trades: &[TradeId],
    curves: &CurveSet,
) -> Vec<Result<PricedTrade, PricingError>> {
    trades
        .par_iter()
        .map(|t| pricer.price(*t, curves))
        .collect()
}
