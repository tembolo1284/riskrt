//! The ferrumquant adapter: positions become `OisSwap`s and `FixedRateBond`s.

use crate::error::PricingError;
use crate::pricer::Pricer;
use fq_core::time::Date;
use fq_rates::bond::FixedRateBond;
use fq_rates::curve::DiscountCurve;
use fq_rates::swap::{OisSwap, SwapSide};
use riskrt_core::{
    CurveId, Ladder, PayRec, Portfolio, Position, PricedTrade, Product, Risk, TradeId, TradeRisk,
};
use riskrt_curves::{BuiltCurve, CurveSet};

/// One priceable instrument. Bonds carry the `notional / face` scale so a
/// 5mm long or a 2mm short is one multiply on top of the per-face price.
#[derive(Clone, Debug)]
enum Instrument {
    Swap(OisSwap),
    Bond { bond: FixedRateBond, scale: f64 },
}

/// A built instrument plus the curve it prices on.
#[derive(Clone, Debug)]
struct Built {
    curve: CurveId,
    instrument: Instrument,
}

/// Prices a fixed portfolio with ferrumquant. Build once, share by reference.
#[derive(Clone, Debug)]
pub struct FqPricer {
    trades: Vec<Built>,
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<FqPricer>();
};

impl FqPricer {
    /// Builds every position's instrument. `valuation` fixes the bond
    /// schedules' anchoring; it must equal the curve set's valuation date.
    pub fn build(portfolio: &Portfolio, valuation: Date) -> Result<Self, PricingError> {
        let trades = portfolio
            .iter()
            .map(|(trade, position)| build_one(trade, position, valuation))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { trades })
    }

    /// The curve trade `trade` prices on.
    pub fn curve_of(&self, trade: TradeId) -> Option<CurveId> {
        self.trades.get(trade.index()).map(|b| b.curve)
    }
}

fn build_one(trade: TradeId, position: &Position, valuation: Date) -> Result<Built, PricingError> {
    let build_err = |e: &dyn std::fmt::Display| PricingError::Build {
        trade,
        reason: e.to_string(),
    };
    let instrument = match position.product {
        Product::Swap {
            start,
            maturity,
            fixed_rate,
            fixed_leg,
        } => {
            if let Some(forward) = position.forward.filter(|f| *f != position.discount) {
                return Err(PricingError::TwoCurveSwap {
                    trade,
                    discount: position.discount,
                    forward,
                });
            }
            let side = match fixed_leg {
                PayRec::Pay => SwapSide::PayFixed,
                PayRec::Receive => SwapSide::ReceiveFixed,
            };
            let swap = OisSwap::usd_sofr(
                Date::from_unix_days(start.0)?,
                Date::from_unix_days(maturity.0)?,
                side,
                position.notional.abs(),
                fixed_rate,
            )
            .map_err(|e| build_err(&e))?;
            Instrument::Swap(swap)
        }
        Product::Bond { maturity, coupon } => {
            let maturity = Date::from_unix_days(maturity.0)?;
            // The position carries no issue date. Anchor the semiannual
            // schedule to maturity and start it at least a year before
            // valuation: with backward generation every period is regular,
            // and coupons paid before valuation are dropped by `npv`.
            let years_back = ((maturity - valuation) as f64 / 365.0).ceil() as i32 + 1;
            let dated = maturity.add_years(-years_back).map_err(|e| build_err(&e))?;
            let bond = FixedRateBond::us_treasury(dated, maturity, coupon, None)
                .map_err(|e| build_err(&e))?;
            let scale = position.notional / bond.face();
            Instrument::Bond { bond, scale }
        }
    };
    Ok(Built {
        curve: position.discount,
        instrument,
    })
}

impl Pricer for FqPricer {
    fn price(&self, trade: TradeId, curves: &CurveSet) -> Result<PricedTrade, PricingError> {
        let built = self
            .trades
            .get(trade.index())
            .ok_or(PricingError::UnknownTrade(trade))?;
        let curve: &BuiltCurve = curves
            .curve(built.curve)
            .ok_or(PricingError::CurveNotBuilt {
                trade,
                curve: built.curve,
            })?;
        let index = curves.index();

        let (pv, dv01, per_bp) = match &built.instrument {
            Instrument::Swap(swap) => {
                let price = |c: &DiscountCurve| swap.npv(index, c).map_err(PricingError::from);
                (
                    price(curve.curve())?,
                    curve.dv01(price)?,
                    curve.par_ladder(price)?,
                )
            }
            Instrument::Bond { bond, scale } => {
                let price = |c: &DiscountCurve| {
                    bond.npv(c).map(|pv| pv * scale).map_err(PricingError::from)
                };
                (
                    price(curve.curve())?,
                    curve.dv01(price)?,
                    curve.par_ladder(price)?,
                )
            }
        };

        Ok(PricedTrade {
            risk: TradeRisk::new(trade, curves.version(), Risk { pv, dv01 }),
            ladder: Ladder {
                curve: built.curve,
                per_bp,
            },
        })
    }

    fn trade_count(&self) -> usize {
        self.trades.len()
    }
}
