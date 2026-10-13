//! What a trade is, in plain data.
//!
//! Core knows economic terms only. Turning a `Product` into something priceable
//! is the job of a `Pricer` adapter in riskrt-pricing, which keeps this crate
//! free of any pricing-library dependency. Schedule conventions (frequency,
//! day count, calendar) come from the curve's convention in the adapter.

use crate::ids::{BookId, CurveId};

/// Calendar date as days since 1970-01-01. Adapters convert to their own type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Day(pub i32);

/// Direction of the fixed leg, from our side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayRec {
    Pay,
    Receive,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Product {
    /// Vanilla fixed-float interest rate swap.
    Swap { start: Day, maturity: Day, fixed_rate: f64, fixed_leg: PayRec },
    /// Fixed-coupon bullet bond.
    Bond { maturity: Day, coupon: f64 },
}

impl Product {
    pub const fn maturity(&self) -> Day {
        match *self {
            Product::Swap { maturity, .. } | Product::Bond { maturity, .. } => maturity,
        }
    }
}

/// One trade. Its `TradeId` is its index in the `Portfolio`, so it is not stored here.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Position {
    pub book: BookId,
    /// Discounting curve.
    pub discount: CurveId,
    /// Projection curve for floating legs; `None` for bonds.
    pub forward: Option<CurveId>,
    /// Signed for bonds (negative = short). Swaps take direction from `fixed_leg`.
    pub notional: f64,
    pub product: Product,
}

impl Position {
    /// Curves this position depends on, without duplicates. Drives the
    /// curve → trades index and therefore what a tick dirties.
    pub fn curves(&self) -> impl Iterator<Item = CurveId> + use<> {
        let forward = self.forward.filter(|f| *f != self.discount);
        core::iter::once(self.discount).chain(forward)
    }

    pub fn validate(&self) -> core::result::Result<(), &'static str> {
        if !self.notional.is_finite() || self.notional == 0.0 {
            return Err("notional must be finite and non-zero");
        }
        match self.product {
            Product::Swap { start, maturity, fixed_rate, .. } => {
                if maturity <= start {
                    return Err("swap maturity must be after start");
                }
                if !fixed_rate.is_finite() {
                    return Err("fixed rate must be finite");
                }
                if self.forward.is_none() {
                    return Err("swap needs a forward curve");
                }
            }
            Product::Bond { coupon, .. } => {
                if !coupon.is_finite() || coupon < 0.0 {
                    return Err("coupon must be finite and non-negative");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn swap(discount: u32, forward: u32) -> Position {
        Position {
            book: BookId(0),
            discount: CurveId(discount),
            forward: Some(CurveId(forward)),
            notional: 10_000_000.0,
            product: Product::Swap {
                start: Day(20_000),
                maturity: Day(21_826),
                fixed_rate: 0.04,
                fixed_leg: PayRec::Pay,
            },
        }
    }

    #[test]
    fn curves_are_deduplicated() {
        assert_eq!(swap(0, 0).curves().count(), 1);
        let two: Vec<_> = swap(0, 1).curves().collect();
        assert_eq!(two, vec![CurveId(0), CurveId(1)]);
    }

    #[test]
    fn valid_swap_passes() {
        assert!(swap(0, 1).validate().is_ok());
    }

    #[test]
    fn zero_notional_is_rejected() {
        let mut p = swap(0, 1);
        p.notional = 0.0;
        assert!(p.validate().is_err());
    }

    #[test]
    fn swap_without_forward_curve_is_rejected() {
        let mut p = swap(0, 1);
        p.forward = None;
        assert!(p.validate().is_err());
    }

    #[test]
    fn inverted_dates_are_rejected() {
        let mut p = swap(0, 1);
        p.product = Product::Swap {
            start: Day(21_826),
            maturity: Day(20_000),
            fixed_rate: 0.04,
            fixed_leg: PayRec::Pay,
        };
        assert!(p.validate().is_err());
    }
}
