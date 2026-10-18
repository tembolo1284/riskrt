//! Risk numbers: what workers produce and the aggregator sums.

use crate::ids::{CurveId, MarketVersion, TradeId};
use core::ops::{Add, AddAssign, Sub, SubAssign};

/// Additive risk measures. Bucketed ladders are a separate type.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Risk {
    /// Present value.
    pub pv: f64,
    /// PV change for a 1bp parallel shift up of all curves.
    pub dv01: f64,
}

impl Risk {
    pub const ZERO: Self = Self { pv: 0.0, dv01: 0.0 };
}

impl Add for Risk {
    type Output = Risk;
    #[inline]
    fn add(self, rhs: Risk) -> Risk {
        Risk {
            pv: self.pv + rhs.pv,
            dv01: self.dv01 + rhs.dv01,
        }
    }
}

impl Sub for Risk {
    type Output = Risk;
    #[inline]
    fn sub(self, rhs: Risk) -> Risk {
        Risk {
            pv: self.pv - rhs.pv,
            dv01: self.dv01 - rhs.dv01,
        }
    }
}

impl AddAssign for Risk {
    #[inline]
    fn add_assign(&mut self, rhs: Risk) {
        *self = *self + rhs;
    }
}

impl SubAssign for Risk {
    #[inline]
    fn sub_assign(&mut self, rhs: Risk) {
        *self = *self - rhs;
    }
}

/// One revaluation result, sent from a worker to the aggregator.
///
/// 32 bytes and `Copy`, tagged with the market version it was priced against
/// so the aggregator can refuse to mix versions in one snapshot.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub struct TradeRisk {
    pub version: MarketVersion,
    pub risk: Risk,
    pub trade: TradeId,
}

const _: () = assert!(size_of::<TradeRisk>() == 32);

impl TradeRisk {
    #[inline]
    pub const fn new(trade: TradeId, version: MarketVersion, risk: Risk) -> Self {
        Self {
            version,
            risk,
            trade,
        }
    }
}

/// Per-pillar par-quote risk on one curve: PV change for +1bp on each input
/// quote, in that curve's node order. Not `Copy`; it travels with a
/// `TradeRisk` in a `PricedTrade`.
#[derive(Clone, Debug, PartialEq)]
pub struct Ladder {
    pub curve: CurveId,
    pub per_bp: Vec<f64>,
}

impl Ladder {
    pub fn zero(curve: CurveId, buckets: usize) -> Self {
        Self {
            curve,
            per_bp: vec![0.0; buckets],
        }
    }

    /// Sum of all buckets. Close to, but not exactly, the zero-rate `dv01`.
    pub fn total(&self) -> f64 {
        self.per_bp.iter().sum()
    }

    /// Adds `rhs` into `self`. Both must be on the same curve with the same
    /// bucket count; returns `false` and leaves `self` unchanged otherwise.
    pub fn add_assign(&mut self, rhs: &Ladder) -> bool {
        if self.curve != rhs.curve || self.per_bp.len() != rhs.per_bp.len() {
            return false;
        }
        for (a, b) in self.per_bp.iter_mut().zip(&rhs.per_bp) {
            *a += b;
        }
        true
    }
}

/// Everything a worker produces for one trade: the additive numbers plus the
/// ladder on the curve it was priced against.
#[derive(Clone, Debug, PartialEq)]
pub struct PricedTrade {
    pub risk: TradeRisk,
    pub ladder: Ladder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_sums_and_adds_only_when_compatible() {
        let mut a = Ladder {
            curve: CurveId(0),
            per_bp: vec![1.0, 2.0],
        };
        let b = Ladder {
            curve: CurveId(0),
            per_bp: vec![0.5, 0.5],
        };
        assert!(a.add_assign(&b));
        assert_eq!(a.per_bp, vec![1.5, 2.5]);
        assert_eq!(a.total(), 4.0);
        let other_curve = Ladder::zero(CurveId(1), 2);
        let other_len = Ladder::zero(CurveId(0), 3);
        assert!(!a.add_assign(&other_curve));
        assert!(!a.add_assign(&other_len));
        assert_eq!(a.per_bp, vec![1.5, 2.5]);
    }

    #[test]
    fn risk_adds_and_subtracts() {
        let a = Risk {
            pv: 100.0,
            dv01: -4.5,
        };
        let b = Risk {
            pv: 25.0,
            dv01: 1.5,
        };
        assert_eq!(
            a + b,
            Risk {
                pv: 125.0,
                dv01: -3.0
            }
        );
        assert_eq!((a + b) - b, a);
    }

    #[test]
    fn assign_ops_match_binary_ops() {
        let mut total = Risk::ZERO;
        total += Risk {
            pv: 10.0,
            dv01: 1.0,
        };
        total -= Risk {
            pv: 4.0,
            dv01: 0.25,
        };
        assert_eq!(
            total,
            Risk {
                pv: 6.0,
                dv01: 0.75
            }
        );
    }

    #[test]
    fn trade_risk_carries_its_version() {
        let r = TradeRisk::new(TradeId(3), MarketVersion(7), Risk::ZERO);
        assert_eq!(r.trade, TradeId(3));
        assert_eq!(r.version, MarketVersion(7));
    }
}
