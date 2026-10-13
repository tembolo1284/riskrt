//! Risk numbers: what workers produce and the aggregator sums.

use crate::ids::{MarketVersion, TradeId};
use core::ops::{Add, AddAssign, Sub, SubAssign};

/// Additive risk measures. Bucketed ladders will be a separate type.
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
        Risk { pv: self.pv + rhs.pv, dv01: self.dv01 + rhs.dv01 }
    }
}

impl Sub for Risk {
    type Output = Risk;
    #[inline]
    fn sub(self, rhs: Risk) -> Risk {
        Risk { pv: self.pv - rhs.pv, dv01: self.dv01 - rhs.dv01 }
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
        Self { version, risk, trade }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risk_adds_and_subtracts() {
        let a = Risk { pv: 100.0, dv01: -4.5 };
        let b = Risk { pv: 25.0, dv01: 1.5 };
        assert_eq!(a + b, Risk { pv: 125.0, dv01: -3.0 });
        assert_eq!((a + b) - b, a);
    }

    #[test]
    fn assign_ops_match_binary_ops() {
        let mut total = Risk::ZERO;
        total += Risk { pv: 10.0, dv01: 1.0 };
        total -= Risk { pv: 4.0, dv01: 0.25 };
        assert_eq!(total, Risk { pv: 6.0, dv01: 0.75 });
    }

    #[test]
    fn trade_risk_carries_its_version() {
        let r = TradeRisk::new(TradeId(3), MarketVersion(7), Risk::ZERO);
        assert_eq!(r.trade, TradeId(3));
        assert_eq!(r.version, MarketVersion(7));
    }
}
