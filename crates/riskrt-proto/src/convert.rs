//! Conversions between wire messages and riskrt-core types.
//!
//! Wire → core conversions are fallible where the wire format admits states
//! core does not (an unspecified pay/receive, a swap with no product set).

use crate::v1;
use riskrt_core::{
    BookId, CurveId, Day, Ladder, MarketVersion, PayRec, Position, PricedTrade, Product, Risk,
    TradeId, TradeRisk,
};

impl From<Risk> for v1::RiskNumbers {
    fn from(r: Risk) -> Self {
        Self {
            pv: r.pv,
            dv01: r.dv01,
        }
    }
}

impl From<v1::RiskNumbers> for Risk {
    fn from(r: v1::RiskNumbers) -> Self {
        Self {
            pv: r.pv,
            dv01: r.dv01,
        }
    }
}

impl From<&Ladder> for v1::Ladder {
    fn from(l: &Ladder) -> Self {
        Self {
            curve_id: l.curve.0,
            per_bp: l.per_bp.clone(),
        }
    }
}

impl From<v1::Ladder> for Ladder {
    fn from(l: v1::Ladder) -> Self {
        Self {
            curve: CurveId(l.curve_id),
            per_bp: l.per_bp,
        }
    }
}

impl From<&PricedTrade> for v1::TradeRisk {
    fn from(p: &PricedTrade) -> Self {
        Self {
            trade_id: p.risk.trade.0,
            version: p.risk.version.0,
            risk: Some(p.risk.risk.into()),
            ladder: Some((&p.ladder).into()),
        }
    }
}

impl From<v1::TradeRisk> for PricedTrade {
    fn from(t: v1::TradeRisk) -> Self {
        Self {
            risk: TradeRisk::new(
                TradeId(t.trade_id),
                MarketVersion(t.version),
                t.risk.map(Risk::from).unwrap_or(Risk::ZERO),
            ),
            ladder: t
                .ladder
                .map(Ladder::from)
                .unwrap_or_else(|| Ladder::zero(CurveId(0), 0)),
        }
    }
}

impl From<PayRec> for v1::PayRec {
    fn from(p: PayRec) -> Self {
        match p {
            PayRec::Pay => v1::PayRec::Pay,
            PayRec::Receive => v1::PayRec::Receive,
        }
    }
}

/// Builds the wire position for trade `id`.
pub fn position_to_wire(id: TradeId, p: &Position) -> v1::Position {
    let product = match p.product {
        Product::Swap {
            start,
            maturity,
            fixed_rate,
            fixed_leg,
        } => v1::position::Product::Swap(v1::Swap {
            start_day: start.0,
            maturity_day: maturity.0,
            fixed_rate,
            fixed_leg: v1::PayRec::from(fixed_leg) as i32,
        }),
        Product::Bond { maturity, coupon } => v1::position::Product::Bond(v1::Bond {
            maturity_day: maturity.0,
            coupon,
        }),
    };
    v1::Position {
        trade_id: id.0,
        book_id: p.book.0,
        discount_curve: p.discount.0,
        forward_curve: p.forward.map(|c| c.0),
        notional: p.notional,
        product: Some(product),
    }
}

/// Reads a core position back off the wire. The trade id is returned
/// separately because core positions do not carry it.
pub fn position_from_wire(w: v1::Position) -> Result<(TradeId, Position), &'static str> {
    let product = match w.product.ok_or("position has no product")? {
        v1::position::Product::Swap(s) => Product::Swap {
            start: Day(s.start_day),
            maturity: Day(s.maturity_day),
            fixed_rate: s.fixed_rate,
            fixed_leg: match v1::PayRec::try_from(s.fixed_leg) {
                Ok(v1::PayRec::Pay) => PayRec::Pay,
                Ok(v1::PayRec::Receive) => PayRec::Receive,
                _ => return Err("swap fixed_leg is unspecified"),
            },
        },
        v1::position::Product::Bond(b) => Product::Bond {
            maturity: Day(b.maturity_day),
            coupon: b.coupon,
        },
    };
    let position = Position {
        book: BookId(w.book_id),
        discount: CurveId(w.discount_curve),
        forward: w.forward_curve.map(CurveId),
        notional: w.notional,
        product,
    };
    Ok((TradeId(w.trade_id), position))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priced_trade_round_trips() {
        let p = PricedTrade {
            risk: TradeRisk::new(
                TradeId(7),
                MarketVersion(42),
                Risk {
                    pv: 1234.5,
                    dv01: -8.25,
                },
            ),
            ladder: Ladder {
                curve: CurveId(2),
                per_bp: vec![0.5, -1.0, 2.5],
            },
        };
        let wire = v1::TradeRisk::from(&p);
        assert_eq!(PricedTrade::from(wire), p);
    }

    #[test]
    fn positions_round_trip_both_products() {
        let swap = Position {
            book: BookId(1),
            discount: CurveId(0),
            forward: Some(CurveId(0)),
            notional: 25e6,
            product: Product::Swap {
                start: Day(20_000),
                maturity: Day(23_652),
                fixed_rate: 0.0375,
                fixed_leg: PayRec::Receive,
            },
        };
        let bond = Position {
            book: BookId(3),
            discount: CurveId(2),
            forward: None,
            notional: -5e6,
            product: Product::Bond {
                maturity: Day(23_000),
                coupon: 0.03,
            },
        };
        for (id, p) in [(TradeId(0), swap), (TradeId(9), bond)] {
            let back = position_from_wire(position_to_wire(id, &p)).unwrap();
            assert_eq!(back, (id, p));
        }
    }

    #[test]
    fn unspecified_pay_rec_is_rejected() {
        let mut w = position_to_wire(
            TradeId(0),
            &Position {
                book: BookId(0),
                discount: CurveId(0),
                forward: Some(CurveId(0)),
                notional: 1e6,
                product: Product::Swap {
                    start: Day(1),
                    maturity: Day(2),
                    fixed_rate: 0.0,
                    fixed_leg: PayRec::Pay,
                },
            },
        );
        if let Some(v1::position::Product::Swap(s)) = &mut w.product {
            s.fixed_leg = 0;
        }
        assert!(position_from_wire(w).is_err());
    }
}
