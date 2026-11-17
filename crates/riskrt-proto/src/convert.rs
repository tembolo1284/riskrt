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
            status: v1::TradeStatus::TradePriced as i32,
            failed_since_version: 0,
            last_error: String::new(),
        }
    }
}

/// A trade's numbers together with its status. `priced` is `None` for a
/// trade that has never priced; it is only sent when it has failed, so a
/// reader can see the failure.
pub fn trade_to_wire(
    trade: TradeId,
    priced: Option<&PricedTrade>,
    status: &riskrt_agg::TradeStatus,
) -> v1::TradeRisk {
    use riskrt_agg::TradeStatus;
    let mut w = match priced {
        Some(p) => v1::TradeRisk::from(p),
        None => v1::TradeRisk {
            trade_id: trade.0,
            ..Default::default()
        },
    };
    match status {
        TradeStatus::Unpriced => w.status = v1::TradeStatus::TradeUnpriced as i32,
        TradeStatus::Priced => w.status = v1::TradeStatus::TradePriced as i32,
        TradeStatus::Failed { since, error } => {
            w.status = v1::TradeStatus::TradeFailed as i32;
            w.failed_since_version = since.0;
            w.last_error = error.to_string();
        }
    }
    w
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

impl From<riskrt_curves::CurveStatus> for v1::CurveStatus {
    fn from(s: riskrt_curves::CurveStatus) -> Self {
        use riskrt_curves::CurveStatus as S;
        match s {
            S::Unbuilt => v1::CurveStatus::Unbuilt,
            S::Current => v1::CurveStatus::Current,
            S::Failed => v1::CurveStatus::Failed,
            S::IndexStale => v1::CurveStatus::IndexStale,
        }
    }
}

/// One curve's health as the engine reports it.
pub fn curve_health_to_wire(curve: CurveId, h: &riskrt_curves::CurveHealth) -> v1::CurveHealth {
    v1::CurveHealth {
        curve_id: curve.0,
        status: v1::CurveStatus::from(h.status()) as i32,
        built_from_quotes_version: h.built_from.0,
        attempted_quotes_version: h.attempted.0,
        failed_since_quotes_version: h.failed_since.map_or(0, |v| v.0),
        last_error: h
            .last_error
            .as_ref()
            .map(|e| e.to_string())
            .unwrap_or_default(),
    }
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

    #[test]
    fn trade_status_rides_along_with_the_numbers() {
        use riskrt_agg::TradeStatus;
        let p = PricedTrade {
            risk: TradeRisk::new(TradeId(4), MarketVersion(9), Risk { pv: 1.0, dv01: 2.0 }),
            ladder: Ladder::zero(CurveId(1), 3),
        };
        let ok = trade_to_wire(TradeId(4), Some(&p), &TradeStatus::Priced);
        assert_eq!(ok.status, v1::TradeStatus::TradePriced as i32);
        assert_eq!(ok.version, 9);

        let failed = TradeStatus::Failed {
            since: MarketVersion(7),
            error: riskrt_sched::PricingError::Invalid {
                trade: TradeId(4),
                reason: "nan".into(),
            },
        };
        let w = trade_to_wire(TradeId(4), Some(&p), &failed);
        assert_eq!(w.status, v1::TradeStatus::TradeFailed as i32);
        assert_eq!(w.failed_since_version, 7);
        assert!(w.last_error.contains("nan"));
        assert_eq!(w.risk.unwrap().pv, 1.0, "last good numbers still travel");

        let never = trade_to_wire(TradeId(4), None, &failed);
        assert_eq!(never.trade_id, 4);
        assert!(never.risk.is_none());
        assert_eq!(never.status, v1::TradeStatus::TradeFailed as i32);
    }
}
