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

/// One built curve's shape: nodes with zero rate, discount factor and the
/// par quote behind each, plus a sampled grid every `sample_months` (0 for
/// none). A screen draws the zero curve from `samples` (or the nodes), the
/// par curve from the nodes, and the DV01 ladder under them in the same
/// bucket order.
pub fn curve_shape_to_wire(
    curve: &riskrt_curves::BuiltCurve,
    health: Option<&riskrt_curves::CurveHealth>,
    sample_months: u32,
) -> v1::CurveShape {
    let dc = curve.curve();
    let reference = dc.reference_date();
    let n = dc.node_count();
    let nodes = (0..n)
        .filter_map(|i| {
            let date = dc.node_date(i).ok()?;
            Some(v1::CurveNode {
                day: date.unix_days(),
                zero_rate: dc.node_zero_rate(i).ok()?,
                discount: dc.discount(date).ok()?,
                par_quote: curve.par_quotes().get(i).copied().unwrap_or(f64::NAN),
                quote_id: curve.pillar_quotes().get(i).map_or(u32::MAX, |q| q.0),
                label: curve
                    .keys()
                    .get(i)
                    .map(|k| k.to_string())
                    .unwrap_or_default(),
            })
        })
        .collect();
    let mut samples = Vec::new();
    if sample_months > 0 && n > 0 {
        if let Ok(last) = dc.node_date(n - 1) {
            let mut k = 1u32;
            while let Ok(d) = reference.add_months(k as i32 * sample_months as i32) {
                if d > last {
                    break;
                }
                if let (Ok(z), Ok(df)) = (dc.zero_rate(d), dc.discount(d)) {
                    samples.push(v1::CurvePoint {
                        day: d.unix_days(),
                        zero_rate: z,
                        discount: df,
                    });
                }
                k += 1;
            }
        }
    }
    v1::CurveShape {
        curve_id: curve.id().0,
        quotes_version: curve.quotes_version().0,
        reference_day: reference.unix_days(),
        nodes,
        samples,
        health: health.map(|h| curve_health_to_wire(curve.id(), h)),
    }
}

/// One wire update for a curve set. `changed_since` limits the shapes to
/// curves rebuilt after that version; health is always complete.
pub fn curve_update_to_wire(
    set: &riskrt_curves::CurveSet,
    changed_since: Option<riskrt_core::MarketVersion>,
    sample_months: u32,
) -> v1::CurveUpdate {
    let curve_health = set
        .healths()
        .iter()
        .enumerate()
        .map(|(c, h)| curve_health_to_wire(CurveId(c as u32), h))
        .collect();
    let curves = (0..set.healths().len())
        .map(|c| CurveId(c as u32))
        .filter(|c| changed_since.is_none_or(|v| set.changed_since(v).any(|x| x == *c)))
        .filter_map(|c| set.curve(c).map(|built| (c, built)))
        .map(|(c, built)| curve_shape_to_wire(built, set.health(c), sample_months))
        .collect();
    v1::CurveUpdate {
        version: set.version().0,
        quotes_version: set.quotes_version().0,
        curves,
        curve_health,
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

    #[test]
    fn curve_shape_carries_nodes_par_quotes_and_samples() {
        use fq_core::time::Date;
        use fq_rates::index::OvernightIndex;
        use riskrt_core::{QuoteId, Tick};
        use riskrt_curves::{BuildConfig, CurveBuilder, CurveSetSpec, CurveSpec};
        use std::sync::Arc;
        let tenors = ["1M", "1Y", "5Y", "10Y"];
        let pillars: Vec<_> = tenors
            .iter()
            .enumerate()
            .map(|(k, t)| (*t, QuoteId(k as u32)))
            .collect();
        let today = Date::from_ymd(2026, 9, 23).unwrap();
        let spec = CurveSetSpec::new(
            riskrt_core::Day(today.unix_days()),
            vec![CurveSpec::sofr_ois(CurveId(0), "SOFR", &pillars).unwrap()],
        )
        .unwrap();
        let ticks: Vec<Tick> = (0..4)
            .map(|k| Tick::new(QuoteId(k), 1, 0, 0.04 + k as f64 * 0.001))
            .collect();
        let quotes = riskrt_market::QuoteSnapshot::empty(4).next(&ticks).unwrap();
        let mut builder = CurveBuilder::new(
            spec,
            Arc::new(OvernightIndex::sofr()),
            BuildConfig::default(),
        );
        let set = builder.on_snapshot(&quotes).unwrap();

        let w = curve_update_to_wire(&set, None, 12);
        assert_eq!(w.version, set.version().0);
        assert_eq!(w.curves.len(), 1);
        assert_eq!(w.curve_health.len(), 1);
        let c = &w.curves[0];
        assert_eq!(c.curve_id, 0);
        assert_eq!(c.reference_day, today.unix_days());
        assert_eq!(c.nodes.len(), 4);
        // Nodes are in maturity order with the input quote beside each.
        assert!(c.nodes.windows(2).all(|p| p[0].day < p[1].day));
        assert_eq!(c.nodes[2].label, "OIS 5Y");
        assert_eq!(c.nodes[2].par_quote, 0.042);
        assert_eq!(c.nodes[2].quote_id, 2);
        assert!(
            c.nodes
                .iter()
                .all(|n| n.zero_rate > 0.03 && n.zero_rate < 0.05)
        );
        assert!(c.nodes.iter().all(|n| n.discount > 0.0 && n.discount < 1.0));
        // Yearly samples out to the 10Y node: 10 points, none past the last node.
        assert_eq!(c.samples.len(), 10);
        assert!(c.samples.iter().all(|s| s.day <= c.nodes[3].day));
        assert!(c.samples.windows(2).all(|p| p[0].day < p[1].day));
        assert_eq!(
            c.health.as_ref().unwrap().status,
            v1::CurveStatus::Current as i32
        );

        // Nodes only, and changed_only against the current version sends nothing.
        let bare = curve_update_to_wire(&set, None, 0);
        assert!(bare.curves[0].samples.is_empty());
        let none = curve_update_to_wire(&set, Some(set.version()), 0);
        assert!(none.curves.is_empty());
        assert_eq!(none.curve_health.len(), 1, "health is always complete");
    }
}
