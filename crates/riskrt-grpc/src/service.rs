//! The `riskrt.v1.Risk` service over the engine's read handles.

use crate::bridge::{self, Delivery};
use riskrt_agg::RiskSnapshot;
use riskrt_core::{CurveId, MarketVersion, Portfolio, TradeId};
use riskrt_curves::{CurveSet, CurveSetSpec, Subscriber};
use riskrt_market::{MarketReader, QuoteSnapshot};
use riskrt_pricing::Pricer;
use riskrt_proto::convert::position_to_wire;
use riskrt_proto::v1;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Everything the server reads. All of it is cheap to clone and none of it
/// can write back into the pipeline.
#[derive(Clone)]
pub struct Handles {
    pub portfolio: Arc<Portfolio>,
    pub spec: Arc<CurveSetSpec>,
    pub snapshots: Subscriber<RiskSnapshot>,
    pub curves: Subscriber<CurveSet>,
    pub quotes: MarketReader,
    pub pricer: Arc<dyn Pricer>,
}

pub struct RiskService {
    h: Handles,
}

impl RiskService {
    pub fn new(h: Handles) -> Self {
        Self { h }
    }
}

type Streaming<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

fn interval(ms: u32) -> Duration {
    Duration::from_millis(u64::from(ms))
}

/// Builds one wire update from a snapshot and the current curve set.
fn risk_update(snap: &RiskSnapshot, set: &CurveSet, include_trades: bool) -> v1::RiskUpdate {
    let ladders = (0..snap.curves().len())
        .filter_map(|c| snap.ladder(CurveId(c as u32)))
        .map(v1::Ladder::from)
        .collect();
    let trades = if include_trades {
        (0..snap.trade_count())
            .filter_map(|t| snap.trade(TradeId(t as u32)))
            .map(v1::TradeRisk::from)
            .collect()
    } else {
        Vec::new()
    };
    v1::RiskUpdate {
        version: snap.version().0,
        curves_version: set.version().0,
        quotes_version: set.quotes_version().0,
        total: Some(snap.total().into()),
        by_book: snap.books().iter().map(|r| (*r).into()).collect(),
        by_curve: snap.curves().iter().map(|r| (*r).into()).collect(),
        ladders,
        trade_count: snap.trade_count() as u32,
        priced_count: snap.priced_count() as u32,
        stale_count: snap.stale_against(set) as u32,
        trades,
    }
}

/// The quotes that changed between `previous` and this snapshot. On the
/// first delivery `previous` is zero, so this is every populated quote.
fn quote_update(snap: &QuoteSnapshot, previous: MarketVersion) -> v1::QuoteUpdate {
    let quotes = snap
        .changed_since(previous)
        .filter_map(|q| snap.get(q))
        .map(|t| v1::Quote {
            quote_id: t.quote.0,
            seq: t.seq,
            ts_ns: t.ts_ns,
            value: t.value,
        })
        .collect();
    v1::QuoteUpdate {
        version: snap.version().0,
        quotes,
    }
}

#[tonic::async_trait]
impl riskrt_proto::Risk for RiskService {
    async fn describe(
        &self,
        _: Request<v1::DescribeRequest>,
    ) -> Result<Response<v1::Description>, Status> {
        let curves = self
            .h
            .spec
            .curves()
            .iter()
            .map(|c| v1::CurveInfo {
                curve_id: c.id.0,
                name: c.name.clone(),
                pillars: c
                    .pillars
                    .iter()
                    .map(|p| v1::PillarInfo {
                        quote_id: p.quote.0,
                        label: p.key.to_string(),
                    })
                    .collect(),
            })
            .collect();
        Ok(Response::new(v1::Description {
            valuation_day: self.h.spec.valuation_date().unix_days(),
            trade_count: self.h.portfolio.len() as u32,
            book_count: self.h.portfolio.book_count() as u32,
            curves,
        }))
    }

    type PositionsStream = Streaming<v1::Position>;

    async fn positions(
        &self,
        _: Request<v1::PositionsRequest>,
    ) -> Result<Response<Self::PositionsStream>, Status> {
        let portfolio = Arc::clone(&self.h.portfolio);
        let items: Vec<_> = portfolio
            .iter()
            .map(|(id, p)| Ok(position_to_wire(id, p)))
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(items))))
    }

    type SubscribeRiskStream = Streaming<v1::RiskUpdate>;

    async fn subscribe_risk(
        &self,
        req: Request<v1::RiskSubscription>,
    ) -> Result<Response<Self::SubscribeRiskStream>, Status> {
        let sub = req.into_inner();
        let include_trades = sub.include_trades;
        let curves = self.h.curves.clone();
        // `from` is zero so the current snapshot goes out first, even if
        // nothing has priced yet. A client always starts from a full state.
        let rx = bridge::versioned(
            "risk",
            self.h.snapshots.clone(),
            MarketVersion::ZERO,
            interval(sub.min_interval_ms),
        );
        let stream = tokio_stream::StreamExt::map(
            ReceiverStream::new(rx),
            move |Delivery { value, .. }| Ok(risk_update(&value, &curves.load(), include_trades)),
        );
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribeQuotesStream = Streaming<v1::QuoteUpdate>;

    async fn subscribe_quotes(
        &self,
        req: Request<v1::QuoteSubscription>,
    ) -> Result<Response<Self::SubscribeQuotesStream>, Status> {
        let sub = req.into_inner();
        let rx = bridge::quotes(
            self.h.quotes.clone(),
            MarketVersion::ZERO,
            interval(sub.min_interval_ms),
        );
        let stream = tokio_stream::StreamExt::map(
            ReceiverStream::new(rx),
            |Delivery { value, previous }| Ok(quote_update(&value, previous)),
        );
        Ok(Response::new(Box::pin(stream)))
    }

    async fn price(
        &self,
        req: Request<v1::PriceRequest>,
    ) -> Result<Response<v1::PricedTrade>, Status> {
        let trade = TradeId(req.into_inner().trade_id);
        if trade.index() >= self.h.pricer.trade_count() {
            return Err(Status::not_found(format!("{trade} is not in the portfolio")));
        }
        let set = self.h.curves.load();
        if set.curve(self.h.portfolio.get(trade).map_err(|e| Status::internal(e.to_string()))?.discount).is_none() {
            return Err(Status::unavailable("curve not built yet"));
        }
        let pricer = Arc::clone(&self.h.pricer);
        let priced = tokio::task::spawn_blocking(move || pricer.price(trade, &set))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(v1::PricedTrade {
            trade: Some((&priced).into()),
        }))
    }
}
