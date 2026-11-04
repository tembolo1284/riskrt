//! A real engine behind a real server, read back through the generated client.

use riskrt_core::{MarketVersion, TradeId};
use riskrt_curves::Wait;
use riskrt_engine::{Engine, EngineConfig, demo};
use riskrt_grpc::{Handles, spawn_server};
use riskrt_proto::RiskClient;
use riskrt_proto::v1;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;

#[test]
fn describe_positions_streams_and_price_agree_with_the_engine() {
    let (curves, books, trades) = (2, 3, 60);
    let portfolio = demo::portfolio(trades, curves, books, 7);
    let spec = demo::curve_specs(curves).unwrap();
    let config = EngineConfig::new(portfolio.clone(), spec.clone()).with_workers(2, 1);
    let engine = Engine::start(config, demo::sim_feeds(curves, 11, None)).unwrap();

    // Let the pipeline produce one complete snapshot before any client asks.
    let snapshots = engine.snapshots();
    let mut seen = MarketVersion::ZERO;
    loop {
        match snapshots.wait_newer(seen, Duration::from_secs(60)) {
            Wait::Updated(s) if s.is_complete() => break,
            Wait::Updated(s) => seen = s.version(),
            other => panic!("engine stalled: {other:?}"),
        }
    }

    let handles = Handles {
        portfolio: Arc::new(portfolio.clone()),
        spec: Arc::new(spec.clone()),
        snapshots: engine.snapshots(),
        curves: engine.curves(),
        quotes: engine.quotes(),
        pricer: engine.pricer(),
    };
    let server = spawn_server(handles, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = server.local_addr();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let mut client = RiskClient::connect(format!("http://{addr}")).await.unwrap();

        // Describe: counts and curve names come from the spec and portfolio.
        let d = client
            .describe(v1::DescribeRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(d.trade_count, trades);
        assert_eq!(d.book_count, books);
        assert_eq!(d.curves.len(), curves as usize);
        assert_eq!(d.curves[1].name, "SOFR-1");
        assert_eq!(d.curves[0].pillars.len(), demo::TENORS.len());
        assert_eq!(d.curves[0].pillars[6].label, "OIS 5Y");
        assert_eq!(d.valuation_day, demo::VALUATION.0);

        // Positions: every trade, in id order, converting back to core exactly.
        let wire: Vec<_> = client
            .positions(v1::PositionsRequest {})
            .await
            .unwrap()
            .into_inner()
            .collect::<Result<_, _>>()
            .await
            .unwrap();
        assert_eq!(wire.len(), trades as usize);
        for (i, w) in wire.into_iter().enumerate() {
            let (id, p) = riskrt_proto::convert::position_from_wire(w).unwrap();
            assert_eq!(id, TradeId(i as u32));
            assert_eq!(&p, portfolio.get(id).unwrap());
        }

        // Risk: first update is the current complete snapshot, with ladders
        // in pillar order and the per-trade table when asked for.
        let mut risk = client
            .subscribe_risk(v1::RiskSubscription {
                include_trades: true,
                min_interval_ms: 0,
            })
            .await
            .unwrap()
            .into_inner();
        let first = risk.next().await.unwrap().unwrap();
        assert_eq!(first.trade_count, trades);
        assert_eq!(first.priced_count, trades);
        assert_eq!(first.by_book.len(), books as usize);
        assert_eq!(first.by_curve.len(), curves as usize);
        assert_eq!(first.ladders.len(), curves as usize);
        assert_eq!(first.ladders[0].per_bp.len(), demo::TENORS.len());
        assert_eq!(first.trades.len(), trades as usize);
        let sum: f64 = first.trades.iter().map(|t| t.risk.unwrap().pv).sum();
        let total = first.total.unwrap().pv;
        assert!((sum - total).abs() <= 1e-6 * total.abs().max(1.0));
        // The engine is live, so a second update arrives with a newer version.
        let second = risk.next().await.unwrap().unwrap();
        assert!(second.version > first.version);

        // Quotes: first update is a full table, later ones are deltas.
        let mut quotes = client
            .subscribe_quotes(v1::QuoteSubscription { min_interval_ms: 0 })
            .await
            .unwrap()
            .into_inner();
        let full = quotes.next().await.unwrap().unwrap();
        assert_eq!(full.quotes.len(), spec.quote_count());
        let delta = quotes.next().await.unwrap().unwrap();
        assert!(delta.version > full.version);
        assert!(!delta.quotes.is_empty() && delta.quotes.len() <= spec.quote_count());

        // Price: the unary path uses the same pricer and curve set.
        let priced = client
            .price(v1::PriceRequest { trade_id: 3 })
            .await
            .unwrap()
            .into_inner()
            .trade
            .unwrap();
        assert_eq!(priced.trade_id, 3);
        assert!(priced.risk.unwrap().pv.is_finite());
        assert!(
            client
                .price(v1::PriceRequest { trade_id: trades + 5 })
                .await
                .is_err()
        );
    });

    // Server shutdown ends its streams; engine shutdown joins every stage.
    server.join().unwrap();
    let stats = engine.shutdown();
    assert_eq!(stats.sched.failures, 0);
}
