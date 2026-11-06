//! The engine as a whole: start, run to the end of the feeds, verify.

use riskrt_core::{Risk, TradeId};
use riskrt_curves::Wait;
use riskrt_engine::{Engine, EngineConfig, demo};
use std::time::Duration;

#[test]
fn runs_to_completion_and_the_last_snapshot_is_exact() {
    let (curves, books, trades) = (3, 4, 150);
    let portfolio = demo::portfolio(trades, curves, books, 1);
    let config =
        EngineConfig::new(portfolio.clone(), demo::curve_specs(curves).unwrap()).with_workers(2, 2);
    let engine = Engine::start(config, demo::sim_feeds(curves, 100, Some(2_000))).unwrap();

    let snapshots = engine.snapshots();
    let curve_sets = engine.curves();
    let pricer = engine.pricer();
    let stats = engine.wait();

    assert_eq!(stats.ticks_sent(), 2_000 * u64::from(curves));
    assert_eq!(stats.conflate.received, stats.ticks_sent());
    assert_eq!(stats.curves.failures, 0, "{:?}", stats.curves.last_failure);
    assert_eq!(stats.sched.failures, 0, "{:?}", stats.sched.last_failure);
    assert_eq!(stats.agg.batches, stats.sched.batches);
    assert!(stats.sched.batches >= 1);

    assert!(snapshots.is_closed());
    let last = snapshots.load();
    let final_set = curve_sets.load();
    assert!(last.is_complete());
    assert!(final_set.is_complete());

    let mut total = Risk::ZERO;
    for t in 0..trades {
        let direct = pricer.price(TradeId(t), &final_set).unwrap();
        assert_eq!(last.trade(TradeId(t)).unwrap().risk.risk, direct.risk.risk);
        total += direct.risk.risk;
    }
    assert!((last.total().pv - total.pv).abs() <= 1e-9 * total.pv.abs().max(1.0));
    assert!(matches!(
        snapshots.wait_newer(last.version(), Duration::from_millis(10)),
        Wait::Closed
    ));
}

#[test]
fn stop_ends_an_endless_feed_and_everything_joins() {
    let curves = 2;
    let portfolio = demo::portfolio(40, curves, 2, 2);
    let config =
        EngineConfig::new(portfolio, demo::curve_specs(curves).unwrap()).with_workers(1, 1);
    let engine = Engine::start(config, demo::sim_feeds(curves, 5, None)).unwrap();
    let snapshots = engine.snapshots();

    // Wait for risk to appear, then pull the plug.
    let Wait::Updated(first) =
        snapshots.wait_newer(riskrt_core::MarketVersion::ZERO, Duration::from_secs(60))
    else {
        panic!("no snapshot");
    };
    assert!(first.priced_count() > 0);
    let stats = engine.shutdown();
    assert!(stats.ticks_sent() > 0);
    assert!(snapshots.is_closed());
    assert!(snapshots.load().version() >= first.version());
}

#[test]
fn portfolio_on_an_unspecified_curve_is_rejected_at_start() {
    let portfolio = demo::portfolio(10, 3, 1, 3);
    let config = EngineConfig::new(portfolio, demo::curve_specs(2).unwrap());
    assert!(matches!(
        Engine::start(config, demo::sim_feeds(2, 1, Some(1))),
        Err(riskrt_engine::EngineError::Config(_))
    ));
}

#[test]
fn dropping_the_engine_stops_endless_feeds() {
    let curves = 2;
    let portfolio = demo::portfolio(20, curves, 2, 4);
    let config =
        EngineConfig::new(portfolio, demo::curve_specs(curves).unwrap()).with_workers(1, 1);
    let engine = Engine::start(config, demo::sim_feeds(curves, 9, None)).unwrap();
    let snapshots = engine.snapshots();
    let quotes = engine.quotes();
    drop(engine);

    // Every stage drains and closes its output once the feeds stop.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !(quotes.is_closed() && snapshots.is_closed()) {
        assert!(
            std::time::Instant::now() < deadline,
            "pipeline did not shut down after drop"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
