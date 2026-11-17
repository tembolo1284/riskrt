//! The engine as a whole: start, run to the end of the feeds, verify.

use riskrt_core::{CurveId, Risk, Tick, TradeId};
use riskrt_curves::{CurveStatus, Wait};
use riskrt_engine::{Engine, EngineConfig, demo};
use riskrt_feed::TickSource;
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

/// A feed that ticks a curve normally, then poisons one pillar, then heals it.
struct Poisoned {
    curve: u32,
    poison: bool,
    seq: u64,
    phase: usize,
}

impl TickSource for Poisoned {
    fn next_tick(&mut self) -> Option<Tick> {
        self.seq += 1;
        let q = |k: usize| demo::quote_id(self.curve, k);
        let tick = match self.phase {
            // Every pillar once, at 4%.
            p if p < demo::TENORS.len() => Tick::new(q(p), self.seq, self.seq, 0.04),
            // Then the 30Y pillar at 5000% (the bootstrap cannot fit it),
            // or a sane re-tick for the curve that is not being poisoned.
            p if p == demo::TENORS.len() => {
                let v = if self.poison { 50.0 } else { 0.04 };
                Tick::new(q(9), self.seq, self.seq, v)
            }
            // Then back to sanity.
            p if p == demo::TENORS.len() + 1 => Tick::new(q(9), self.seq, self.seq, 0.041),
            _ => return None,
        };
        self.phase += 1;
        // Give the pipeline time to react between phases so each state is observable.
        if self.phase >= demo::TENORS.len() {
            std::thread::sleep(Duration::from_millis(300));
        }
        Some(tick)
    }
}

#[test]
fn a_curve_that_fails_to_rebuild_is_reported_and_recovers() {
    let curves = 2;
    let portfolio = demo::portfolio(30, curves, 2, 5);
    let config =
        EngineConfig::new(portfolio.clone(), demo::curve_specs(curves).unwrap()).with_workers(1, 1);
    let feeds: Vec<Box<dyn TickSource>> = (0..curves)
        .map(|c| {
            Box::new(Poisoned {
                curve: c,
                poison: c == 0,
                seq: 0,
                phase: 0,
            }) as Box<dyn TickSource>
        })
        .collect();
    let engine = Engine::start(config, feeds).unwrap();
    let sets = engine.curves();
    let snapshots = engine.snapshots();

    // Watch the curve sets go by and record what each said about curve 0.
    let mut seen = riskrt_core::MarketVersion::ZERO;
    let mut statuses = Vec::new();
    let mut stale_while_failed = None;
    loop {
        match sets.wait_newer(seen, Duration::from_secs(30)) {
            Wait::Updated(set) => {
                seen = set.version();
                let status = set.health(CurveId(0)).unwrap().status();
                if status == CurveStatus::Failed && stale_while_failed.is_none() {
                    // While curve 0 is failed, every trade on it is reported stale,
                    // even though their numbers were priced successfully.
                    std::thread::sleep(Duration::from_millis(100));
                    stale_while_failed = Some(snapshots.load().stale_against(&set));
                }
                // Curve 1 may publish before curve 0 has all its quotes, so a
                // leading Unbuilt is possible and not interesting.
                if status != CurveStatus::Unbuilt && statuses.last() != Some(&status) {
                    statuses.push(status);
                }
            }
            Wait::TimedOut => panic!("stalled"),
            Wait::Closed => break,
        }
    }
    let stats = engine.wait();

    assert_eq!(
        statuses,
        vec![
            CurveStatus::Current,
            CurveStatus::Failed,
            CurveStatus::Current
        ]
    );
    assert_eq!(stats.curves.failures, 1, "only curve 0 was poisoned");
    let on_curve_0 = portfolio_on(&portfolio, CurveId(0));
    let stale = stale_while_failed.expect("saw the failed state");
    assert_eq!(
        stale, on_curve_0,
        "every trade on the failed curve, and only those, is stale"
    );
    let last = sets.load();
    assert!(last.is_healthy());
    assert_eq!(snapshots.load().stale_against(&last), 0);
}

fn portfolio_on(p: &riskrt_core::Portfolio, curve: CurveId) -> usize {
    p.positions()
        .iter()
        .filter(|pos| pos.discount == curve || pos.forward == Some(curve))
        .count()
}
