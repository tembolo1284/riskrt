//! Feeds -> conflator -> quotes -> curves -> scheduler -> collected batches.
//!
//! The checks are the scheduler's contract: every result carries its batch's
//! version, per-trade versions never go backwards, no trade is left stale
//! against the final curve set, and every final number equals a direct
//! single-threaded reprice against that set.

use fq_core::time::Date;
use fq_rates::index::OvernightIndex;
use riskrt_core::{
    BookId, CurveId, Day, MarketVersion, PayRec, Portfolio, Position, Product, QuoteId, TradeId,
};
use riskrt_curves::{BuildConfig, CurveBuilder, CurveSetSpec, CurveSpec, spawn_curve_builder};
use riskrt_deps::CurveIndex;
use riskrt_feed::{SimConfig, SimSource, spawn_conflator, spawn_feed, tick_channel};
use riskrt_market::market_store;
use riskrt_pricing::{FqPricer, Pricer};
use riskrt_sched::{SchedConfig, Scheduler, spawn_scheduler};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

const TODAY: Day = Day(20_719); // 2026-09-23
const TENORS: [&str; 10] = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "30Y"];
const CURVES: u32 = 2;
const QUOTE_COUNT: usize = (CURVES as usize) * TENORS.len();
const TICKS_PER_FEED: u64 = 3_000;
const TRADES: u32 = 120;

fn day(y: i32, m: u32, d: u32) -> Day {
    Day(Date::from_ymd(y, m, d).unwrap().unix_days())
}

fn curve_spec(c: u32) -> CurveSpec {
    let pillars: Vec<_> = TENORS
        .iter()
        .enumerate()
        .map(|(k, t)| (*t, QuoteId(c * 10 + k as u32)))
        .collect();
    CurveSpec::sofr_ois(CurveId(c), &format!("SOFR{c}"), &pillars).unwrap()
}

fn feed_config(c: u32) -> SimConfig {
    SimConfig {
        first_quote: c * 10,
        quotes: 10,
        seed: 99 + u64::from(c),
        start_value: 0.04,
        limit: Some(TICKS_PER_FEED),
        ..Default::default()
    }
}

/// Swaps and bonds spread over both curves.
fn portfolio() -> Portfolio {
    let mut b = Portfolio::builder();
    for i in 0..TRADES {
        let curve = CurveId(i % CURVES);
        let position = if i % 4 == 3 {
            Position {
                book: BookId(i % 3),
                discount: curve,
                forward: None,
                notional: 1e6 * (f64::from(i % 7) - 3.0).max(0.5),
                product: Product::Bond {
                    maturity: day(2036, 8, 15),
                    coupon: 0.0425,
                },
            }
        } else {
            Position {
                book: BookId(i % 3),
                discount: curve,
                forward: Some(curve),
                notional: 1e6 * f64::from(1 + i % 10),
                product: Product::Swap {
                    start: day(2026, 9, 25),
                    maturity: day(2027 + (i % 9) as i32, 9, 25),
                    fixed_rate: 0.03 + 0.002 * f64::from(i % 10),
                    fixed_leg: if i % 2 == 0 {
                        PayRec::Pay
                    } else {
                        PayRec::Receive
                    },
                },
            }
        };
        b.add(position).unwrap();
    }
    b.build()
}

#[test]
fn every_trade_ends_priced_against_the_final_curve_set() {
    let portfolio = portfolio();
    let valuation = Date::from_unix_days(TODAY.0).unwrap();
    let pricer: Arc<dyn Pricer> = Arc::new(FqPricer::build(&portfolio, valuation).unwrap());
    let index = CurveIndex::build(&portfolio, CURVES as usize).unwrap();

    let spec = CurveSetSpec::new(TODAY, (0..CURVES).map(curve_spec).collect()).unwrap();
    let builder = CurveBuilder::new(
        spec,
        Arc::new(OvernightIndex::sofr()),
        BuildConfig::default(),
    );
    let (writer, reader) = market_store(QUOTE_COUNT);
    let (tx, rx) = tick_channel(1_024);
    let stop = Arc::new(AtomicBool::new(false));
    let (_control_tx, control_rx) = mpsc::channel();

    let (curve_thread, sets) = spawn_curve_builder(reader, builder, control_rx, None).unwrap();
    let scheduler = Scheduler::new(Arc::clone(&pricer), index);
    let cfg = SchedConfig {
        workers: 2,
        ..Default::default()
    };
    let (sched_thread, batches) = spawn_scheduler(sets.clone(), scheduler, cfg).unwrap();

    let conflator = spawn_conflator(rx, QUOTE_COUNT, 4_096, writer.into_sink()).unwrap();
    let feeds: Vec<_> = (0..CURVES)
        .map(|c| {
            spawn_feed(
                &c.to_string(),
                SimSource::new(feed_config(c)),
                tx.clone(),
                Arc::clone(&stop),
            )
            .unwrap()
        })
        .collect();
    drop(tx);

    // Consume batches as the aggregator would, keeping the last result per trade.
    let mut last: Vec<Option<(MarketVersion, riskrt_core::PricedTrade)>> =
        vec![None; TRADES as usize];
    let mut batch_count = 0u64;
    let mut priced_total = 0u64;
    let mut last_version = MarketVersion::ZERO;
    for batch in batches.iter() {
        batch_count += 1;
        assert!(
            batch.version > last_version,
            "batches must arrive in version order"
        );
        last_version = batch.version;
        assert!(!batch.trades.is_empty());
        assert!(
            batch
                .trades
                .windows(2)
                .all(|w| w[0].risk.trade < w[1].risk.trade),
            "sorted by trade"
        );
        for priced in batch.trades {
            assert_eq!(
                priced.risk.version, batch.version,
                "a result must carry its batch version"
            );
            let slot = &mut last[priced.risk.trade.index()];
            if let Some((v, _)) = slot {
                assert!(
                    priced.risk.version > *v,
                    "per-trade versions never go backwards"
                );
            }
            *slot = Some((batch.version, priced));
            priced_total += 1;
        }
    }

    for feed in feeds {
        feed.join().unwrap();
    }
    conflator.join().unwrap();
    let curve_stats = curve_thread.join().unwrap();
    let sched_stats = sched_thread.join().unwrap();
    let final_set = sets.load();

    assert_eq!(curve_stats.failures, 0);
    assert_eq!(sched_stats.failures, 0, "{:?}", sched_stats.last_failure);
    assert_eq!(sched_stats.batches, batch_count);
    assert_eq!(sched_stats.trades_priced, priced_total);
    assert!(
        sched_stats.batches <= curve_stats.sets,
        "no more batches than curve sets"
    );
    assert!(final_set.is_complete());

    // No trade is stale: its curve has not changed since it was last priced,
    // and its numbers equal a direct reprice against the final set.
    for (i, slot) in last.iter().enumerate() {
        let trade = TradeId(i as u32);
        let (version, priced) = slot.as_ref().expect("every trade priced at least once");
        let curve = priced.ladder.curve;
        assert!(
            final_set.changed_since(*version).all(|c| c != curve),
            "{trade} priced at {version:?} but {curve} changed after that"
        );
        let direct = pricer.price(trade, &final_set).unwrap();
        assert_eq!(priced.risk.risk, direct.risk.risk, "{trade}");
        assert_eq!(priced.ladder, direct.ladder, "{trade}");
    }
}
