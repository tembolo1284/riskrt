//! Feeds -> conflator -> quotes -> curves -> scheduler -> aggregator -> snapshot.
//!
//! The final snapshot's totals must equal a from-scratch sum of every trade
//! priced directly against the final curve set, on one thread.

use fq_core::time::Date;
use fq_rates::index::OvernightIndex;
use riskrt_agg::{Aggregator, spawn_aggregator};
use riskrt_core::{
    BookId, CurveId, Day, MarketVersion, PayRec, Portfolio, Position, Product, QuoteId, Risk,
    TradeId,
};
use riskrt_curves::{
    BuildConfig, CurveBuilder, CurveSetSpec, CurveSpec, Wait, spawn_curve_builder,
};
use riskrt_deps::CurveIndex;
use riskrt_feed::{SimConfig, SimSource, spawn_conflator, spawn_feed, tick_channel};
use riskrt_market::market_store;
use riskrt_pricing::{FqPricer, Pricer};
use riskrt_sched::{SchedConfig, Scheduler, spawn_scheduler};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::time::Duration;

const TODAY: Day = Day(20_719); // 2026-09-23
const TENORS: [&str; 10] = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "30Y"];
const CURVES: u32 = 2;
const BOOKS: u32 = 3;
const QUOTE_COUNT: usize = (CURVES as usize) * TENORS.len();
const TICKS_PER_FEED: u64 = 3_000;
const TRADES: u32 = 90;

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
        seed: 7 + u64::from(c),
        start_value: 0.04,
        limit: Some(TICKS_PER_FEED),
        ..Default::default()
    }
}

fn portfolio() -> Portfolio {
    let mut b = Portfolio::builder();
    for i in 0..TRADES {
        let curve = CurveId(i % CURVES);
        let book = BookId(i % BOOKS);
        let position = if i % 5 == 4 {
            Position {
                book,
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
                book,
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

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

#[test]
fn final_snapshot_equals_a_single_threaded_sum_over_the_final_curve_set() {
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
    let (sched_thread, batches) = spawn_scheduler(
        sets.clone(),
        scheduler,
        SchedConfig {
            workers: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let (agg_thread, snapshots) =
        spawn_aggregator(batches, Aggregator::new(&portfolio, CURVES as usize)).unwrap();

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

    // Follow the snapshots like a UI would: versions only go up, every
    // snapshot is internally consistent.
    let mut seen = MarketVersion::ZERO;
    let mut observed = 0u64;
    loop {
        match snapshots.wait_newer(seen, Duration::from_secs(120)) {
            Wait::Updated(s) => {
                assert!(s.version() > seen);
                seen = s.version();
                observed += 1;
                let by_book: f64 = s.books().iter().map(|r| r.pv).sum();
                let by_curve: f64 = s.curves().iter().map(|r| r.pv).sum();
                assert!(close(by_book, s.total().pv));
                assert!(close(by_curve, s.total().pv));
            }
            Wait::TimedOut => panic!("snapshot follower starved"),
            Wait::Closed => break,
        }
    }

    for feed in feeds {
        feed.join().unwrap();
    }
    conflator.join().unwrap();
    curve_thread.join().unwrap();
    let sched_stats = sched_thread.join().unwrap();
    let agg_stats = agg_thread.join().unwrap();
    assert_eq!(agg_stats.batches, sched_stats.batches);
    assert_eq!(agg_stats.trades_applied, sched_stats.trades_priced);
    assert_eq!(agg_stats.stale_dropped + agg_stats.unknown_dropped, 0);
    assert!(observed >= 1 && observed <= agg_stats.batches);

    // Ground truth: price everything directly against the final curve set.
    let final_set = sets.load();
    let last = snapshots.load();
    assert!(last.is_complete());
    assert_eq!(
        last.stale_against(&final_set),
        0,
        "nothing left to catch up"
    );
    let mut total = Risk::ZERO;
    let mut by_book = vec![Risk::ZERO; BOOKS as usize];
    let mut by_curve = vec![Risk::ZERO; CURVES as usize];
    let mut ladders: Vec<Vec<f64>> = vec![vec![0.0; TENORS.len()]; CURVES as usize];
    for (trade, position) in portfolio.iter() {
        let direct = pricer.price(trade, &final_set).unwrap();
        total += direct.risk.risk;
        by_book[position.book.index()] += direct.risk.risk;
        by_curve[direct.ladder.curve.index()] += direct.risk.risk;
        for (acc, x) in ladders[direct.ladder.curve.index()]
            .iter_mut()
            .zip(&direct.ladder.per_bp)
        {
            *acc += x;
        }
        let held = last.trade(trade).unwrap();
        assert_eq!(held.risk.risk, direct.risk.risk, "{trade}");
    }
    assert!(close(last.total().pv, total.pv) && close(last.total().dv01, total.dv01));
    for b in 0..BOOKS {
        let got = last.book(BookId(b)).unwrap();
        assert!(close(got.pv, by_book[b as usize].pv) && close(got.dv01, by_book[b as usize].dv01));
    }
    for c in 0..CURVES {
        let got = last.curve(CurveId(c)).unwrap();
        assert!(close(got.pv, by_curve[c as usize].pv));
        let ladder = last.ladder(CurveId(c)).unwrap();
        for (a, b) in ladder.per_bp.iter().zip(&ladders[c as usize]) {
            assert!(close(*a, *b));
        }
    }
    let _ = TradeId(0);
}
