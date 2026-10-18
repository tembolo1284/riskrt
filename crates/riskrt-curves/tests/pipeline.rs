//! Feeds -> conflator -> quote store -> curve builder -> follower.

use fq_rates::index::OvernightIndex;
use riskrt_core::{CurveId, Day, MarketVersion, QuoteId};
use riskrt_curves::{
    BuildConfig, CurveBuilder, CurveSetSpec, CurveSpec, Wait, build_curve, spawn_curve_builder,
};
use riskrt_feed::{SimConfig, SimSource, TickSource, spawn_conflator, spawn_feed, tick_channel};
use riskrt_market::{QuoteSnapshot, market_store};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const TODAY: Day = Day(20_719); // 2026-09-23
const TENORS: [&str; 10] = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "30Y"];
const CURVES: u32 = 3;
const TICKS_PER_FEED: u64 = 20_000;
const QUOTE_COUNT: usize = (CURVES as usize) * TENORS.len();

/// Curve `c` owns quotes `10c .. 10c + 10`, and one feed drives each curve.
fn curve_spec(c: u32) -> CurveSpec {
    let pillars: Vec<_> = TENORS
        .iter()
        .enumerate()
        .map(|(k, t)| (*t, QuoteId(c * 10 + k as u32)))
        .collect();
    CurveSpec::sofr_ois(CurveId(c), &format!("SOFR{c}"), &pillars).unwrap()
}

fn set_spec() -> CurveSetSpec {
    CurveSetSpec::new(TODAY, (0..CURVES).map(curve_spec).collect()).unwrap()
}

/// A random walk around 4%, half a basis point per tick at most.
fn feed_config(c: u32) -> SimConfig {
    SimConfig {
        first_quote: c * 10,
        quotes: 10,
        seed: 42 + u64::from(c),
        start_value: 0.04,
        limit: Some(TICKS_PER_FEED),
        ..Default::default()
    }
}

/// The quotes after every feed has finished, computed with no threads.
fn final_quotes() -> QuoteSnapshot {
    let mut last = vec![None; QUOTE_COUNT];
    for c in 0..CURVES {
        let mut source = SimSource::new(feed_config(c));
        while let Some(tick) = source.next_tick() {
            last[tick.quote.index()] = Some(tick);
        }
    }
    let batch: Vec<_> = last.into_iter().flatten().collect();
    QuoteSnapshot::empty(QUOTE_COUNT).next(&batch).unwrap()
}

#[test]
fn curves_end_on_the_single_threaded_answer_and_followers_see_it() {
    let index = Arc::new(OvernightIndex::sofr());
    let builder = CurveBuilder::new(set_spec(), Arc::clone(&index), BuildConfig::default());

    let (writer, reader) = market_store(QUOTE_COUNT);
    let (tx, rx) = tick_channel(1_024);
    let stop = Arc::new(AtomicBool::new(false));
    let (_control_tx, control_rx) = mpsc::channel();

    let (curve_thread, sets) =
        spawn_curve_builder(reader.clone(), builder, control_rx, None).unwrap();

    // Stands in for the pricing scheduler: wakes on each new curve set and
    // tracks, from deltas only, which set version each curve came from.
    let follower = {
        let sets = sets.clone();
        thread::spawn(move || {
            let mut seen = MarketVersion::ZERO;
            let mut curve_version = vec![MarketVersion::ZERO; CURVES as usize];
            let mut wakes = 0u64;
            loop {
                match sets.wait_newer(seen, Duration::from_secs(120)) {
                    Wait::Updated(set) => {
                        assert!(set.version() > seen);
                        assert!(set.quotes_version() >= MarketVersion(1));
                        let dirty: Vec<_> = set.changed_since(seen).collect();
                        assert!(
                            !dirty.is_empty(),
                            "a set was published with nothing changed"
                        );
                        for id in dirty {
                            let curve = set.curve(id).expect("a changed curve must be built");
                            assert!(curve.quotes_version() <= set.quotes_version());
                            curve_version[id.index()] = set.version();
                        }
                        seen = set.version();
                        wakes += 1;
                    }
                    Wait::TimedOut => panic!("follower starved"),
                    Wait::Closed => return (seen, curve_version, wakes),
                }
            }
        })
    };

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

    let sent: u64 = feeds.into_iter().map(|h| h.join().unwrap().sent).sum();
    let conflate = conflator.join().unwrap();
    let stats = curve_thread.join().unwrap();
    let (follower_seen, follower_curve_version, wakes) = follower.join().unwrap();

    assert_eq!(sent, u64::from(CURVES) * TICKS_PER_FEED);
    assert_eq!(stats.failures, 0, "{:?}", stats.last_failure);

    // The final set is complete and reflects the final quotes.
    let last = sets.load();
    let quotes = reader.load();
    assert!(sets.is_closed());
    assert!(last.is_complete());
    assert_eq!(last.quotes_version(), quotes.version());
    assert_eq!(quotes.ticks(), final_quotes().ticks());

    // Each curve equals a from-scratch, single-threaded build off the final
    // quotes: same nodes, same Jacobian, bit for bit.
    let one_thread = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    for c in 0..CURVES {
        let expected = one_thread
            .install(|| {
                build_curve(
                    &curve_spec(c),
                    last.valuation_date(),
                    &final_quotes(),
                    &index,
                    &BuildConfig::default(),
                )
            })
            .unwrap();
        let got = last.curve(CurveId(c)).unwrap();
        assert_eq!(got.curve(), expected.curve(), "curve {c}");
        assert_eq!(got.jacobian(), expected.jacobian(), "curve {c}");
        assert_eq!(got.pillar_quotes(), expected.pillar_quotes());
    }

    // The builder conflates: far fewer curve sets than quote versions.
    assert_eq!(stats.sets, last.version().0);
    assert!(stats.sets <= conflate.batches);
    assert!(stats.curves_built >= u64::from(CURVES));

    // The follower ended on the final set and knows where each curve came from.
    assert_eq!(follower_seen, last.version());
    assert!(wakes >= 1 && wakes <= stats.sets);
    for c in 0..CURVES {
        let rebuilt_at: Vec<_> = last
            .changed_since(MarketVersion(follower_curve_version[c as usize].0 - 1))
            .collect();
        assert!(rebuilt_at.contains(&CurveId(c)));
        assert!(
            last.changed_since(follower_curve_version[c as usize])
                .all(|id| id != CurveId(c))
        );
    }
}
