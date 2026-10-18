//! End to end: several feed threads, one conflator, one sink.

use riskrt_core::{QuoteId, Tick};
use riskrt_feed::{SimConfig, SimSource, TickSource, spawn_conflator, spawn_feed, tick_channel};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const FEEDS: u32 = 4;
const QUOTES_PER_FEED: u32 = 16;
const TICKS_PER_FEED: u64 = 50_000;

fn feed_config(feed: u32) -> SimConfig {
    SimConfig {
        first_quote: feed * QUOTES_PER_FEED,
        quotes: QUOTES_PER_FEED,
        seed: 1_000 + u64::from(feed),
        limit: Some(TICKS_PER_FEED),
        ..Default::default()
    }
}

/// Final tick per quote, computed on one thread with no pipeline at all.
fn expected_final_state() -> Vec<Option<Tick>> {
    let mut last = vec![None; (FEEDS * QUOTES_PER_FEED) as usize];
    for feed in 0..FEEDS {
        let mut source = SimSource::new(feed_config(feed));
        while let Some(tick) = source.next_tick() {
            last[tick.quote.index()] = Some(tick);
        }
    }
    last
}

/// Runs the threaded pipeline and returns (final state seen by the sink,
/// ticks sent by feeds, conflator stats).
fn run_pipeline(
    channel_capacity: usize,
    sink_delay: Option<Duration>,
) -> (Vec<Option<Tick>>, u64, riskrt_feed::ConflateStats) {
    let quote_count = (FEEDS * QUOTES_PER_FEED) as usize;
    let (tx, rx) = tick_channel(channel_capacity);
    let stop = Arc::new(AtomicBool::new(false));

    let seen = Arc::new(Mutex::new(vec![None; quote_count]));
    let sink_seen = Arc::clone(&seen);
    let conflator = spawn_conflator(rx, quote_count, 4_096, move |batch| {
        let mut seen = sink_seen.lock().unwrap();
        for tick in batch {
            let slot: &mut Option<Tick> = &mut seen[tick.quote.index()];
            // Per quote, the sink must only ever see sequence numbers go up.
            assert!(slot.is_none_or(|prev| tick.seq > prev.seq));
            *slot = Some(*tick);
        }
        drop(seen);
        if let Some(d) = sink_delay {
            std::thread::sleep(d);
        }
    })
    .unwrap();

    let feeds: Vec<_> = (0..FEEDS)
        .map(|f| {
            let source = SimSource::new(feed_config(f));
            spawn_feed(&f.to_string(), source, tx.clone(), Arc::clone(&stop)).unwrap()
        })
        .collect();
    // The conflator exits when the last sender goes away; ours must go too.
    drop(tx);

    let sent: u64 = feeds.into_iter().map(|h| h.join().unwrap().sent).sum();
    let stats = conflator.join().unwrap();
    let state = seen.lock().unwrap().clone();
    (state, sent, stats)
}

#[test]
fn threaded_pipeline_converges_to_the_single_threaded_answer() {
    let (state, sent, stats) = run_pipeline(1_024, None);
    assert_eq!(sent, u64::from(FEEDS) * TICKS_PER_FEED);
    assert_eq!(stats.received, sent);
    assert_eq!(stats.stale + stats.invalid + stats.unknown, 0);
    assert_eq!(stats.received, stats.published + stats.conflated);
    assert_eq!(state, expected_final_state());
}

#[test]
fn slow_sink_is_absorbed_by_conflation_not_by_a_backlog() {
    let (state, sent, stats) = run_pipeline(1_024, Some(Duration::from_micros(200)));
    assert_eq!(stats.received, sent);
    // A slow consumer must see far fewer ticks than were produced...
    assert!(stats.conflated > 0);
    assert!(stats.published < stats.received);
    // ...and still end on exactly the right values.
    assert_eq!(state, expected_final_state());
}

#[test]
fn tiny_channel_still_delivers_everything() {
    let (state, sent, stats) = run_pipeline(1, None);
    assert_eq!(sent, u64::from(FEEDS) * TICKS_PER_FEED);
    assert_eq!(stats.received, sent);
    assert_eq!(state, expected_final_state());
}

#[test]
fn quote_ids_do_not_overlap_between_feeds() {
    let state = expected_final_state();
    assert!(state.iter().all(Option::is_some));
    assert_eq!(state[0].unwrap().quote, QuoteId(0));
}
