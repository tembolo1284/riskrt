//! Feeds -> conflator -> store -> follower, using riskrt-feed for the front half.

use riskrt_core::{MarketVersion, Tick};
use riskrt_feed::{SimConfig, SimSource, TickSource, spawn_conflator, spawn_feed, tick_channel};
use riskrt_market::{Wait, market_store};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

const FEEDS: u32 = 4;
const QUOTES_PER_FEED: u32 = 16;
const TICKS_PER_FEED: u64 = 50_000;
const QUOTE_COUNT: usize = (FEEDS * QUOTES_PER_FEED) as usize;

fn feed_config(feed: u32) -> SimConfig {
    SimConfig {
        first_quote: feed * QUOTES_PER_FEED,
        quotes: QUOTES_PER_FEED,
        seed: 7_000 + u64::from(feed),
        limit: Some(TICKS_PER_FEED),
        ..Default::default()
    }
}

/// Final tick per quote, computed on one thread with no pipeline at all.
fn expected_final_state() -> Vec<Option<Tick>> {
    let mut last = vec![None; QUOTE_COUNT];
    for feed in 0..FEEDS {
        let mut source = SimSource::new(feed_config(feed));
        while let Some(tick) = source.next_tick() {
            last[tick.quote.index()] = Some(tick);
        }
    }
    last
}

#[test]
fn store_ends_on_the_single_threaded_answer_and_followers_see_it() {
    let (writer, reader) = market_store(QUOTE_COUNT);
    let (tx, rx) = tick_channel(1_024);
    let stop = Arc::new(AtomicBool::new(false));

    // Stands in for the curve builder: wakes on each new version, works out
    // what moved since the version it last handled, and stops on Closed.
    let follower = {
        let reader = reader.clone();
        thread::spawn(move || {
            let mut seen = MarketVersion::ZERO;
            let mut state: Vec<Option<Tick>> = vec![None; QUOTE_COUNT];
            let mut wakes = 0u64;
            loop {
                match reader.wait_newer(seen, Duration::from_secs(30)) {
                    Wait::Updated(snap) => {
                        assert!(snap.version() > seen);
                        // Rebuild local state from deltas only. If
                        // changed_since ever missed a quote across skipped
                        // versions, the final comparison below would fail.
                        for quote in snap.changed_since(seen) {
                            state[quote.index()] = snap.get(quote);
                        }
                        seen = snap.version();
                        wakes += 1;
                    }
                    Wait::TimedOut => panic!("follower starved"),
                    Wait::Closed => return (seen, state, wakes),
                }
            }
        })
    };

    // The writer moves into the conflator thread and is dropped when it ends,
    // which is what closes the store and releases the follower.
    let conflator = spawn_conflator(rx, QUOTE_COUNT, 4_096, writer.into_sink()).unwrap();
    let feeds: Vec<_> = (0..FEEDS)
        .map(|f| {
            let source = SimSource::new(feed_config(f));
            spawn_feed(&f.to_string(), source, tx.clone(), Arc::clone(&stop)).unwrap()
        })
        .collect();
    drop(tx);

    let sent: u64 = feeds.into_iter().map(|h| h.join().unwrap().sent).sum();
    let conflate = conflator.join().unwrap();
    let (follower_version, follower_state, wakes) = follower.join().unwrap();

    let expected = expected_final_state();
    let last = reader.load();

    assert_eq!(sent, u64::from(FEEDS) * TICKS_PER_FEED);
    assert!(reader.is_closed());
    assert!(last.is_complete());
    assert_eq!(last.ticks(), expected.as_slice());
    // One store version per conflated batch.
    assert_eq!(last.version(), MarketVersion(conflate.batches));
    // The follower ended on the final version with the right state, having
    // woken no more often than there were versions.
    assert_eq!(follower_version, last.version());
    assert_eq!(follower_state, expected);
    assert!(wakes >= 1 && wakes <= conflate.batches);
}
