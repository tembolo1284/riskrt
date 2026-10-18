//! Single-writer, many-reader publication of `QuoteSnapshot`s.

use crate::snapshot::QuoteSnapshot;
use arc_swap::ArcSwap;
use riskrt_core::{MarketVersion, Tick};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

struct Shared {
    current: ArcSwap<QuoteSnapshot>,
    closed: AtomicBool,
    /// Guards nothing; exists so `wait_newer` cannot miss a wake-up.
    /// It is never touched by `load`, so pricing threads never contend on it.
    gate: Mutex<()>,
    wake: Condvar,
}

impl Shared {
    fn lock_gate(&self) -> MutexGuard<'_, ()> {
        // The gate protects no data, so a poisoned lock is still usable.
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn notify(&self) {
        // Taking the gate orders this notify after any waiter's check-then-wait.
        drop(self.lock_gate());
        self.wake.notify_all();
    }
}

/// Creates a store for `quote_count` quotes at version zero.
pub fn market_store(quote_count: usize) -> (MarketWriter, MarketReader) {
    let initial = Arc::new(QuoteSnapshot::empty(quote_count));
    let shared = Arc::new(Shared {
        current: ArcSwap::new(Arc::clone(&initial)),
        closed: AtomicBool::new(false),
        gate: Mutex::new(()),
        wake: Condvar::new(),
    });
    let writer = MarketWriter {
        shared: Arc::clone(&shared),
        current: initial,
        stats: PublishStats::default(),
    };
    (writer, MarketReader { shared })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishStats {
    /// Versions published (also the current version number).
    pub versions: u64,
    /// Ticks applied across all versions.
    pub applied: u64,
    /// Ticks skipped as stale, invalid or unknown.
    pub rejected: u64,
}

/// The write side. Deliberately not `Clone`: one writer, so publishing is a
/// plain read-copy-update with no compare-and-swap retry loop.
///
/// Dropping the writer closes the store, which releases blocked readers.
pub struct MarketWriter {
    shared: Arc<Shared>,
    /// The writer's own handle on the latest snapshot; saves a load per publish.
    current: Arc<QuoteSnapshot>,
    stats: PublishStats,
}

impl MarketWriter {
    /// Applies `batch` and publishes the result as the next version.
    ///
    /// The whole batch becomes visible at once. Returns `None`, and publishes
    /// nothing, if no tick in the batch was applicable.
    pub fn publish(&mut self, batch: &[Tick]) -> Option<MarketVersion> {
        let Some(next) = self.current.next(batch) else {
            self.stats.rejected += batch.len() as u64;
            return None;
        };
        self.stats.versions += 1;
        self.stats.applied += next.updates() as u64;
        self.stats.rejected += (batch.len() - next.updates()) as u64;

        let version = next.version();
        self.current = Arc::new(next);
        self.shared.current.store(Arc::clone(&self.current));
        self.shared.notify();
        Some(version)
    }

    pub fn version(&self) -> MarketVersion {
        self.current.version()
    }

    pub fn stats(&self) -> PublishStats {
        self.stats
    }

    /// Another read handle on this store.
    pub fn reader(&self) -> MarketReader {
        MarketReader {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Turns the writer into a batch callback, e.g. the sink of
    /// `riskrt_feed::spawn_conflator`. The store closes when the callback is
    /// dropped, which is when the thread that owns it finishes.
    pub fn into_sink(mut self) -> impl FnMut(&[Tick]) + Send + 'static {
        move |batch| {
            self.publish(batch);
        }
    }
}

impl Drop for MarketWriter {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notify();
    }
}

/// Outcome of `MarketReader::wait_newer`.
#[derive(Clone, Debug)]
pub enum Wait {
    /// A version newer than the one the caller had seen.
    Updated(Arc<QuoteSnapshot>),
    /// Nothing newer arrived in time; the writer is still alive.
    TimedOut,
    /// The writer is gone and the caller has already seen the final version.
    Closed,
}

/// The read side. Cheap to clone; hand one to every thread that needs prices.
#[derive(Clone)]
pub struct MarketReader {
    shared: Arc<Shared>,
}

impl MarketReader {
    /// The latest snapshot. Wait-free, and the returned `Arc` pins that
    /// version: later publishes do not affect it.
    pub fn load(&self) -> Arc<QuoteSnapshot> {
        self.shared.current.load_full()
    }

    /// The latest version number, without taking a reference to the snapshot.
    pub fn version(&self) -> MarketVersion {
        self.shared.current.load().version()
    }

    /// True once the writer has been dropped. The last snapshot stays loadable.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Blocks until there is a version newer than `seen`.
    ///
    /// Returns the *latest* snapshot, which may be several versions past
    /// `seen`; use `QuoteSnapshot::changed_since(seen)` to find what moved.
    /// After the writer is dropped this still returns the final version once
    /// to any caller that has not seen it, and `Closed` after that.
    pub fn wait_newer(&self, seen: MarketVersion, timeout: Duration) -> Wait {
        let deadline = Instant::now() + timeout;
        let mut gate = self.shared.lock_gate();
        loop {
            // Read `closed` before the snapshot: the writer's last publish
            // precedes its drop, so closed == true means the snapshot loaded
            // next is final.
            let closed = self.shared.closed.load(Ordering::Acquire);
            let snapshot = self.shared.current.load_full();
            if snapshot.version() > seen {
                return Wait::Updated(snapshot);
            }
            if closed {
                return Wait::Closed;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Wait::TimedOut;
            };
            if remaining.is_zero() {
                return Wait::TimedOut;
            }
            gate = self
                .shared
                .wake
                .wait_timeout(gate, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

const _: () = {
    const fn assert_send<T: Send>() {}
    const fn assert_shareable<T: Send + Sync + Clone>() {}
    assert_send::<MarketWriter>();
    assert_shareable::<MarketReader>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use riskrt_core::QuoteId;
    use std::thread;

    const LONG: Duration = Duration::from_secs(30);

    fn tick(quote: u32, seq: u64, value: f64) -> Tick {
        Tick::new(QuoteId(quote), seq, seq * 1_000, value)
    }

    #[test]
    fn starts_empty_at_version_zero() {
        let (writer, reader) = market_store(4);
        assert_eq!(writer.version(), MarketVersion::ZERO);
        assert_eq!(reader.version(), MarketVersion::ZERO);
        assert_eq!(reader.load().populated(), 0);
        assert!(!reader.is_closed());
    }

    #[test]
    fn publish_makes_a_new_version_visible_to_readers() {
        let (mut writer, reader) = market_store(2);
        assert_eq!(writer.publish(&[tick(0, 1, 0.04)]), Some(MarketVersion(1)));
        assert_eq!(writer.publish(&[tick(1, 1, 0.05)]), Some(MarketVersion(2)));
        let snap = reader.load();
        assert_eq!(snap.version(), MarketVersion(2));
        assert_eq!(snap.value(QuoteId(0)), Some(0.04));
        assert_eq!(snap.value(QuoteId(1)), Some(0.05));
    }

    #[test]
    fn a_loaded_snapshot_is_pinned_across_later_publishes() {
        let (mut writer, reader) = market_store(1);
        writer.publish(&[tick(0, 1, 0.04)]);
        let pinned = reader.load();
        writer.publish(&[tick(0, 2, 0.05)]);
        writer.publish(&[tick(0, 3, 0.06)]);
        assert_eq!(pinned.version(), MarketVersion(1));
        assert_eq!(pinned.value(QuoteId(0)), Some(0.04));
        assert_eq!(reader.load().value(QuoteId(0)), Some(0.06));
    }

    #[test]
    fn inapplicable_batch_publishes_nothing() {
        let (mut writer, reader) = market_store(1);
        writer.publish(&[tick(0, 5, 0.04)]);
        assert_eq!(writer.publish(&[]), None);
        assert_eq!(writer.publish(&[tick(0, 4, 0.99)]), None);
        assert_eq!(reader.version(), MarketVersion(1));
    }

    #[test]
    fn stats_account_for_every_tick() {
        let (mut writer, _reader) = market_store(2);
        writer.publish(&[tick(0, 2, 0.04), tick(1, 1, 0.05)]);
        writer.publish(&[tick(0, 1, 0.99), tick(1, 2, 0.06)]);
        writer.publish(&[tick(7, 1, 0.99)]);
        assert_eq!(
            writer.stats(),
            PublishStats {
                versions: 2,
                applied: 3,
                rejected: 2
            }
        );
    }

    #[test]
    fn wait_newer_returns_at_once_when_already_behind() {
        let (mut writer, reader) = market_store(1);
        writer.publish(&[tick(0, 1, 0.04)]);
        match reader.wait_newer(MarketVersion::ZERO, LONG) {
            Wait::Updated(s) => assert_eq!(s.version(), MarketVersion(1)),
            other => panic!("expected Updated, got {other:?}"),
        }
    }

    #[test]
    fn wait_newer_times_out_when_nothing_is_published() {
        let (_writer, reader) = market_store(1);
        let outcome = reader.wait_newer(MarketVersion::ZERO, Duration::from_millis(20));
        assert!(matches!(outcome, Wait::TimedOut));
    }

    #[test]
    fn wait_newer_is_woken_by_a_publish() {
        let (mut writer, reader) = market_store(1);
        let waiter = thread::spawn(move || reader.wait_newer(MarketVersion::ZERO, LONG));
        thread::sleep(Duration::from_millis(20));
        writer.publish(&[tick(0, 1, 0.04)]);
        assert!(matches!(waiter.join().unwrap(), Wait::Updated(_)));
    }

    #[test]
    fn dropping_the_writer_releases_a_blocked_reader() {
        let (writer, reader) = market_store(1);
        let waiter = thread::spawn(move || reader.wait_newer(MarketVersion::ZERO, LONG));
        thread::sleep(Duration::from_millis(20));
        drop(writer);
        assert!(matches!(waiter.join().unwrap(), Wait::Closed));
    }

    #[test]
    fn final_version_is_delivered_before_closed() {
        let (mut writer, reader) = market_store(1);
        writer.publish(&[tick(0, 1, 0.04)]);
        drop(writer);
        assert!(reader.is_closed());
        let Wait::Updated(last) = reader.wait_newer(MarketVersion::ZERO, LONG) else {
            panic!("the final version must still be delivered");
        };
        assert!(matches!(
            reader.wait_newer(last.version(), LONG),
            Wait::Closed
        ));
        assert_eq!(reader.load().version(), MarketVersion(1));
    }

    #[test]
    fn into_sink_publishes_and_closes_when_dropped() {
        let (writer, reader) = market_store(1);
        let mut sink = writer.into_sink();
        sink(&[tick(0, 1, 0.04)]);
        assert_eq!(reader.version(), MarketVersion(1));
        assert!(!reader.is_closed());
        drop(sink);
        assert!(reader.is_closed());
    }

    /// The property the whole design exists for. Every batch sets all quotes
    /// to the same value, so a reader that ever saw two different values in
    /// one snapshot would have caught a half-applied batch.
    #[test]
    fn readers_never_observe_a_torn_batch() {
        const QUOTES: u32 = 64;
        const VERSIONS: u64 = 20_000;
        const READERS: usize = 4;

        let (mut writer, reader) = market_store(QUOTES as usize);
        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                let reader = reader.clone();
                thread::spawn(move || {
                    let mut last = MarketVersion::ZERO;
                    let mut loads = 0u64;
                    while !reader.is_closed() || reader.version() > last {
                        let snap = reader.load();
                        assert!(snap.version() >= last, "version went backwards");
                        last = snap.version();
                        if last > MarketVersion::ZERO {
                            let expect = last.0 as f64;
                            assert!(
                                snap.ticks().iter().all(|t| t.unwrap().value == expect),
                                "torn snapshot at {last:?}"
                            );
                        }
                        loads += 1;
                    }
                    (last, loads)
                })
            })
            .collect();

        let mut batch = Vec::with_capacity(QUOTES as usize);
        for v in 1..=VERSIONS {
            batch.clear();
            batch.extend((0..QUOTES).map(|q| tick(q, v, v as f64)));
            assert_eq!(writer.publish(&batch), Some(MarketVersion(v)));
        }
        drop(writer);

        for handle in readers {
            let (last, loads) = handle.join().unwrap();
            assert_eq!(last, MarketVersion(VERSIONS));
            assert!(loads > 0);
        }
    }
}
