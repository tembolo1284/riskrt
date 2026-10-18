//! Single-writer, many-reader publication of any versioned value.
//!
//! The same shape as `riskrt_market`'s quote store, made generic so later
//! stages (curve sets here, risk snapshots in the aggregator) can reuse it.
//! Readers `load()` without locking; a stage that wants to sleep until there
//! is something new uses `wait_newer`.

use arc_swap::ArcSwap;
use riskrt_core::MarketVersion;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// A value that carries a monotonically increasing version.
pub trait Versioned {
    fn version(&self) -> MarketVersion;
}

struct Shared<T> {
    current: ArcSwap<T>,
    closed: AtomicBool,
    /// Guards nothing; orders `notify` after a waiter's check-then-wait.
    /// `load` never touches it.
    gate: Mutex<()>,
    wake: Condvar,
}

impl<T> Shared<T> {
    fn lock_gate(&self) -> MutexGuard<'_, ()> {
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn notify(&self) {
        drop(self.lock_gate());
        self.wake.notify_all();
    }
}

/// Creates a publisher/subscriber pair starting at `initial`.
pub fn channel<T: Versioned>(initial: Arc<T>) -> (Publisher<T>, Subscriber<T>) {
    let shared = Arc::new(Shared {
        current: ArcSwap::new(initial),
        closed: AtomicBool::new(false),
        gate: Mutex::new(()),
        wake: Condvar::new(),
    });
    (
        Publisher {
            shared: Arc::clone(&shared),
        },
        Subscriber { shared },
    )
}

/// The write side. Not `Clone`: exactly one writer per channel.
/// Dropping it closes the channel and releases blocked subscribers.
pub struct Publisher<T> {
    shared: Arc<Shared<T>>,
}

impl<T: Versioned> Publisher<T> {
    /// Makes `value` the current one.
    ///
    /// # Panics
    /// If `value`'s version is not newer than the current one. That would
    /// break every `wait_newer` caller, so it is treated as a bug.
    pub fn publish(&mut self, value: Arc<T>) {
        let current = self.shared.current.load().version();
        assert!(
            value.version() > current,
            "published {:?} after {current:?}",
            value.version()
        );
        self.shared.current.store(value);
        self.shared.notify();
    }

    pub fn subscriber(&self) -> Subscriber<T> {
        Subscriber {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Publisher<T> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.notify();
    }
}

/// Outcome of `Subscriber::wait_newer`.
#[derive(Debug)]
pub enum Wait<T> {
    /// A version newer than the one the caller had seen.
    Updated(Arc<T>),
    /// Nothing newer arrived in time; the publisher is still alive.
    TimedOut,
    /// The publisher is gone and the caller has seen its final version.
    Closed,
}

/// The read side. Cheap to clone; give one to every thread that reads.
pub struct Subscriber<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Versioned> Subscriber<T> {
    /// The current value. Wait-free; the returned `Arc` pins that version.
    pub fn load(&self) -> Arc<T> {
        self.shared.current.load_full()
    }

    pub fn version(&self) -> MarketVersion {
        self.shared.current.load().version()
    }

    /// True once the publisher has been dropped. The last value stays loadable.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Blocks until there is a version newer than `seen`, and returns the
    /// latest one (which may be several versions ahead). After the publisher
    /// is dropped, the final version is still delivered once to a caller that
    /// has not seen it, then `Closed`.
    pub fn wait_newer(&self, seen: MarketVersion, timeout: Duration) -> Wait<T> {
        let deadline = Instant::now() + timeout;
        let mut gate = self.shared.lock_gate();
        loop {
            // Read `closed` first: the last publish precedes the drop, so
            // closed == true means the value loaded next is final.
            let closed = self.shared.closed.load(Ordering::Acquire);
            let value = self.shared.current.load_full();
            if value.version() > seen {
                return Wait::Updated(value);
            }
            if closed {
                return Wait::Closed;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[derive(Debug, PartialEq)]
    struct Item(u64, [u64; 8]);

    impl Versioned for Item {
        fn version(&self) -> MarketVersion {
            MarketVersion(self.0)
        }
    }

    fn item(v: u64) -> Arc<Item> {
        Arc::new(Item(v, [v; 8]))
    }

    const LONG: Duration = Duration::from_secs(30);

    #[test]
    fn publish_is_visible_and_old_values_stay_pinned() {
        let (mut publisher, subscriber) = channel(item(0));
        let pinned = subscriber.load();
        publisher.publish(item(1));
        publisher.publish(item(2));
        assert_eq!(pinned.0, 0);
        assert_eq!(subscriber.version(), MarketVersion(2));
        assert_eq!(publisher.subscriber().load().0, 2);
    }

    #[test]
    #[should_panic(expected = "published")]
    fn publishing_a_stale_version_panics() {
        let (mut publisher, _subscriber) = channel(item(3));
        publisher.publish(item(3));
    }

    #[test]
    fn wait_newer_covers_ready_timeout_wake_and_close() {
        let (mut publisher, subscriber) = channel(item(0));
        assert!(matches!(
            subscriber.wait_newer(MarketVersion(0), Duration::from_millis(10)),
            Wait::TimedOut
        ));

        let waiter = {
            let subscriber = subscriber.clone();
            thread::spawn(move || subscriber.wait_newer(MarketVersion(0), LONG))
        };
        thread::sleep(Duration::from_millis(20));
        publisher.publish(item(1));
        assert!(matches!(waiter.join().unwrap(), Wait::Updated(v) if v.0 == 1));

        // Final version is delivered before Closed.
        drop(publisher);
        assert!(subscriber.is_closed());
        assert!(
            matches!(subscriber.wait_newer(MarketVersion(0), LONG), Wait::Updated(v) if v.0 == 1)
        );
        assert!(matches!(
            subscriber.wait_newer(MarketVersion(1), LONG),
            Wait::Closed
        ));
    }

    #[test]
    fn dropping_the_publisher_releases_a_blocked_subscriber() {
        let (publisher, subscriber) = channel(item(0));
        let waiter = thread::spawn(move || subscriber.wait_newer(MarketVersion(0), LONG));
        thread::sleep(Duration::from_millis(20));
        drop(publisher);
        assert!(matches!(waiter.join().unwrap(), Wait::Closed));
    }

    #[test]
    fn readers_only_ever_see_whole_values() {
        const VERSIONS: u64 = 50_000;
        let (mut publisher, subscriber) = channel(item(0));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let subscriber = subscriber.clone();
                thread::spawn(move || {
                    let mut last = 0;
                    while !subscriber.is_closed() || subscriber.version().0 > last {
                        let value = subscriber.load();
                        assert!(value.0 >= last);
                        assert!(value.1.iter().all(|x| *x == value.0));
                        last = value.0;
                    }
                    last
                })
            })
            .collect();
        for v in 1..=VERSIONS {
            publisher.publish(item(v));
        }
        drop(publisher);
        for reader in readers {
            assert_eq!(reader.join().unwrap(), VERSIONS);
        }
    }
}
