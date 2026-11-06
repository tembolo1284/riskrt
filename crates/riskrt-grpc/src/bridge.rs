//! Blocking `wait_newer` loops turned into tokio streams.
//!
//! One std thread per subscriber. It blocks the way an engine stage does and
//! pushes into a tokio channel of capacity one. If the client has not taken
//! the previous update yet, the newer one is held as `pending` and retried
//! until it goes or a newer version replaces it. A slow client therefore
//! always ends on the latest version, even if the market then goes quiet,
//! and never receives a backlog.
//!
//! Each delivery carries the version of the delivery before it (the last one
//! that actually went into the channel), so a delta stream can ask
//! `changed_since(previous)` and never miss a change across the versions
//! that were conflated away.

use riskrt_core::MarketVersion;
use riskrt_curves::{Subscriber, Versioned, Wait};
use riskrt_market::{MarketReader, QuoteSnapshot, Wait as MarketWait};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// How long a bridge thread blocks before checking whether its client left.
const POLL: Duration = Duration::from_millis(250);
/// Shorter wait while an update is pending, so a client that drains the
/// channel gets it promptly even if nothing new is published.
const RETRY: Duration = Duration::from_millis(10);

pub struct Delivery<T> {
    pub value: Arc<T>,
    /// Version of the previous delivery on this stream; `from` on the first.
    pub previous: MarketVersion,
}

/// Outcome of one blocking wait, with the two concrete wait types unified.
enum Next<T> {
    Updated(Arc<T>),
    TimedOut,
    Closed,
}

/// Streams every version newer than `from` that the client keeps up with,
/// spaced at least `min_interval` apart. `wait` is the blocking primitive.
fn run<T: Send + Sync + 'static>(
    name: &str,
    from: MarketVersion,
    min_interval: Duration,
    version_of: impl Fn(&T) -> MarketVersion + Send + 'static,
    wait: impl Fn(MarketVersion, Duration) -> Next<T> + Send + 'static,
) -> mpsc::Receiver<Delivery<T>> {
    let (tx, rx) = mpsc::channel::<Delivery<T>>(1);
    let name = format!("grpc-{name}");
    // If the thread cannot be spawned the sender drops and the receiver
    // reports the stream as closed, which the RPC turns into an end of stream.
    let _ = thread::Builder::new().name(name).spawn(move || {
        let mut seen = from;
        let mut queued = from;
        let mut pending: Option<Arc<T>> = None;
        let mut last_sent = Instant::now() - min_interval;
        loop {
            if tx.is_closed() {
                return;
            }
            // Deliver what is pending before looking for anything newer.
            if let Some(value) = pending.take() {
                pace(&mut last_sent, min_interval);
                let version = version_of(&value);
                match tx.try_send(Delivery {
                    value,
                    previous: queued,
                }) {
                    Ok(()) => queued = version,
                    Err(mpsc::error::TrySendError::Full(d)) => pending = Some(d.value),
                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                }
            }
            let timeout = if pending.is_some() { RETRY } else { POLL };
            match wait(seen, timeout) {
                Next::Updated(v) => {
                    seen = version_of(&v);
                    pending = Some(v); // newer replaces anything still waiting
                }
                Next::TimedOut => {}
                Next::Closed => {
                    // The publisher is gone; make a last effort to hand over
                    // the final version before ending the stream.
                    if let Some(value) = pending.take() {
                        let _ = tx.blocking_send(Delivery {
                            value,
                            previous: queued,
                        });
                    }
                    return;
                }
            }
        }
    });
    rx
}

/// Bridge for the generic publisher (`CurveSet`, `RiskSnapshot`).
pub fn versioned<T: Versioned + Send + Sync + 'static>(
    name: &str,
    subscriber: Subscriber<T>,
    from: MarketVersion,
    min_interval: Duration,
) -> mpsc::Receiver<Delivery<T>> {
    run(
        name,
        from,
        min_interval,
        |v: &T| v.version(),
        move |seen, timeout| match subscriber.wait_newer(seen, timeout) {
            Wait::Updated(v) => Next::Updated(v),
            Wait::TimedOut => Next::TimedOut,
            Wait::Closed => Next::Closed,
        },
    )
}

/// Bridge for the quote store, which has its own reader type.
pub fn quotes(
    reader: MarketReader,
    from: MarketVersion,
    min_interval: Duration,
) -> mpsc::Receiver<Delivery<QuoteSnapshot>> {
    run(
        "quotes",
        from,
        min_interval,
        |s: &QuoteSnapshot| s.version(),
        move |seen, timeout| match reader.wait_newer(seen, timeout) {
            MarketWait::Updated(v) => Next::Updated(v),
            MarketWait::TimedOut => Next::TimedOut,
            MarketWait::Closed => Next::Closed,
        },
    )
}

fn pace(last_sent: &mut Instant, min_interval: Duration) {
    if min_interval.is_zero() {
        return;
    }
    let due = *last_sent + min_interval;
    let now = Instant::now();
    if due > now {
        thread::sleep(due - now);
    }
    *last_sent = Instant::now();
}

#[cfg(test)]
mod tests {
    use super::*;
    use riskrt_curves::publish;

    #[derive(Debug)]
    struct Item(u64);

    impl Versioned for Item {
        fn version(&self) -> MarketVersion {
            MarketVersion(self.0)
        }
    }

    fn recv<T>(rx: &mut mpsc::Receiver<Delivery<T>>, within: Duration) -> Option<Delivery<T>> {
        let deadline = Instant::now() + within;
        loop {
            match rx.try_recv() {
                Ok(d) => return Some(d),
                Err(mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return None,
            }
        }
    }

    #[test]
    fn slow_client_still_ends_on_the_latest_version_after_the_market_goes_quiet() {
        let (mut publisher, subscriber) = publish::channel(Arc::new(Item(0)));
        let mut rx = versioned("test", subscriber, MarketVersion::ZERO, Duration::ZERO);

        // Version 1 lands in the channel slot while the client is not reading...
        publisher.publish(Arc::new(Item(1)));
        thread::sleep(Duration::from_millis(100));
        // ...then two more go by with the slot still full, and the market goes quiet.
        publisher.publish(Arc::new(Item(2)));
        publisher.publish(Arc::new(Item(3)));
        thread::sleep(Duration::from_millis(100));

        let first = recv(&mut rx, Duration::from_secs(5)).expect("first delivery");
        assert_eq!(first.value.0, 1, "the slot held the first version");
        assert_eq!(first.previous, MarketVersion(0));

        // Nothing new is published. The client must still get version 3.
        let second = recv(&mut rx, Duration::from_secs(5)).expect("latest version delivered");
        assert_eq!(second.value.0, 3);
        assert_eq!(
            second.previous,
            MarketVersion(1),
            "delta base is the last delivered version"
        );
        assert!(
            recv(&mut rx, Duration::from_millis(100)).is_none(),
            "and nothing else"
        );
    }

    #[test]
    fn final_version_is_delivered_when_the_publisher_closes() {
        let (mut publisher, subscriber) = publish::channel(Arc::new(Item(0)));
        let mut rx = versioned("test", subscriber, MarketVersion::ZERO, Duration::ZERO);
        publisher.publish(Arc::new(Item(1)));
        thread::sleep(Duration::from_millis(100)); // 1 is in the slot
        publisher.publish(Arc::new(Item(2)));
        thread::sleep(Duration::from_millis(50)); // 2 is pending
        drop(publisher);

        assert_eq!(recv(&mut rx, Duration::from_secs(5)).unwrap().value.0, 1);
        assert_eq!(recv(&mut rx, Duration::from_secs(5)).unwrap().value.0, 2);
        assert!(
            recv(&mut rx, Duration::from_secs(1)).is_none(),
            "stream ends"
        );
    }

    #[test]
    fn min_interval_spaces_deliveries() {
        let (mut publisher, subscriber) = publish::channel(Arc::new(Item(0)));
        let mut rx = versioned(
            "test",
            subscriber,
            MarketVersion::ZERO,
            Duration::from_millis(100),
        );
        let started = Instant::now();
        publisher.publish(Arc::new(Item(1)));
        let _ = recv(&mut rx, Duration::from_secs(5)).unwrap();
        publisher.publish(Arc::new(Item(2)));
        let second = recv(&mut rx, Duration::from_secs(5)).unwrap();
        assert_eq!(second.value.0, 2);
        assert!(started.elapsed() >= Duration::from_millis(100));
    }
}
