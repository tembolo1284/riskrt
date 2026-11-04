//! Blocking `wait_newer` loops turned into tokio streams.
//!
//! One std thread per subscriber. It blocks the way an engine stage does and
//! pushes into a tokio channel of capacity one using `try_send`. If the
//! client has not taken the previous update yet, the new one is dropped and
//! the next wake sees the newer version anyway. The client therefore gets
//! the latest version it can keep up with, never a queue.
//!
//! Each delivery also carries the version of the delivery before it (the
//! last one that actually went into the channel, not the last one seen), so
//! a delta stream can ask `changed_since(previous)` and never miss a change
//! across the versions that were conflated away.

use riskrt_core::MarketVersion;
use riskrt_curves::{Subscriber, Versioned, Wait};
use riskrt_market::{MarketReader, QuoteSnapshot, Wait as MarketWait};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// How long a bridge thread blocks before checking whether its client left.
const POLL: Duration = Duration::from_millis(250);

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
        let mut last_sent = Instant::now() - min_interval;
        loop {
            if tx.is_closed() {
                return;
            }
            let value = match wait(seen, POLL) {
                Next::Updated(v) => v,
                Next::TimedOut => continue,
                Next::Closed => return,
            };
            seen = version_of(&value);
            pace(&mut last_sent, min_interval);
            match tx.try_send(Delivery {
                value,
                previous: queued,
            }) {
                Ok(()) => queued = seen,
                Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => return,
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
