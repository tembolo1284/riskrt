//! The source abstraction and the thread that drives one.

use crossbeam::channel::{SendTimeoutError, Sender};
use riskrt_core::Tick;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Anything that yields ticks in sequence order. `None` means end of stream.
pub trait TickSource: Send {
    fn next_tick(&mut self) -> Option<Tick>;
}

impl<S: TickSource + ?Sized> TickSource for Box<S> {
    fn next_tick(&mut self) -> Option<Tick> {
        (**self).next_tick()
    }
}

/// What one feed thread did, returned from its `JoinHandle`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedStats {
    /// Ticks delivered to the channel.
    pub sent: u64,
    /// Ticks dropped at the source for a NaN or infinite value.
    pub invalid: u64,
    /// Times a send waited a full poll interval on a full channel.
    pub stalls: u64,
}

/// How often a blocked sender re-checks the stop flag.
const SEND_POLL: Duration = Duration::from_millis(50);

/// Runs `source` on its own named thread, pushing ticks into `tx`.
///
/// The thread ends when the source is exhausted, `stop` is set, or the
/// receiving side has gone away. Dropping `tx` on exit is what lets the
/// conflator detect that all feeds are finished.
pub fn spawn_feed<S>(
    name: &str,
    mut source: S,
    tx: Sender<Tick>,
    stop: Arc<AtomicBool>,
) -> io::Result<JoinHandle<FeedStats>>
where
    S: TickSource + 'static,
{
    thread::Builder::new()
        .name(format!("feed-{name}"))
        .spawn(move || {
            let mut stats = FeedStats::default();
            while !stop.load(Ordering::Relaxed) {
                let Some(tick) = source.next_tick() else {
                    break;
                };
                if !tick.is_valid() {
                    stats.invalid += 1;
                    continue;
                }
                let mut pending = tick;
                loop {
                    match tx.send_timeout(pending, SEND_POLL) {
                        Ok(()) => {
                            stats.sent += 1;
                            break;
                        }
                        Err(SendTimeoutError::Timeout(t)) => {
                            stats.stalls += 1;
                            if stop.load(Ordering::Relaxed) {
                                return stats;
                            }
                            pending = t;
                        }
                        Err(SendTimeoutError::Disconnected(_)) => return stats,
                    }
                }
            }
            stats
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tick_channel;
    use riskrt_core::QuoteId;

    struct Fixed(std::vec::IntoIter<Tick>);

    impl TickSource for Fixed {
        fn next_tick(&mut self) -> Option<Tick> {
            self.0.next()
        }
    }

    fn fixed(values: &[f64]) -> Fixed {
        let ticks: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Tick::new(QuoteId(0), i as u64 + 1, 0, *v))
            .collect();
        Fixed(ticks.into_iter())
    }

    #[test]
    fn delivers_all_ticks_then_disconnects() {
        let (tx, rx) = tick_channel(16);
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn_feed("t", fixed(&[0.01, 0.02, 0.03]), tx, stop).unwrap();
        let got: Vec<_> = rx.iter().map(|t| t.seq).collect();
        assert_eq!(got, vec![1, 2, 3]);
        assert_eq!(h.join().unwrap().sent, 3);
    }

    #[test]
    fn invalid_ticks_are_dropped_at_the_source() {
        let (tx, rx) = tick_channel(16);
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn_feed("t", fixed(&[0.01, f64::NAN, 0.03]), tx, stop).unwrap();
        assert_eq!(rx.iter().count(), 2);
        let stats = h.join().unwrap();
        assert_eq!((stats.sent, stats.invalid), (2, 1));
    }

    #[test]
    fn stop_flag_ends_a_feed_blocked_on_a_full_channel() {
        let (tx, rx) = tick_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn_feed("t", fixed(&[0.01; 100]), tx, Arc::clone(&stop)).unwrap();
        stop.store(true, Ordering::Relaxed);
        let stats = h.join().unwrap();
        assert!(stats.sent < 100);
        drop(rx);
    }

    #[test]
    fn feed_ends_when_receiver_is_dropped() {
        let (tx, rx) = tick_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        drop(rx);
        let h = spawn_feed("t", fixed(&[0.01; 100]), tx, stop).unwrap();
        assert_eq!(h.join().unwrap().sent, 0);
    }
}
