//! The scheduler thread and its worker pool.

use crate::scheduler::{PricedBatch, SchedStats, Scheduler};
use crossbeam::channel::{Receiver, Sender, bounded};
use riskrt_core::MarketVersion;
use riskrt_curves::{CurveSet, Subscriber, Wait};
use std::io;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedConfig {
    /// Pricing worker threads. `0` lets rayon pick (one per core).
    pub workers: usize,
    /// Batches the output channel holds before the scheduler blocks. Keep it
    /// small: a backed-up aggregator should slow pricing, not buffer stale
    /// batches.
    pub output_capacity: usize,
    /// How long the scheduler sleeps on the curve subscriber before
    /// re-checking whether it has closed.
    pub poll: Duration,
}

impl Default for SchedConfig {
    fn default() -> Self {
        Self {
            workers: 0,
            output_capacity: 4,
            poll: Duration::from_millis(50),
        }
    }
}

/// Runs `scheduler` on its own thread with a dedicated rayon pool.
///
/// The thread wakes on each new curve set, prices the dirty trades, and sends
/// one `PricedBatch` down the returned channel. It exits when the curve
/// subscriber closes, after pricing whatever the final set dirtied; dropping
/// the thread's `Sender` then closes the channel for the aggregator.
pub fn spawn_scheduler(
    sets: Subscriber<CurveSet>,
    mut scheduler: Scheduler,
    cfg: SchedConfig,
) -> io::Result<(JoinHandle<SchedStats>, Receiver<PricedBatch>)> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .thread_name(|i| format!("pricer-{i}"))
        .build()
        .map_err(|e| io::Error::other(e.to_string()))?;
    let (tx, rx): (Sender<PricedBatch>, Receiver<PricedBatch>) = bounded(cfg.output_capacity);

    let handle = thread::Builder::new()
        .name("scheduler".into())
        .spawn(move || {
            let mut seen = MarketVersion::ZERO;
            loop {
                let set = match sets.wait_newer(seen, cfg.poll) {
                    Wait::Updated(set) => set,
                    Wait::TimedOut => continue,
                    Wait::Closed => break,
                };
                seen = set.version();
                // One pinned Arc<CurveSet> for the whole batch.
                let batch = pool.install(|| scheduler.round(&set));
                if let Some(batch) = batch {
                    if tx.send(batch).is_err() {
                        break; // aggregator gone
                    }
                }
            }
            scheduler.stats().clone()
        })?;
    Ok((handle, rx))
}
