//! Conflation: keep only the latest tick per quote between flushes.
//!
//! `Conflator` is a plain single-threaded structure, so the rule is testable
//! without threads. `spawn_conflator` puts it on a thread between the feed
//! channel and a sink.

use crossbeam::channel::Receiver;
use riskrt_core::{QuoteId, Tick};
use std::io;
use std::thread::{self, JoinHandle};

/// What happened to an offered tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Offer {
    /// First update for this quote since the last drain.
    Accepted,
    /// Replaced an update that had not been drained yet.
    Conflated,
    /// Sequence number not newer than the latest seen for this quote.
    Stale,
    /// NaN or infinite value.
    Invalid,
    /// Quote index outside the table.
    UnknownQuote,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConflateStats {
    pub received: u64,
    /// Ticks passed downstream. `received - published` is what conflation saved.
    pub published: u64,
    pub conflated: u64,
    pub stale: u64,
    pub invalid: u64,
    pub unknown: u64,
    /// Non-empty batches handed to the sink.
    pub batches: u64,
}

/// Dense latest-value table indexed by `QuoteId`, plus the set of quotes that
/// changed since the last drain.
#[derive(Clone, Debug)]
pub struct Conflator {
    latest: Vec<Option<Tick>>,
    pending: Vec<bool>,
    dirty: Vec<QuoteId>,
    stats: ConflateStats,
}

impl Conflator {
    pub fn new(quote_count: usize) -> Self {
        Self {
            latest: vec![None; quote_count],
            pending: vec![false; quote_count],
            dirty: Vec::with_capacity(quote_count),
            stats: ConflateStats::default(),
        }
    }

    pub fn offer(&mut self, tick: Tick) -> Offer {
        self.stats.received += 1;
        if !tick.is_valid() {
            self.stats.invalid += 1;
            return Offer::Invalid;
        }
        let i = tick.quote.index();
        let Some(slot) = self.latest.get_mut(i) else {
            self.stats.unknown += 1;
            return Offer::UnknownQuote;
        };
        // The last tick stays in the table after a drain, so a late
        // out-of-order tick is still caught as stale.
        if slot.is_some_and(|prev| !tick.supersedes(&prev)) {
            self.stats.stale += 1;
            return Offer::Stale;
        }
        *slot = Some(tick);
        if self.pending[i] {
            self.stats.conflated += 1;
            Offer::Conflated
        } else {
            self.pending[i] = true;
            self.dirty.push(tick.quote);
            Offer::Accepted
        }
    }

    /// Appends the latest tick of every changed quote to `out`, in the order
    /// the quotes first changed, and clears the changed set.
    pub fn drain(&mut self, out: &mut Vec<Tick>) -> usize {
        let n = self.dirty.len();
        for quote in self.dirty.drain(..) {
            let i = quote.index();
            self.pending[i] = false;
            if let Some(tick) = self.latest[i] {
                out.push(tick);
            }
        }
        self.stats.published += n as u64;
        n
    }

    /// Number of quotes with an undrained update.
    pub fn pending(&self) -> usize {
        self.dirty.len()
    }

    /// Latest accepted tick for a quote, drained or not.
    pub fn latest(&self, quote: QuoteId) -> Option<Tick> {
        self.latest.get(quote.index()).copied().flatten()
    }

    pub fn stats(&self) -> ConflateStats {
        self.stats
    }
}

/// Runs a conflator on its own thread.
///
/// Each cycle blocks for one tick, then takes whatever else is already queued
/// (up to `max_drain` ticks) and hands the sink one conflated batch. While the
/// sink is busy, ticks accumulate in the channel and collapse on the next
/// cycle, so the batch size adapts to load by itself.
///
/// `max_drain` bounds how long a flood can delay a flush. The thread exits once
/// every `Sender` has been dropped and the channel is empty.
pub fn spawn_conflator<F>(
    rx: Receiver<Tick>,
    quote_count: usize,
    max_drain: usize,
    mut sink: F,
) -> io::Result<JoinHandle<ConflateStats>>
where
    F: FnMut(&[Tick]) + Send + 'static,
{
    thread::Builder::new()
        .name("conflator".into())
        .spawn(move || {
            let mut conflator = Conflator::new(quote_count);
            let mut batch = Vec::with_capacity(quote_count);
            while let Ok(first) = rx.recv() {
                conflator.offer(first);
                let mut taken = 1;
                while taken < max_drain {
                    let Ok(tick) = rx.try_recv() else { break };
                    conflator.offer(tick);
                    taken += 1;
                }
                batch.clear();
                if conflator.drain(&mut batch) > 0 {
                    conflator.stats.batches += 1;
                    sink(&batch);
                }
            }
            conflator.stats()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(quote: u32, seq: u64, value: f64) -> Tick {
        Tick::new(QuoteId(quote), seq, seq * 1_000, value)
    }

    #[test]
    fn latest_tick_wins_within_a_batch() {
        let mut c = Conflator::new(4);
        assert_eq!(c.offer(tick(1, 1, 0.040)), Offer::Accepted);
        assert_eq!(c.offer(tick(1, 2, 0.041)), Offer::Conflated);
        assert_eq!(c.offer(tick(1, 3, 0.042)), Offer::Conflated);
        let mut out = Vec::new();
        assert_eq!(c.drain(&mut out), 1);
        assert_eq!(out, vec![tick(1, 3, 0.042)]);
    }

    #[test]
    fn drain_preserves_first_changed_order_and_clears() {
        let mut c = Conflator::new(4);
        c.offer(tick(2, 1, 0.01));
        c.offer(tick(0, 1, 0.02));
        c.offer(tick(2, 2, 0.03));
        let mut out = Vec::new();
        c.drain(&mut out);
        let quotes: Vec<_> = out.iter().map(|t| t.quote.0).collect();
        assert_eq!(quotes, vec![2, 0]);
        assert_eq!(c.pending(), 0);
        out.clear();
        assert_eq!(c.drain(&mut out), 0);
    }

    #[test]
    fn stale_tick_is_rejected_even_after_a_drain() {
        let mut c = Conflator::new(2);
        c.offer(tick(0, 5, 0.04));
        c.drain(&mut Vec::new());
        assert_eq!(c.offer(tick(0, 5, 0.99)), Offer::Stale);
        assert_eq!(c.offer(tick(0, 4, 0.99)), Offer::Stale);
        assert_eq!(c.pending(), 0);
        assert_eq!(c.latest(QuoteId(0)).unwrap().value, 0.04);
    }

    #[test]
    fn a_quote_can_update_again_after_a_drain() {
        let mut c = Conflator::new(2);
        c.offer(tick(0, 1, 0.04));
        c.drain(&mut Vec::new());
        assert_eq!(c.offer(tick(0, 2, 0.05)), Offer::Accepted);
    }

    #[test]
    fn invalid_and_unknown_ticks_are_rejected() {
        let mut c = Conflator::new(2);
        assert_eq!(c.offer(tick(0, 1, f64::NAN)), Offer::Invalid);
        assert_eq!(c.offer(tick(9, 1, 0.04)), Offer::UnknownQuote);
        assert_eq!(c.pending(), 0);
    }

    #[test]
    fn stats_account_for_every_tick() {
        let mut c = Conflator::new(2);
        c.offer(tick(0, 1, 0.04)); // accepted
        c.offer(tick(0, 2, 0.05)); // conflated
        c.offer(tick(0, 1, 0.06)); // stale
        c.offer(tick(1, 1, f64::INFINITY)); // invalid
        c.offer(tick(7, 1, 0.04)); // unknown
        c.drain(&mut Vec::new());
        let s = c.stats();
        assert_eq!(s.received, 5);
        assert_eq!(
            s.received,
            s.published + s.conflated + s.stale + s.invalid + s.unknown
        );
    }
}
