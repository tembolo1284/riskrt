//! The unit of market data as it leaves a feed thread.

use crate::ids::QuoteId;

/// One quote update for one curve input.
///
/// 32 bytes, `Copy`, no pointers: two fit in a cache line and it moves through
/// SPSC queues by value. Static facts about the quote (instrument type, tenor,
/// owning curve) live in reference data, not here.
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
pub struct Tick {
    /// Per-feed monotonic sequence number.
    pub seq: u64,
    /// Feed timestamp, nanoseconds since the Unix epoch.
    pub ts_ns: u64,
    /// Quoted value: a rate in decimal (0.0425) or a futures price.
    pub value: f64,
    pub quote: QuoteId,
}

const _: () = assert!(size_of::<Tick>() == 32);

impl Tick {
    #[inline]
    pub const fn new(quote: QuoteId, seq: u64, ts_ns: u64, value: f64) -> Self {
        Self { seq, ts_ns, value, quote }
    }

    /// Conflation rule: a tick replaces an older one for the same quote.
    #[inline]
    pub fn supersedes(&self, other: &Tick) -> bool {
        self.quote == other.quote && self.seq > other.seq
    }

    /// Rejects NaN and infinities before they reach a curve build.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.value.is_finite()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_tick_supersedes_same_quote() {
        let old = Tick::new(QuoteId(7), 1, 1_000, 0.0425);
        let new = Tick::new(QuoteId(7), 2, 2_000, 0.0426);
        assert!(new.supersedes(&old));
        assert!(!old.supersedes(&new));
    }

    #[test]
    fn different_quotes_never_supersede() {
        let a = Tick::new(QuoteId(7), 1, 1_000, 0.0425);
        let b = Tick::new(QuoteId(8), 2, 2_000, 0.0426);
        assert!(!b.supersedes(&a));
    }

    #[test]
    fn nan_is_invalid() {
        assert!(!Tick::new(QuoteId(1), 1, 0, f64::NAN).is_valid());
        assert!(Tick::new(QuoteId(1), 1, 0, 0.03).is_valid());
    }
}
