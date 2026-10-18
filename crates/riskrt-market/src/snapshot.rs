//! One immutable, versioned view of every quote.

use riskrt_core::{MarketVersion, QuoteId, Tick};

/// The state of all quotes at one `MarketVersion`.
///
/// Never mutated after construction. `next` builds the following version by
/// copying the table and applying a batch; at 40 bytes per quote that copy is
/// a few microseconds for thousands of quotes, and it is paid once per
/// conflated batch, not once per tick.
#[derive(Clone, Debug, PartialEq)]
pub struct QuoteSnapshot {
    version: MarketVersion,
    ticks: Vec<Option<Tick>>,
    /// Version at which each quote last changed. Lets a consumer that skipped
    /// versions still find everything that moved since the one it last saw.
    changed_at: Vec<MarketVersion>,
    populated: usize,
    updates: usize,
    as_of_ns: u64,
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<QuoteSnapshot>();
};

impl QuoteSnapshot {
    /// Version zero: `quote_count` quotes, none with a value yet.
    pub fn empty(quote_count: usize) -> Self {
        Self {
            version: MarketVersion::ZERO,
            ticks: vec![None; quote_count],
            changed_at: vec![MarketVersion::ZERO; quote_count],
            populated: 0,
            updates: 0,
            as_of_ns: 0,
        }
    }

    /// The next version with `batch` applied, or `None` if no tick in the
    /// batch was applicable (so no new version is warranted).
    ///
    /// A tick is skipped if its value is not finite, its quote is outside the
    /// table, or it does not supersede the tick already held for that quote.
    pub fn next(&self, batch: &[Tick]) -> Option<Self> {
        let mut next: Option<Self> = None;
        for tick in batch {
            let current = next.as_ref().unwrap_or(self);
            let Some(held) = current.ticks.get(tick.quote.index()) else {
                continue;
            };
            if !tick.is_valid() || held.is_some_and(|prev| !tick.supersedes(&prev)) {
                continue;
            }
            // Copy the table only once we know something will change.
            let snap = next.get_or_insert_with(|| Self {
                version: self.version.next(),
                updates: 0,
                ..self.clone()
            });
            let i = tick.quote.index();
            if snap.ticks[i].is_none() {
                snap.populated += 1;
            }
            snap.ticks[i] = Some(*tick);
            snap.changed_at[i] = snap.version;
            snap.updates += 1;
            snap.as_of_ns = snap.as_of_ns.max(tick.ts_ns);
        }
        next
    }

    pub fn version(&self) -> MarketVersion {
        self.version
    }

    /// Latest tick for a quote; `None` if unknown or not yet quoted.
    pub fn get(&self, quote: QuoteId) -> Option<Tick> {
        self.ticks.get(quote.index()).copied().flatten()
    }

    /// Latest value for a quote; `None` if unknown or not yet quoted.
    pub fn value(&self, quote: QuoteId) -> Option<f64> {
        self.get(quote).map(|t| t.value)
    }

    /// Dense table; a `QuoteId` indexes straight into it.
    pub fn ticks(&self) -> &[Option<Tick>] {
        &self.ticks
    }

    pub fn quote_count(&self) -> usize {
        self.ticks.len()
    }

    /// Number of quotes that have a value.
    pub fn populated(&self) -> usize {
        self.populated
    }

    /// True once every quote has a value, i.e. curves can be built.
    pub fn is_complete(&self) -> bool {
        self.populated == self.ticks.len()
    }

    /// Ticks applied to produce this version from the previous one.
    pub fn updates(&self) -> usize {
        self.updates
    }

    /// Latest feed timestamp across all quotes.
    pub fn as_of_ns(&self) -> u64 {
        self.as_of_ns
    }

    /// Version at which `quote` last changed; `None` if unknown or never quoted.
    pub fn changed_at(&self, quote: QuoteId) -> Option<MarketVersion> {
        self.get(quote).map(|_| self.changed_at[quote.index()])
    }

    /// Quotes that changed after version `seen`, in id order.
    ///
    /// Correct even if the caller skipped versions in between, which is the
    /// normal case for a consumer slower than the publisher.
    pub fn changed_since(&self, seen: MarketVersion) -> impl Iterator<Item = QuoteId> + '_ {
        self.changed_at
            .iter()
            .enumerate()
            .filter(move |(_, at)| **at > seen)
            .map(|(i, _)| QuoteId(i as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(quote: u32, seq: u64, value: f64) -> Tick {
        Tick::new(QuoteId(quote), seq, seq * 1_000, value)
    }

    #[test]
    fn empty_snapshot_is_version_zero_with_no_values() {
        let s = QuoteSnapshot::empty(3);
        assert_eq!(s.version(), MarketVersion::ZERO);
        assert_eq!(s.quote_count(), 3);
        assert_eq!(s.populated(), 0);
        assert!(!s.is_complete());
        assert_eq!(s.get(QuoteId(0)), None);
        assert_eq!(s.changed_since(MarketVersion::ZERO).count(), 0);
    }

    #[test]
    fn next_applies_the_batch_and_bumps_the_version() {
        let v0 = QuoteSnapshot::empty(3);
        let v1 = v0.next(&[tick(0, 1, 0.040), tick(2, 1, 0.045)]).unwrap();
        assert_eq!(v1.version(), MarketVersion(1));
        assert_eq!(v1.value(QuoteId(0)), Some(0.040));
        assert_eq!(v1.value(QuoteId(1)), None);
        assert_eq!(v1.value(QuoteId(2)), Some(0.045));
        assert_eq!((v1.populated(), v1.updates()), (2, 2));
        assert_eq!(v1.as_of_ns(), 1_000);
    }

    #[test]
    fn next_leaves_the_original_untouched() {
        let v0 = QuoteSnapshot::empty(2);
        let before = v0.clone();
        let _v1 = v0.next(&[tick(0, 1, 0.04)]).unwrap();
        assert_eq!(v0, before);
    }

    #[test]
    fn untouched_quotes_carry_over() {
        let v1 = QuoteSnapshot::empty(2).next(&[tick(0, 1, 0.04)]).unwrap();
        let v2 = v1.next(&[tick(1, 1, 0.05)]).unwrap();
        assert_eq!(v2.get(QuoteId(0)), v1.get(QuoteId(0)));
        assert!(v2.is_complete());
        assert_eq!(v2.updates(), 1);
    }

    #[test]
    fn batch_with_nothing_applicable_yields_no_version() {
        let v1 = QuoteSnapshot::empty(2).next(&[tick(0, 5, 0.04)]).unwrap();
        assert!(v1.next(&[]).is_none());
        assert!(v1.next(&[tick(0, 5, 0.99)]).is_none()); // same seq
        assert!(v1.next(&[tick(0, 4, 0.99)]).is_none()); // older seq
        assert!(v1.next(&[tick(9, 1, 0.04)]).is_none()); // unknown quote
        assert!(v1.next(&[tick(1, 1, f64::NAN)]).is_none()); // invalid value
    }

    #[test]
    fn bad_ticks_are_skipped_but_good_ones_in_the_batch_apply() {
        let v1 = QuoteSnapshot::empty(2).next(&[tick(0, 5, 0.04)]).unwrap();
        let v2 = v1
            .next(&[tick(0, 4, 0.99), tick(9, 1, 0.99), tick(1, 1, 0.05)])
            .unwrap();
        assert_eq!(v2.value(QuoteId(0)), Some(0.04));
        assert_eq!(v2.value(QuoteId(1)), Some(0.05));
        assert_eq!(v2.updates(), 1);
    }

    #[test]
    fn later_tick_in_the_same_batch_wins() {
        let v1 = QuoteSnapshot::empty(1)
            .next(&[tick(0, 1, 0.040), tick(0, 2, 0.041)])
            .unwrap();
        assert_eq!(v1.get(QuoteId(0)).unwrap().seq, 2);
        assert_eq!((v1.populated(), v1.updates()), (1, 2));
    }

    #[test]
    fn changed_since_survives_skipped_versions() {
        let v1 = QuoteSnapshot::empty(4).next(&[tick(0, 1, 0.01)]).unwrap();
        let v2 = v1.next(&[tick(1, 1, 0.02)]).unwrap();
        let v3 = v2.next(&[tick(3, 1, 0.03)]).unwrap();

        let since = |seen: u64| -> Vec<u32> {
            v3.changed_since(MarketVersion(seen)).map(|q| q.0).collect()
        };
        assert_eq!(since(0), vec![0, 1, 3]); // a reader that saw nothing
        assert_eq!(since(1), vec![1, 3]); // a reader that skipped v2
        assert_eq!(since(2), vec![3]);
        assert_eq!(since(3), Vec::<u32>::new());
        assert_eq!(v3.changed_at(QuoteId(1)), Some(MarketVersion(2)));
        assert_eq!(v3.changed_at(QuoteId(2)), None);
    }
}
