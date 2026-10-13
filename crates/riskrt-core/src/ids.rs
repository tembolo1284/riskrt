//! Dense integer identifiers.
//!
//! Names ("USD-SOFR", "5Y swap") are interned to `u32` once at load time. On the
//! hot path every lookup is then a `Vec` index: no hashing, no string compares.

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(transparent)]
        pub struct $name(pub u32);

        impl $name {
            /// Position in the dense table this id indexes.
            #[inline]
            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }

        impl From<u32> for $name {
            #[inline]
            fn from(v: u32) -> Self {
                Self(v)
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

id_type!(
    /// A curve (e.g. USD-SOFR). Unit of dirtiness: a tick dirties one curve.
    CurveId
);
id_type!(
    /// One quoted input to a curve (a deposit, future or swap node).
    QuoteId
);
id_type!(
    /// One trade in the portfolio.
    TradeId
);
id_type!(
    /// Aggregation bucket for rollups.
    BookId
);

/// Monotonic version of the published curve set.
///
/// Every revaluation batch pins exactly one version, and every risk number is
/// tagged with it, so a published snapshot can be checked for consistency.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MarketVersion(pub u64);

impl MarketVersion {
    pub const ZERO: Self = Self(0);

    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

// Compile-time guarantee that ids can cross threads and queues freely.
const _: () = {
    const fn assert_plain<T: Send + Sync + Copy>() {}
    assert_plain::<CurveId>();
    assert_plain::<QuoteId>();
    assert_plain::<TradeId>();
    assert_plain::<BookId>();
    assert_plain::<MarketVersion>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_round_trips() {
        let id = TradeId::from(42);
        assert_eq!(id.index(), 42);
        assert_eq!(id.to_string(), "TradeId(42)");
    }

    #[test]
    fn ids_are_four_bytes() {
        assert_eq!(size_of::<CurveId>(), 4);
        assert_eq!(size_of::<QuoteId>(), 4);
    }

    #[test]
    fn version_is_monotonic() {
        let v0 = MarketVersion::ZERO;
        let v1 = v0.next();
        assert!(v1 > v0);
        assert_eq!(v1.next(), MarketVersion(2));
    }
}
