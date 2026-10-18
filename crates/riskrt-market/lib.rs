//! riskrt-market: the versioned quote store.
//!
//! ```text
//! conflator ─▶ MarketWriter::publish(&[Tick]) ─▶ ArcSwap<QuoteSnapshot> ─▶ MarketReader::load()
//!                 (exactly one)                                               (any number)
//! ```
//!
//! - A `QuoteSnapshot` is immutable. Publishing builds the next one and swaps
//!   a pointer, so a reader can never observe a half-applied batch.
//! - Readers are wait-free: `load()` is an `Arc` clone, never a lock.
//! - There is exactly one writer, enforced by the type system: `MarketWriter`
//!   is not `Clone` and `publish` takes `&mut self`.
//! - Whoever holds an `Arc<QuoteSnapshot>` has pinned that market version for
//!   as long as it likes. That is how a revaluation batch stays consistent.

#![forbid(unsafe_code)]

pub mod snapshot;
pub mod store;

pub use snapshot::QuoteSnapshot;
pub use store::{MarketReader, MarketWriter, PublishStats, Wait, market_store};
