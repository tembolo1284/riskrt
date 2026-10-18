//! riskrt-deps: which trades must be repriced when a curve moves.
//!
//! ```text
//! CurveSet::changed_since(seen) ─▶ DirtySet::mark_curves(&CurveIndex, ..) ─▶ drain_into(&mut batch)
//!        (dirty curves)                  (dedup across curves)                 (sorted TradeIds)
//! ```
//!
//! - `CurveIndex` is the inverted dependency map, curve -> trades. It is built
//!   once from the portfolio, is immutable, and is shared between threads.
//! - `DirtySet` accumulates the trades waiting to be repriced. A trade that
//!   depends on two dirty curves is queued once. It is owned by one thread
//!   (the scheduler), so it needs no synchronisation.
//!
//! No threads and no pricing-library dependency: this crate is plain data
//! structures over `riskrt-core` ids.

#![forbid(unsafe_code)]

pub mod dirty;
pub mod index;

pub use dirty::DirtySet;
pub use index::{CurveIndex, DepsError};
