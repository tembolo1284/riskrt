//! riskrt-feed: market data ingest. The first crate with threads in it.
//!
//! ```text
//! feed thread ─┐
//! feed thread ─┼─▶ bounded channel ─▶ conflator thread ─▶ sink(&[Tick])
//! feed thread ─┘
//! ```
//!
//! - Each feed thread drives one `TickSource` (simulated or replayed).
//! - The conflator keeps only the latest tick per quote and hands the sink one
//!   batch per wake-up, so a slow consumer sees fewer, fresher ticks instead of
//!   a growing backlog.
//! - Shutdown is by ownership: when every feed has finished and dropped its
//!   sender, the conflator drains what is left and exits.
//!
//! Contract: each quote is published by exactly one feed, because `Tick::seq`
//! is only comparable within a feed.

#![forbid(unsafe_code)]

pub mod conflate;
pub mod replay;
pub mod sim;
pub mod source;

pub use conflate::{ConflateStats, Conflator, Offer, spawn_conflator};
pub use replay::{ReplayError, ReplaySource};
pub use sim::{SimConfig, SimSource};
pub use source::{FeedStats, TickSource, spawn_feed};

pub use crossbeam::channel::{Receiver, Sender};

/// The channel between feed threads and the conflator.
///
/// Bounded, so a stalled conflator applies backpressure to the feeds instead
/// of letting memory grow.
pub fn tick_channel(capacity: usize) -> (Sender<riskrt_core::Tick>, Receiver<riskrt_core::Tick>) {
    crossbeam::channel::bounded(capacity)
}
