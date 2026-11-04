//! riskrt-agg: rolls priced trades up into published risk snapshots.
//!
//! ```text
//! Receiver<PricedBatch> ─▶ aggregator thread ─▶ Publisher<RiskSnapshot> ─▶ readers (UI, CLI, tests)
//!                           (one, owns the         (ArcSwap: wait-free
//!                            per-trade table)       loads, pinned versions)
//! ```
//!
//! - The aggregator keeps the latest result per trade. Each batch replaces
//!   the entries it carries and produces a new, immutable `RiskSnapshot`.
//! - Totals (portfolio, per book, per curve, and per-curve par ladders) are
//!   recomputed from the per-trade table on every batch rather than updated
//!   incrementally, so a snapshot's numbers never depend on the history of
//!   batches that led to it and never drift.
//! - Per-trade results are held behind `Arc`, so building a snapshot copies
//!   pointers, not ladders.
//! - A snapshot is published as one value: a reader never sees a book total
//!   from one batch and a curve total from another.

#![forbid(unsafe_code)]

pub mod aggregator;
pub mod snapshot;

pub use aggregator::{AggStats, Aggregator, spawn_aggregator};
pub use snapshot::RiskSnapshot;
