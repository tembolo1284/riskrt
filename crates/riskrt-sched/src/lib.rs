//! riskrt-sched: decides what to reprice and fans it out over a worker pool.
//!
//! ```text
//! Subscriber<CurveSet> ─▶ scheduler thread ─▶ rayon worker pool ─▶ bounded channel ─▶ aggregator
//!                          (one, owns the        (price_batch over       (PricedBatch, one
//!                           DirtySet)             one pinned set)         per set version)
//! ```
//!
//! - One scheduler thread owns the `DirtySet`. On each new curve set it marks
//!   the trades on curves that changed since the last set it handled, drains
//!   them as one sorted batch, pins that set, and prices the batch in parallel.
//! - If curve sets arrive faster than batches finish, the scheduler skips to
//!   the latest one: `changed_since` across the skipped versions still marks
//!   every affected trade once. Work conflates the same way ticks do upstream.
//! - A trade whose curve is not built yet stays dirty and is retried when the
//!   curve appears; any other pricing failure is counted and dropped.
//! - Every result in a batch carries the batch's set version, so the
//!   aggregator can never mix versions inside one batch.

#![forbid(unsafe_code)]

pub mod scheduler;
pub mod worker;

pub use scheduler::{PricedBatch, SchedStats, Scheduler};
pub use worker::{SchedConfig, spawn_scheduler};
