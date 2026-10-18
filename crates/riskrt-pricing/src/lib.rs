//! riskrt-pricing: from a `Position` and a `CurveSet` to risk numbers.
//!
//! ```text
//! Portfolio ──(once, at load)──▶ FqPricer { OisSwap | FixedRateBond per trade }
//!                                      │
//! CurveSet ──(every tick)──▶ Pricer::price(trade, &curves) ──▶ PricedTrade
//! ```
//!
//! - `Pricer` is the only thing the scheduler and workers see. It is
//!   `Send + Sync`, takes `&self`, and allocates nothing but the ladder, so
//!   any number of workers can call it on one shared instance.
//! - `FqPricer` is the ferrumquant adapter. Instruments are built once from
//!   the portfolio; each tick only prices against the pinned `CurveSet`.
//! - Every number is per basis point and tagged with the curve set version.
//! - `price_batch` is the fan-out other stages will use: the same trades in
//!   the same order give the same bytes whatever the thread count.

#![forbid(unsafe_code)]

pub mod error;
pub mod fq_adapter;
pub mod pricer;

pub use error::PricingError;
pub use fq_adapter::FqPricer;
pub use pricer::{Pricer, price_batch};
