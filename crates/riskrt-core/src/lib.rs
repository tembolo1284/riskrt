//! riskrt-core: plain data types shared by every pipeline stage.
//!
//! Rules for this crate:
//! - no threads, no I/O, no dependencies
//! - every type is `Send + Sync`, and `Copy` where it crosses a queue

#![forbid(unsafe_code)]

pub mod error;
pub mod ids;
pub mod portfolio;
pub mod position;
pub mod risk;
pub mod tick;

pub use error::{Error, Result};
pub use ids::{BookId, CurveId, MarketVersion, QuoteId, TradeId};
pub use portfolio::{Portfolio, PortfolioBuilder};
pub use position::{Day, PayRec, Position, Product};
pub use risk::{Ladder, PricedTrade, Risk, TradeRisk};
pub use tick::Tick;
