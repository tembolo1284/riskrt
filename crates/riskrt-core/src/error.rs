//! Errors for the data layer. Pipeline stages define their own and wrap this.

use crate::ids::TradeId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// A `TradeId` that is not in the portfolio.
    UnknownTrade(TradeId),
    /// A position failed validation when added.
    InvalidPosition { trade: TradeId, reason: &'static str },
    /// More than `u32::MAX` trades.
    CapacityExceeded,
}

pub type Result<T> = core::result::Result<T, Error>;

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::UnknownTrade(id) => write!(f, "unknown trade {id}"),
            Error::InvalidPosition { trade, reason } => {
                write!(f, "invalid position {trade}: {reason}")
            }
            Error::CapacityExceeded => write!(f, "portfolio capacity exceeded"),
        }
    }
}

impl std::error::Error for Error {}
