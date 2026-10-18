//! Errors from building instruments and pricing them.

use fq_core::time::DateError;
use fq_rates::bond::BondError;
use fq_rates::curve::CurveError;
use fq_rates::swap::SwapError;
use riskrt_core::{CurveId, TradeId};

#[derive(Clone, Debug, PartialEq)]
pub enum PricingError {
    /// A `TradeId` outside the portfolio this pricer was built from.
    UnknownTrade(TradeId),
    /// The curve set has not built this trade's curve yet. Normal while the
    /// market is warming up; the trade stays dirty and is retried.
    CurveNotBuilt {
        trade: TradeId,
        curve: CurveId,
    },
    /// A swap whose projection curve differs from its discount curve. Not
    /// supported in v1: USD SOFR OIS is single-curve.
    TwoCurveSwap {
        trade: TradeId,
        discount: CurveId,
        forward: CurveId,
    },
    /// The instrument could not be built from the position at load time.
    Build {
        trade: TradeId,
        reason: String,
    },
    Date(DateError),
    Swap(SwapError),
    Bond(BondError),
    Curve(CurveError),
}

impl core::fmt::Display for PricingError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownTrade(t) => write!(f, "unknown trade {t}"),
            Self::CurveNotBuilt { trade, curve } => write!(f, "{trade}: {curve} not built yet"),
            Self::TwoCurveSwap {
                trade,
                discount,
                forward,
            } => {
                write!(
                    f,
                    "{trade}: two-curve swap ({discount} / {forward}) not supported"
                )
            }
            Self::Build { trade, reason } => {
                write!(f, "{trade}: cannot build instrument: {reason}")
            }
            Self::Date(e) => write!(f, "{e}"),
            Self::Swap(e) => write!(f, "{e}"),
            Self::Bond(e) => write!(f, "{e}"),
            Self::Curve(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PricingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Date(e) => Some(e),
            Self::Swap(e) => Some(e),
            Self::Bond(e) => Some(e),
            Self::Curve(e) => Some(e),
            _ => None,
        }
    }
}

impl From<DateError> for PricingError {
    fn from(e: DateError) -> Self {
        Self::Date(e)
    }
}

impl From<SwapError> for PricingError {
    fn from(e: SwapError) -> Self {
        Self::Swap(e)
    }
}

impl From<BondError> for PricingError {
    fn from(e: BondError) -> Self {
        Self::Bond(e)
    }
}

impl From<CurveError> for PricingError {
    fn from(e: CurveError) -> Self {
        Self::Curve(e)
    }
}
