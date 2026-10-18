//! Errors from specifying and building curves.

use fq_core::time::DateError;
use fq_rates::curve::{BootstrapError, CurveError};
use riskrt_core::{CurveId, QuoteId};

#[derive(Clone, Debug, PartialEq)]
pub enum CurveBuildError {
    /// The curve specification is inconsistent; found at start-up.
    Spec(String),
    /// A pillar quote has no value yet. Normal while the market is warming up.
    NotReady {
        curve: CurveId,
        missing: QuoteId,
    },
    Date(DateError),
    Bootstrap(BootstrapError),
    Curve(CurveError),
}

impl core::fmt::Display for CurveBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Spec(msg) => write!(f, "invalid curve spec: {msg}"),
            Self::NotReady { curve, missing } => {
                write!(f, "{curve} not ready: no value for {missing}")
            }
            Self::Date(e) => write!(f, "{e}"),
            Self::Bootstrap(e) => write!(f, "{e}"),
            Self::Curve(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CurveBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Date(e) => Some(e),
            Self::Bootstrap(e) => Some(e),
            Self::Curve(e) => Some(e),
            Self::Spec(_) | Self::NotReady { .. } => None,
        }
    }
}

impl From<DateError> for CurveBuildError {
    fn from(e: DateError) -> Self {
        Self::Date(e)
    }
}

impl From<BootstrapError> for CurveBuildError {
    fn from(e: BootstrapError) -> Self {
        Self::Bootstrap(e)
    }
}

impl From<CurveError> for CurveBuildError {
    fn from(e: CurveError) -> Self {
        Self::Curve(e)
    }
}
