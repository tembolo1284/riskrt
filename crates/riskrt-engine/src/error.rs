//! Start-up errors. Once running, stages report through their statistics.

use riskrt_curves::CurveBuildError;
use riskrt_deps::DepsError;
use riskrt_pricing::PricingError;

#[derive(Debug)]
pub enum EngineError {
    Curves(CurveBuildError),
    Deps(DepsError),
    Pricing(PricingError),
    /// A thread or pool could not be created.
    Spawn(std::io::Error),
    /// The configuration is inconsistent, e.g. a feed's quotes are not in any curve.
    Config(String),
}

impl core::fmt::Display for EngineError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Curves(e) => write!(f, "curve spec: {e}"),
            Self::Deps(e) => write!(f, "dependencies: {e}"),
            Self::Pricing(e) => write!(f, "pricing: {e}"),
            Self::Spawn(e) => write!(f, "spawn: {e}"),
            Self::Config(msg) => write!(f, "config: {msg}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<CurveBuildError> for EngineError {
    fn from(e: CurveBuildError) -> Self {
        Self::Curves(e)
    }
}
impl From<DepsError> for EngineError {
    fn from(e: DepsError) -> Self {
        Self::Deps(e)
    }
}
impl From<PricingError> for EngineError {
    fn from(e: PricingError) -> Self {
        Self::Pricing(e)
    }
}
impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        Self::Spawn(e)
    }
}
