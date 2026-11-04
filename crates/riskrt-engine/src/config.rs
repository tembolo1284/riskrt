//! Everything the engine needs to start.

use fq_rates::index::OvernightIndex;
use riskrt_core::Portfolio;
use riskrt_curves::{BuildConfig, CurveSetSpec};
use riskrt_sched::SchedConfig;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub portfolio: Portfolio,
    pub curves: CurveSetSpec,
    /// Start-of-day fixings; replaced intraday through `Engine::set_index`.
    pub index: Arc<OvernightIndex>,
    pub build: BuildConfig,
    pub sched: SchedConfig,
    /// Threads for the curve builder's bumped bootstraps. `0` uses rayon's
    /// global pool; a dedicated pool keeps curve builds off the pricing workers.
    pub curve_workers: usize,
    /// Tick channel capacity between feeds and the conflator.
    pub tick_capacity: usize,
    /// Most ticks the conflator takes in one drain before flushing a batch.
    pub conflate_max_drain: usize,
}

impl EngineConfig {
    pub fn new(portfolio: Portfolio, curves: CurveSetSpec) -> Self {
        Self {
            portfolio,
            curves,
            index: Arc::new(OvernightIndex::sofr()),
            build: BuildConfig::default(),
            sched: SchedConfig::default(),
            curve_workers: 2,
            tick_capacity: 4_096,
            conflate_max_drain: 4_096,
        }
    }

    pub fn with_workers(mut self, pricing: usize, curve: usize) -> Self {
        self.sched.workers = pricing;
        self.curve_workers = curve;
        self
    }
}
