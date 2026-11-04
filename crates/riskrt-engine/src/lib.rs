//! riskrt-engine: wires the seven stages into one running pipeline.
//!
//! ```text
//! feeds ─▶ conflator ─▶ quote store ─▶ curve builder ─▶ scheduler + workers ─▶ aggregator ─▶ snapshots
//! ```
//!
//! `Engine::start` spawns every thread and hands back the two things a caller
//! needs: a `Subscriber<RiskSnapshot>` to read live risk, and the handle to
//! stop the engine. Shutdown is by ownership, stage by stage: stopping the
//! feeds closes the tick channel, which ends the conflator, which closes the
//! quote store, and so on down to the snapshot publisher. `Engine::wait`
//! joins everything in that order and returns every stage's statistics.
//!
//! `demo` builds synthetic portfolios, curve specs and simulated feeds so the
//! CLI, tests and benches share one setup.

#![forbid(unsafe_code)]

pub mod config;
pub mod demo;
pub mod engine;
pub mod error;

pub use config::EngineConfig;
pub use engine::{Engine, EngineStats};
pub use error::EngineError;
