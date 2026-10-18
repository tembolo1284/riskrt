//! riskrt-curves: turns quote snapshots into priced-ready curve sets.
//!
//! ```text
//! MarketReader ─▶ CurveBuilder ─▶ Publisher<CurveSet> ─▶ Subscriber<CurveSet>
//!  (quotes)        (one thread,     (ArcSwap)              (any number of
//!                   fans out on                             pricing threads)
//!                   rayon)
//! ```
//!
//! - Only curves whose quotes moved are rebuilt; the rest are carried into the
//!   next `CurveSet` by `Arc` clone.
//! - One rebuild is 2n + 1 bootstraps (base plus the par-quote Jacobian). The
//!   2n bumped bootstraps are independent and run in parallel.
//! - The parallel and per-node shifted curves are built once per rebuild and
//!   shared by every position, instead of once per position.
//! - A `CurveSet` bundles the curves, their Jacobians and the fixing index
//!   under one version, so a pricing thread can never mix a curve from one
//!   tick with a Jacobian or fixings from another.
//!
//! This is the first crate that depends on ferrumquant (`fq-core`, `fq-rates`).

#![forbid(unsafe_code)]

pub mod build;
pub mod builder;
pub mod error;
pub mod publish;
pub mod set;
pub mod spec;

pub use build::{BP, BuildConfig, BuiltCurve, build_curve};
pub use builder::{BuildStats, Control, CurveBuilder, spawn_curve_builder};
pub use error::CurveBuildError;
pub use publish::{Publisher, Subscriber, Versioned, Wait};
pub use set::CurveSet;
pub use spec::{CurveSetSpec, CurveSpec, Pillar};
