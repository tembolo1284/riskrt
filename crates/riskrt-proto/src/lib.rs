//! riskrt-proto: the gRPC wire types and their conversions to riskrt-core.
//!
//! Generated code lives in `v1`. Everything else in the workspace talks in
//! riskrt-core types and goes through the `From` impls in `convert`, so no
//! other crate touches a prost struct directly.

#![forbid(unsafe_code)]

pub mod v1 {
    tonic::include_proto!("riskrt.v1");
}

pub mod convert;

pub use v1::risk_client::RiskClient;
pub use v1::risk_server::{Risk, RiskServer};
