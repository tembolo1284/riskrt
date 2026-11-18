[Back to README](../README.md)

# Workspace layout

Twelve crates, each one pipeline stage or one shared concern. Dependencies point strictly downward in this list.

| Crate | What it holds | Threads |
|---|---|---|
| `riskrt-core` | Plain data: ids, ticks, positions, portfolio, risk numbers. No dependencies, no threads, no I/O. | none |
| `riskrt-feed` | Tick sources (simulated, replayed), the feed thread, the conflator. | 1 per feed + 1 |
| `riskrt-market` | The versioned quote store: immutable `QuoteSnapshot`s published through `ArcSwap`. | none (library) |
| `riskrt-curves` | Curve specs, the bootstrap with par-quote Jacobian, the `CurveSet` bundle, the curve builder thread, and a generic versioned publisher. | 1 + rayon pool |
| `riskrt-deps` | Curve-to-trades index and the dirty set. | none |
| `riskrt-pricing` | The `Pricer` trait and the ferrumquant adapter. | none |
| `riskrt-sched` | The scheduler thread and the pricing worker pool. | 1 + rayon pool |
| `riskrt-agg` | The per-trade result table and the published `RiskSnapshot`. | 1 |
| `riskrt-engine` | Wires every stage, owns shutdown, provides synthetic demo inputs, re-exports the reader types. | none of its own |
| `riskrt-proto` | The protobuf schema, generated tonic code, and conversions to and from core types. | none |
| `riskrt-grpc` | A gRPC server over the engine's read handles: describe, positions, streaming risk, curves and quotes, on-demand pricing. | 1 + tokio runtime |
| `riskrt-cli` | The `riskrt` binary: runs the engine on simulated feeds, prints live risk, optionally serves gRPC. | main |

```
riskrt/
├── Cargo.toml
├── rust-toolchain.toml
├── build.sh
├── .github/workflows/ci.yml
└── crates/
    ├── riskrt-core/     src/{lib,ids,tick,position,portfolio,risk,error}.rs
    ├── riskrt-feed/     src/{lib,source,sim,replay,conflate}.rs      tests/pipeline.rs
    ├── riskrt-market/   src/{lib,snapshot,store}.rs                  tests/feed_to_store.rs
    ├── riskrt-curves/   src/{lib,spec,build,set,publish,builder,error}.rs   tests/pipeline.rs
    ├── riskrt-deps/     src/{lib,index,dirty}.rs                     tests/against_brute_force.rs
    ├── riskrt-pricing/  src/{lib,pricer,fq_adapter,error}.rs         tests/against_ferrumquant.rs
    ├── riskrt-sched/    src/{lib,scheduler,worker}.rs                tests/pipeline.rs
    ├── riskrt-agg/      src/{lib,snapshot,aggregator}.rs             tests/pipeline.rs
    ├── riskrt-engine/   src/{lib,config,engine,demo,error}.rs        tests/engine.rs
    │                    benches/{curve_build,price_batch,quote_to_risk}.rs
    ├── riskrt-proto/    proto/riskrt/v1/risk.proto  src/{lib,convert}.rs  build.rs
    ├── riskrt-grpc/     src/{lib,service,bridge}.rs                  tests/round_trip.rs
    └── riskrt-cli/      src/main.rs
```

The engine itself depends on four crates: `crossbeam` (bounded channels), `arc-swap` (lock-free snapshot publication), `rayon` (work-stealing pools) and `clap` (the CLI), plus `ctrlc` for the CLI's signal handler. The network edge adds `tonic`, `prost`, `tokio` and `protox` (a pure-Rust protobuf compiler, so no system `protoc` is needed). The pricing kernel, ferrumquant, is a path dependency expected at `../ferrumquant`.

## File naming in review batches

Files handed over for review follow one convention so they can be dropped into place without a tree: `crate__file.rs` is `crates/crate/src/file.rs`, `crate__tests__file.rs` is `crates/crate/tests/file.rs`, `crate__benches__file.rs` is `crates/crate/benches/file.rs`, `crate__Cargo.toml` is `crates/crate/Cargo.toml`, and `riskrt-proto__risk.proto` is `crates/riskrt-proto/proto/riskrt/v1/risk.proto`. A single underscore is part of the file name; a double underscore separates path parts.
