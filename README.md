# riskrt

A multithreaded real-time portfolio risk engine in Rust. It turns streaming rate quotes into versioned yield curves and produces incremental PV, DV01 and par-quote bucket ladders for a portfolio of swaps and bonds, with [ferrumquant](https://github.com/tembolo1284/ferrumquant) as the pricing kernel.

The problem it solves is the one every rates desk has. Quotes tick thousands of times a second, every tick on a curve dirties every trade on that curve, and the desk wants to see the current risk, not a backlog of stale risk. riskrt is built so that each stage keeps up with the one below it by conflating work rather than queueing it, and so that a reader can never observe a half-applied update.

On a 10-second run with 2,000 trades on 3 curves and 6,000 ticks per second, the engine produced 1,492 full portfolio reprices (about 149 per second, 2.98 million trade prices with ladders) with zero failures and the risk snapshot never more than one curve set behind the market.

When something does fail, it is visible rather than silent. Every curve carries a health status and every trade carries a pricing status, both of them in the snapshot, over gRPC and on the CLI. A curve that cannot be rebuilt or a trade that cannot be priced keeps its last good numbers and is flagged, so a reader always knows which numbers reflect the market and which have stopped updating.

## Pipeline

```
feed threads ──channel──▶ conflator ──▶ quote store ──▶ curve builder ──▶ scheduler + workers ──channel──▶ aggregator ──▶ risk snapshots ──▶ gRPC server
 (N threads)              (1 thread)    (ArcSwap)       (1 thread +        (1 thread +                       (1 thread)     (ArcSwap)         (optional)
                                                         rayon pool)        rayon pool)
```

Data flows left to right. Two boundaries are bounded channels (backpressure); the rest are immutable versioned snapshots behind an `ArcSwap` that a slow consumer skips through to the latest (conflation). Every number carries the `MarketVersion` it was computed against. Shutdown runs the same direction: closing the feeds drains every stage in order with no stop flags.

| Crate | Role |
|---|---|
| `riskrt-core` | Plain data: ids, ticks, positions, portfolio, risk numbers |
| `riskrt-feed` | Tick sources, feed threads, the conflator |
| `riskrt-market` | The versioned quote store |
| `riskrt-curves` | Bootstrap with par-quote Jacobian, `CurveSet` with per-curve health, the curve builder |
| `riskrt-deps` | Curve-to-trades index and dirty set |
| `riskrt-pricing` | The `Pricer` trait, result validation, the ferrumquant adapter |
| `riskrt-sched` | Scheduler and pricing worker pool |
| `riskrt-agg` | Per-trade result table, `RiskSnapshot` with per-trade status |
| `riskrt-engine` | Wires the stages, owns shutdown, demo inputs, benches |
| `riskrt-proto`, `riskrt-grpc` | Protobuf schema and a gRPC server streaming risk, curves and quotes |
| `riskrt-cli` | The `riskrt` binary |

## Quick start

Rust 1.85 or later, and ferrumquant checked out beside this repository.

```
git clone https://github.com/tembolo1284/ferrumquant ../ferrumquant
./build.sh check                                   # fmt, build, test, clippy -D warnings
./build.sh run --trades 2000 --curves 3 --seconds 10
./build.sh run --grpc 127.0.0.1:50051              # same, served over gRPC
./build.sh bench quote_to_risk -- --iters 1000     # tick-to-risk latency percentiles
```

## Documentation

| Document | Read it for |
|---|---|
| [docs/design.md](docs/design.md) | Each stage in detail, failure handling, the architectural decisions and the threading model |
| [docs/layout.md](docs/layout.md) | The workspace tree, dependencies, where things go |
| [docs/building.md](docs/building.md) | `build.sh`, CLI flags and what the output means, CI, the ferrumquant path dependency |
| [docs/validation.md](docs/validation.md) | Every correctness guarantee and its test, the flake loop, benchmarks and measured numbers |
| [docs/finance.md](docs/finance.md) | What is priced and how, risk conventions, what riskrt asked of ferrumquant, the commodities slice |

## Status

Done: the full pipeline with conflation and versioning, par-quote bucket risk through a Jacobian, per-curve health and per-trade status with failure injection tests, gRPC streaming of risk, curve shapes and quotes, CI, and criterion plus end-to-end latency benches. 161 tests.

Known limitations: one valuation date per session, an immutable portfolio per session, single-curve swaps only, no core pinning yet, and ferrumquant unpinned by choice while both repositories move.

Next: a commodities slice on ferrumquant's `fq-commod-core`, `fq-gas` and `fq-oil`; core pinning judged by the `quote_to_risk` bench; `riskrt-scenario` for whole-portfolio shifts and twists.
