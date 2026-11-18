[Back to README](../README.md)

# Validation

What is guaranteed, how each guarantee is tested, and what the benchmarks measure.

# Correctness guarantees and how they are tested

161 tests across the workspace. Beyond the unit tests, each crate from `riskrt-feed` onward has an integration test that runs the real threaded pipeline up to that stage and checks it against a single-threaded reference:

| Guarantee | Test |
|---|---|
| The conflator ends on exactly the ticks a single-threaded replay produces, through any channel size and with a slow sink | `riskrt-feed/tests/pipeline.rs` |
| No reader ever sees a half-applied batch; a follower rebuilding state from deltas across skipped versions ends on the right state | `riskrt-market` (unit) and `tests/feed_to_store.rs` |
| Every built curve equals a single-threaded build off the final quotes, byte for byte, including the Jacobian | `riskrt-curves/tests/pipeline.rs` |
| The dependency index and dirty set agree with a brute-force scan over random portfolios and tick sequences | `riskrt-deps/tests/against_brute_force.rs` |
| A failed curve rebuild keeps the old curve, publishes the failure as health, and does not mark the curve changed; an index change with a failing curve leaves it on its old index | `riskrt-curves` (unit) |
| The adapter's PV and DV01 equal direct ferrumquant calls; receivers mirror payers; bonds scale by notional over face; a curve prices with the index it was built on | `riskrt-pricing/tests/against_ferrumquant.rs` |
| Six ways for a pricer to return a bad result (NaN, infinite bucket, short ladder, wrong trade, wrong version, unknown curve) are each rejected by `validate` with the right reason, and the real adapter passes it | `riskrt-pricing/tests/against_ferrumquant.rs` |
| A failing trade is reported in the batch, not requeued, and retried only when its own curve moves; a round where everything fails still produces a batch | `riskrt-sched` (unit) |
| A failed trade keeps its last good numbers, `since` survives a run of failures, recovery clears it, and a never-priced failure is stale but not in the totals | `riskrt-agg` (unit) |
| Every result carries its batch's version, per-trade versions never go backwards, no trade is stale against the final curve set, and every final number equals a direct reprice against that set | `riskrt-sched/tests/pipeline.rs` |
| The final snapshot's total, every book, every curve and every ladder equal a single-threaded sum over the final curve set | `riskrt-agg/tests/pipeline.rs` |
| The whole engine runs to completion with exact results, `stop` ends an endless feed with every thread joining, dropping an engine joins everything, and a poisoned curve goes `Current`, `Failed`, `Current` with exactly its trades reported stale meanwhile | `riskrt-engine/tests/engine.rs` |
| The gRPC bridge delivers the latest version to a slow client and never a backlog; a real client's streamed totals, statuses, health and curve shapes equal the engine's own state | `riskrt-grpc` (unit) and `tests/round_trip.rs` |

`./build.sh flake` runs the threaded crates' tests repeatedly. Each threaded test was also run 10 to 25 times, debug and release, during development. No flakiness was found.

# Performance notes

From the 10-second CLI run quoted in the README (2,000 trades, 3 curves, 6,000 ticks per second total, pricing workers one per core):

```
feeds        59686 ticks sent (5945/s), 0 stalls
conflator    59686 received, 59623 published in 48412 batches (0.1% conflated)
curves       1493 sets from 1522 snapshots, 4474 curve builds, 0 failures
scheduler    1492 batches, 2980605 trades priced (296872/s), max batch 2000, 0 requeued, 0 failures
aggregator   1492 batches applied, 0 stale dropped
```

What those lines mean:

- The conflator shows almost no conflation because its consumer, the quote store publish, takes microseconds. The conflation happened one stage down: 48,412 quote versions became 1,522 snapshots examined by the curve builder.
- 4,474 curve builds for 1,493 sets is three per set. At this tick rate every curve moved between every pair of sets.
- 1,492 pricing batches for 1,493 sets. The scheduler kept up with every curve set, about 6.7 ms per full 2,000-trade reprice including ladders.

Measured costs in ferrumquant on the development machine (2 cores): a 10-pillar bootstrap is about 0.27 ms, the Jacobian on top about 4.5 ms serial or 3.75 ms on two threads, a 30-year swap NPV about 5 µs, and its 10-bucket zero ladder about 110 µs. The Jacobian is the per-tick cost that scales with cores; the ladder is the per-trade cost that scales with workers.

# Benchmarks

Three benches in `crates/riskrt-engine/benches`, run with `./build.sh bench` (or `./build.sh bench quote_to_risk -- --iters 1000`). The first two are criterion microbenchmarks of the two costs that set the engine's ceiling; the third drives the real threaded engine and reports a latency distribution.

| Bench | What it measures |
|---|---|
| `curve_build` | `bootstrap` alone (the kernel floor), then `build_curve` (base bootstrap, 2n bumped bootstraps for the Jacobian, shifted copies) on rayon pools of 1, 2, 4... threads up to the machine's parallelism. This is the per-tick cost that extra cores buy back. |
| `price_batch` | `price_one` for a swap and a bond (NPV, DV01, ten-bucket par ladder), then `price_batch` over the 2,000-trade demo portfolio on pools of 1, 2, 4... threads, as trades per second. Where the throughput stops doubling is where pricing saturates. |
| `quote_to_risk` | One tick is injected into a feed and the bench waits for it to appear in a published `CurveSet` (quote to curve) and then for every trade on that curve to be repriced against that set in a published `RiskSnapshot` (quote to risk). One tick in flight at a time, p50/p90/p95/p99/max over the run. Two scenarios: `idle`, where the other curves are quiet, and `loaded`, where simulated feeds tick the other curves at `--rate` per second each. |

`quote_to_risk` takes `--iters`, `--warmup`, `--trades`, `--curves`, `--rate`, `--workers` and `--curve-workers`, and prints a Markdown row per scenario.

Numbers from the 2-core development container (2,000 trades, 3 curves, 2 pricing workers, 2 curve workers), to show the shape rather than the ceiling. Run it on real hardware for real numbers.

| Microbenchmark | 1 thread | 2 threads |
|---|---|---|
| bootstrap, 10 pillars | 183 µs | |
| build_curve, 10 pillars | 4.31 ms | 2.36 ms |
| price_one, swap | 37 µs | |
| price_one, bond | 3.5 µs | |
| price_batch, 2,000 trades | 58 ms (34k trades/s) | 34 ms (60k trades/s) |

| quote_to_risk | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|
| idle (quote to curve) | 2.4 | 3.7 | 4.9 | 7.8 |
| idle (quote to risk) | 13.3 | 17.4 | 20.5 | 21.0 |
| loaded, 2,000 ticks/s on each other curve (quote to risk) | 103 | 134 | 146 | 155 |

What the shape says. The Jacobian scales with cores (4.3 ms to 2.4 ms on two threads) and is the floor of quote-to-curve. Pricing scales with workers at about 1.75x on two threads, with the remainder being the bootstrap and the aggregator on the same two cores. The loaded scenario is the two pools and the feed threads contending for two cores: the curve builder and the pricing workers each slow the other down, so the one-at-a-time latency is about eight times the idle figure. On a machine with cores to separate the pools this gap is what core pinning should close, and this bench is how to tell whether it did.
