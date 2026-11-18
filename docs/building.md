[Back to README](../README.md)

# Building and running

Requirements: Rust 1.85 or later (edition 2024), and ferrumquant checked out beside this repository.

```
git clone https://github.com/tembolo1284/ferrumquant ../ferrumquant
./build.sh check
```

`build.sh` is the one entry point for building and verifying, and it is what CI runs:

| Command | What it does |
|---|---|
| `./build.sh build` | `cargo build --workspace --all-targets` |
| `./build.sh test` | build, then `cargo test --workspace` (161 tests) |
| `./build.sh check` | the full gate: `fmt --check`, build, test, `clippy -D warnings` |
| `./build.sh flake [N]` | run the threaded crates' tests N times (default 5), stop on the first failure |
| `./build.sh run [args]` | run the CLI in release mode |
| `./build.sh bench [name]` | the benchmarks in [validation.md](validation.md#benchmarks); no name runs all three |
| `./build.sh fmt` | format in place |

It refuses to start if ferrumquant is missing and says where to clone it. `FERRUMQUANT_DIR` overrides the location.

CI (`.github/workflows/ci.yml`) checks out ferrumquant beside riskrt and runs `./build.sh check` on every push and pull request. ferrumquant is deliberately not pinned to a revision while both repositories are moving: if a ferrumquant change breaks riskrt, CI says so the same day, and the fix is normally a few lines in `fq_adapter.rs` or `build.rs`, the only two files that know ferrumquant's shape. Pinning is a five-line change when the kernel settles.

Run the engine on simulated feeds:

```
./build.sh run --trades 2000 --curves 3 --seconds 10
```

Add `--grpc 127.0.0.1:50051` to serve the same live risk over gRPC while it runs. Ctrl-C stops the feeds and lets every stage drain and join, so the statistics block still prints.

Every half second it prints the latest snapshot: portfolio PV and DV01, the same by book and by curve, and each curve's par ladder (PV change per basis point on each input quote). When the run ends it prints every stage's statistics.

Useful flags:

| Flag | Default | Meaning |
|---|---|---|
| `--trades` | 2000 | Size of the synthetic portfolio |
| `--curves` | 3 | Number of curves, one simulated feed each |
| `--books` | 4 | Number of books to roll up by |
| `--workers` | 0 (one per core) | Pricing worker threads |
| `--curve-workers` | 2 | Threads for the bumped bootstraps |
| `--rate` | 2000 | Ticks per second per feed, 0 for unpaced |
| `--seconds` | 10 | Run length, 0 to run until the feeds end |
| `--ticks` | 0 (endless) | Ticks per feed before it ends |
| `--seed` | 42 | Seed for the portfolio and the feeds |
| `--refresh` | 500 | Screen refresh interval in milliseconds |
| `--grpc` | off | Serve live risk over gRPC on this address |

Each refresh line reads `priced P/T  behind B  failed F`. "Behind" counts trades whose numbers cannot be trusted against the current curve set: their curve has been rebuilt since they were priced, their curve is degraded (see [Failure handling](design.md#failure-handling)), or their own last pricing round failed. At high tick rates every curve moves between one pricing round and the next, so the first kind alone makes this read as the whole portfolio, which means the engine is at most one curve set behind the market. It reads zero when the feed is slower than a pricing round. "Failed" counts trades whose latest pricing round failed; it should be zero, and when it is not the line below names the first one and its error. The per-curve lines carry the curve's health (`Current`, `Failed`, `IndexStale`, `Unbuilt`) and its last error if any.
