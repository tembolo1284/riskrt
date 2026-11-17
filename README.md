# riskrt
 
A multithreaded real-time portfolio risk engine in Rust. It turns streaming rate quotes into versioned yield curves and produces incremental PV, DV01 and par-quote bucket ladders for a portfolio of swaps and bonds, with [ferrumquant](https://github.com/tembolo1284/ferrumquant) as the pricing kernel.
 
The problem it solves is the one every rates desk has. Quotes tick thousands of times a second, every tick on a curve dirties every trade on that curve, and the desk wants to see the current risk, not a backlog of stale risk. riskrt is built so that each stage keeps up with the one below it by conflating work rather than queueing it, and so that a reader can never observe a half-applied update.
 
On a 10-second run with 2,000 trades on 3 curves and 6,000 ticks per second, the engine produced 1,492 full portfolio reprices (about 149 per second, 2.98 million trade prices with ladders) with zero failures and the risk snapshot never more than one curve set behind the market.
 
When something does fail, it is visible rather than silent. Every curve carries a health status and every trade carries a pricing status, both of them in the snapshot, over gRPC and on the CLI. A curve that cannot be rebuilt or a trade that cannot be priced keeps its last good numbers and is flagged, so a reader always knows which numbers reflect the market and which have stopped updating.
 
## Contents
 
1. [Pipeline overview](#pipeline-overview)
2. [Workspace layout](#workspace-layout)
3. [Building and running](#building-and-running)
4. [Stage by stage](#stage-by-stage)
5. [Failure handling](#failure-handling)
6. [Architectural decisions](#architectural-decisions)
7. [Threading model](#threading-model)
8. [Correctness guarantees and how they are tested](#correctness-guarantees-and-how-they-are-tested)
9. [Performance notes](#performance-notes)
10. [Benchmarks](#benchmarks)
11. [Integration with ferrumquant](#integration-with-ferrumquant)
12. [Known limitations and what comes next](#known-limitations-and-what-comes-next)
## Pipeline overview
 
```
feed threads ──channel──▶ conflator ──▶ quote store ──▶ curve builder ──▶ scheduler + workers ──channel──▶ aggregator ──▶ risk snapshots ──▶ gRPC server
 (N threads)              (1 thread)    (ArcSwap)       (1 thread +        (1 thread +                       (1 thread)     (ArcSwap)         (optional)
                                                         rayon pool)        rayon pool)
```
 
Data flows left to right. Each arrow is one of two things: a bounded channel, where the producer blocks if the consumer falls behind, or a versioned snapshot behind an `ArcSwap`, where the consumer wakes up, takes the latest version and skips whatever it missed. The first kind gives backpressure. The second kind gives conflation. Two boundaries are channels (feeds to conflator, scheduler to aggregator); the rest are snapshots. Which one each boundary uses is a deliberate choice, explained under [Architectural decisions](#architectural-decisions). The gRPC server is one more reader of the snapshots and feeds nothing back.
 
Shutdown runs the same direction. Stopping the feeds closes the tick channel, which ends the conflator, which closes the quote store, which ends the curve builder, and so on until the snapshot publisher closes. No stage needs a stop flag of its own. It exits when its input is exhausted.
 
## Workspace layout
 
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
| `riskrt-grpc` | A gRPC server over the engine's read handles: describe, positions, streaming risk and quotes, on-demand pricing. | 1 + tokio runtime |
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
 
## Building and running
 
Requirements: Rust 1.85 or later (edition 2024), and ferrumquant checked out beside this repository.
 
```
git clone https://github.com/tembolo1284/ferrumquant ../ferrumquant
./build.sh check
```
 
`build.sh` is the one entry point for building and verifying, and it is what CI runs:
 
| Command | What it does |
|---|---|
| `./build.sh build` | `cargo build --workspace --all-targets` |
| `./build.sh test` | build, then `cargo test --workspace` (159 tests) |
| `./build.sh check` | the full gate: `fmt --check`, build, test, `clippy -D warnings` |
| `./build.sh flake [N]` | run the threaded crates' tests N times (default 5), stop on the first failure |
| `./build.sh run [args]` | run the CLI in release mode |
| `./build.sh bench [name]` | the benchmarks below; no name runs all three |
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
 
Each refresh line reads `priced P/T  behind B  failed F`. "Behind" counts trades whose numbers cannot be trusted against the current curve set: their curve has been rebuilt since they were priced, their curve is degraded (see [Failure handling](#failure-handling)), or their own last pricing round failed. At high tick rates every curve moves between one pricing round and the next, so the first kind alone makes this read as the whole portfolio, which means the engine is at most one curve set behind the market. It reads zero when the feed is slower than a pricing round. "Failed" counts trades whose latest pricing round failed; it should be zero, and when it is not the line below names the first one and its error. The per-curve lines carry the curve's health (`Current`, `Failed`, `IndexStale`, `Unbuilt`) and its last error if any.
 
## Stage by stage
 
### riskrt-core
 
Everything downstream shares these types, so the rules are strict: no dependencies, no threads, every type `Send + Sync`, and `Copy` wherever a value crosses a queue.
 
Identifiers (`CurveId`, `QuoteId`, `TradeId`, `BookId`) are dense `u32`s. Names like "USD-SOFR" or an external trade id are interned once at load time. On the hot path every lookup is a `Vec` index, with no hashing and no string compares.
 
`Tick` is 32 bytes with no pointers: a per-feed sequence number, a timestamp, a value and a `QuoteId`. Two fit in a cache line. Static facts about a quote (its tenor, which curve it feeds) live in reference data, not in the tick.
 
`Position` is economic terms only: book, discount and forward curve ids, signed notional, and a `Product` (a vanilla swap or a fixed-coupon bond). Core knows nothing about how to price it. `Portfolio` is immutable once built, and a `TradeId` is its index into the portfolio.
 
`Risk` is the additive pair (PV, DV01). `TradeRisk` is a `Risk` tagged with a `TradeId` and the `MarketVersion` it was priced against, 32 bytes and `Copy`. `Ladder` carries the per-pillar par-quote risk on one curve, and `PricedTrade` is a `TradeRisk` plus its `Ladder`.
 
`MarketVersion` is a monotonic `u64`. Every published snapshot carries one, every risk number is tagged with the version it was computed against, and that tag is what makes consistency checkable all the way through the pipeline.
 
### riskrt-feed
 
A `TickSource` is anything that yields ticks in sequence order. Two are provided. `SimSource` is a seeded random walk over a block of quotes, so the same seed gives the same tick stream every time. `ReplaySource` reads a `seq,ts_ns,quote,value` file and fails up front on a bad line, before any thread starts.
 
`spawn_feed` runs one source on its own named thread and pushes into a bounded crossbeam channel shared by all feeds. A stop flag ends the thread between ticks, and dropping the sender on exit is what lets the conflator know when every feed is finished.
 
The conflator keeps a dense latest-tick table indexed by `QuoteId` and the list of quotes that changed since the last drain. Each cycle it blocks for one tick, takes whatever else is already queued, and hands its sink one batch holding only the latest tick per changed quote. A tick whose sequence number is not newer than the one already held for that quote is rejected as stale, even after a drain, so out-of-order delivery cannot move a quote backwards.
 
One contract matters here: each quote belongs to exactly one feed, because sequence numbers are only comparable within a feed.
 
### riskrt-market
 
`QuoteSnapshot` is the state of every quote at one `MarketVersion`. It is never mutated. Applying a batch builds the next snapshot by copying the table (40 bytes per quote) and writing the changes, then swaps a pointer. A reader therefore sees either the whole batch or none of it.
 
Each quote also records the version at which it last changed. `changed_since(seen)` returns every quote that moved after version `seen`, which is correct even when the caller skipped versions in between. That is the normal case for a consumer slower than the publisher, and it is how the curve builder decides what to rebuild without ever seeing the intermediate snapshots.
 
`MarketWriter` is deliberately not `Clone`. With exactly one writer, publishing is a plain read-copy-update with no compare-and-swap retry loop. `MarketReader::load()` is an `Arc` clone and never takes a lock. `wait_newer` lets a consumer sleep until there is a newer version, using a mutex and condvar that the writer touches briefly on each publish and that `load()` never touches, so pricing threads stay wait-free.
 
### riskrt-curves
 
`CurveSetSpec` is the reference data: which quotes feed which curve, validated once at start-up (dense curve ids, no quote feeding two curves, no duplicate pillar). Each `Pillar` carries a ferrumquant `InstrumentKey`, so a curve can mix deposits, SOFR futures and OIS.
 
`build_curve` does one curve's work for one quote snapshot: the base bootstrap, then the par-quote Jacobian (bump each input quote both ways and re-bootstrap), then the parallel-shifted and per-node-shifted copies of the curve. For n pillars that is 2n + 1 bootstraps. The 2n bumped ones are independent and run in parallel on rayon. The result does not depend on the thread count, which a test enforces by comparing a 1-thread build with an 8-thread build byte for byte.
 
The pre-shifted curves are the key to cheap per-trade risk. A trade's DV01 is two prices against curves built once per tick and shared by every position. Its zero-rate ladder is 2n prices against the per-node-shifted curves, also shared. Its par ladder is that zero ladder multiplied through the Jacobian. No curve is ever rebuilt for a single position.
 
`CurveSet` bundles every built curve, their Jacobians and the fixing index under one version. Rebuilding one curve produces a new set that shares the untouched curves by `Arc`, so publishing costs a small vector of pointers. It records the version at which each curve was last rebuilt, giving the same `changed_since` as the quote store one level up. Alongside each curve it keeps a `CurveHealth`: the quote version the curve was built from, the latest version a rebuild was attempted at, when the current run of failures began, the last error, and whether the curve is on the set's current fixing index. `health(id).status()` folds that into `Unbuilt`, `Current`, `Failed` or `IndexStale`.
 
Each `BuiltCurve` also carries the `OvernightIndex` it was calibrated with. The pricer uses the curve's index, not the set's, so a curve that stays on yesterday's fixings while others move prices consistently with itself and is reported `IndexStale` rather than silently mixing inputs.
 
`CurveBuilder` decides what to rebuild: exactly the curves with a pillar quote newer than the last snapshot it handled, in parallel. A curve whose rebuild fails keeps its previous version, is published as failed in a new set version (without being marked changed, so nothing reprices for no reason), and is retried when one of its quotes next moves. A new fixing index (the daily SOFR print) forces every curve to rebuild.
 
`publish.rs` is the single-writer, many-reader publisher from the quote store made generic over any `Versioned` value. The curve builder and the aggregator both use it.
 
### riskrt-deps
 
`CurveIndex` is the inverted dependency map from curve to trades, stored as compressed sparse rows: one flat array of trade ids grouped by curve and an offsets array. A lookup is two array reads and returns a contiguous slice sorted by trade id. It is built once from the portfolio and fails at start-up if any trade references a curve the engine does not build, so a mis-mapped trade cannot sit in the portfolio and silently never reprice.
 
`DirtySet` is the set of trades waiting to be priced: a flag per trade for O(1) idempotent marking, plus a list of marked ids so a drain costs the number of dirty trades rather than the size of the portfolio. Draining returns trades in ascending id order regardless of which curve ticked first, which keeps the batches handed to workers deterministic.
 
Both structures are checked against a brute-force scan of random portfolios, including the case where several curve updates arrive before a drain.
 
### riskrt-pricing
 
`Pricer` is the only thing the scheduler and workers see: `price(&self, TradeId, &CurveSet) -> PricedTrade`. It is `Send + Sync` and prices through `&self`, so one instance is shared by every worker.
 
`FqPricer` is the ferrumquant adapter. At load time it turns each `Position` into an `OisSwap` or a `FixedRateBond` once. On each tick it prices against the pinned `CurveSet`: one NPV, one DV01 from the pre-shifted pair, and one par ladder from the per-node pair and the Jacobian. Every number is per basis point and tagged with the set version.
 
`price_batch` fans a list of trades out over rayon and returns one result per input in the same order. A failure for one trade does not affect the others. Every result passes `validate` before it leaves the crate: it must be for the trade asked, tagged with the set's version, on a curve the set has built, with a ladder of that curve's bucket count, and every number finite. This is the contract any `Pricer` is held to, so a bad adapter produces a failed trade rather than a corrupted total.
 
### riskrt-sched
 
The scheduler thread owns the `DirtySet`. On each new curve set it marks the trades on curves that changed since the last set it handled, drains them as one sorted batch, pins that set with an `Arc`, and prices the batch on a dedicated rayon pool. The output is a `PricedBatch` whose every result carries the batch's set version, sent down a small bounded channel.
 
If curve sets arrive faster than batches finish, the scheduler skips to the latest one. `changed_since` across the skipped versions still marks every affected trade exactly once. A trade whose curve is not built yet is requeued rather than failed and goes out when the curve appears. Any other failure travels in the batch's `failed` list with its error, so a round where every trade fails still produces a batch and the failure is never silent. A failed trade is retried when its curve next moves.
 
### riskrt-agg
 
The aggregator keeps the latest result per trade behind `Arc`, so building a snapshot copies pointers rather than ladders. Each batch replaces the entries it carries and produces a new immutable `RiskSnapshot` with the portfolio total, totals by book and by curve, and a summed par ladder per curve.
 
Totals are recomputed from the per-trade table on every batch rather than updated incrementally. It is O(trades) per batch, which is trivial, and in return a snapshot's numbers never depend on the history of batches that led to it and never accumulate floating-point drift. A test applies the same trades in two different batch orders and checks the totals are identical.
 
A result for a trade that already holds a newer version is dropped. The scheduler never sends one, but a replayed batch cannot move a trade backwards.
 
Beside its numbers each trade has a `TradeStatus`: `Unpriced`, `Priced`, or `Failed { since, error }`. A failed trade keeps its last good result in the table and in the totals, and `since` is preserved across a run of failures so a reader can tell a blip from a trade that has been broken since the open. `failed_count()`, `failed()` and `is_healthy()` summarise, and `stale_against(set)` counts failed trades along with those on moved or degraded curves.
 
### riskrt-engine and riskrt-cli
 
`Engine::start(config, feeds)` spawns every stage in reverse order, so nothing publishes before someone is listening, and returns handles for live snapshots, curve sets, quotes and the pricer. If any stage fails to start, the ones already running are stopped and joined before the error is returned. `stop()` flags the feeds. `wait()` joins every stage in pipeline order and returns all their statistics, and dropping an `Engine` without calling it does the same, so no thread outlives its owner. `demo` builds synthetic portfolios, curve specs and simulated feeds so the CLI, the tests and the benches share one setup. The crate re-exports `RiskSnapshot`, `TradeStatus`, `CurveSet`, `CurveHealth`, `CurveStatus`, `Subscriber` and `Wait`, so a reader needs only `riskrt_engine`.
 
The CLI runs the engine on paced or unpaced simulated feeds, prints the live snapshot on a timer, and prints the statistics block at the end. With `--grpc` it also starts the server below.
 
### riskrt-proto and riskrt-grpc
 
`riskrt-proto` holds `risk.proto` and the generated tonic code, compiled at build time with protox. The `Risk` service has five RPCs: `Describe` (valuation date, counts, curve and pillar reference data), `Positions` (a stream of the portfolio), `SubscribeRisk` (a stream of `RiskUpdate`), `SubscribeQuotes` (a stream of changed quotes) and `Price` (price one trade now against the current set). A `RiskUpdate` carries the totals, by-book and by-curve numbers, the ladders, `priced_count`, `stale_count`, `failed_count`, one `CurveHealth` per curve, and the per-trade rows when asked for. Failed trades are always sent, even without `include_trades`, so a client cannot miss them.
 
`riskrt-grpc` is one more reader of the engine's handles and feeds nothing back. The tokio runtime lives on its own thread, so the engine and the CLI stay synchronous. Each streaming RPC gets a small bridge thread that blocks on `wait_newer` like an in-process stage and forwards into a tokio channel of capacity one. If the client is slower than the engine, the bridge overwrites the pending update rather than queueing, so the client always gets the latest version and never a backlog. The round-trip test starts a real engine and a real server, connects a real client, and checks that the streamed totals equal the engine's own snapshot.
 
## Failure handling
 
The rule everywhere is the same: keep the last good value, publish the failure as data, never block the pipeline and never hide it.
 
| Failure | What happens | Where it shows |
|---|---|---|
| A pillar quote has no value yet | The curve is `Unbuilt`; trades on it wait in the dirty set, not an error | curve health, `requeued` stat |
| A curve fails to bootstrap (an absurd quote, say) | The previous curve stays in the set, a new set version is published carrying the failure, nothing is repriced for it | `CurveStatus::Failed` with `failed_since` and the error; every trade on it counted in `stale_count` |
| The fixing index changes but one curve cannot rebuild on it | That curve keeps its old index and prices consistently with itself | `CurveStatus::IndexStale` |
| A trade fails to price, or the pricer returns a bad result (NaN, short ladder, wrong curve) | `validate` rejects it; the trade keeps its last good numbers in the totals and is retried when its curve moves | `TradeStatus::Failed` with `since` and the error, `failed_count`, `stale_count` |
| A client reads slower than the engine publishes | It receives the latest version and skips the rest | `version` gaps on the stream |
| The process is interrupted | Ctrl-C stops the feeds; every stage drains and joins in order | the statistics block still prints |
 
An engine-level test injects a 5000% quote into one curve's 30-year pillar, watches that curve go `Current`, `Failed`, `Current` while the other curve keeps updating, checks that exactly the trades on the failed curve are reported stale while it is failed, and that the final state is healthy with nothing stale.
 
## Architectural decisions
 
### Immutable versioned snapshots instead of shared mutable state
 
Quotes, curve sets and risk snapshots are all immutable values published by swapping a pointer. Nothing downstream ever holds a lock on live data. The costs are a copy per publish (40 bytes per quote for the quote table, a vector of `Arc`s for a curve set or a risk snapshot) and some garbage when old versions are dropped. The benefits are that a reader can never observe a torn update, that a worker can pin one version for as long as a batch takes while the publisher moves on, and that every number can be tagged with the version it came from. The torn-read test in `riskrt-market` writes 20,000 versions where every quote carries the version number while four readers spin, and asserts that no reader ever sees two different values in one snapshot.
 
### Conflation at the snapshot boundaries, backpressure at the two channels
 
The feed-to-conflator channel is bounded, so a stalled conflator slows the feeds rather than letting memory grow. The scheduler-to-aggregator channel is bounded too (four batches by default): a batch is the result of real work and must not be dropped, so if the aggregator falls behind the scheduler waits, and the curve sets it would have handled meanwhile are conflated at its own input. Every other boundary is a versioned snapshot, so a slow consumer skips to the latest version. This gives a property that matters on a real desk: the risk you see is always computed from the newest market data the pipeline has reached, never from a queue of old data being worked through. The 10-second run above shows it in the statistics. Quotes went through 48,000 versions, the curve builder examined 1,500 of them, and the scheduler priced against 1,492 curve sets.
 
### Dirty tracking by "changed since version", not by event
 
Each store records, per item, the version at which it last changed. A consumer remembers the last version it handled and asks for everything that changed after it. This works regardless of how many versions the consumer skipped, which an event queue would not, and it costs one comparison per item. The same mechanism is used three times: quotes to curves, curves to trades, and (through `stale_against`) snapshots to the current curve set for the "behind" figure.
 
### Pre-shifted curves shared across positions
 
The naive way to get a bucket ladder is to shift the curve, reprice, and repeat per bucket per position. With n pillars and m positions that is 2nm curve copies per tick. Building the shifted curves once per tick and sharing them makes it 2n copies and 2nm prices. Price is the operation that is cheap in ferrumquant, since a swap NPV is about 60 curve lookups after the telescoping fast path, so this is the right side of the trade.
 
### Par-quote risk through a Jacobian, not re-bootstrapping
 
A desk hedges with the instruments it quoted, so bucketed risk should be per input quote, not per zero-rate node. Getting that by bumping each quote and re-bootstrapping per position would cost 2n bootstraps per position per tick. Instead the curve builder computes the Jacobian of node zero rates with respect to input quotes once per curve rebuild (2n bootstraps per curve per tick, run in parallel), and each position's zero ladder is multiplied through it. The test in `riskrt-curves` compares the Jacobian ladder with direct bump-and-rebootstrap per bucket and agrees to within 0.01 per basis point on a 16,000 per basis point bucket.
 
### Single-owner stages, parallelism only inside a stage
 
Every stage thread owns its state outright: the conflator owns its table, the curve builder its builder, the scheduler its dirty set, the aggregator its result table. Nothing is shared mutably between threads. Parallelism happens inside a stage through rayon, on work that is independent by construction: the bumped bootstraps of one curve, the trades of one batch. That keeps the concurrency reasoning local. The only synchronisation primitives in the whole engine are the bounded channels, the `ArcSwap`s, and one mutex-and-condvar pair per publisher used solely for sleeping.
 
### Determinism as a tested property
 
Every parallel computation is required to give the same bytes on one thread as on many. Curve builds, batch pricing and scheduler rounds all have tests that run them on different pool sizes and compare results with `assert_eq`. This makes the parallel engine checkable against a single-threaded reference, and it makes production discrepancies reproducible.
 
### The pricing library behind one trait, with one honest caveat
 
Instruments and the bootstrap live in two files: `riskrt-pricing/src/fq_adapter.rs` and `riskrt-curves/src/build.rs`. Core, feed, market, deps, sched and agg never see an instrument type, and the engine was developed against a stand-in pricer before ferrumquant was ready. The caveat is that the kernel is not fully hidden: `Pricer::price` takes a `&CurveSet`, and a `BuiltCurve` inside it is a ferrumquant `DiscountCurve` with its Jacobian and index. That is deliberate. The pre-shifted curves and the Jacobian are what make per-trade risk cheap, and they only make sense as the kernel's own types. So the boundary is: the pipeline crates never construct or inspect kernel types, and a second `Pricer` (an options kernel, or ferrumquant's commodities crates when they land) is added as one more adapter plus a `Product` variant, with the `validate` contract already defining what it must return. Swapping the curve representation itself would touch `riskrt-curves` as well as the adapter.
 
### One writer per store, enforced by the type system
 
`MarketWriter` and `Publisher` are not `Clone` and publish through `&mut self`. A second writer is a compile error. This is what lets publishing be a plain store rather than a compare-and-swap loop, and it is why every version number in the system is strictly increasing.
 
## Threading model
 
For a run with F feeds, W pricing workers and C curve workers, the engine has F + 4 threads of its own plus two rayon pools. With `--grpc`, add one thread for the tokio runtime and one bridge thread per open streaming RPC:
 
| Thread | Owns | Wakes on |
|---|---|---|
| `feed-N` (F of them) | one `TickSource` | its own pacing, or immediately |
| `conflator` | latest-tick table | a tick on the channel |
| `curve-builder` | `CurveBuilder` | a newer quote version, or a control message |
| `scheduler` | `DirtySet` | a newer curve set |
| `aggregator` | result table | a batch on the channel |
| `curve-N` pool (C) | nothing | bumped bootstraps from the builder |
| `pricer-N` pool (W) | nothing | trades from the scheduler |
| `grpc` (optional) | the tokio runtime | client connections |
| `grpc-risk` / `grpc-quotes` (one per subscriber) | nothing | a newer snapshot, forwarded to the client |
 
The two rayon pools are separate on purpose, so that curve builds (a few milliseconds each, bursty) do not compete with pricing workers for the same threads. `--curve-workers 0` folds curve builds onto rayon's global pool instead.
 
Core pinning is not implemented yet. The right place for it is the pools' `start_handler`, and the decision waits on the benchmarks.

## Correctness guarantees and how they are tested
 
159 tests across the workspace. Beyond the unit tests, each crate from `riskrt-feed` onward has an integration test that runs the real threaded pipeline up to that stage and checks it against a single-threaded reference:
 
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
| The gRPC bridge delivers the latest version to a slow client and never a backlog; a real client's streamed totals, statuses and health equal the engine's snapshot | `riskrt-grpc` (unit) and `tests/round_trip.rs` |
 
`./build.sh flake` runs the threaded crates' tests repeatedly. Each threaded test was also run 10 to 25 times, debug and release, during development. No flakiness was found.
 
## Performance notes
 
From the 10-second CLI run quoted at the top (2,000 trades, 3 curves, 6,000 ticks per second total, pricing workers one per core):
 
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
 
## Benchmarks
 
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
 
## Integration with ferrumquant
 
riskrt drove several changes in ferrumquant, all made so the library could serve a real-time engine:
 
- A telescoping fast path in `OvernightIndex::compounded_factor`. For plain compounding over a future period with no stored fixings, the daily product collapses to `DF(start) / DF(end)`. A 30-year swap NPV went from about 15,000 calendar steps to about 60 curve lookups.
- `OisSwap` caches its period rows at construction, so valuation allocates nothing.
- `FixedRateBond::npv` and `dv01` off a discount curve, alongside the yield-based analytics.
- `Date::from_unix_days` and `unix_days`, since riskrt stores dates as days since 1970-01-01.
- `bootstrap_sofr_with_jacobian`, `zero_bucket_ladder`, `CurveJacobian::to_par_ladder`, and node accessors on `DiscountCurve`.
- Compile-time `Send + Sync` assertions on every type riskrt shares across threads, so a future `Rc` or `RefCell` is a build error.
Conventions carried across the boundary:
 
- All risk in riskrt is per basis point. `OisSwap::dv01` and `FixedRateBond::dv01` are both "PV change for +1bp" and match `Risk::dv01` directly. (`YieldAnalytics::dv01` has the opposite sign and is not used.)
- A swap's direction comes from `fixed_leg`; its notional is taken as absolute. A bond's notional is signed, so a short bond has positive DV01.
- Bond prices are per face (100); the adapter scales by notional over face.
- `Position::Bond` carries no issue date. The adapter anchors a semiannual schedule to maturity and starts it over a year before valuation, which gives regular periods and the same PV as the real issue. A test checks this.
- USD SOFR OIS is single-curve, so a swap's projection and discount curve must be the same curve. A two-curve swap is rejected at load time.
## Known limitations and what comes next
 
- **Fixed valuation date per session.** Rolling the date intraday means rebuilding every instrument and curve, which the engine supports mechanically (publish a new portfolio and curve set) but does not orchestrate yet.
- **Intraday portfolio changes.** `Portfolio`, `CurveIndex` and `FqPricer` are immutable. Adding a trade means building new ones and restarting the scheduler and aggregator on them. The design allows it; the engine does not expose it yet.
- **Single-curve swaps only.** Split projection and discount curves need DV01 across two curves and are rejected for now.
- **No core pinning.** It belongs in the rayon pools' `start_handler`; `quote_to_risk` loaded vs idle is the evidence it waits on.
- **ferrumquant is not pinned.** By choice while both repositories move; see [Building and running](#building-and-running).
Next in the plan:
 
1. A commodities slice: `FuturesCurve`-backed curves in `CurveSet` (no bootstrap, no Jacobian; the strip is its own ladder), gas and oil swap products, and a second arm in the pricer, against ferrumquant's `fq-commod-core`, `fq-gas` and `fq-oil`.
2. Core pinning in the rayon pools' `start_handler`, judged by `quote_to_risk` loaded vs idle.
3. `riskrt-scenario`: parallel shift and twist ladders over the whole portfolio on the worker pool, reusing the pre-shifted curves.
