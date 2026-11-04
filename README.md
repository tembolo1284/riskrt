# riskrt

A multithreaded real-time portfolio risk engine in Rust. It turns streaming rate quotes into versioned yield curves and produces incremental PV, DV01 and par-quote bucket ladders for a portfolio of swaps and bonds, with [ferrumquant](https://github.com/tembolo1284/ferrumquant) as the pricing kernel.

The problem it solves is the one every rates desk has. Quotes tick thousands of times a second, every tick on a curve dirties every trade on that curve, and the desk wants to see the current risk, not a backlog of stale risk. riskrt is built so that each stage keeps up with the one below it by conflating work rather than queueing it, and so that a reader can never observe a half-applied update.

On a 10-second run with 2,000 trades on 3 curves and 6,000 ticks per second, the engine produced 1,492 full portfolio reprices (about 149 per second, 2.98 million trade prices with ladders) with zero failures, zero stale results and the risk snapshot never more than one curve set behind the market.

## Contents

1. [Pipeline overview](#pipeline-overview)
2. [Workspace layout](#workspace-layout)
3. [Building and running](#building-and-running)
4. [Stage by stage](#stage-by-stage)
5. [Architectural decisions](#architectural-decisions)
6. [Threading model](#threading-model)
7. [Correctness guarantees and how they are tested](#correctness-guarantees-and-how-they-are-tested)
8. [Performance notes](#performance-notes)
9. [Integration with ferrumquant](#integration-with-ferrumquant)
10. [Known limitations and what comes next](#known-limitations-and-what-comes-next)

## Pipeline overview

```
feed threads ──channel──▶ conflator ──▶ quote store ──▶ curve builder ──▶ scheduler + workers ──▶ aggregator ──▶ risk snapshots
 (N threads)              (1 thread)    (ArcSwap)       (1 thread +        (1 thread +            (1 thread)     (ArcSwap)
                                                         rayon pool)        rayon pool)
```

Data flows left to right. Each arrow is one of two things: a bounded channel, where the producer blocks if the consumer falls behind, or a versioned snapshot behind an `ArcSwap`, where the consumer wakes up, takes the latest version and skips whatever it missed. The first kind gives backpressure. The second kind gives conflation. Which one each boundary uses is a deliberate choice, explained under [Architectural decisions](#architectural-decisions).

Shutdown runs the same direction. Stopping the feeds closes the tick channel, which ends the conflator, which closes the quote store, which ends the curve builder, and so on until the snapshot publisher closes. No stage needs a stop flag of its own. It exits when its input is exhausted.

## Workspace layout

Ten crates, each one pipeline stage or one shared concern. Dependencies point strictly downward in this list.

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
| `riskrt-engine` | Wires every stage, owns shutdown, provides synthetic demo inputs. | none of its own |
| `riskrt-cli` | The `riskrt` binary: runs the engine on simulated feeds and prints live risk. | main |

```
riskrt/
├── Cargo.toml
├── rust-toolchain.toml
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
    └── riskrt-cli/      src/main.rs
```

External dependencies are kept to four: `crossbeam` (bounded channels), `arc-swap` (lock-free snapshot publication), `rayon` (work-stealing pools) and `clap` (the CLI). The pricing kernel, ferrumquant, is a path dependency expected at `../ferrumquant`.

## Building and running

Requirements: Rust 1.85 or later (edition 2024), and ferrumquant checked out beside this repository.

```
git clone https://github.com/tembolo1284/ferrumquant ../ferrumquant
cargo build --release
cargo test --workspace          # 139 tests
cargo clippy --workspace --all-targets
```

Run the engine on simulated feeds:

```
./target/release/riskrt --trades 2000 --curves 3 --seconds 10
```

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

The "behind" figure on each refresh is the number of trades whose curve has been rebuilt since they were last priced. At high tick rates every curve moves between one pricing round and the next, so this reads as the whole portfolio and means the engine is exactly one curve set behind the market. It only reads zero when the feed is slower than a pricing round.

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

`CurveSet` bundles every built curve, their Jacobians and the fixing index under one version. Rebuilding one curve produces a new set that shares the untouched curves by `Arc`, so publishing costs a small vector of pointers. It records the version at which each curve was last rebuilt, giving the same `changed_since` as the quote store one level up.

`CurveBuilder` decides what to rebuild: exactly the curves with a pillar quote newer than the last snapshot it handled, in parallel. A curve whose rebuild fails keeps its previous version and is retried when one of its quotes next moves. A new fixing index (the daily SOFR print) forces every curve to rebuild.

`publish.rs` is the single-writer, many-reader publisher from the quote store made generic over any `Versioned` value. The curve builder and the aggregator both use it.

### riskrt-deps

`CurveIndex` is the inverted dependency map from curve to trades, stored as compressed sparse rows: one flat array of trade ids grouped by curve and an offsets array. A lookup is two array reads and returns a contiguous slice sorted by trade id. It is built once from the portfolio and fails at start-up if any trade references a curve the engine does not build, so a mis-mapped trade cannot sit in the portfolio and silently never reprice.

`DirtySet` is the set of trades waiting to be priced: a flag per trade for O(1) idempotent marking, plus a list of marked ids so a drain costs the number of dirty trades rather than the size of the portfolio. Draining returns trades in ascending id order regardless of which curve ticked first, which keeps the batches handed to workers deterministic.

Both structures are checked against a brute-force scan of random portfolios, including the case where several curve updates arrive before a drain.

### riskrt-pricing

`Pricer` is the only thing the scheduler and workers see: `price(&self, TradeId, &CurveSet) -> PricedTrade`. It is `Send + Sync` and prices through `&self`, so one instance is shared by every worker.

`FqPricer` is the ferrumquant adapter. At load time it turns each `Position` into an `OisSwap` or a `FixedRateBond` once. On each tick it prices against the pinned `CurveSet`: one NPV, one DV01 from the pre-shifted pair, and one par ladder from the per-node pair and the Jacobian. Every number is per basis point and tagged with the set version.

`price_batch` fans a list of trades out over rayon and returns one result per input in the same order. A failure for one trade does not affect the others.

### riskrt-sched

The scheduler thread owns the `DirtySet`. On each new curve set it marks the trades on curves that changed since the last set it handled, drains them as one sorted batch, pins that set with an `Arc`, and prices the batch on a dedicated rayon pool. The output is a `PricedBatch` whose every result carries the batch's set version, sent down a small bounded channel.

If curve sets arrive faster than batches finish, the scheduler skips to the latest one. `changed_since` across the skipped versions still marks every affected trade exactly once. A trade whose curve is not built yet is requeued rather than failed and goes out when the curve appears.

### riskrt-agg

The aggregator keeps the latest result per trade behind `Arc`, so building a snapshot copies pointers rather than ladders. Each batch replaces the entries it carries and produces a new immutable `RiskSnapshot` with the portfolio total, totals by book and by curve, and a summed par ladder per curve.

Totals are recomputed from the per-trade table on every batch rather than updated incrementally. It is O(trades) per batch, which is trivial, and in return a snapshot's numbers never depend on the history of batches that led to it and never accumulate floating-point drift. A test applies the same trades in two different batch orders and checks the totals are identical.

A result for a trade that already holds a newer version is dropped. The scheduler never sends one, but a replayed batch cannot move a trade backwards.

### riskrt-engine and riskrt-cli

`Engine::start(config, feeds)` spawns every stage in reverse order, so nothing publishes before someone is listening, and returns handles for live snapshots, curve sets, quotes and the pricer. `stop()` flags the feeds. `wait()` joins every stage in pipeline order and returns all their statistics. `demo` builds synthetic portfolios, curve specs and simulated feeds so the CLI, the tests and the benches share one setup.

The CLI runs the engine on paced or unpaced simulated feeds, prints the live snapshot on a timer, and prints the statistics block at the end.

## Architectural decisions

### Immutable versioned snapshots instead of shared mutable state

Quotes, curve sets and risk snapshots are all immutable values published by swapping a pointer. Nothing downstream ever holds a lock on live data. The costs are a copy per publish (40 bytes per quote for the quote table, a vector of `Arc`s for a curve set or a risk snapshot) and some garbage when old versions are dropped. The benefits are that a reader can never observe a torn update, that a worker can pin one version for as long as a batch takes while the publisher moves on, and that every number can be tagged with the version it came from. The torn-read test in `riskrt-market` writes 20,000 versions where every quote carries the version number while four readers spin, and asserts that no reader ever sees two different values in one snapshot.

### Conflation at every boundary, backpressure only at ingest

The feed-to-conflator channel is bounded, so a stalled conflator slows the feeds rather than letting memory grow. Every other boundary is a versioned snapshot, so a slow consumer skips to the latest version. This gives a property that matters on a real desk: the risk you see is always computed from the newest market data the pipeline has reached, never from a queue of old data being worked through. The 10-second run above shows it in the statistics. Quotes went through 48,000 versions, the curve builder examined 1,500 of them, and the scheduler priced against 1,492 curve sets.

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

### The pricing library behind one trait

Only `riskrt-pricing` and `riskrt-curves` know about ferrumquant. Core, feed, market, deps, sched and agg never see a curve type or an instrument type. The engine was developed against a stand-in pricer before ferrumquant was ready, and another kernel (an options library, say) can be added as a second `Pricer` implementation without touching the pipeline.

### One writer per store, enforced by the type system

`MarketWriter` and `Publisher` are not `Clone` and publish through `&mut self`. A second writer is a compile error. This is what lets publishing be a plain store rather than a compare-and-swap loop, and it is why every version number in the system is strictly increasing.

## Threading model

For a run with F feeds, W pricing workers and C curve workers, the engine has F + 4 threads of its own plus two rayon pools:

| Thread | Owns | Wakes on |
|---|---|---|
| `feed-N` (F of them) | one `TickSource` | its own pacing, or immediately |
| `conflator` | latest-tick table | a tick on the channel |
| `curve-builder` | `CurveBuilder` | a newer quote version, or a control message |
| `scheduler` | `DirtySet` | a newer curve set |
| `aggregator` | result table | a batch on the channel |
| `curve-N` pool (C) | nothing | bumped bootstraps from the builder |
| `pricer-N` pool (W) | nothing | trades from the scheduler |

The two rayon pools are separate on purpose, so that curve builds (a few milliseconds each, bursty) do not compete with pricing workers for the same threads. `--curve-workers 0` folds curve builds onto rayon's global pool instead.

Core pinning is not implemented yet. The right place for it is the pools' `start_handler`, and the decision waits on the benchmarks.

## Correctness guarantees and how they are tested

139 tests across the workspace. Beyond the unit tests, each crate from `riskrt-feed` onward has an integration test that runs the real threaded pipeline up to that stage and checks it against a single-threaded reference:

| Guarantee | Test |
|---|---|
| The conflator ends on exactly the ticks a single-threaded replay produces, through any channel size and with a slow sink | `riskrt-feed/tests/pipeline.rs` |
| No reader ever sees a half-applied batch; a follower rebuilding state from deltas across skipped versions ends on the right state | `riskrt-market` (unit) and `tests/feed_to_store.rs` |
| Every built curve equals a single-threaded build off the final quotes, byte for byte, including the Jacobian | `riskrt-curves/tests/pipeline.rs` |
| The dependency index and dirty set agree with a brute-force scan over random portfolios and tick sequences | `riskrt-deps/tests/against_brute_force.rs` |
| The adapter's PV and DV01 equal direct ferrumquant calls; receivers mirror payers; bonds scale by notional over face | `riskrt-pricing/tests/against_ferrumquant.rs` |
| Every result carries its batch's version, per-trade versions never go backwards, no trade is stale against the final curve set, and every final number equals a direct reprice against that set | `riskrt-sched/tests/pipeline.rs` |
| The final snapshot's total, every book, every curve and every ladder equal a single-threaded sum over the final curve set | `riskrt-agg/tests/pipeline.rs` |
| The whole engine runs to completion with exact results, and `stop` ends an endless feed with every thread joining | `riskrt-engine/tests/engine.rs` |

Each threaded test was also run repeatedly (10 to 25 times, debug and release) during development to check for flakiness. None was found.

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
- **No core pinning.** It belongs in the rayon pools' `start_handler` and waits on benchmark evidence.
- **No signal handling in the CLI.** Ctrl-C kills the process before the statistics print. Use `--seconds` for now.

Next in the plan:

1. `benches/` with criterion: tick-to-risk latency and trades per second against worker count, to find where the engine saturates and whether core pinning earns its keep.
2. `riskrt-scenario`: parallel shift and twist ladders over the whole portfolio on the worker pool, reusing the pre-shifted curves.
3. A `ctrlc` handler in the CLI so an interrupted run still shuts down in order.
