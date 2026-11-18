[Back to README](../README.md)

# Design

How riskrt works, stage by stage, and why it is built the way it is. Companion to the pipeline overview in the README.

# Pipeline overview

```
feed threads ──channel──▶ conflator ──▶ quote store ──▶ curve builder ──▶ scheduler + workers ──channel──▶ aggregator ──▶ risk snapshots ──▶ gRPC server
 (N threads)              (1 thread)    (ArcSwap)       (1 thread +        (1 thread +                       (1 thread)     (ArcSwap)         (optional)
                                                         rayon pool)        rayon pool)
```

Data flows left to right. Each arrow is one of two things: a bounded channel, where the producer blocks if the consumer falls behind, or a versioned snapshot behind an `ArcSwap`, where the consumer wakes up, takes the latest version and skips whatever it missed. The first kind gives backpressure. The second kind gives conflation. Two boundaries are channels (feeds to conflator, scheduler to aggregator); the rest are snapshots. Which one each boundary uses is a deliberate choice, explained under [Architectural decisions](design.md#architectural-decisions). The gRPC server is one more reader of the snapshots and feeds nothing back.

Shutdown runs the same direction. Stopping the feeds closes the tick channel, which ends the conflator, which closes the quote store, which ends the curve builder, and so on until the snapshot publisher closes. No stage needs a stop flag of its own. It exits when its input is exhausted.

# Stage by stage

## riskrt-core

Everything downstream shares these types, so the rules are strict: no dependencies, no threads, every type `Send + Sync`, and `Copy` wherever a value crosses a queue.

Identifiers (`CurveId`, `QuoteId`, `TradeId`, `BookId`) are dense `u32`s. Names like "USD-SOFR" or an external trade id are interned once at load time. On the hot path every lookup is a `Vec` index, with no hashing and no string compares.

`Tick` is 32 bytes with no pointers: a per-feed sequence number, a timestamp, a value and a `QuoteId`. Two fit in a cache line. Static facts about a quote (its tenor, which curve it feeds) live in reference data, not in the tick.

`Position` is economic terms only: book, discount and forward curve ids, signed notional, and a `Product` (a vanilla swap or a fixed-coupon bond). Core knows nothing about how to price it. `Portfolio` is immutable once built, and a `TradeId` is its index into the portfolio.

`Risk` is the additive pair (PV, DV01). `TradeRisk` is a `Risk` tagged with a `TradeId` and the `MarketVersion` it was priced against, 32 bytes and `Copy`. `Ladder` carries the per-pillar par-quote risk on one curve, and `PricedTrade` is a `TradeRisk` plus its `Ladder`.

`MarketVersion` is a monotonic `u64`. Every published snapshot carries one, every risk number is tagged with the version it was computed against, and that tag is what makes consistency checkable all the way through the pipeline.

## riskrt-feed

A `TickSource` is anything that yields ticks in sequence order. Two are provided. `SimSource` is a seeded random walk over a block of quotes, so the same seed gives the same tick stream every time. `ReplaySource` reads a `seq,ts_ns,quote,value` file and fails up front on a bad line, before any thread starts.

`spawn_feed` runs one source on its own named thread and pushes into a bounded crossbeam channel shared by all feeds. A stop flag ends the thread between ticks, and dropping the sender on exit is what lets the conflator know when every feed is finished.

The conflator keeps a dense latest-tick table indexed by `QuoteId` and the list of quotes that changed since the last drain. Each cycle it blocks for one tick, takes whatever else is already queued, and hands its sink one batch holding only the latest tick per changed quote. A tick whose sequence number is not newer than the one already held for that quote is rejected as stale, even after a drain, so out-of-order delivery cannot move a quote backwards.

One contract matters here: each quote belongs to exactly one feed, because sequence numbers are only comparable within a feed.

## riskrt-market

`QuoteSnapshot` is the state of every quote at one `MarketVersion`. It is never mutated. Applying a batch builds the next snapshot by copying the table (40 bytes per quote) and writing the changes, then swaps a pointer. A reader therefore sees either the whole batch or none of it.

Each quote also records the version at which it last changed. `changed_since(seen)` returns every quote that moved after version `seen`, which is correct even when the caller skipped versions in between. That is the normal case for a consumer slower than the publisher, and it is how the curve builder decides what to rebuild without ever seeing the intermediate snapshots.

`MarketWriter` is deliberately not `Clone`. With exactly one writer, publishing is a plain read-copy-update with no compare-and-swap retry loop. `MarketReader::load()` is an `Arc` clone and never takes a lock. `wait_newer` lets a consumer sleep until there is a newer version, using a mutex and condvar that the writer touches briefly on each publish and that `load()` never touches, so pricing threads stay wait-free.

## riskrt-curves

`CurveSetSpec` is the reference data: which quotes feed which curve, validated once at start-up (dense curve ids, no quote feeding two curves, no duplicate pillar). Each `Pillar` carries a ferrumquant `InstrumentKey`, so a curve can mix deposits, SOFR futures and OIS.

`build_curve` does one curve's work for one quote snapshot: the base bootstrap, then the par-quote Jacobian (bump each input quote both ways and re-bootstrap), then the parallel-shifted and per-node-shifted copies of the curve. For n pillars that is 2n + 1 bootstraps. The 2n bumped ones are independent and run in parallel on rayon. The result does not depend on the thread count, which a test enforces by comparing a 1-thread build with an 8-thread build byte for byte.

The pre-shifted curves are the key to cheap per-trade risk. A trade's DV01 is two prices against curves built once per tick and shared by every position. Its zero-rate ladder is 2n prices against the per-node-shifted curves, also shared. Its par ladder is that zero ladder multiplied through the Jacobian. No curve is ever rebuilt for a single position.

`CurveSet` bundles every built curve, their Jacobians and the fixing index under one version. Rebuilding one curve produces a new set that shares the untouched curves by `Arc`, so publishing costs a small vector of pointers. It records the version at which each curve was last rebuilt, giving the same `changed_since` as the quote store one level up. Alongside each curve it keeps a `CurveHealth`: the quote version the curve was built from, the latest version a rebuild was attempted at, when the current run of failures began, the last error, and whether the curve is on the set's current fixing index. `health(id).status()` folds that into `Unbuilt`, `Current`, `Failed` or `IndexStale`.

Each `BuiltCurve` also carries the `OvernightIndex` it was calibrated with. The pricer uses the curve's index, not the set's, so a curve that stays on yesterday's fixings while others move prices consistently with itself and is reported `IndexStale` rather than silently mixing inputs.

`CurveBuilder` decides what to rebuild: exactly the curves with a pillar quote newer than the last snapshot it handled, in parallel. A curve whose rebuild fails keeps its previous version, is published as failed in a new set version (without being marked changed, so nothing reprices for no reason), and is retried when one of its quotes next moves. A new fixing index (the daily SOFR print) forces every curve to rebuild.

`publish.rs` is the single-writer, many-reader publisher from the quote store made generic over any `Versioned` value. The curve builder and the aggregator both use it.

## riskrt-deps

`CurveIndex` is the inverted dependency map from curve to trades, stored as compressed sparse rows: one flat array of trade ids grouped by curve and an offsets array. A lookup is two array reads and returns a contiguous slice sorted by trade id. It is built once from the portfolio and fails at start-up if any trade references a curve the engine does not build, so a mis-mapped trade cannot sit in the portfolio and silently never reprice.

`DirtySet` is the set of trades waiting to be priced: a flag per trade for O(1) idempotent marking, plus a list of marked ids so a drain costs the number of dirty trades rather than the size of the portfolio. Draining returns trades in ascending id order regardless of which curve ticked first, which keeps the batches handed to workers deterministic.

Both structures are checked against a brute-force scan of random portfolios, including the case where several curve updates arrive before a drain.

## riskrt-pricing

`Pricer` is the only thing the scheduler and workers see: `price(&self, TradeId, &CurveSet) -> PricedTrade`. It is `Send + Sync` and prices through `&self`, so one instance is shared by every worker.

`FqPricer` is the ferrumquant adapter. At load time it turns each `Position` into an `OisSwap` or a `FixedRateBond` once. On each tick it prices against the pinned `CurveSet`: one NPV, one DV01 from the pre-shifted pair, and one par ladder from the per-node pair and the Jacobian. Every number is per basis point and tagged with the set version.

`price_batch` fans a list of trades out over rayon and returns one result per input in the same order. A failure for one trade does not affect the others. Every result passes `validate` before it leaves the crate: it must be for the trade asked, tagged with the set's version, on a curve the set has built, with a ladder of that curve's bucket count, and every number finite. This is the contract any `Pricer` is held to, so a bad adapter produces a failed trade rather than a corrupted total.

## riskrt-sched

The scheduler thread owns the `DirtySet`. On each new curve set it marks the trades on curves that changed since the last set it handled, drains them as one sorted batch, pins that set with an `Arc`, and prices the batch on a dedicated rayon pool. The output is a `PricedBatch` whose every result carries the batch's set version, sent down a small bounded channel.

If curve sets arrive faster than batches finish, the scheduler skips to the latest one. `changed_since` across the skipped versions still marks every affected trade exactly once. A trade whose curve is not built yet is requeued rather than failed and goes out when the curve appears. Any other failure travels in the batch's `failed` list with its error, so a round where every trade fails still produces a batch and the failure is never silent. A failed trade is retried when its curve next moves.

## riskrt-agg

The aggregator keeps the latest result per trade behind `Arc`, so building a snapshot copies pointers rather than ladders. Each batch replaces the entries it carries and produces a new immutable `RiskSnapshot` with the portfolio total, totals by book and by curve, and a summed par ladder per curve.

Totals are recomputed from the per-trade table on every batch rather than updated incrementally. It is O(trades) per batch, which is trivial, and in return a snapshot's numbers never depend on the history of batches that led to it and never accumulate floating-point drift. A test applies the same trades in two different batch orders and checks the totals are identical.

A result for a trade that already holds a newer version is dropped. The scheduler never sends one, but a replayed batch cannot move a trade backwards.

Beside its numbers each trade has a `TradeStatus`: `Unpriced`, `Priced`, or `Failed { since, error }`. A failed trade keeps its last good result in the table and in the totals, and `since` is preserved across a run of failures so a reader can tell a blip from a trade that has been broken since the open. `failed_count()`, `failed()` and `is_healthy()` summarise, and `stale_against(set)` counts failed trades along with those on moved or degraded curves.

## riskrt-engine and riskrt-cli

`Engine::start(config, feeds)` spawns every stage in reverse order, so nothing publishes before someone is listening, and returns handles for live snapshots, curve sets, quotes and the pricer. If any stage fails to start, the ones already running are stopped and joined before the error is returned. `stop()` flags the feeds. `wait()` joins every stage in pipeline order and returns all their statistics, and dropping an `Engine` without calling it does the same, so no thread outlives its owner. `demo` builds synthetic portfolios, curve specs and simulated feeds so the CLI, the tests and the benches share one setup. The crate re-exports `RiskSnapshot`, `TradeStatus`, `CurveSet`, `CurveHealth`, `CurveStatus`, `Subscriber` and `Wait`, so a reader needs only `riskrt_engine`.

The CLI runs the engine on paced or unpaced simulated feeds, prints the live snapshot on a timer, and prints the statistics block at the end. With `--grpc` it also starts the gRPC server (next section).

## riskrt-proto and riskrt-grpc

`riskrt-proto` holds `risk.proto` and the generated tonic code, compiled at build time with protox. The `Risk` service has six RPCs: `Describe` (valuation date, counts, curve and pillar reference data), `Positions` (a stream of the portfolio), `SubscribeRisk` (a stream of `RiskUpdate`), `SubscribeCurves` (a stream of `CurveUpdate`: each built curve's nodes with zero rate, discount factor, the par quote behind it and its label, an optional grid sampled every N months for drawing the interpolated curve, and every curve's health), `SubscribeQuotes` (a stream of changed quotes) and `Price` (price one trade now against the current set). Risk and curve updates share the curve set version, so a screen can draw the zero curve, the par curve and the DV01 ladder under them from one consistent state. A `RiskUpdate` carries the totals, by-book and by-curve numbers, the ladders, `priced_count`, `stale_count`, `failed_count`, one `CurveHealth` per curve, and the per-trade rows when asked for. Failed trades are always sent, even without `include_trades`, so a client cannot miss them.

`riskrt-grpc` is one more reader of the engine's handles and feeds nothing back. The tokio runtime lives on its own thread, so the engine and the CLI stay synchronous. Each streaming RPC gets a small bridge thread that blocks on `wait_newer` like an in-process stage and forwards into a tokio channel of capacity one. If the client is slower than the engine, the bridge overwrites the pending update rather than queueing, so the client always gets the latest version and never a backlog. The round-trip test starts a real engine and a real server, connects a real client, and checks that the streamed totals equal the engine's own snapshot.

# Failure handling

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

# Architectural decisions

## Immutable versioned snapshots instead of shared mutable state

Quotes, curve sets and risk snapshots are all immutable values published by swapping a pointer. Nothing downstream ever holds a lock on live data. The costs are a copy per publish (40 bytes per quote for the quote table, a vector of `Arc`s for a curve set or a risk snapshot) and some garbage when old versions are dropped. The benefits are that a reader can never observe a torn update, that a worker can pin one version for as long as a batch takes while the publisher moves on, and that every number can be tagged with the version it came from. The torn-read test in `riskrt-market` writes 20,000 versions where every quote carries the version number while four readers spin, and asserts that no reader ever sees two different values in one snapshot.

## Conflation at the snapshot boundaries, backpressure at the two channels

The feed-to-conflator channel is bounded, so a stalled conflator slows the feeds rather than letting memory grow. The scheduler-to-aggregator channel is bounded too (four batches by default): a batch is the result of real work and must not be dropped, so if the aggregator falls behind the scheduler waits, and the curve sets it would have handled meanwhile are conflated at its own input. Every other boundary is a versioned snapshot, so a slow consumer skips to the latest version. This gives a property that matters on a real desk: the risk you see is always computed from the newest market data the pipeline has reached, never from a queue of old data being worked through. The 10-second run in [validation.md](validation.md#performance-notes) shows it in the statistics. Quotes went through 48,000 versions, the curve builder examined 1,500 of them, and the scheduler priced against 1,492 curve sets.

## Dirty tracking by "changed since version", not by event

Each store records, per item, the version at which it last changed. A consumer remembers the last version it handled and asks for everything that changed after it. This works regardless of how many versions the consumer skipped, which an event queue would not, and it costs one comparison per item. The same mechanism is used three times: quotes to curves, curves to trades, and (through `stale_against`) snapshots to the current curve set for the "behind" figure.

## Pre-shifted curves shared across positions

The naive way to get a bucket ladder is to shift the curve, reprice, and repeat per bucket per position. With n pillars and m positions that is 2nm curve copies per tick. Building the shifted curves once per tick and sharing them makes it 2n copies and 2nm prices. Price is the operation that is cheap in ferrumquant, since a swap NPV is about 60 curve lookups after the telescoping fast path, so this is the right side of the trade.

## Par-quote risk through a Jacobian, not re-bootstrapping

A desk hedges with the instruments it quoted, so bucketed risk should be per input quote, not per zero-rate node. Getting that by bumping each quote and re-bootstrapping per position would cost 2n bootstraps per position per tick. Instead the curve builder computes the Jacobian of node zero rates with respect to input quotes once per curve rebuild (2n bootstraps per curve per tick, run in parallel), and each position's zero ladder is multiplied through it. The test in `riskrt-curves` compares the Jacobian ladder with direct bump-and-rebootstrap per bucket and agrees to within 0.01 per basis point on a 16,000 per basis point bucket.

## Single-owner stages, parallelism only inside a stage

Every stage thread owns its state outright: the conflator owns its table, the curve builder its builder, the scheduler its dirty set, the aggregator its result table. Nothing is shared mutably between threads. Parallelism happens inside a stage through rayon, on work that is independent by construction: the bumped bootstraps of one curve, the trades of one batch. That keeps the concurrency reasoning local. The only synchronisation primitives in the whole engine are the bounded channels, the `ArcSwap`s, and one mutex-and-condvar pair per publisher used solely for sleeping.

## Determinism as a tested property

Every parallel computation is required to give the same bytes on one thread as on many. Curve builds, batch pricing and scheduler rounds all have tests that run them on different pool sizes and compare results with `assert_eq`. This makes the parallel engine checkable against a single-threaded reference, and it makes production discrepancies reproducible.

## The pricing library behind one trait, with one honest caveat

Instruments and the bootstrap live in two files: `riskrt-pricing/src/fq_adapter.rs` and `riskrt-curves/src/build.rs`. Core, feed, market, deps, sched and agg never see an instrument type, and the engine was developed against a stand-in pricer before ferrumquant was ready. The caveat is that the kernel is not fully hidden: `Pricer::price` takes a `&CurveSet`, and a `BuiltCurve` inside it is a ferrumquant `DiscountCurve` with its Jacobian and index. That is deliberate. The pre-shifted curves and the Jacobian are what make per-trade risk cheap, and they only make sense as the kernel's own types. So the boundary is: the pipeline crates never construct or inspect kernel types, and a second `Pricer` (an options kernel, or ferrumquant's commodities crates when they land) is added as one more adapter plus a `Product` variant, with the `validate` contract already defining what it must return. Swapping the curve representation itself would touch `riskrt-curves` as well as the adapter.

## One writer per store, enforced by the type system

`MarketWriter` and `Publisher` are not `Clone` and publish through `&mut self`. A second writer is a compile error. This is what lets publishing be a plain store rather than a compare-and-swap loop, and it is why every version number in the system is strictly increasing.

# Threading model

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
| `grpc-risk` / `grpc-curves` / `grpc-quotes` (one per subscriber) | nothing | a newer snapshot, forwarded to the client |

The two rayon pools are separate on purpose, so that curve builds (a few milliseconds each, bursty) do not compete with pricing workers for the same threads. `--curve-workers 0` folds curve builds onto rayon's global pool instead.

Core pinning is not implemented. The benchmarks in [validation.md](validation.md#benchmarks) show the engine bounded by arithmetic (the bootstrap and the per-trade ladder), not by threads contending for cores, so pinning would not move the numbers on current hardware. The right place for it, if that changes, is the pools' `start_handler`.
