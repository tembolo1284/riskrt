//! End-to-end latency: from a tick entering a feed to a risk snapshot that
//! reflects it, through the real threaded engine.
//!
//! This is not a criterion microbenchmark. The quantity of interest is a
//! distribution, so it drives one tick at a time, waits for the pipeline to
//! absorb it, and reports percentiles at two points:
//!
//! - quote -> curve: the tick is in a published `CurveSet` (conflator, quote
//!   store, bootstrap plus Jacobian).
//! - quote -> risk: every trade on that curve has been repriced against that
//!   set and the `RiskSnapshot` carrying them is published.
//!
//! Two scenarios. `idle` ticks one curve while the others sit still, which
//! gives the floor. `loaded` ticks one curve while simulated feeds drive the
//! other curves at a fixed rate, which shows what sharing the pools costs.
//!
//! Usage (any argument may be omitted):
//!
//!   cargo bench --bench quote_to_risk -- --iters 500 --trades 2000 --curves 3 --rate 2000 --workers 0
//!
//! The last line of each scenario is a Markdown table row for the README.

use crossbeam::channel::{Receiver, Sender, bounded};
use riskrt_core::{CurveId, MarketVersion, Tick, TradeId};
use riskrt_engine::{Engine, EngineConfig, Wait, demo};
use riskrt_feed::{SimConfig, SimSource, TickSource};
use std::thread;
use std::time::{Duration, Instant};

struct Opts {
    iters: usize,
    warmup: usize,
    trades: u32,
    curves: u32,
    rate: u64,
    workers: usize,
    curve_workers: usize,
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        iters: 500,
        warmup: 50,
        trades: 2_000,
        curves: 3,
        rate: 2_000,
        workers: 0,
        curve_workers: riskrt_engine::default_curve_workers(),
    };
    let args: Vec<String> = std::env::args().collect();
    fn val<T: std::str::FromStr>(args: &[String], i: usize, cur: T) -> T {
        args.get(i + 1).and_then(|v| v.parse().ok()).unwrap_or(cur)
    }
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--iters" => o.iters = val(&args, i, o.iters),
            "--warmup" => o.warmup = val(&args, i, o.warmup),
            "--trades" => o.trades = val(&args, i, o.trades),
            "--curves" => o.curves = val(&args, i, o.curves),
            "--rate" => o.rate = val(&args, i, o.rate),
            "--workers" => o.workers = val(&args, i, o.workers),
            "--curve-workers" => o.curve_workers = val(&args, i, o.curve_workers),
            // `cargo bench` passes `--bench`; criterion-style flags are ignored.
            _ => {
                i += 1;
                continue;
            }
        }
        i += 2;
    }
    o
}

/// A feed the benchmark drives by hand. Blocks until a tick is sent; ends
/// when the sender is dropped.
struct Injected(Receiver<Tick>);

impl TickSource for Injected {
    fn next_tick(&mut self) -> Option<Tick> {
        self.0.recv().ok()
    }
}

/// Slows a source down to a fixed tick rate (same as the CLI's).
struct Paced<S> {
    inner: S,
    interval: Duration,
    next_at: Instant,
}

impl<S: TickSource> TickSource for Paced<S> {
    fn next_tick(&mut self) -> Option<Tick> {
        let now = Instant::now();
        if self.next_at > now {
            thread::sleep(self.next_at - now);
        }
        self.next_at = self.next_at.max(now) + self.interval;
        self.inner.next_tick()
    }
}

/// All ten pillars of `curve` at a gently sloped 4%.
fn initial_ticks(curve: u32, seq: &mut u64) -> Vec<Tick> {
    (0..demo::TENORS.len())
        .map(|k| {
            *seq += 1;
            Tick::new(demo::quote_id(curve, k), *seq, 0, 0.04 + k as f64 * 0.0005)
        })
        .collect()
}

struct Sample {
    to_curve: Duration,
    to_risk: Duration,
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let n = sorted.len();
    let rank = ((p / 100.0) * (n as f64 - 1.0)).round() as usize;
    sorted[rank.min(n - 1)]
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn report(name: &str, samples: &[Sample]) {
    let mut curve: Vec<Duration> = samples.iter().map(|s| s.to_curve).collect();
    let mut risk: Vec<Duration> = samples.iter().map(|s| s.to_risk).collect();
    curve.sort();
    risk.sort();
    let row = |label: &str, v: &[Duration]| {
        println!(
            "  {label:<16} p50 {:>9.0} us   p90 {:>9.0} us   p95 {:>9.0} us   p99 {:>9.0} us   max {:>9.0} us",
            us(percentile(v, 50.0)),
            us(percentile(v, 90.0)),
            us(percentile(v, 95.0)),
            us(percentile(v, 99.0)),
            us(*v.last().unwrap()),
        );
    };
    println!("{name}: {} samples", samples.len());
    row("quote -> curve", &curve);
    row("quote -> risk", &risk);
    println!(
        "  | {name} | {:.2} | {:.2} | {:.2} | {:.2} |",
        us(percentile(&risk, 50.0)) / 1000.0,
        us(percentile(&risk, 95.0)) / 1000.0,
        us(percentile(&risk, 99.0)) / 1000.0,
        us(*risk.last().unwrap()) / 1000.0,
    );
}

/// Runs one scenario. Curve 0 is always injected; the others are either
/// injected once and left alone (`background_rate == 0`) or driven by paced
/// simulated feeds.
fn scenario(name: &str, o: &Opts, background_rate: u64) {
    let portfolio = demo::portfolio(o.trades, o.curves, 4, 42);
    let on_curve_0: Vec<TradeId> = portfolio
        .positions()
        .iter()
        .enumerate()
        .filter(|(_, p)| p.discount == CurveId(0))
        .map(|(i, _)| TradeId(i as u32))
        .collect();
    let config = EngineConfig::new(portfolio, demo::curve_specs(o.curves).unwrap())
        .with_workers(o.workers, o.curve_workers);

    // One injected feed per curve for the setup ticks, then curve 0 stays
    // injected and the others are either silent or replaced by sim feeds.
    let mut senders: Vec<Sender<Tick>> = Vec::new();
    let mut feeds: Vec<Box<dyn TickSource>> = Vec::new();
    for c in 0..o.curves {
        if c == 0 || background_rate == 0 {
            let (tx, rx) = bounded::<Tick>(64);
            senders.push(tx);
            feeds.push(Box::new(Injected(rx)));
        } else {
            let cfg = SimConfig {
                first_quote: demo::quote_id(c, 0).0,
                quotes: demo::TENORS.len() as u32,
                seed: 42 + u64::from(c),
                start_value: 0.04,
                limit: None,
                ..Default::default()
            };
            feeds.push(Box::new(Paced {
                inner: SimSource::new(cfg),
                interval: Duration::from_nanos(1_000_000_000 / background_rate),
                next_at: Instant::now(),
            }));
        }
    }

    let engine = Engine::start(config, feeds).expect("engine starts");
    let quotes = engine.quotes();
    let sets = engine.curves();
    let snapshots = engine.snapshots();
    let timeout = Duration::from_secs(30);

    // Seed every injected curve so all curves build and every trade prices once.
    let mut seq = 0u64;
    for (i, tx) in senders.iter().enumerate() {
        for t in initial_ticks(i as u32, &mut seq) {
            tx.send(t).unwrap();
        }
    }
    let mut seen_set = MarketVersion::ZERO;
    loop {
        match sets.wait_newer(seen_set, timeout) {
            Wait::Updated(set) => {
                seen_set = set.version();
                if set.is_healthy() && set.healths().len() == o.curves as usize {
                    break;
                }
            }
            _ => panic!("curves never built"),
        }
    }
    let mut seen_snap = MarketVersion::ZERO;
    loop {
        match snapshots.wait_newer(seen_snap, timeout) {
            Wait::Updated(s) => {
                seen_snap = s.version();
                if s.is_complete() {
                    break;
                }
            }
            _ => panic!("portfolio never priced"),
        }
    }

    // The measured loop: one tick on curve 0's 5Y pillar, then wait for it
    // to come out the other end before sending the next.
    let probe = demo::quote_id(0, 6);
    let tx = &senders[0];
    let mut seen_quote = quotes.version();
    let mut samples = Vec::with_capacity(o.iters);
    for i in 0..(o.warmup + o.iters) {
        seq += 1;
        let value = 0.0431 + (i as f64) * 1e-7;
        let t0 = Instant::now();
        tx.send(Tick::new(probe, seq, 0, value)).unwrap();

        // 1. The quote store has it.
        let quote_version = loop {
            match quotes.wait_newer(seen_quote, timeout) {
                riskrt_market::Wait::Updated(q) => {
                    seen_quote = q.version();
                    if q.value(probe) == Some(value) {
                        break q.version();
                    }
                }
                _ => panic!("iteration {i}: quote never published"),
            }
        };
        // 2. A curve set built from that quote version (or later) is out.
        let set = loop {
            match sets.wait_newer(seen_set, timeout) {
                Wait::Updated(set) => {
                    seen_set = set.version();
                    let built_from = set.curve(CurveId(0)).unwrap().quotes_version();
                    if built_from >= quote_version {
                        break set;
                    }
                }
                _ => panic!("iteration {i}: curve never rebuilt"),
            }
        };
        let t_curve = Instant::now();
        // 3. Every trade on curve 0 has been repriced at or after the set
        //    version where curve 0 was rebuilt. Not `set.version()`: under
        //    load the set we observed may already be several versions past
        //    that rebuild (other curves moved), and curve 0's trades were
        //    priced against the rebuild version and will not move again
        //    until the next probe.
        let rebuilt_at = set.rebuilt_at(CurveId(0));
        loop {
            match snapshots.wait_newer(seen_snap, timeout) {
                Wait::Updated(s) => {
                    seen_snap = s.version();
                    let done = on_curve_0
                        .iter()
                        .all(|t| s.trade(*t).is_some_and(|p| p.risk.version >= rebuilt_at));
                    if done {
                        break;
                    }
                }
                _ => panic!("iteration {i}: risk never published"),
            }
        }
        let t_risk = Instant::now();
        if i > 0 && i % 250 == 0 {
            eprintln!("  {name}: {i}/{}", o.warmup + o.iters);
        }
        if i >= o.warmup {
            samples.push(Sample {
                to_curve: t_curve - t0,
                to_risk: t_risk - t0,
            });
        }
    }

    drop(senders);
    engine.stop();
    let stats = engine.wait();
    report(name, &samples);
    println!(
        "  (engine: {} sets, {} curve builds, {} batches, {} trades priced, {} failures)\n",
        stats.curves.sets,
        stats.curves.curves_built,
        stats.sched.batches,
        stats.sched.trades_priced,
        stats.curves.failures + stats.sched.failures,
    );
}

fn main() {
    let o = parse_opts();
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    println!(
        "quote_to_risk: {} trades, {} curves, {} pricing workers, {} curve workers, {} cores, {} iterations after {} warm-up",
        o.trades,
        o.curves,
        if o.workers == 0 { cores } else { o.workers },
        o.curve_workers,
        cores,
        o.iters,
        o.warmup
    );
    println!("  table columns: scenario | p50 ms | p95 ms | p99 ms | max ms\n");
    scenario("idle", &o, 0);
    if o.curves > 1 {
        scenario(
            &format!("loaded ({} ticks/s on the other curves)", o.rate),
            &o,
            o.rate,
        );
    }
}
