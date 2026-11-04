//! `riskrt`: run the engine on simulated feeds and print live risk.

use clap::Parser;
use riskrt_core::{BookId, CurveId, Tick};
use riskrt_engine::{Engine, EngineConfig, EngineStats, demo};
use riskrt_feed::TickSource;
use riskrt_grpc::{Handles, spawn_server};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "riskrt",
    about = "Real-time portfolio risk on simulated SOFR curves"
)]
struct Args {
    /// Trades in the synthetic portfolio.
    #[arg(long, default_value_t = 2_000)]
    trades: u32,
    /// Curves (one simulated feed each).
    #[arg(long, default_value_t = 3)]
    curves: u32,
    /// Books to roll up by.
    #[arg(long, default_value_t = 4)]
    books: u32,
    /// Pricing worker threads (0 = one per core).
    #[arg(long, default_value_t = 0)]
    workers: usize,
    /// Curve-build threads (0 = rayon's global pool).
    #[arg(long, default_value_t = 2)]
    curve_workers: usize,
    /// Ticks per second per feed (0 = as fast as possible).
    #[arg(long, default_value_t = 2_000)]
    rate: u64,
    /// Run for this many seconds, then stop (0 = until the feeds end).
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    /// Ticks per feed before it ends (0 = endless).
    #[arg(long, default_value_t = 0)]
    ticks: u64,
    /// Screen refresh interval in milliseconds.
    #[arg(long, default_value_t = 500)]
    refresh: u64,
    /// Seed for the portfolio and the feeds.
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Serve live risk over gRPC on this address (e.g. 127.0.0.1:50051).
    #[arg(long)]
    grpc: Option<SocketAddr>,
}

/// Slows a source down to a fixed tick rate.
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let limit = (args.ticks > 0).then_some(args.ticks);

    let portfolio = demo::portfolio(args.trades, args.curves, args.books, args.seed);
    let curves = demo::curve_specs(args.curves)?;
    let config = EngineConfig::new(portfolio.clone(), curves.clone())
        .with_workers(args.workers, args.curve_workers);

    let interval = 1_000_000_000u64
        .checked_div(args.rate)
        .map(Duration::from_nanos);
    let feeds: Vec<Box<dyn TickSource>> = demo::sim_feeds(args.curves, args.seed, limit)
        .into_iter()
        .map(|inner| -> Box<dyn TickSource> {
            match interval {
                None => inner,
                Some(interval) => Box::new(Paced {
                    inner,
                    interval,
                    next_at: Instant::now(),
                }),
            }
        })
        .collect();

    let engine = Engine::start(config, feeds)?;
    let snapshots = engine.snapshots();
    let curve_sets = engine.curves();
    let server = match args.grpc {
        Some(addr) => {
            let handles = Handles {
                portfolio: Arc::new(portfolio),
                spec: Arc::new(curves),
                snapshots: engine.snapshots(),
                curves: engine.curves(),
                quotes: engine.quotes(),
                pricer: engine.pricer(),
            };
            let server = spawn_server(handles, addr)?;
            println!("gRPC: serving riskrt.v1.Risk on {}", server.local_addr());
            Some(server)
        }
        None => None,
    };
    let started = Instant::now();
    let deadline = (args.seconds > 0).then(|| started + Duration::from_secs(args.seconds));

    println!(
        "riskrt: {} trades, {} curves, {} books, {} pricing workers",
        args.trades,
        args.curves,
        args.books,
        if args.workers == 0 {
            "auto".to_owned()
        } else {
            args.workers.to_string()
        }
    );
    loop {
        thread::sleep(Duration::from_millis(args.refresh));
        let snap = snapshots.load();
        let set = curve_sets.load();
        println!(
            "\n[{:>6.1}s] risk v{} (curves v{}, quotes v{})  priced {}/{}  behind {}",
            started.elapsed().as_secs_f64(),
            snap.version().0,
            set.version().0,
            set.quotes_version().0,
            snap.priced_count(),
            snap.trade_count(),
            snap.stale_against(&set),
        );
        let t = snap.total();
        println!("  total     pv {:>16.2}   dv01 {:>12.2}", t.pv, t.dv01);
        for (b, r) in snap.books().iter().enumerate() {
            println!("  book {:<4} pv {:>16.2}   dv01 {:>12.2}", b, r.pv, r.dv01);
        }
        for (c, r) in snap.curves().iter().enumerate() {
            let ladder = snap
                .ladder(CurveId(c as u32))
                .map(|l| {
                    l.per_bp
                        .iter()
                        .map(|x| format!("{x:>9.0}"))
                        .collect::<Vec<_>>()
                        .join("")
                })
                .unwrap_or_else(|| "  (not built)".to_owned());
            println!(
                "  curve {:<3} pv {:>16.2}   dv01 {:>12.2}   ladder{ladder}",
                c, r.pv, r.dv01
            );
        }
        let _ = BookId(0);
        if snapshots.is_closed() || deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
    }

    let stats = engine.shutdown();
    if let Some(server) = server {
        server.join().map_err(|e| e.to_string())?;
    }
    print_stats(&stats);
    Ok(())
}

fn print_stats(s: &EngineStats) {
    let secs = s.elapsed.as_secs_f64();
    println!("\n--- run statistics ({secs:.1}s) ---");
    println!(
        "feeds        {} ticks sent ({:.0}/s), {} stalls",
        s.ticks_sent(),
        s.ticks_sent() as f64 / secs,
        s.feeds.iter().map(|f| f.stalls).sum::<u64>()
    );
    println!(
        "conflator    {} received, {} published in {} batches ({:.1}% conflated)",
        s.conflate.received,
        s.conflate.published,
        s.conflate.batches,
        100.0 * s.conflate.conflated as f64 / s.conflate.received.max(1) as f64
    );
    println!(
        "curves       {} sets from {} snapshots, {} curve builds, {} failures",
        s.curves.sets, s.curves.snapshots, s.curves.curves_built, s.curves.failures
    );
    println!(
        "scheduler    {} batches, {} trades priced ({:.0}/s), max batch {}, {} requeued, {} failures",
        s.sched.batches,
        s.sched.trades_priced,
        s.sched.trades_priced as f64 / secs,
        s.sched.max_batch,
        s.sched.requeued,
        s.sched.failures
    );
    println!(
        "aggregator   {} batches applied, {} stale dropped",
        s.agg.batches, s.agg.stale_dropped
    );
}
