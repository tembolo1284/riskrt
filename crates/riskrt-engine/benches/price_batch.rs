//! Cost of repricing a portfolio against one curve set, and how it scales
//! with pricing workers.
//!
//! `price_one` is the per-trade floor: NPV, DV01 from the pre-shifted pair,
//! and the ten-bucket par ladder, for a swap and for a bond. `price_batch`
//! prices the whole demo portfolio on pools of 1, 2, 4... threads and
//! reports throughput in trades per second. Perfect scaling doubles the
//! throughput per doubling of threads; where it stops is where the engine
//! saturates on this machine.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fq_core::time::Date;
use fq_rates::index::OvernightIndex;
use riskrt_core::{Product, Tick, TradeId};
use riskrt_curves::{BuildConfig, CurveBuilder, CurveSet};
use riskrt_engine::demo;
use riskrt_market::QuoteSnapshot;
use riskrt_pricing::{FqPricer, Pricer, price_batch};
use std::sync::Arc;

const TRADES: u32 = 2_000;
const CURVES: u32 = 3;

fn pool_sizes() -> Vec<usize> {
    let max = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut sizes = vec![1];
    let mut n = 2;
    while n <= max {
        sizes.push(n);
        n *= 2;
    }
    if *sizes.last().unwrap() != max {
        sizes.push(max);
    }
    sizes
}

/// Every curve built once from a flat-ish 4% snapshot.
fn curve_set() -> Arc<CurveSet> {
    let spec = demo::curve_specs(CURVES).unwrap();
    let quote_count = (CURVES as usize) * demo::TENORS.len();
    let ticks: Vec<Tick> = (0..CURVES)
        .flat_map(|c| {
            (0..demo::TENORS.len())
                .map(move |k| Tick::new(demo::quote_id(c, k), 1, 0, 0.04 + k as f64 * 0.0005))
        })
        .collect();
    let quotes = QuoteSnapshot::empty(quote_count).next(&ticks).unwrap();
    let mut builder = CurveBuilder::new(
        spec,
        Arc::new(OvernightIndex::sofr()),
        BuildConfig::default(),
    );
    builder.on_snapshot(&quotes).expect("every curve builds")
}

fn bench(c: &mut Criterion) {
    let portfolio = demo::portfolio(TRADES, CURVES, 4, 42);
    let valuation = Date::from_unix_days(demo::VALUATION.0).unwrap();
    let pricer = FqPricer::build(&portfolio, valuation).unwrap();
    let set = curve_set();

    let swap = portfolio
        .positions()
        .iter()
        .position(|p| matches!(p.product, Product::Swap { .. }))
        .map(|i| TradeId(i as u32))
        .unwrap();
    let bond = portfolio
        .positions()
        .iter()
        .position(|p| matches!(p.product, Product::Bond { .. }))
        .map(|i| TradeId(i as u32))
        .unwrap();
    c.bench_function("price_one/swap", |b| {
        b.iter(|| pricer.price(swap, &set).unwrap())
    });
    c.bench_function("price_one/bond", |b| {
        b.iter(|| pricer.price(bond, &set).unwrap())
    });

    let trades: Vec<TradeId> = (0..TRADES).map(TradeId).collect();
    let mut g = c.benchmark_group(format!("price_batch/{TRADES} trades"));
    g.throughput(Throughput::Elements(u64::from(TRADES)));
    g.sample_size(20);
    for threads in pool_sizes() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        g.bench_with_input(BenchmarkId::new("threads", threads), &threads, |b, _| {
            b.iter(|| {
                let out = pool.install(|| price_batch(&pricer, &trades, &set));
                assert!(out.iter().all(Result::is_ok));
                out
            })
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
