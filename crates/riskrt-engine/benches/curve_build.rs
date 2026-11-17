//! Cost of turning one quote snapshot into one risk-ready curve.
//!
//! `bootstrap` is the kernel floor: one `bootstrap_sofr` on ten pillars.
//! `build_curve` is what the curve builder actually does per curve per tick:
//! the base bootstrap, the 2n bumped bootstraps for the Jacobian, and the
//! shifted copies. The bumped bootstraps run on rayon, so it is measured on
//! pools of 1, 2, 4... threads up to the machine's parallelism to show how
//! much of the per-tick cost the extra cores buy back.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fq_core::time::Date;
use fq_rates::curve::bootstrap_sofr;
use fq_rates::index::OvernightIndex;
use riskrt_core::Tick;
use riskrt_curves::BuildConfig;
use riskrt_curves::build_curve;
use riskrt_engine::demo;
use riskrt_market::QuoteSnapshot;
use std::sync::Arc;

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

fn snapshot() -> QuoteSnapshot {
    let ticks: Vec<Tick> = (0..demo::TENORS.len())
        .map(|k| Tick::new(demo::quote_id(0, k), 1, 0, 0.04 + k as f64 * 0.0005))
        .collect();
    QuoteSnapshot::empty(demo::TENORS.len())
        .next(&ticks)
        .unwrap()
}

fn bench(c: &mut Criterion) {
    let spec = demo::curve_specs(1).unwrap();
    let spec = &spec.curves()[0];
    let valuation = Date::from_unix_days(demo::VALUATION.0).unwrap();
    let index = Arc::new(OvernightIndex::sofr());
    let cfg = BuildConfig::default();
    let quotes = snapshot();

    let instruments: Vec<_> = spec
        .pillars
        .iter()
        .map(|p| p.instrument(quotes.value(p.quote).unwrap()))
        .collect();
    c.bench_function("bootstrap/10 pillars", |b| {
        b.iter(|| bootstrap_sofr(valuation, &index, &instruments, cfg.bootstrap).unwrap())
    });

    let mut g = c.benchmark_group("build_curve/10 pillars");
    for threads in pool_sizes() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        g.bench_with_input(BenchmarkId::new("threads", threads), &threads, |b, _| {
            b.iter(|| pool.install(|| build_curve(spec, valuation, &quotes, &index, &cfg).unwrap()))
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
