//! The running pipeline and its orderly shutdown.

use crate::config::EngineConfig;
use crate::error::EngineError;
use fq_rates::index::OvernightIndex;
use riskrt_agg::{AggStats, Aggregator, RiskSnapshot, spawn_aggregator};
use riskrt_curves::{BuildStats, Control, CurveBuilder, CurveSet, Subscriber, spawn_curve_builder};
use riskrt_deps::CurveIndex;
use riskrt_feed::{
    ConflateStats, FeedStats, TickSource, spawn_conflator, spawn_feed, tick_channel,
};
use riskrt_market::{MarketReader, market_store};
use riskrt_pricing::{FqPricer, Pricer};
use riskrt_sched::{SchedStats, Scheduler, spawn_scheduler};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Every stage's statistics, collected when the engine is joined.
#[derive(Clone, Debug, Default)]
pub struct EngineStats {
    pub feeds: Vec<FeedStats>,
    pub conflate: ConflateStats,
    pub curves: BuildStats,
    pub sched: SchedStats,
    pub agg: AggStats,
    pub elapsed: Duration,
}

impl EngineStats {
    pub fn ticks_sent(&self) -> u64 {
        self.feeds.iter().map(|f| f.sent).sum()
    }
}

/// A running pipeline.
///
/// `wait` joins it and returns statistics. Dropping it without waiting stops
/// the feeds, and every stage then drains and exits on its own; the threads
/// are detached, not abandoned mid-work.
pub struct Engine {
    started: Instant,
    stop: Arc<AtomicBool>,
    control: Sender<Control>,
    quotes: MarketReader,
    curves: Subscriber<CurveSet>,
    snapshots: Subscriber<RiskSnapshot>,
    pricer: Arc<dyn Pricer>,
    /// `None` once `wait` has taken them.
    threads: Option<Threads>,
}

struct Threads {
    feeds: Vec<JoinHandle<FeedStats>>,
    conflator: JoinHandle<ConflateStats>,
    curve_builder: JoinHandle<BuildStats>,
    scheduler: JoinHandle<SchedStats>,
    aggregator: JoinHandle<AggStats>,
}

impl Engine {
    /// Spawns every stage. Each feed gets its own thread; its quotes must all
    /// belong to curves in `config.curves`, which is checked up front.
    ///
    /// If anything fails part way through, whatever was already started is
    /// stopped before the error is returned: stages exit as their inputs are
    /// dropped, and any feeds already running see the stop flag.
    pub fn start(
        config: EngineConfig,
        feeds: Vec<Box<dyn TickSource>>,
    ) -> Result<Self, EngineError> {
        let stop = Arc::new(AtomicBool::new(false));
        let result = Self::start_with(config, feeds, Arc::clone(&stop));
        if result.is_err() {
            stop.store(true, Ordering::Relaxed);
        }
        result
    }

    fn start_with(
        config: EngineConfig,
        feeds: Vec<Box<dyn TickSource>>,
        stop: Arc<AtomicBool>,
    ) -> Result<Self, EngineError> {
        let started = Instant::now();
        let curve_count = config.curves.curve_count();
        let quote_count = config.curves.quote_count();
        if config.portfolio.curve_count() > curve_count {
            return Err(EngineError::Config(format!(
                "portfolio uses {} curves but only {curve_count} are specified",
                config.portfolio.curve_count()
            )));
        }

        // Static structures, built once.
        let index = CurveIndex::build(&config.portfolio, curve_count)?;
        let pricer: Arc<dyn Pricer> = Arc::new(FqPricer::build(
            &config.portfolio,
            config.curves.valuation_date(),
        )?);

        // Stage 6 and 7 first, so nothing is published before someone listens.
        let (writer, quotes) = market_store(quote_count);
        let (control, control_rx) = mpsc::channel();
        let pool = (config.curve_workers > 0)
            .then(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(config.curve_workers)
                    .thread_name(|i| format!("curve-{i}"))
                    .build()
                    .map(Arc::new)
            })
            .transpose()
            .map_err(|e| EngineError::Spawn(std::io::Error::other(e.to_string())))?;
        let builder = CurveBuilder::new(config.curves, Arc::clone(&config.index), config.build);
        let (curve_builder, curves) =
            spawn_curve_builder(quotes.clone(), builder, control_rx, pool)?;

        let scheduler = Scheduler::new(Arc::clone(&pricer), index);
        let (scheduler, batches) = spawn_scheduler(curves.clone(), scheduler, config.sched)?;
        let aggregator = Aggregator::new(&config.portfolio, curve_count);
        let (aggregator, snapshots) = spawn_aggregator(batches, aggregator)?;

        // Ingest last.
        let (tx, rx) = tick_channel(config.tick_capacity);
        let conflator = spawn_conflator(
            rx,
            quote_count,
            config.conflate_max_drain,
            writer.into_sink(),
        )?;
        let mut handles = Vec::with_capacity(feeds.len());
        for (i, source) in feeds.into_iter().enumerate() {
            handles.push(spawn_feed(
                &i.to_string(),
                source,
                tx.clone(),
                Arc::clone(&stop),
            )?);
        }
        drop(tx); // the conflator ends when the last feed does

        Ok(Self {
            started,
            stop,
            control,
            quotes,
            curves,
            snapshots,
            pricer,
            threads: Some(Threads {
                feeds: handles,
                conflator,
                curve_builder,
                scheduler,
                aggregator,
            }),
        })
    }

    /// Live risk. Clone it freely; `load()` is wait-free.
    pub fn snapshots(&self) -> Subscriber<RiskSnapshot> {
        self.snapshots.clone()
    }

    pub fn curves(&self) -> Subscriber<CurveSet> {
        self.curves.clone()
    }

    pub fn quotes(&self) -> MarketReader {
        self.quotes.clone()
    }

    /// The pricer the workers use, for on-demand what-ifs outside the pipeline.
    pub fn pricer(&self) -> Arc<dyn Pricer> {
        Arc::clone(&self.pricer)
    }

    /// New fixings (the daily SOFR print): every curve is rebuilt against them.
    pub fn set_index(&self, index: Arc<OvernightIndex>) {
        // Ignoring the error is right: it only fails if the builder has exited,
        // and then there is nothing left to rebuild.
        let _ = self.control.send(Control::SetIndex(index));
    }

    /// Asks the feeds to stop. Everything downstream drains and exits in turn.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Joins every stage in pipeline order and returns their statistics.
    /// Call `stop` first unless the feeds end on their own.
    pub fn wait(mut self) -> EngineStats {
        fn join<T>(name: &str, h: JoinHandle<T>) -> T {
            h.join()
                .unwrap_or_else(|_| panic!("{name} thread panicked"))
        }
        let threads = self.threads.take().expect("threads are only taken once");
        let feeds = threads.feeds.into_iter().map(|h| join("feed", h)).collect();
        let conflate = join("conflator", threads.conflator);
        let curves = join("curve-builder", threads.curve_builder);
        let sched = join("scheduler", threads.scheduler);
        let agg = join("aggregator", threads.aggregator);
        EngineStats {
            feeds,
            conflate,
            curves,
            sched,
            agg,
            elapsed: self.started.elapsed(),
        }
    }

    /// `stop` then `wait`.
    pub fn shutdown(self) -> EngineStats {
        self.stop();
        self.wait()
    }
}

impl Drop for Engine {
    /// Stops the feeds. Threads not yet joined are detached and finish on
    /// their own as the pipeline drains; `wait` is the way to collect them.
    fn drop(&mut self) {
        self.stop();
    }
}
