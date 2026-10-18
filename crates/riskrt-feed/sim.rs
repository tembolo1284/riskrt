//! Deterministic simulated feed: a seeded random walk over a block of quotes.

use crate::source::TickSource;
use riskrt_core::{QuoteId, Tick};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SimConfig {
    /// First `QuoteId` this feed owns.
    pub first_quote: u32,
    /// Number of consecutive quotes it owns.
    pub quotes: u32,
    /// Same seed, same tick stream.
    pub seed: u64,
    /// Starting value for every quote (0.04 = 4%).
    pub start_value: f64,
    /// Maximum move per tick; each tick moves uniformly in `[-step, step)`.
    pub step: f64,
    /// Synthetic clock advance per tick, in nanoseconds.
    pub interval_ns: u64,
    /// Stop after this many ticks; `None` runs until the feed is stopped.
    pub limit: Option<u64>,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            first_quote: 0,
            quotes: 1,
            seed: 1,
            start_value: 0.04,
            step: 0.000_05, // half a basis point
            interval_ns: 1_000,
            limit: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimSource {
    cfg: SimConfig,
    values: Vec<f64>,
    rng: SplitMix64,
    seq: u64,
    ts_ns: u64,
}

impl SimSource {
    pub fn new(cfg: SimConfig) -> Self {
        Self {
            cfg,
            values: vec![cfg.start_value; cfg.quotes as usize],
            rng: SplitMix64(cfg.seed),
            seq: 0,
            ts_ns: 0,
        }
    }
}

impl TickSource for SimSource {
    fn next_tick(&mut self) -> Option<Tick> {
        if self.values.is_empty() || self.cfg.limit.is_some_and(|n| self.seq >= n) {
            return None;
        }
        let i = (self.rng.next_u64() % self.values.len() as u64) as usize;
        let shock = 2.0 * self.rng.next_unit() - 1.0;
        self.values[i] += self.cfg.step * shock;
        self.seq += 1;
        self.ts_ns += self.cfg.interval_ns;
        let quote = QuoteId(self.cfg.first_quote + i as u32);
        Some(Tick::new(quote, self.seq, self.ts_ns, self.values[i]))
    }
}

/// SplitMix64: small, fast, good enough for test data, and no dependency.
#[derive(Clone, Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)` from the top 53 bits.
    fn next_unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take(cfg: SimConfig, n: usize) -> Vec<Tick> {
        let mut s = SimSource::new(cfg);
        (0..n).map_while(|_| s.next_tick()).collect()
    }

    #[test]
    fn same_seed_gives_same_stream() {
        let cfg = SimConfig {
            quotes: 8,
            seed: 42,
            ..Default::default()
        };
        assert_eq!(take(cfg, 500), take(cfg, 500));
    }

    #[test]
    fn different_seeds_diverge() {
        let a = SimConfig {
            quotes: 8,
            seed: 1,
            ..Default::default()
        };
        let b = SimConfig { seed: 2, ..a };
        assert_ne!(take(a, 100), take(b, 100));
    }

    #[test]
    fn seq_and_time_are_strictly_increasing() {
        let ticks = take(
            SimConfig {
                quotes: 4,
                ..Default::default()
            },
            200,
        );
        assert!(
            ticks
                .windows(2)
                .all(|w| w[1].seq == w[0].seq + 1 && w[1].ts_ns > w[0].ts_ns)
        );
    }

    #[test]
    fn quotes_stay_inside_the_owned_block() {
        let cfg = SimConfig {
            first_quote: 10,
            quotes: 5,
            ..Default::default()
        };
        assert!(take(cfg, 500).iter().all(|t| (10..15).contains(&t.quote.0)));
    }

    #[test]
    fn limit_ends_the_stream() {
        let cfg = SimConfig {
            limit: Some(7),
            ..Default::default()
        };
        assert_eq!(take(cfg, 100).len(), 7);
    }

    #[test]
    fn each_move_is_bounded_by_step() {
        let cfg = SimConfig {
            quotes: 1,
            ..Default::default()
        };
        let ticks = take(cfg, 1_000);
        let mut prev = cfg.start_value;
        for t in ticks {
            assert!((t.value - prev).abs() <= cfg.step);
            prev = t.value;
        }
    }

    #[test]
    fn zero_quotes_yields_nothing() {
        let cfg = SimConfig {
            quotes: 0,
            ..Default::default()
        };
        assert!(SimSource::new(cfg).next_tick().is_none());
    }
}
