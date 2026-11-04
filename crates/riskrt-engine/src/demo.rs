//! Synthetic inputs shared by the CLI, tests and benches.

use fq_core::time::Date;
use riskrt_core::{BookId, CurveId, Day, PayRec, Portfolio, Position, Product, QuoteId};
use riskrt_curves::{CurveSetSpec, CurveSpec};
use riskrt_feed::{SimConfig, SimSource, TickSource};

pub const TENORS: [&str; 10] = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "30Y"];

/// 2026-09-23 as days since the Unix epoch.
pub const VALUATION: Day = Day(20_719);

/// Quote id of pillar `k` on curve `c`.
pub fn quote_id(c: u32, k: usize) -> QuoteId {
    QuoteId(c * TENORS.len() as u32 + k as u32)
}

/// `curves` OIS curves, each with the ten standard pillars on its own quotes.
pub fn curve_specs(curves: u32) -> Result<CurveSetSpec, riskrt_curves::CurveBuildError> {
    let specs = (0..curves)
        .map(|c| {
            let pillars: Vec<_> = TENORS
                .iter()
                .enumerate()
                .map(|(k, t)| (*t, quote_id(c, k)))
                .collect();
            CurveSpec::sofr_ois(CurveId(c), &format!("SOFR-{c}"), &pillars)
        })
        .collect::<Result<Vec<_>, _>>()?;
    CurveSetSpec::new(VALUATION, specs)
}

/// A deterministic mix of swaps and bonds spread over `curves` and `books`.
pub fn portfolio(trades: u32, curves: u32, books: u32, seed: u64) -> Portfolio {
    let d = |y, m, dd| Day(Date::from_ymd(y, m, dd).expect("valid date").unix_days());
    let mut rng = SplitMix64(seed);
    let mut b = Portfolio::builder();
    for _ in 0..trades {
        let curve = CurveId(rng.below(curves));
        let book = BookId(rng.below(books));
        let position = if rng.below(5) == 0 {
            let lots = f64::from(rng.below(9)) - 4.0;
            Position {
                book,
                discount: curve,
                forward: None,
                notional: 1e6 * if lots == 0.0 { 1.0 } else { lots },
                product: Product::Bond {
                    maturity: d(2029 + rng.below(8) as i32, 8, 15),
                    coupon: 0.03 + 0.0025 * f64::from(rng.below(8)),
                },
            }
        } else {
            Position {
                book,
                discount: curve,
                forward: Some(curve),
                notional: 1e6 * f64::from(1 + rng.below(50)),
                product: Product::Swap {
                    start: d(2026, 9, 25),
                    maturity: d(2027 + rng.below(20) as i32, 9, 25),
                    fixed_rate: 0.025 + 0.0005 * f64::from(rng.below(50)),
                    fixed_leg: if rng.below(2) == 0 {
                        PayRec::Pay
                    } else {
                        PayRec::Receive
                    },
                },
            }
        };
        b.add(position).expect("synthetic position is valid");
    }
    b.build()
}

/// One simulated feed per curve, each a random walk around 4%.
/// `limit` bounds ticks per feed; `None` runs until the engine is stopped.
pub fn sim_feeds(curves: u32, seed: u64, limit: Option<u64>) -> Vec<Box<dyn TickSource>> {
    (0..curves)
        .map(|c| {
            let cfg = SimConfig {
                first_quote: quote_id(c, 0).0,
                quotes: TENORS.len() as u32,
                seed: seed + u64::from(c),
                start_value: 0.04,
                limit,
                ..Default::default()
            };
            Box::new(SimSource::new(cfg)) as Box<dyn TickSource>
        })
        .collect()
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u32) -> u32 {
        (self.next() % u64::from(n)) as u32
    }
}
