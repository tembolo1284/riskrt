//! The index and dirty set must agree with a direct scan of the portfolio,
//! for portfolios and tick sequences nobody hand-picked.

use riskrt_core::{BookId, CurveId, Day, PayRec, Portfolio, Position, Product, TradeId};
use riskrt_deps::{CurveIndex, DirtySet};

/// SplitMix64, so the test needs no dependency and is reproducible.
struct Rng(u64);

impl Rng {
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

fn random_portfolio(rng: &mut Rng, trades: u32, curves: u32) -> Portfolio {
    let mut b = Portfolio::builder();
    for _ in 0..trades {
        let discount = CurveId(rng.below(curves));
        let position = if rng.below(3) == 0 {
            Position {
                book: BookId(rng.below(4)),
                discount,
                forward: None,
                notional: 5e6,
                product: Product::Bond {
                    maturity: Day(24_333),
                    coupon: 0.04,
                },
            }
        } else {
            Position {
                book: BookId(rng.below(4)),
                discount,
                // Often the same curve as discount, sometimes a different one.
                forward: Some(if rng.below(2) == 0 {
                    discount
                } else {
                    CurveId(rng.below(curves))
                }),
                notional: 1e7,
                product: Product::Swap {
                    start: Day(20_721),
                    maturity: Day(22_547),
                    fixed_rate: 0.04,
                    fixed_leg: PayRec::Pay,
                },
            }
        };
        b.add(position).unwrap();
    }
    b.build()
}

/// Ground truth: scan every position and ask whether it uses a dirty curve.
fn brute_force(portfolio: &Portfolio, dirty_curves: &[CurveId]) -> Vec<TradeId> {
    portfolio
        .iter()
        .filter(|(_, position)| position.curves().any(|c| dirty_curves.contains(&c)))
        .map(|(trade, _)| trade)
        .collect()
}

#[test]
fn index_matches_a_scan_of_the_portfolio() {
    let mut rng = Rng(7);
    for case in 0..200 {
        let curves = 1 + rng.below(8);
        let trades = rng.below(300);
        let portfolio = random_portfolio(&mut rng, trades, curves);
        let index = CurveIndex::build(&portfolio, curves as usize).unwrap();

        let mut edges = 0;
        for c in 0..curves {
            let expected = brute_force(&portfolio, &[CurveId(c)]);
            assert_eq!(
                index.trades_on(CurveId(c)),
                expected.as_slice(),
                "case {case}, curve {c}"
            );
            edges += expected.len();
        }
        assert_eq!(index.edge_count(), edges, "case {case}");
        assert_eq!(index.trade_count(), portfolio.len());
    }
}

#[test]
fn dirty_set_matches_a_scan_over_many_rounds() {
    let mut rng = Rng(11);
    for case in 0..100 {
        let curves = 1 + rng.below(8);
        let trades = 1 + rng.below(300);
        let portfolio = random_portfolio(&mut rng, trades, curves);
        let index = CurveIndex::build(&portfolio, curves as usize).unwrap();
        let mut dirty = DirtySet::new(portfolio.len());
        let mut batch = Vec::new();

        // Reuse the same set across rounds, as the scheduler will.
        for round in 0..20 {
            // A burst of curve ticks, with repeats, in arbitrary order.
            let ticked: Vec<CurveId> = (0..rng.below(6))
                .map(|_| CurveId(rng.below(curves)))
                .collect();
            let newly = dirty.mark_curves(&index, ticked.iter().copied());

            let expected = brute_force(&portfolio, &ticked);
            assert_eq!(newly, expected.len(), "case {case}, round {round}");
            assert_eq!(dirty.len(), expected.len());

            batch.clear();
            dirty.drain_into(&mut batch);
            assert_eq!(batch, expected, "case {case}, round {round}");
            assert!(dirty.is_empty());
        }
    }
}

#[test]
fn marks_accumulate_across_ticks_until_drained() {
    let mut rng = Rng(23);
    let curves = 6;
    let portfolio = random_portfolio(&mut rng, 500, curves);
    let index = CurveIndex::build(&portfolio, curves as usize).unwrap();
    let mut dirty = DirtySet::new(portfolio.len());

    // Three separate curve-set updates arrive before the workers are free.
    let updates = [
        vec![CurveId(0)],
        vec![CurveId(3), CurveId(0)],
        vec![CurveId(5)],
    ];
    for update in &updates {
        dirty.mark_curves(&index, update.iter().copied());
    }
    let mut batch = Vec::new();
    dirty.drain_into(&mut batch);

    let all: Vec<CurveId> = updates.concat();
    assert_eq!(batch, brute_force(&portfolio, &all));
}
