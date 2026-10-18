//! The inverted dependency map: curve -> trades.

use riskrt_core::{CurveId, Portfolio, TradeId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DepsError {
    /// A trade references a curve the engine does not build.
    UnknownCurve {
        trade: TradeId,
        curve: CurveId,
        curve_count: usize,
    },
}

impl core::fmt::Display for DepsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownCurve {
                trade,
                curve,
                curve_count,
            } => {
                write!(
                    f,
                    "{trade} depends on {curve}, but only {curve_count} curves are built"
                )
            }
        }
    }
}

impl std::error::Error for DepsError {}

/// For each curve, the trades that depend on it.
///
/// Stored as two flat arrays (compressed sparse rows): `trades` holds every
/// (curve, trade) pair grouped by curve, and `offsets[c]..offsets[c + 1]` is
/// curve `c`'s slice of it. A lookup is two array reads and returns a
/// contiguous slice, with no hashing and no per-curve allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurveIndex {
    offsets: Vec<u32>,
    trades: Vec<TradeId>,
    trade_count: usize,
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<CurveIndex>();
};

impl CurveIndex {
    /// Builds the index for `portfolio` against an engine that builds curves
    /// `0..curve_count`. Fails if a trade references a curve outside that
    /// range, which would otherwise be a trade that silently never reprices.
    pub fn build(portfolio: &Portfolio, curve_count: usize) -> Result<Self, DepsError> {
        // Pass 1: count dependants per curve.
        let mut counts = vec![0u32; curve_count];
        for (trade, position) in portfolio.iter() {
            for curve in position.curves() {
                let Some(count) = counts.get_mut(curve.index()) else {
                    return Err(DepsError::UnknownCurve {
                        trade,
                        curve,
                        curve_count,
                    });
                };
                *count += 1;
            }
        }

        // Prefix sums turn counts into each curve's start offset.
        let mut offsets = Vec::with_capacity(curve_count + 1);
        let mut total = 0u32;
        offsets.push(0);
        for count in &counts {
            total += count;
            offsets.push(total);
        }

        // Pass 2: place each trade. Trades are visited in id order, so each
        // curve's slice comes out sorted by `TradeId`.
        let mut cursor: Vec<u32> = offsets[..curve_count].to_vec();
        let mut trades = vec![TradeId(0); total as usize];
        for (trade, position) in portfolio.iter() {
            for curve in position.curves() {
                let slot = &mut cursor[curve.index()];
                trades[*slot as usize] = trade;
                *slot += 1;
            }
        }

        Ok(Self {
            offsets,
            trades,
            trade_count: portfolio.len(),
        })
    }

    /// Trades that depend on `curve`, in ascending id order. Empty for a
    /// curve nothing depends on, or an id outside the index.
    pub fn trades_on(&self, curve: CurveId) -> &[TradeId] {
        let i = curve.index();
        match (self.offsets.get(i), self.offsets.get(i + 1)) {
            (Some(&start), Some(&end)) => &self.trades[start as usize..end as usize],
            _ => &[],
        }
    }

    /// Number of trades that depend on `curve`.
    pub fn fan_out(&self, curve: CurveId) -> usize {
        self.trades_on(curve).len()
    }

    pub fn curve_count(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Number of trades in the portfolio this index was built from.
    pub fn trade_count(&self) -> usize {
        self.trade_count
    }

    /// Total (curve, trade) dependency pairs.
    pub fn edge_count(&self) -> usize {
        self.trades.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riskrt_core::{BookId, Day, PayRec, Position, Product};

    fn swap(discount: u32, forward: u32) -> Position {
        Position {
            book: BookId(0),
            discount: CurveId(discount),
            forward: Some(CurveId(forward)),
            notional: 1e7,
            product: Product::Swap {
                start: Day(20_721),
                maturity: Day(22_547),
                fixed_rate: 0.04,
                fixed_leg: PayRec::Pay,
            },
        }
    }

    fn bond(curve: u32) -> Position {
        Position {
            book: BookId(0),
            discount: CurveId(curve),
            forward: None,
            notional: 5e6,
            product: Product::Bond {
                maturity: Day(24_333),
                coupon: 0.0425,
            },
        }
    }

    fn portfolio(positions: &[Position]) -> Portfolio {
        let mut b = Portfolio::builder();
        for p in positions {
            b.add(*p).unwrap();
        }
        b.build()
    }

    fn ids(trades: &[TradeId]) -> Vec<u32> {
        trades.iter().map(|t| t.0).collect()
    }

    #[test]
    fn groups_trades_by_curve_in_id_order() {
        // trade: 0 -> c0, 1 -> c1, 2 -> c0, 3 -> c2, 4 -> c0
        let p = portfolio(&[swap(0, 0), bond(1), swap(0, 0), bond(2), bond(0)]);
        let index = CurveIndex::build(&p, 3).unwrap();
        assert_eq!(ids(index.trades_on(CurveId(0))), vec![0, 2, 4]);
        assert_eq!(ids(index.trades_on(CurveId(1))), vec![1]);
        assert_eq!(ids(index.trades_on(CurveId(2))), vec![3]);
        assert_eq!(
            (index.curve_count(), index.trade_count(), index.edge_count()),
            (3, 5, 5)
        );
    }

    #[test]
    fn two_curve_trade_appears_under_both_curves() {
        let p = portfolio(&[swap(0, 1), swap(1, 1), bond(0)]);
        let index = CurveIndex::build(&p, 2).unwrap();
        assert_eq!(ids(index.trades_on(CurveId(0))), vec![0, 2]);
        assert_eq!(ids(index.trades_on(CurveId(1))), vec![0, 1]);
        // Trade 1 uses curve 1 for both legs but is listed once.
        assert_eq!(index.edge_count(), 4);
    }

    #[test]
    fn curve_with_no_dependants_and_unknown_curve_are_empty() {
        let p = portfolio(&[bond(0), bond(2)]);
        let index = CurveIndex::build(&p, 4).unwrap();
        assert!(index.trades_on(CurveId(1)).is_empty());
        assert!(index.trades_on(CurveId(3)).is_empty());
        assert!(index.trades_on(CurveId(99)).is_empty());
        assert_eq!(index.fan_out(CurveId(2)), 1);
    }

    #[test]
    fn empty_portfolio_gives_an_empty_index() {
        let index = CurveIndex::build(&portfolio(&[]), 2).unwrap();
        assert_eq!(
            (index.curve_count(), index.trade_count(), index.edge_count()),
            (2, 0, 0)
        );
        assert!(index.trades_on(CurveId(0)).is_empty());
    }

    #[test]
    fn trade_on_a_curve_the_engine_does_not_build_is_an_error() {
        let p = portfolio(&[bond(0), swap(0, 3)]);
        assert_eq!(
            CurveIndex::build(&p, 2),
            Err(DepsError::UnknownCurve {
                trade: TradeId(1),
                curve: CurveId(3),
                curve_count: 2
            })
        );
    }
}
