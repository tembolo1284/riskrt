//! The set of trades being risk-managed.
//!
//! Immutable once built and shared between threads as `Arc<Portfolio>`. Intraday
//! trade changes will publish a whole new portfolio rather than mutate this one.

use crate::error::{Error, Result};
use crate::ids::TradeId;
use crate::position::Position;

#[derive(Clone, Debug, Default)]
pub struct Portfolio {
    positions: Vec<Position>,
    book_count: usize,
    curve_count: usize,
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<Portfolio>();
};

impl Portfolio {
    pub fn builder() -> PortfolioBuilder {
        PortfolioBuilder::default()
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub fn get(&self, id: TradeId) -> Result<&Position> {
        self.positions.get(id.index()).ok_or(Error::UnknownTrade(id))
    }

    /// Dense slice; a `TradeId` indexes straight into it.
    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = (TradeId, &Position)> {
        self.positions
            .iter()
            .enumerate()
            .map(|(i, p)| (TradeId(i as u32), p))
    }

    /// One past the highest `BookId` in use. Sizes dense per-book tables.
    pub fn book_count(&self) -> usize {
        self.book_count
    }

    /// One past the highest `CurveId` in use. Sizes dense per-curve tables.
    pub fn curve_count(&self) -> usize {
        self.curve_count
    }
}

#[derive(Debug, Default)]
pub struct PortfolioBuilder {
    inner: Portfolio,
}

impl PortfolioBuilder {
    /// Validates the position and assigns it the next dense `TradeId`.
    pub fn add(&mut self, position: Position) -> Result<TradeId> {
        let Ok(raw) = u32::try_from(self.inner.positions.len()) else {
            return Err(Error::CapacityExceeded);
        };
        let trade = TradeId(raw);
        position
            .validate()
            .map_err(|reason| Error::InvalidPosition { trade, reason })?;

        self.inner.book_count = self.inner.book_count.max(position.book.index() + 1);
        for curve in position.curves() {
            self.inner.curve_count = self.inner.curve_count.max(curve.index() + 1);
        }
        self.inner.positions.push(position);
        Ok(trade)
    }

    pub fn build(self) -> Portfolio {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{BookId, CurveId};
    use crate::position::{Day, Product};

    fn bond(book: u32, curve: u32) -> Position {
        Position {
            book: BookId(book),
            discount: CurveId(curve),
            forward: None,
            notional: 5_000_000.0,
            product: Product::Bond { maturity: Day(23_000), coupon: 0.0375 },
        }
    }

    #[test]
    fn ids_are_assigned_densely() {
        let mut b = Portfolio::builder();
        assert_eq!(b.add(bond(0, 0)).unwrap(), TradeId(0));
        assert_eq!(b.add(bond(1, 0)).unwrap(), TradeId(1));
        let p = b.build();
        assert_eq!(p.len(), 2);
        assert_eq!(p.get(TradeId(1)).unwrap().book, BookId(1));
    }

    #[test]
    fn counts_track_highest_id() {
        let mut b = Portfolio::builder();
        b.add(bond(3, 2)).unwrap();
        let p = b.build();
        assert_eq!(p.book_count(), 4);
        assert_eq!(p.curve_count(), 3);
    }

    #[test]
    fn invalid_position_is_rejected_and_not_stored() {
        let mut b = Portfolio::builder();
        let mut bad = bond(0, 0);
        bad.notional = f64::NAN;
        assert!(matches!(
            b.add(bad),
            Err(Error::InvalidPosition { trade: TradeId(0), .. })
        ));
        assert!(b.build().is_empty());
    }

    #[test]
    fn unknown_trade_is_an_error() {
        let p = Portfolio::builder().build();
        assert_eq!(p.get(TradeId(9)), Err(Error::UnknownTrade(TradeId(9))));
    }

    #[test]
    fn iter_yields_ids_in_order() {
        let mut b = Portfolio::builder();
        b.add(bond(0, 0)).unwrap();
        b.add(bond(0, 0)).unwrap();
        let p = b.build();
        let ids: Vec<_> = p.iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec![TradeId(0), TradeId(1)]);
    }
}
