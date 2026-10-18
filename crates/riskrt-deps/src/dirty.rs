//! The set of trades waiting to be repriced.

use crate::index::CurveIndex;
use riskrt_core::{CurveId, TradeId};

/// Trades marked dirty since the last drain, each at most once.
///
/// A flag per trade makes marking O(1) and idempotent; a side list of the
/// marked ids makes draining cost the number of dirty trades, not the size of
/// the portfolio. Owned by a single thread.
#[derive(Clone, Debug)]
pub struct DirtySet {
    marked: Vec<bool>,
    list: Vec<TradeId>,
}

impl DirtySet {
    /// An empty set for a portfolio of `trade_count` trades.
    pub fn new(trade_count: usize) -> Self {
        Self {
            marked: vec![false; trade_count],
            list: Vec::new(),
        }
    }

    /// Marks one trade. Returns `true` if it was not already marked.
    /// An id outside the portfolio is ignored and returns `false`.
    pub fn mark(&mut self, trade: TradeId) -> bool {
        match self.marked.get_mut(trade.index()) {
            Some(flag) if !*flag => {
                *flag = true;
                self.list.push(trade);
                true
            }
            _ => false,
        }
    }

    /// Marks every trade that depends on `curve`. Returns how many were newly
    /// marked, which is less than the curve's fan-out when some were already
    /// dirty through another curve.
    pub fn mark_curve(&mut self, index: &CurveIndex, curve: CurveId) -> usize {
        debug_assert_eq!(
            index.trade_count(),
            self.marked.len(),
            "index and dirty set are for different portfolios"
        );
        index
            .trades_on(curve)
            .iter()
            .filter(|trade| self.mark(**trade))
            .count()
    }

    /// Marks every trade that depends on any of `curves`, e.g. the output of
    /// `CurveSet::changed_since`. Returns how many were newly marked.
    pub fn mark_curves(
        &mut self,
        index: &CurveIndex,
        curves: impl IntoIterator<Item = CurveId>,
    ) -> usize {
        curves
            .into_iter()
            .map(|curve| self.mark_curve(index, curve))
            .sum()
    }

    /// Marks the whole portfolio, e.g. on start-up. Returns how many were
    /// newly marked.
    pub fn mark_all(&mut self) -> usize {
        (0..self.marked.len() as u32)
            .filter(|i| self.mark(TradeId(*i)))
            .count()
    }

    pub fn contains(&self, trade: TradeId) -> bool {
        self.marked.get(trade.index()).copied().unwrap_or(false)
    }

    /// Number of trades currently marked.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Size of the portfolio this set covers.
    pub fn capacity(&self) -> usize {
        self.marked.len()
    }

    /// Appends every marked trade to `out` in ascending id order and clears
    /// the set. Returns how many were appended.
    ///
    /// The order is the same however the trades came to be marked, so the
    /// batches handed to workers do not depend on which curve ticked first.
    pub fn drain_into(&mut self, out: &mut Vec<TradeId>) -> usize {
        let n = self.list.len();
        self.list.sort_unstable();
        for trade in self.list.drain(..) {
            self.marked[trade.index()] = false;
            out.push(trade);
        }
        n
    }

    /// Unmarks everything without reporting it.
    pub fn clear(&mut self) {
        for trade in self.list.drain(..) {
            self.marked[trade.index()] = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riskrt_core::{BookId, Day, PayRec, Portfolio, Position, Product};

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

    /// trade: 0 -> c0, 1 -> c0+c1, 2 -> c1, 3 -> c2, 4 -> c0
    fn fixture() -> (CurveIndex, DirtySet) {
        let mut b = Portfolio::builder();
        for p in [swap(0, 0), swap(0, 1), swap(1, 1), swap(2, 2), swap(0, 0)] {
            b.add(p).unwrap();
        }
        let portfolio = b.build();
        (
            CurveIndex::build(&portfolio, 3).unwrap(),
            DirtySet::new(portfolio.len()),
        )
    }

    fn drained(set: &mut DirtySet) -> Vec<u32> {
        let mut out = Vec::new();
        set.drain_into(&mut out);
        out.iter().map(|t| t.0).collect()
    }

    #[test]
    fn mark_is_idempotent() {
        let (_, mut set) = fixture();
        assert!(set.mark(TradeId(2)));
        assert!(!set.mark(TradeId(2)));
        assert_eq!(set.len(), 1);
        assert!(set.contains(TradeId(2)));
        assert!(!set.contains(TradeId(3)));
    }

    #[test]
    fn out_of_range_trade_is_ignored() {
        let (_, mut set) = fixture();
        assert!(!set.mark(TradeId(99)));
        assert!(!set.contains(TradeId(99)));
        assert!(set.is_empty());
    }

    #[test]
    fn mark_curve_marks_exactly_its_dependants() {
        let (index, mut set) = fixture();
        assert_eq!(set.mark_curve(&index, CurveId(0)), 3);
        assert_eq!(drained(&mut set), vec![0, 1, 4]);
    }

    #[test]
    fn trade_on_two_dirty_curves_is_queued_once() {
        let (index, mut set) = fixture();
        assert_eq!(set.mark_curve(&index, CurveId(0)), 3); // trades 0, 1, 4
        assert_eq!(set.mark_curve(&index, CurveId(1)), 1); // trade 2; trade 1 already dirty
        assert_eq!(drained(&mut set), vec![0, 1, 2, 4]);
    }

    #[test]
    fn mark_curves_takes_any_iterator_of_curves() {
        let (index, mut set) = fixture();
        let newly = set.mark_curves(&index, [CurveId(2), CurveId(1), CurveId(1)]);
        assert_eq!(newly, 3);
        assert_eq!(drained(&mut set), vec![1, 2, 3]);
        assert_eq!(set.mark_curves(&index, std::iter::empty()), 0);
    }

    #[test]
    fn drain_order_does_not_depend_on_mark_order() {
        let (index, mut a) = fixture();
        let mut b = a.clone();
        a.mark_curves(&index, [CurveId(0), CurveId(1), CurveId(2)]);
        b.mark_curves(&index, [CurveId(2), CurveId(1), CurveId(0)]);
        assert_eq!(drained(&mut a), drained(&mut b));
    }

    #[test]
    fn drain_clears_and_the_set_is_reusable() {
        let (index, mut set) = fixture();
        set.mark_curve(&index, CurveId(0));
        assert_eq!(drained(&mut set).len(), 3);
        assert!(set.is_empty());
        assert!(!set.contains(TradeId(0)));
        assert_eq!(drained(&mut set), Vec::<u32>::new());
        // Marking works again after a drain.
        assert_eq!(set.mark_curve(&index, CurveId(0)), 3);
    }

    #[test]
    fn drain_appends_to_the_output() {
        let (index, mut set) = fixture();
        let mut out = vec![TradeId(77)];
        set.mark_curve(&index, CurveId(2));
        assert_eq!(set.drain_into(&mut out), 1);
        assert_eq!(out, vec![TradeId(77), TradeId(3)]);
    }

    #[test]
    fn mark_all_and_clear() {
        let (_, mut set) = fixture();
        set.mark(TradeId(1));
        assert_eq!(set.mark_all(), 4);
        assert_eq!((set.len(), set.capacity()), (5, 5));
        set.clear();
        assert!(set.is_empty());
        assert!(!set.contains(TradeId(1)));
        assert_eq!(set.mark_all(), 5);
    }
}
