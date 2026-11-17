//! One immutable, versioned view of portfolio risk.

use riskrt_core::{BookId, CurveId, Ladder, MarketVersion, PricedTrade, Risk, TradeId};
use riskrt_curves::{CurveSet, Versioned};
use riskrt_sched::PricingError;
use std::sync::Arc;

/// What the snapshot knows about one trade, beside its numbers.
///
/// A failed trade keeps its last good `PricedTrade` (if any) in the table and
/// in every total, the way a failed curve keeps its last good curve. The
/// status is how a reader learns those numbers stopped updating.
#[derive(Clone, Debug, PartialEq)]
pub enum TradeStatus {
    /// No result yet.
    Unpriced,
    /// The latest pricing round for this trade succeeded.
    Priced,
    /// The latest round failed. `since` is the first set version of the
    /// current run of failures; `error` is the most recent one.
    Failed {
        since: MarketVersion,
        error: PricingError,
    },
}

impl TradeStatus {
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }
}

/// Portfolio risk as of one batch.
///
/// `version` is the curve set version of the batch that produced it. Trades
/// priced against earlier sets keep their own versions in `trade(..)`; a
/// trade is only ever repriced when its curve moves, so an older version is
/// not stale unless `stale_count` says so.
#[derive(Clone, Debug)]
pub struct RiskSnapshot {
    version: MarketVersion,
    trades: Vec<Option<Arc<PricedTrade>>>,
    status: Vec<TradeStatus>,
    failed: usize,
    total: Risk,
    by_book: Vec<Risk>,
    by_curve: Vec<Risk>,
    ladders: Vec<Option<Ladder>>,
    priced: usize,
}

impl RiskSnapshot {
    /// Version zero: nothing priced.
    pub fn empty(trade_count: usize, book_count: usize, curve_count: usize) -> Self {
        Self {
            version: MarketVersion::ZERO,
            trades: vec![None; trade_count],
            status: vec![TradeStatus::Unpriced; trade_count],
            failed: 0,
            total: Risk::ZERO,
            by_book: vec![Risk::ZERO; book_count],
            by_curve: vec![Risk::ZERO; curve_count],
            ladders: vec![None; curve_count],
            priced: 0,
        }
    }

    /// Builds a snapshot from a per-trade table, recomputing every total.
    ///
    /// `books[t]` is the book of trade `t`. Trades are summed in id order,
    /// so the totals are the same whatever order the batches arrived in.
    pub(crate) fn from_table(
        version: MarketVersion,
        trades: Vec<Option<Arc<PricedTrade>>>,
        status: Vec<TradeStatus>,
        books: &[BookId],
        book_count: usize,
        curve_count: usize,
    ) -> Self {
        let mut total = Risk::ZERO;
        let mut by_book = vec![Risk::ZERO; book_count];
        let mut by_curve = vec![Risk::ZERO; curve_count];
        let mut ladders: Vec<Option<Ladder>> = vec![None; curve_count];
        let mut priced = 0;
        for (i, priced_trade) in trades.iter().enumerate() {
            let Some(p) = priced_trade else { continue };
            priced += 1;
            let risk = p.risk.risk;
            total += risk;
            by_book[books[i].index()] += risk;
            let curve = p.ladder.curve;
            by_curve[curve.index()] += risk;
            match &mut ladders[curve.index()] {
                Some(ladder) => {
                    ladder.add_assign(&p.ladder);
                }
                slot => *slot = Some(p.ladder.clone()),
            }
        }
        debug_assert_eq!(status.len(), trades.len());
        let failed = status.iter().filter(|s| s.is_failed()).count();
        Self {
            version,
            trades,
            status,
            failed,
            total,
            by_book,
            by_curve,
            ladders,
            priced,
        }
    }

    pub fn version(&self) -> MarketVersion {
        self.version
    }

    /// Whole-portfolio PV and DV01.
    pub fn total(&self) -> Risk {
        self.total
    }

    pub fn book(&self, book: BookId) -> Option<Risk> {
        self.by_book.get(book.index()).copied()
    }

    pub fn books(&self) -> &[Risk] {
        &self.by_book
    }

    pub fn curve(&self, curve: CurveId) -> Option<Risk> {
        self.by_curve.get(curve.index()).copied()
    }

    pub fn curves(&self) -> &[Risk] {
        &self.by_curve
    }

    /// Portfolio par-quote ladder on `curve`: PV change for +1bp on each of
    /// its input quotes. `None` until something on that curve has priced.
    pub fn ladder(&self, curve: CurveId) -> Option<&Ladder> {
        self.ladders.get(curve.index())?.as_ref()
    }

    /// Latest good result for a trade; `None` if it has never priced. Check
    /// `status` to learn whether this result is current.
    pub fn trade(&self, trade: TradeId) -> Option<&PricedTrade> {
        self.trades.get(trade.index())?.as_deref()
    }

    pub fn status(&self, trade: TradeId) -> Option<&TradeStatus> {
        self.status.get(trade.index())
    }

    /// One per trade, indexed by `TradeId`.
    pub fn statuses(&self) -> &[TradeStatus] {
        &self.status
    }

    /// Trades whose latest pricing round failed.
    pub fn failed_count(&self) -> usize {
        self.failed
    }

    pub fn failed(&self) -> impl Iterator<Item = (TradeId, &TradeStatus)> {
        self.status
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_failed())
            .map(|(i, s)| (TradeId(i as u32), s))
    }

    /// Every trade priced and none failed.
    pub fn is_healthy(&self) -> bool {
        self.is_complete() && self.failed == 0
    }

    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }

    /// Trades with at least one result.
    pub fn priced_count(&self) -> usize {
        self.priced
    }

    pub fn is_complete(&self) -> bool {
        self.priced == self.trades.len()
    }

    /// Trades whose result predates `since`. With `since` = the current
    /// curve set version this counts trades not yet caught up; a trade whose
    /// curve did not move is counted too, so treat it as an upper bound.
    pub fn older_than(&self, since: MarketVersion) -> usize {
        self.trades
            .iter()
            .flatten()
            .filter(|p| p.risk.version < since)
            .count()
    }

    /// Trades whose numbers cannot be trusted against `set`: their curve has
    /// been rebuilt since they were priced, their curve is degraded (its
    /// last rebuild failed, or it is on an older fixing index), or their own
    /// last pricing round failed. The first kind catches up on the next
    /// pricing round; the others do not until the curve or the trade
    /// recovers, which is why a count alone is not enough and the per-curve
    /// and per-trade statuses travel alongside it.
    pub fn stale_against(&self, set: &CurveSet) -> usize {
        self.trades
            .iter()
            .zip(&self.status)
            .filter(|(p, status)| match p {
                Some(p) => {
                    let curve = p.ladder.curve;
                    status.is_failed()
                        || set.health(curve).is_some_and(|h| h.is_degraded())
                        || set.changed_since(p.risk.version).any(|c| c == curve)
                }
                None => status.is_failed(),
            })
            .count()
    }

    pub(crate) fn table(&self) -> &[Option<Arc<PricedTrade>>] {
        &self.trades
    }
}

impl Versioned for RiskSnapshot {
    fn version(&self) -> MarketVersion {
        self.version
    }
}

const _: () = {
    const fn assert_shareable<T: Send + Sync>() {}
    assert_shareable::<RiskSnapshot>();
};
