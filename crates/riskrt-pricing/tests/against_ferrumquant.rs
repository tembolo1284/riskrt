//! The adapter must give the same numbers as calling ferrumquant by hand,
//! with riskrt's sign and scaling conventions on top.

use fq_core::time::Date;
use fq_rates::bond::FixedRateBond;
use fq_rates::index::OvernightIndex;
use fq_rates::swap::{OisSwap, SwapSide};
use riskrt_core::{
    BookId, CurveId, Day, MarketVersion, PayRec, Portfolio, Position, Product, QuoteId, Tick,
    TradeId,
};
use riskrt_core::{Ladder, PricedTrade, Risk, TradeRisk};
use riskrt_curves::{BP, BuildConfig, CurveBuilder, CurveSet, CurveSetSpec, CurveSpec};
use riskrt_market::QuoteSnapshot;
use riskrt_pricing::{FqPricer, Pricer, PricingError, price_batch, validate};
use std::sync::Arc;

const TODAY: Day = Day(20_719); // 2026-09-23
const QUOTES: [(&str, f64); 10] = [
    ("1M", 0.0430),
    ("3M", 0.0425),
    ("6M", 0.0415),
    ("1Y", 0.0400),
    ("2Y", 0.0380),
    ("3Y", 0.0375),
    ("5Y", 0.0378),
    ("7Y", 0.0385),
    ("10Y", 0.0395),
    ("30Y", 0.0410),
];

fn today() -> Date {
    Date::from_unix_days(TODAY.0).unwrap()
}

fn day(y: i32, m: u32, d: u32) -> Day {
    Day(Date::from_ymd(y, m, d).unwrap().unix_days())
}

fn swap(notional: f64, rate: f64, leg: PayRec) -> Position {
    Position {
        book: BookId(0),
        discount: CurveId(0),
        forward: Some(CurveId(0)),
        notional,
        product: Product::Swap {
            start: day(2026, 9, 25),
            maturity: day(2031, 9, 25),
            fixed_rate: rate,
            fixed_leg: leg,
        },
    }
}

fn bond(notional: f64) -> Position {
    Position {
        book: BookId(1),
        discount: CurveId(0),
        forward: None,
        notional,
        product: Product::Bond {
            maturity: day(2036, 8, 15),
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

/// One built SOFR curve at set version 1.
fn curves() -> Arc<CurveSet> {
    let pillars: Vec<_> = QUOTES
        .iter()
        .enumerate()
        .map(|(k, (t, _))| (*t, QuoteId(k as u32)))
        .collect();
    let spec = CurveSetSpec::new(
        TODAY,
        vec![CurveSpec::sofr_ois(CurveId(0), "SOFR", &pillars).unwrap()],
    )
    .unwrap();
    let mut builder = CurveBuilder::new(
        spec,
        Arc::new(OvernightIndex::sofr()),
        BuildConfig::default(),
    );
    let batch: Vec<_> = QUOTES
        .iter()
        .enumerate()
        .map(|(k, (_, r))| Tick::new(QuoteId(k as u32), 1, 0, *r))
        .collect();
    builder
        .on_snapshot(&QuoteSnapshot::empty(10).next(&batch).unwrap())
        .unwrap()
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

#[test]
fn payer_swap_matches_ferrumquant_directly() {
    let curves = curves();
    let pricer = FqPricer::build(&portfolio(&[swap(1e7, 0.035, PayRec::Pay)]), today()).unwrap();
    let priced = pricer.price(TradeId(0), &curves).unwrap();

    let index = OvernightIndex::sofr();
    let direct = OisSwap::usd_sofr(
        Date::from_ymd(2026, 9, 25).unwrap(),
        Date::from_ymd(2031, 9, 25).unwrap(),
        SwapSide::PayFixed,
        1e7,
        0.035,
    )
    .unwrap();
    let c = curves.curve(CurveId(0)).unwrap().curve();
    assert!(close(priced.risk.risk.pv, direct.npv(&index, c).unwrap()));
    assert!(close(
        priced.risk.risk.dv01,
        direct.dv01(&index, c).unwrap()
    ));
    assert!(
        priced.risk.risk.pv > 0.0,
        "paying 3.5% on a ~3.8% curve is in the money"
    );
    assert!(priced.risk.risk.dv01 > 0.0, "a payer gains when rates rise");
    assert_eq!(priced.risk.version, MarketVersion(1));
    assert_eq!(priced.risk.trade, TradeId(0));
    assert_eq!(priced.ladder.curve, CurveId(0));
    assert_eq!(priced.ladder.per_bp.len(), 10);
    // Par ladder: a 5Y spot swap loads the 5Y pillar and nothing past it.
    let ladder = &priced.ladder.per_bp;
    assert!(ladder[6] / priced.ladder.total() > 0.95, "{ladder:?}");
    assert!(ladder[7].abs() < 1e-6 && ladder[9].abs() < 1e-6);
    // Ladder total is the par-quote DV01: same order as the zero-rate one.
    assert!((priced.ladder.total() - priced.risk.risk.dv01).abs() < 0.03 * priced.risk.risk.dv01);
}

#[test]
fn receiver_mirrors_payer() {
    let curves = curves();
    let p = portfolio(&[
        swap(1e7, 0.035, PayRec::Pay),
        swap(1e7, 0.035, PayRec::Receive),
    ]);
    let pricer = FqPricer::build(&p, today()).unwrap();
    let pay = pricer.price(TradeId(0), &curves).unwrap();
    let rec = pricer.price(TradeId(1), &curves).unwrap();
    assert!(close(pay.risk.risk.pv, -rec.risk.risk.pv));
    assert!(close(pay.risk.risk.dv01, -rec.risk.risk.dv01));
    for (a, b) in pay.ladder.per_bp.iter().zip(&rec.ladder.per_bp) {
        assert!(close(*a, -b));
    }
}

#[test]
fn bond_scales_by_notional_over_face_and_shorts_flip_sign() {
    let curves = curves();
    let p = portfolio(&[bond(5e6), bond(-2e6)]);
    let pricer = FqPricer::build(&p, today()).unwrap();
    let long = pricer.price(TradeId(0), &curves).unwrap();
    let short = pricer.price(TradeId(1), &curves).unwrap();

    let c = curves.curve(CurveId(0)).unwrap().curve();
    let d = |y, m, dd| Date::from_ymd(y, m, dd).unwrap();
    let direct = FixedRateBond::us_treasury(d(2026, 8, 15), d(2036, 8, 15), 0.0425, None).unwrap();
    let per_face_pv = direct.npv(c).unwrap();
    let per_face_dv01 = direct.dv01(c).unwrap();

    assert!(close(long.risk.risk.pv, per_face_pv * 5e6 / 100.0));
    assert!(close(long.risk.risk.dv01, per_face_dv01 * 5e6 / 100.0));
    assert!(
        long.risk.risk.dv01 < 0.0,
        "a long bond loses when rates rise"
    );
    assert!(close(short.risk.risk.pv, per_face_pv * -2e6 / 100.0));
    assert!(short.risk.risk.dv01 > 0.0);
    // Ten years out: the 10Y bucket carries most of it.
    assert!(long.ladder.per_bp[8] / long.ladder.total() > 0.8);
}

#[test]
fn bond_schedule_is_anchored_to_maturity_whatever_the_derived_start() {
    // The adapter invents a dated date; the priced coupons must still fall
    // on the Feb/Aug 15 cycle, i.e. the same PV as a bond issued 2026-08-15.
    let curves = curves();
    let pricer = FqPricer::build(&portfolio(&[bond(100.0)]), today()).unwrap();
    let priced = pricer.price(TradeId(0), &curves).unwrap();
    let d = |y, m, dd| Date::from_ymd(y, m, dd).unwrap();
    let issued = FixedRateBond::us_treasury(d(2026, 8, 15), d(2036, 8, 15), 0.0425, None).unwrap();
    assert!(close(
        priced.risk.risk.pv,
        issued
            .npv(curves.curve(CurveId(0)).unwrap().curve())
            .unwrap()
    ));
}

#[test]
fn batch_is_deterministic_across_thread_counts() {
    let curves = curves();
    let positions: Vec<_> = (0..40)
        .map(|i| match i % 3 {
            0 => swap(1e6 * (i + 1) as f64, 0.03 + 0.001 * i as f64, PayRec::Pay),
            1 => swap(
                1e6 * (i + 1) as f64,
                0.03 + 0.001 * i as f64,
                PayRec::Receive,
            ),
            _ => bond(1e6 * (i as f64 - 20.0)),
        })
        .filter(|p| p.notional != 0.0)
        .collect();
    let pricer = FqPricer::build(&portfolio(&positions), today()).unwrap();
    let trades: Vec<_> = (0..pricer.trade_count() as u32).map(TradeId).collect();
    let run = |threads: usize| {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| price_batch(&pricer, &trades, &curves))
    };
    let one = run(1);
    let eight = run(8);
    assert_eq!(one.len(), trades.len());
    assert!(one.iter().all(Result::is_ok));
    assert_eq!(one, eight);
    for (t, r) in trades.iter().zip(&one) {
        assert_eq!(r.as_ref().unwrap().risk.trade, *t);
    }
}

#[test]
fn errors_are_reported_per_trade() {
    let two_curve = Position {
        forward: Some(CurveId(1)),
        ..swap(1e6, 0.04, PayRec::Pay)
    };
    let err = FqPricer::build(&portfolio(&[two_curve]), today()).unwrap_err();
    assert!(matches!(
        err,
        PricingError::TwoCurveSwap {
            trade: TradeId(0),
            ..
        }
    ));

    let pricer = FqPricer::build(&portfolio(&[swap(1e6, 0.04, PayRec::Pay)]), today()).unwrap();
    let curves = curves();
    assert!(matches!(
        pricer.price(TradeId(5), &curves),
        Err(PricingError::UnknownTrade(TradeId(5)))
    ));
    assert_eq!(pricer.curve_of(TradeId(0)), Some(CurveId(0)));

    // A curve set with nothing built yet.
    let empty = CurveSet::empty(1, today(), Arc::new(OvernightIndex::sofr()));
    assert!(matches!(
        pricer.price(TradeId(0), &empty),
        Err(PricingError::CurveNotBuilt {
            trade: TradeId(0),
            curve: CurveId(0)
        })
    ));
    let batch = price_batch(&pricer, &[TradeId(0), TradeId(9)], &curves);
    assert!(batch[0].is_ok());
    assert!(matches!(batch[1], Err(PricingError::UnknownTrade(_))));
    let _ = BP;
}

#[test]
fn a_trade_is_priced_with_the_index_its_curve_was_built_with() {
    // Curve built on the start-of-day index, then the set's index moves on
    // while the curve (say it failed to rebuild) stays behind. The trade
    // must keep pricing against the old index: that is what the curve was
    // calibrated with, and the set reports the curve IndexStale instead.
    let old_set = curves();
    let old_curve = Arc::clone(old_set.curve(CurveId(0)).unwrap());
    let mut fixed = OvernightIndex::sofr();
    fixed.add_fixing(today(), 0.0431);
    let moved = CurveSet::empty(1, today(), Arc::new(OvernightIndex::sofr())).next(
        MarketVersion(1),
        Arc::new(fixed),
        vec![(*old_curve).clone()],
        vec![],
    );
    assert_eq!(
        moved.health(CurveId(0)).unwrap().status(),
        riskrt_curves::CurveStatus::IndexStale
    );

    let pricer = FqPricer::build(&portfolio(&[swap(1e7, 0.035, PayRec::Pay)]), today()).unwrap();
    let on_old = pricer.price(TradeId(0), &old_set).unwrap();
    let on_moved = pricer.price(TradeId(0), &moved).unwrap();
    assert_eq!(
        on_old.risk.risk, on_moved.risk.risk,
        "same curve, same index, same numbers"
    );
}

/// An adapter that lies: wraps the real pricer and corrupts one trade.
struct Corrupting<F: Fn(&mut PricedTrade) + Send + Sync> {
    inner: FqPricer,
    victim: TradeId,
    corrupt: F,
}

impl<F: Fn(&mut PricedTrade) + Send + Sync> Pricer for Corrupting<F> {
    fn price(&self, trade: TradeId, curves: &CurveSet) -> Result<PricedTrade, PricingError> {
        let mut p = self.inner.price(trade, curves)?;
        if trade == self.victim {
            (self.corrupt)(&mut p);
        }
        Ok(p)
    }
    fn trade_count(&self) -> usize {
        self.inner.trade_count()
    }
}

#[test]
fn validate_accepts_what_the_real_adapter_produces() {
    let set = curves();
    let pricer = FqPricer::build(
        &portfolio(&[swap(1e7, 0.035, PayRec::Pay), bond(5e6)]),
        today(),
    )
    .unwrap();
    for t in 0..2 {
        let p = pricer.price(TradeId(t), &set).unwrap();
        validate(&p, TradeId(t), &set).unwrap();
    }
}

#[test]
fn price_batch_turns_a_bad_result_into_a_failed_trade() {
    let set = curves();
    let portfolio = portfolio(&[
        swap(1e7, 0.035, PayRec::Pay),
        swap(1e7, 0.035, PayRec::Receive),
        bond(5e6),
    ]);
    let inner = || FqPricer::build(&portfolio, today()).unwrap();
    let trades = [TradeId(0), TradeId(1), TradeId(2)];
    let reason = |r: &Result<PricedTrade, PricingError>| match r {
        Err(PricingError::Invalid { trade, reason }) => Some((*trade, reason.clone())),
        _ => None,
    };

    let nan = Corrupting {
        inner: inner(),
        victim: TradeId(1),
        corrupt: |p: &mut PricedTrade| p.risk.risk.pv = f64::NAN,
    };
    let out = price_batch(&nan, &trades, &set);
    assert!(
        out[0].is_ok() && out[2].is_ok(),
        "the others are unaffected"
    );
    let (t, why) = reason(&out[1]).expect("NaN is rejected");
    assert_eq!(t, TradeId(1));
    assert!(why.contains("non-finite risk"), "{why}");

    let short = Corrupting {
        inner: inner(),
        victim: TradeId(0),
        corrupt: |p: &mut PricedTrade| {
            p.ladder.per_bp.pop();
        },
    };
    let why = reason(&price_batch(&short, &trades, &set)[0]).unwrap().1;
    assert!(why.contains("buckets"), "{why}");

    let inf_bucket = Corrupting {
        inner: inner(),
        victim: TradeId(2),
        corrupt: |p: &mut PricedTrade| p.ladder.per_bp[3] = f64::INFINITY,
    };
    let why = reason(&price_batch(&inf_bucket, &trades, &set)[2])
        .unwrap()
        .1;
    assert!(why.contains("bucket 3"), "{why}");

    let wrong_trade = Corrupting {
        inner: inner(),
        victim: TradeId(0),
        corrupt: |p: &mut PricedTrade| p.risk.trade = TradeId(2),
    };
    let why = reason(&price_batch(&wrong_trade, &trades, &set)[0])
        .unwrap()
        .1;
    assert!(why.contains("result is for"), "{why}");

    let old_version = Corrupting {
        inner: inner(),
        victim: TradeId(0),
        corrupt: |p: &mut PricedTrade| p.risk.version = MarketVersion::ZERO,
    };
    let why = reason(&price_batch(&old_version, &trades, &set)[0])
        .unwrap()
        .1;
    assert!(why.contains("priced against set"), "{why}");

    let no_curve = Corrupting {
        inner: inner(),
        victim: TradeId(0),
        corrupt: |p: &mut PricedTrade| p.ladder.curve = CurveId(7),
    };
    let why = reason(&price_batch(&no_curve, &trades, &set)[0]).unwrap().1;
    assert!(why.contains("not built"), "{why}");

    // A hand-made result on an unbuilt curve of an empty set.
    let empty = CurveSet::empty(1, today(), Arc::new(OvernightIndex::sofr()));
    let fake = PricedTrade {
        risk: TradeRisk::new(TradeId(0), empty.version(), Risk::ZERO),
        ladder: Ladder::zero(CurveId(0), 10),
    };
    assert!(matches!(
        validate(&fake, TradeId(0), &empty),
        Err(PricingError::Invalid { .. })
    ));
}
