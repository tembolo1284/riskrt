[Back to README](../README.md)

# Finance and the ferrumquant kernel

What riskrt prices, the conventions it holds the kernel to, and what the next product slice needs.

# What riskrt asked of ferrumquant

riskrt drove several changes in ferrumquant, all made so the library could serve a real-time engine:

- A telescoping fast path in `OvernightIndex::compounded_factor`. For plain compounding over a future period with no stored fixings, the daily product collapses to `DF(start) / DF(end)`. A 30-year swap NPV went from about 15,000 calendar steps to about 60 curve lookups.
- `OisSwap` caches its period rows at construction, so valuation allocates nothing.
- `FixedRateBond::npv` and `dv01` off a discount curve, alongside the yield-based analytics.
- `Date::from_unix_days` and `unix_days`, since riskrt stores dates as days since 1970-01-01.
- `bootstrap_sofr_with_jacobian`, `zero_bucket_ladder`, `CurveJacobian::to_par_ladder`, and node accessors on `DiscountCurve`.
- Compile-time `Send + Sync` assertions on every type riskrt shares across threads, so a future `Rc` or `RefCell` is a build error.

Conventions carried across the boundary:

- All risk in riskrt is per basis point. `OisSwap::dv01` and `FixedRateBond::dv01` are both "PV change for +1bp" and match `Risk::dv01` directly. (`YieldAnalytics::dv01` has the opposite sign and is not used.)
- A swap's direction comes from `fixed_leg`; its notional is taken as absolute. A bond's notional is signed, so a short bond has positive DV01.
- Bond prices are per face (100); the adapter scales by notional over face.
- `Position::Bond` carries no issue date. The adapter anchors a semiannual schedule to maturity and starts it over a year before valuation, which gives regular periods and the same PV as the real issue. A test checks this.
- USD SOFR OIS is single-curve, so a swap's projection and discount curve must be the same curve. A two-curve swap is rejected at load time.

## What is priced today

| Product | Kernel type | PV | DV01 | Bucket ladder |
|---|---|---|---|---|
| USD SOFR OIS swap | `fq_rates::swap::OisSwap` | `npv` against the base curve and its index | PV difference against the pre-shifted pair, per basis point | zero ladder against the per-node-shifted pair, multiplied through the par Jacobian |
| Fixed-coupon bond | `fq_rates::bond::FixedRateBond` | `npv` off the discount curve, scaled by notional over face | same shifted-pair difference | same zero-to-par route |

Curves are USD SOFR discount curves bootstrapped from deposits, SOFR futures and OIS (`fq_rates::curve::bootstrap_sofr`), one `OvernightIndex` of fixings shared by every curve in a set and pinned to each curve at build time.

## Risk conventions

- Every sensitivity is PV change for a +1 basis point move, in the position's currency, signed from the holder's side. Totals are plain sums.
- The bucket ladder is per input quote (par risk), not per zero node. A desk hedges with the instruments it quoted; the Jacobian is what converts.
- Parallel DV01 is computed from the parallel-shifted curve, not as the sum of the ladder, so the two can differ by convexity across buckets. Both are reported.
- `MarketVersion` is the provenance of every number. Two numbers with the same version came from the same curve set.

## What the commodities slice needs

ferrumquant's `fq-commod-core` provides `FuturesCurve`: a strip of `(ContractMonth, price)` with `shifted`, `scaled` and `bumped_month`. There is no bootstrap and no Jacobian, because a futures curve is its quotes. In riskrt terms:

- A `CurveKind::Futures` curve in `CurveSet` built straight from the quote snapshot, with the parallel-shifted copy and one bumped copy per month. Health, versioning and `changed_since` apply unchanged; such a curve is never `IndexStale`.
- `Product::GasSwap` and `Product::OilSwap`, with a commodity curve for projection and the SOFR curve for discounting. This is the first product with a real two-curve dependency and exercises `forward != discount` in `CurveIndex`.
- A second arm in `FqPricer`. PV from the sector swap's `value`, first-order risk from the shifted pair, bucket ladder from the per-month bumped copies.
- A units decision: a futures delta is per unit of price, not per basis point, so `Risk` needs the curve it is against or a unit tag. The by-curve rollup is where units are homogeneous; the portfolio total stays PV only.
