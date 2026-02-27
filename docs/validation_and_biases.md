# Validation and biases

Condensed checklist for PIT correctness and backtest bias. Use in the validation notebook and code reviews.

## Lookahead (most dangerous)

- **SF1:** Use ARQ/ART/ARY only; never MRQ/MRT/MRY. Join on **datekey** (filing date), not calendardate or reportperiod.
- **DAILY:** Confirm metrics use filed-date fundamentals (they do in Sharadar); join on ticker + date.
- **Forward returns:** Compute using prices from D+1 onward; no same-day close.
- **FRED:** Use **release date** for series that are revised (e.g. CPI: release in mid-November for October, not Oct 31). Period-end date = lookahead.
- **METRICS:** For beta1y etc., use most recent row where METRICS.date <= D.

## Survivorship bias

- **Universe:** Include delisted and bankrupt companies in history; ACTIONS must have full history. Ticker is out of universe only on or after the delist/bankruptcy/merger action date.
- **Forward flags:** Keep fwd_delisted_30d, fwd_delisted_90d, fwd_acquired_90d in the master table for label construction and exclusion in return prediction.
- **Training:** Do not train only on “currently active” tickers; use historical universe per date.

## Selection / universe bias

- **Liquidity filter:** Filtering on volume/market cap can remove small caps that later fail; use PIT market cap (DAILY) and document threshold.
- **Market cap:** From DAILY (PIT), not current.

## Less obvious

- **Earnings timing:** Fundamentals public on datekey; price often moves on earnings call (before filing). For event-based labels, consider EVENTS (8-K, eventcode 22).
- **Index reconstitution:** Use SP500 table for historical membership when benchmarking.
- **Fiscal year:** calendardate normalizes; same-quarter comparisons could still need care.
- **Price adjustment:** Use closeadj for returns; be consistent (e.g. no mix of split-only close).
- **Short history:** 10y NCFO CAGR implies 10y of data; early history has NaN; handle explicitly (missingness can be informative).

## Validation steps

1. **Spot-check:** Pick 5–10 (ticker, date) pairs; verify against raw SF1/SEP/ACTIONS that no value uses information after that date.
2. **Known events:** For 2–3 known earnings or acquisition dates, assert pipeline does not expose post-event data on pre-event dates.
3. **Delisted tickers:** Confirm a few delisted names appear in universe up to delist date, then drop.
4. **Distributions:** Check feature distributions for obvious data errors (e.g. negative prices, impossible ratios).

Reference: sharadar_dataset_guide.md sections 9–10 and 13.
