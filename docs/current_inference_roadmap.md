# Current inference dataset roadmap

This document maps each feature used in `experiments/walk_forward_validation.ipynb` (SQL SELECT in `load_fold`, lines 10–24) to how to reconstruct it for a **current** inference dataset: one row per non-delisted ticker, using latest-available data only (no PIT grid).

**Goal**: Build inference data so the trained walk-forward model can score all currently listed names and produce picks. Training data ends 2024-12-31.

**Ticker universe**: Only tickers with `isdelisted = 'N'` in TICKERS parquet (one row per ticker; use `table = 'SF1'` for SF1 rows).

**Efficiency**: Compute only the most recent row per ticker. **Sharadar dimensions**: MRY and MRQ are full time series including restatements (everything currently known); use MRY for annual quality series (last 10y), MRQ for quarterly (last 8q). MRT for TTM valuation snapshot. Latest trading date for price; latest known value per FRED series for macro.

---

## Meta and label

| Feature | Source | Current reconstruction |
|--------|--------|-------------------------|
| **ticker** | — | One row per listed ticker; from TICKERS (isdelisted = 'N'). |
| **date** | universe | Use a single *as_of* date (e.g. latest trading day or today); same value for all rows. |
| **sector** | universe / TICKERS | From TICKERS parquet (SF1 row): `sector` column. |
| **famaindustry** | universe / TICKERS | From TICKERS parquet (SF1 row): `famaindustry` column. |
| **fwd_ret_5td** | 07_labels | Omit or set to NaN for inference; not used as model input (in EXCLUDE). |

**Reuse**: TICKERS parquet; no pipeline step for “current” meta beyond reading TICKERS and filtering isdelisted.

---

## Price features

Source: `pipeline/03_price_features.py` (SEP → returns, vol, 52w, MA, ATR, skew).

| Feature | Current reconstruction |
|---------|------------------------|
| **ret_1m** | Latest trading date per ticker: `closeadj / LAG(closeadj, 21) - 1` over SEP (partition by ticker, order by date). |
| **ret_3m** | Same row: LAG 63. |
| **ret_6m** | Same row: LAG 126. |
| **ret_12m** | Same row: LAG 252. |
| **vol_20d** | STDDEV(daily_ret) over last 20 rows × √252. |
| **vol_60d** | STDDEV(daily_ret) over last 60 rows × √252. |
| **vol_ratio** | vol_20d / vol_60d. |
| **volume_ratio_1m** | vol_20d_avg / vol_60d_avg (AVG(volume) over 20 and 60 rows). |
| **pct_52w_range** | (closeadj - low_52w) / (high_52w - low_52w); low_52w/high_52w = MIN/MAX over 252 rows. |
| **ma50_cross** | (closeadj - ma50) / ma50; ma50 = AVG(closeadj) over 50 rows. |
| **ma200_cross** | (closeadj - ma200) / ma200; ma200 = AVG(closeadj) over 200 rows. |
| **atr_14d_normalized** | Pipeline writes `atr_14d`; master renames to atr_14d_normalized. ATR = AVG(GREATEST(hl_range, hc_range, lc_range)) over 14 rows / closeadj. |
| **momentum_skew_60d** | SKEWNESS(daily_ret) over last 60 rows. |

**Reuse**: Same formulas as in 03’s `price_features_full` (daily → base → derived). For current: filter SEP to listed tickers and date range `[as_of - 252 days, as_of]`; compute features only for the row where date = latest date per ticker (one row per ticker).

---

## Quality metrics

Source: `pipeline/02_fundamentals.py` (quality from `compute_quality_metrics_table`) + `pipeline/fundamental_quality.py`.

| Feature | Current reconstruction |
|---------|------------------------|
| **ncfo_r2_5y** | **MRY** (dimension = 'MRY'): full annual time series with restatements. Take last 10y per ticker; call `compute_quality_metrics_for_ticker(annual_data, quarterly_data)`; use returned `ncfo_r2_5y`. |
| **ncfo_cagr_5y** | Same; `ncfo_cagr_5y`. |
| **ncfo_r2_10y** | Same; `ncfo_r2_10y`. |
| **ncfo_cagr_10y** | Same; `ncfo_cagr_10y`. |
| **ncfo_pct_positive** | Same; `ncfo_pct_positive`. |
| **grossmargin_slope** | **MRQ** (dimension = 'MRQ'): full quarterly time series with restatements. Last 8 quarters per ticker in `quarterly_data` for 8Q slope. |
| **capex_intensity** | From MRT snapshot (see Valuation): `ABS(capex) / revenue` (or revenueusd). |
| **accrual_ratio** | From MRT: `(netinccmn - ncfo) / assets`. |

**Reuse**: `pipeline/fundamental_quality.compute_quality_metrics_for_ticker(annual_data, quarterly_data)`. Build annual_data from **MRY** rows (one row per year; already restated). Build quarterly_data from **MRQ** (last 8 quarters). No datekey filter needed—MRY/MRQ are “everything currently known.” MRT snapshot gives capex_intensity and accrual_ratio.

---

## Valuation and fundamentals (MRT path)

Source: `pipeline/02_fundamentals.py` (TTM snapshot + SEP price). Use **MRT** (Most Recent Trailing = TTM with restatements) for current snapshot.

| Feature | Current reconstruction |
|---------|------------------------|
| **pcf_pit** | Latest MRT per ticker: (closeadj × shareswa) / ncfo; closeadj from latest SEP. |
| **roe** | MRT: roe or netinccmn / equity. |
| **current_ratio** | MRT: currentratio. |
| **pe_pit** | (closeadj × shareswa) / netinccmn. |
| **pb_pit** | (closeadj × shareswa) / equity. |
| **ps_pit** | (closeadj × shareswa) / revenue (or revenueusd). |
| **evebitda_pit** | ((closeadj × shareswa) + debt - cashnequsd) / ebitda. |
| **dividend_yield** | MRT: divyield or dps / closeadj. |
| **pretax_margin** | ebt / revenue (or revenueusd). |
| **debt_to_equity** | debt / equity. |
| **liabilities_to_assets** | liabilities / assets. |
| **payout_ratio** | dps / epsdil. |
| **earnings_growth_yoy** | (epsdil - epsdil_prior) / epsdil_prior; epsdil_prior from ART with datekey ≤ as_of - 1 year. |

**Reuse**: Same formulas as in 02’s fundamental_pit view. For current: one **MRT** row per ticker (latest TTM restated), join to latest SEP closeadj. Prior-year epsdil from ART (datekey ≤ as_of - 1 year) for earnings_growth_yoy.

---

## Sector-relative features

Source: `pipeline/05_sector_relative.py`.

| Feature | Current reconstruction |
|---------|------------------------|
| **pe_vs_sector** | For single cross-section: MEDIAN(pe) by famaindustry (≥5 tickers); then pe / sector_median_pe per ticker. |
| **pb_vs_sector** | Same with pb. |
| **ps_vs_sector** | Same with ps. |
| **pcf_vs_sector** | Same with pcf_pit. |
| **evebitda_vs_sector** | Same with evebitda. |
| **roic_vs_sector** | roic_level - sector_median_roic (difference, not ratio). |
| **ret_3m_vs_sector** | ret_3m - sector_median_ret_3m. |
| **vol_vs_sector** | vol_20d - sector_median_vol (05 uses vol_20d for sector median). |
| **ret_3m_rank_sector** | PERCENT_RANK() OVER (PARTITION BY famaindustry ORDER BY ret_3m). |

**Reuse**: 05’s logic: combined view (pe, pb, ps, pcf, roic_level, ret_3m, vol_20d from fundamental + price), sector_medians by (famaindustry) with HAVING COUNT(*) >= 5, then ratios and rank. For current: one cross-section (one “date”); no date in GROUP BY.

---

## Macro features

Source: `pipeline/04_macro_features.py` (FRED parquets + SPY from SEP).

| Feature | Current reconstruction |
|---------|------------------------|
| **yield_curve** | Latest row in FRED_DIR/yield_curve.parquet; take last value by date. |
| **hy_spread** | Latest row in hy_spread.parquet. |
| **vix** | Latest row in vix.parquet. |
| **vix_change_20d** | If FRED vix is daily: vix - LAG(vix, 20) on vix series, then take value for latest date. Alternatively: read last 21 rows of vix, compute difference. |
| **nfci** | Latest row in nfci.parquet. |
| **real_rate** | treasury_10y - cpi_yoy (%). For “current”: latest treasury_10y and cpi_yoy (cpi_yoy = (cpi - LAG(cpi, 252)) / LAG(cpi, 252) * 100 if using daily grid; or use latest monthly cpi and 12m-ago). |
| **spy_regime_ma** | From SEP: SPY closeadj vs MA200; 1.0 if closeadj > ma200 else 0.0; use latest trading date. |
| **spy_ret_12m** | From SEP: closeadj / LAG(closeadj, 252) - 1 for SPY at latest date. |

**Reuse**: 04’s FRED series list (yield_curve, hy_spread, vix, nfci, fed_funds, cpi, treasury_10y) and derived (vix_change_20d, cpi_yoy, real_rate). For current: read each parquet, take last row by date; compute derived; broadcast one scalar per series to all tickers.

---

## WHERE filter (training universe)

The notebook filters training rows with:

- `ncfo_r2_5y > 0.5 AND fcf_cagr_5y > 0 AND roe > 0.12 AND debt_to_equity < 1.5 AND grossmargin_slope > 0`
- `ret_12m > 0 AND ret_1m > -0.15 AND ret_6m > ret_3m`

**Decision**: Either (a) apply the same filter to the current inference rows before scoring (so we only score names that would have been in the backtest universe), or (b) score all listed tickers and document that the backtest applied this filter. The roadmap leaves this choice to the implementation; recommend (a) for consistency with backtest universe.

---

## Summary: pipeline reuse

| Component | Reuse |
|-----------|--------|
| Quality | `fundamental_quality.compute_quality_metrics_for_ticker` with **MRY** (last 10y annual) and **MRQ** (last 8q); MRY/MRQ are full restated time series. |
| Valuation / TTM | **MRT** (TTM restated) snapshot + latest SEP; prior-year ART for earnings_growth_yoy. |
| Price | 03’s daily/base/derived logic on SEP filtered to [as_of - 252, as_of], keep only row where date = max(date) per ticker. |
| Sector-relative | 05’s median-by-famaindustry and ratio/rank logic on one cross-section. |
| Macro | 04’s FRED stems and derived; read each parquet, last row, broadcast. |

No institutional/insider features (06) or TOP_25 list; only the SQL SELECT columns above.
