# Data assumptions

[← README](../README.md) · [Data sources](data_sources.md) · [Validation checklist](validation_and_biases.md)

The pipeline relies on the following assumptions about raw data. These are documented so they can be validated in notebooks (e.g. `00_testing.ipynb`) and in post-generation checks; if a source changes, we want to catch it.

---

## Sharadar SF1 (fundamentals)

- **Capex sign:** Capex is stored as **negative** (cash outflow). FCF reconstruction is `ncfo + capex` everywhere.
- **Accrual ratio:** Sloan definition `(net income − operating cash flow) / total assets`; we use `(netinccmn − ncfo) / assets` (not FCF-based).
- **Gross margin (ARQ):** The `grossmargin` field is a **ratio in [0, 1]** (e.g. 0.38 for 38%), not gross profit in dollars.
- **Revenue:** We use `COALESCE(revenueusd, revenue)`; revenueusd is preferred for consistency with USD market cap; for non-USD filers it uses period exchange rate.

## Sharadar SEP (prices)

- **Columns:** We assume `date`, `ticker`, `high`, `low`, `volume`, `closeadj` (and optionally `open`, `close`, `closeunadj`, `lastupdated`) are present. Used for returns, volatility, ATR, 52w range, volume ratio, and MA cross.
- **Returns:** Return windows use **trading-day row lags** (e.g. LAG 21 for ~1 month). Gaps in SEP (halts, missing days) mean the lag is not exactly calendar time.

## Universe and grid

- The daily universe (grid) from `01_universe.py` is the join key for all feature scripts; pipeline expects one row per (ticker, date) in the grid for the configured date range.

## PIT correctness

- Fundamentals use ASOF joins and vintage-based quality metrics so only data known as of each date is used; no lookahead.

## Macro features (04, FRED)

- **Frequency:** FRED series have mixed frequency (VIX/treasury daily; CPI, NFCI, fed funds monthly). The fetch script (`00_fetch_fred.py`) reindexes each series to a business-day grid and forward-fills before writing parquet, so all series are daily. The feature script then applies `LAST_VALUE(col IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` so any remaining gaps (e.g. weekends if the date grid is calendar) are forward-filled. No fill occurs before the first observation.
- **cpi_yoy:** Computed as percent change over **LAG 365** rows on the forward-filled daily CPI. This is approximate YoY (365 calendar days back, not an exact 12‑month period). Sufficient for macro regime signal; not for official inflation reporting.
- **real_rate:** `treasury_10y - cpi_yoy` (both in percent).
- **SPY regime:** From SEP (`ticker = 'SPY'`): `spy_ret_12m` = LAG 252 return; `spy_regime_ma` = 1 if close above 200d MA else 0. Single-ticker window so no `PARTITION BY` needed.
- **Date grid:** If `DAILY_UNIVERSE_PATH` exists, the macro date grid is trading dates from the universe so row count matches and there are no weekend rows; otherwise calendar days are used.

## Fundamentals (02, fundamental_pit)

- **SF1 date types:** Sharadar stores `datekey` and `reportperiod` as **VARCHAR** in parquet. Every ASOF join in 02_fundamentals uses `CAST(datekey AS DATE)` (or a subquery that exposes `datekey_date`) so comparisons with grid `date` are type-consistent. Do not compare datekey to date without casting.
- **FCF reconstruction:** `ncfo + capex` (capex is negative in Sharadar); matches Sharadar's pre-computed `fcf` field.
- **Schema verification:** Written parquet columns are checked via `SELECT * FROM read_parquet(...) LIMIT 0` and `.columns`, not `parquet_schema()`, because DuckDB's `parquet_schema()` return columns vary by version.

## Sector-relative (05)

- **Grouping:** Peer groups use **famaindustry** (Fama-French 48) from the daily universe. Minimum **5 peers** per (date, famaindustry); sectors with fewer are excluded (`HAVING COUNT(*) >= 5`) so medians are not computed and sector-relative columns are NULL for those rows.
- **Valuation sources:** When `data/DAILY.parquet` exists, valuation ratios (pe, pb, ps, evebitda) use DAILY (Sharadar's PIT-correct daily ratios). Otherwise they fall back to fundamental_pit (pe_pit, pb_pit, ps_pit, evebitda_pit). PCF has no DAILY equivalent and always comes from fundamental_pit.
- **Ratio vs difference:** Valuation metrics use **company / sector_median** (ratio; &lt;1 = cheaper). Quality/performance (roic, ret_3m, vol_20d) use **company − sector_median** (signed difference). Valuation denominators are guarded with `sector_median > 0` to avoid invalid ratios.
- **Checks after write:** `pe_vs_sector` median ≈ 1.0 (mean is skewed by extremes); `roic_vs_sector` mean ≈ 0 (assert within 0.05); log `pe_vs_sector` null rate for tickers with non-null famaindustry.

## Insider and institutional (06)

- **SF2 PIT:** **filingdate** is the PIT boundary; no offset (Form 4 due within 2 business days of transaction).
- **SF2 filter:** Open-market transactions only: `transactioncode IN ('P', 'S')`, `transactionshares IS NOT NULL AND transactionshares <> 0`. No securityadcode filter (inconsistent for P in Sharadar). Excludes grants, option exercises, tax withholding, gifts.
- **SF2 sign:** We normalize with `transactioncode` (P ⇒ +abs(transactionshares), S ⇒ −abs(transactionshares)) so both positive-only and signed conventions in the data are handled.
- **insider_net_ratio_90d:** Denominator is **AVG(sharesownedbeforetransaction)** over the 90-day window. Alternative for cross-ticker comparability: normalize by shares outstanding (SF1) if joined later.
- **SF3A PIT:** 13F filings are due **45 calendar days** after quarter end. We use `calendardate + INTERVAL '45 days'` as the PIT date; **do not reduce to 0** — the fixed 45-day lag is conservative and correct.
- **inst_put_call_ratio:** Capped at 100 in pipeline (raw ratio explodes when cllunits is tiny). Many tickers have zero cllunits, so this is often NULL; treat as a sparse sentiment/hedging signal.
- **Checks:** Optional `check.py` (not in repo) documents assumptions and can print: SF2 raw (P/S counts, date range, pipeline window, isofficer values, AAPL sample); insider buy % (expect ~5–15%); AAPL inst_shrunits change count (~4 per year); put/call median and max. The [validation script](validation_checks.md) and notebook cover these checks.

## Price features (03)

- **Skewness:** `momentum_skew_60d` uses DuckDB native `SKEWNESS(daily_ret) OVER (...)` (no UDF).
- **vol_ratio:** `vol_20d / NULLIF(vol_60d, 0)`. Both vols are annualized, so the ratio is dimensionless. **vol_ratio > 1** = recent volatility exceeds longer-term (stock becoming more volatile); **vol_ratio < 1** = volatility compressing. Useful regime signal.

---

Validation of these assumptions (and any post-generation sanity checks) should live in notebooks and in the [validation checklist](validation_and_biases.md). The full validation notebook and [validation_checks.md](validation_checks.md) document all 13 checks. Optional spot-check scripts: [validation_scripts.md](validation_scripts.md).

[← README](../README.md)
