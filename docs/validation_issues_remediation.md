# Validation issues: remediation guide

This document addresses each finding from the validation report: critical bugs, real bugs, and false alarms. For each we give investigation steps, root cause, and fix priority.

---

## Critical issues

### 1. quality_null is 100%

**Symptom:** `ncfo_r2_adjusted` (and related quality metrics) are NULL for every row in `master_features` across all 25 years.

**Meaning:** Either `02_fundamentals.py` failed silently, or `compute_quality_metrics_table` returned an empty DataFrame and the quality_metrics view was created as all-NULL, or the merge in `07_merge` is not joining quality into master correctly.

**Investigation (run in DuckDB over parquet; paths relative to project root):**

```sql
SELECT COUNT(*) AS total,
       SUM(CASE WHEN ncfo_r2_adjusted IS NOT NULL THEN 1 ELSE 0 END) AS non_null
FROM read_parquet('outputs/features/fundamental_pit.parquet')
```

- If **fundamental_pit** also has all NULLs for quality metrics → the bug is in `02_fundamentals.py` / `compute_quality_metrics_table`. Re-run `02_fundamentals.py` with logging set to **DEBUG** and inspect what `compute_quality_metrics_table` returns (row count, sample of `ncfo_r2_adjusted`). Check that SF1 ARQ data exists for the date range and that the function is not returning an empty DataFrame (e.g. ARQ pull failed or filtered everything out).
- If **fundamental_pit** has non-NULL quality → the bug is in `07_merge` (e.g. join key or view name for quality_metrics).

**Fix priority:** Highest. Quality metrics are core inputs; 100% null invalidates any quality-based signals.

**References:** `pipeline/fundamental_quality.py` (`compute_quality_metrics_table`), `pipeline/02_fundamentals.py` (quality view creation and merge into fundamental_pit).

---

### 2. scalemarketcap groupby is broken

**Symptom:** `scalemarketcap` has ~270k distinct values instead of 6 (1–6 categories). Insider buy rate by cap tier is meaningless because each “tier” is effectively a unique numeric value.

**Meaning:** `scalemarketcap` is storing raw market cap (e.g. dollars) or another continuous value instead of the categorical scale (1–6). TICKERS has `scalemarketcap` as a categorical string (e.g. `"1 - Nano"`, `"2 - Micro"`, …). Somewhere in the pipeline this was cast to float or replaced by `marketcap_daily` and never converted to the 1–6 code.

**Investigation:**

- Confirm universe schema: `daily_universe.parquet` may have been written with a column named `scalemarketcap` that is actually `marketcap_daily` or an unparsed numeric.
- Check `01_universe.py`: it computes `scalemarketcap` from `marketcap_daily` (scale 1–6 by thresholds) in `daily_universe`; the universe output no longer includes `marketcap_rank_annual` (rank can be computed later when needed). If `07_merge` expects `u.scalemarketcap`, it is provided by `01_universe`.

**Fix (in 01_universe):**

- **Option A:** Pull `scalemarketcap` from TICKERS (e.g. in `candidate_ticker_dates` / `universe_core`) and parse the leading digit from strings like `"1 - Nano"` to integer 1..6; propagate through to `daily_universe` and write it as an integer column (or cast to float 1–6 for schema compatibility).
- **Option B:** Do not use TICKERS.scalemarketcap; instead compute a scale 1–6 from `marketcap_daily` using fixed thresholds (e.g. Nano &lt; $50M, Micro &lt; $300M, …, Mega ≥ $200B) and add that as `scalemarketcap` in the universe output.

**Fix priority:** Critical. All “by scalemarketcap” analyses (e.g. insider buy rate by cap) are wrong until this is fixed.

**References:** `pipeline/01_universe.py`, `docs/artifact_schema.md`, Sharadar `indicators.csv` (TICKERS.scalemarketcap semantics).

---

## Real bugs

### 3. HTZ delisted check FAIL

**Symptom:** Validation reports “Delisted HTZ (2020-05-22): FAIL”. HTZ filed for bankruptcy on 2020-05-22; the check expects that after that date HTZ does **not** appear as `in_universe = TRUE`.

**Meaning:** Either (a) HTZ still appears as `in_universe = TRUE` after the bankruptcy date (bug), or (b) HTZ disappears from the universe **before** the bankruptcy date (survivorship bias: we never see the final period).

**Investigation (run when the check fails):**

```sql
SELECT date, in_universe, fwd_delisted_30d, fwd_delisted_90d
FROM read_parquet('outputs/universe/daily_universe.parquet')
WHERE ticker = 'HTZ'
  AND CAST(date AS DATE) BETWEEN '2020-05-01' AND '2020-07-01'
ORDER BY date
```

- If there are rows with `date >= 2020-05-22` and `in_universe = TRUE` → fix universe construction so that removal (e.g. bankruptcy) sets `in_universe = FALSE` for dates on or after the action date (or drop those rows).
- If HTZ has no rows in that window or last date &lt; 2020-05-22 → removal logic or ACTIONS is causing early exclusion; check `removal_per_ticker` and that ACTIONS has the correct bankruptcy date for HTZ.

**Fix priority:** High. Survivorship bias distorts backtests.

**References:** `pipeline/01_universe.py` (removal_per_ticker, delist_events, candidate_ticker_dates).

---

### 4. Yield curve Aug–Oct 2019 wrong sign

**Symptom:** Mean yield curve for Aug–Oct 2019 is **+0.089**; it should be **&lt; 0** (T10Y2Y inverted before the COVID recession).

**Meaning:** Either the wrong FRED series was fetched, the series is stored inverted, or date alignment is wrong.

**Investigation (run when the check fails):**

```sql
SELECT date, yield_curve
FROM read_parquet('outputs/features/macro_features.parquet')
WHERE CAST(date AS DATE) BETWEEN '2019-07-01' AND '2019-12-31'
ORDER BY date
```

If values are positive (~0.08–0.15) in that period, then:

- Check `00_fetch_fred.py`: it maps `yield_curve` → FRED code **T10Y2Y**. Confirm that the downloaded series is 10Y minus 2Y (not 2Y minus 10Y). If FRED returns 2Y−10Y, negate the series when building macro features.
- Check `04_macro_features.py`: ensure the column written as `yield_curve` is the one from the T10Y2Y parquet and not negated by mistake elsewhere.

**Fix priority:** High. Macro regime and yield-curve signals are wrong for 2019.

**References:** `pipeline/00_fetch_fred.py`, `pipeline/04_macro_features.py`.

---

### 5. pe_pit vs DAILY.pe correlation 0.172 (expect >0.95)

**Symptom:** Correlation between `pe_pit` and Sharadar `DAILY.pe` is ~0.17; median ratio ~0.91. They should be highly correlated and ratio ~1.0 (same definition: price × shares / net income).

**Meaning:** Likely a definition mismatch:

- **Shares:** DAILY uses `sharesbas` (basic); fundamental_pit uses `shareswa` (weighted average). That can cause a consistent ratio (e.g. ~0.5 or ~2x).
- **Dimension:** DAILY.pe may use a different earnings dimension or period than our ART (trailing 12 months).
- **Market cap:** DAILY may use its own marketcap; we use `closeadj × shareswa`.

**Investigation (single-ticker to isolate):**

```sql
SELECT m.date, m.pe_pit, d.pe, m.pe_pit / NULLIF(d.pe, 0) AS ratio
FROM read_parquet('outputs/features/fundamental_pit.parquet') m
JOIN read_parquet('data/DAILY.parquet') d
    ON d.ticker = m.ticker AND CAST(d.date AS DATE) = CAST(m.date AS DATE)
WHERE m.ticker = 'AAPL'
  AND m.pe_pit IS NOT NULL AND d.pe IS NOT NULL
  AND CAST(m.date AS DATE) BETWEEN '2020-01-01' AND '2020-12-31'
ORDER BY m.date
LIMIT 20
```

- If ratio is consistently ~2x or ~0.5x → shares definition (shareswa vs sharesbas); consider aligning to DAILY’s definition or documenting the difference.
- If ratio is noisy → dimension/period mismatch (ART vs MRY/ARY); check how DAILY.pe is computed and align our earnings denominator.

**Fix priority:** High. Valuation signals should align with vendor PE where possible.

**References:** `pipeline/02_fundamentals.py` (pe_pit construction), `docs/artifact_schema.md` (fundamental_pit), Sharadar DAILY documentation.

---

### 6. pcf_pit vs pe_pit correlation 0.019 (expect 0.3–0.7)

**Symptom:** Near-zero correlation between PCF and PE. Both share the same denominator (market cap); only the numerator differs (operating cash flow vs net income), so they should be correlated.

**Meaning:** Often a **consequence** of the same shares/dimension issues that cause the pe_pit vs DAILY.pe discrepancy. Fix the pe_pit definition first, then re-run validation.

**Action:** After fixing pe_pit (and quality nulls if relevant), recheck this correlation. If it remains &lt;0.2, investigate pcf_pit construction (cash flow source, TTM vs point-in-time) separately.

**Fix priority:** Medium (after pe_pit and quality).

**References:** Same as pe_pit; fundamental_pit construction in `02_fundamentals.py`.

---

## False alarms (no fix required in data)

### 7. Extreme ret_12m (e.g. ILXRQ 6499x)

**Symptom:** Some rows have `ret_12m > 5` (e.g. 6499). Example: ILXRQ 2010-04-13.

**Interpretation:** Penny stocks can have extreme returns (e.g. $0.001 → $6.50). This is real, not a data error. The backtest universe filter (e.g. `min_scalemarketcap >= 3` or similar) will exclude these. No change needed in the data pipeline; filter in analysis/backtest as appropriate.

---

### 8. vol_20d > 5 — many rows

**Symptom:** 131k+ rows with `vol_20d > 5` (500%+ annualized vol).

**Interpretation:** Micro caps and penny stocks can have very high daily volatility; 3% per day translates to &gt;500% annualized. Mathematically correct. Filter by universe (e.g. market cap / scale) in the backtest rather than treating as a data error.

---

### 9. AAPL pcf_pit jumps on day 0

**Symptom:** AAPL shows pcf_pit jumps &gt;50% on certain dates; those rows have `days_since_filing = 0`.

**Interpretation:** `days_since_filing = 0` means **filing date**. A large change in PCF on the day new quarterly cash flow is filed is expected. This is correct PIT behavior, not a bug.

---

## Priority order for fixes

1. **quality_null = 100%** — Debug and fix `02_fundamentals.py` / `compute_quality_metrics_table` (and merge into master).
2. **scalemarketcap** — Fix `01_universe.py` so scalemarketcap is categorical 1–6 (from TICKERS or from bucketing marketcap_daily).
3. **Yield curve 2019** — Check `00_fetch_fred.py` and macro feature build; ensure T10Y2Y sign and alignment.
4. **pe_pit vs DAILY.pe** — Align shares/dimension in fundamental_pit so correlation &gt;0.95 and ratio ~1.0.
5. **HTZ delist** — Fix universe so HTZ is not in_universe after 2020-05-22 (or document if by design).
6. **pcf_pit vs pe_pit** — Recheck after pe_pit and quality fixes; then investigate pcf_pit if still low.

Validation script `08_validation.py` now includes assertions and diagnostic queries for items 1–5 so that failures are detected and the above SQL snippets can be run automatically or from the report.
