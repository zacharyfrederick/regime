# Pipeline run errors: remediation

This document catalogs errors and failures observed in a typical debug run (e.g. `.\run_pipeline.ps1` with `DEBUG = True`), classifies them as **Critical** (real bug) or **Bad assumption** (test/expectation wrong for debug or data), and states the planned or applied fix.

## Summary table

| Error | Source | Critical? | Status / fix |
|-------|--------|-----------|--------------|
| ARQ datekey type mismatch → empty quality metrics (R² all null) | 02_fundamentals / fundamental_quality | Yes | **Fixed:** Cast `datekey` to DATE in ARQ BETWEEN clause |
| Staleness check "Invalid format specifier" | validate_debug Section 3 | Yes | **Fixed:** Coerce DuckDB numeric to float before formatting |
| AAPL ret_12m null for first 252 rows FAIL | validate_debug Section 2 | No (bad assumption) | Documented: only applies when grid starts at ticker inception |
| HTZ survivorship / after 2020-05-22 FAIL | 09_validation, validate_debug Section 5 | No (bad assumption) | **Fixed:** Skip HTZ delist check when no rows in delist window (re-listed only) |
| Sector relative all NaN (pe_vs_sector 100% null) | 05_sector_relative, 09_validation Section 5 | No (bad assumption) | Documented: debug has &lt;5 tickers per sector so HAVING COUNT(*) >= 5 yields no medians |

---

## Critical errors (fix in code)

### 1. ARQ datekey type mismatch → empty quality metrics (R² all null)

- **What:** `Binder Error: Cannot mix values of type VARCHAR and TIMESTAMP in BETWEEN clause - an explicit cast is required` at `pipeline/fundamental_quality.py` ARQ query (`datekey BETWEEN (...)`).
- **Cause:** In DuckDB, `datekey` in SF1 can be TIMESTAMP (or mixed with DATE bounds). Mixing types in BETWEEN triggers the error; the query fails and we fall back to empty `quality_df` → empty quality_metrics view → 100% null `ncfo_r2_adjusted` in master.
- **Fix (applied):** In `pipeline/fundamental_quality.py`, cast `datekey` to DATE in the WHERE clause:  
  `AND CAST(datekey AS DATE) BETWEEN ('{date_start}'::DATE - INTERVAL '10 years') AND '{date_end}'::DATE`

---

### 2. Staleness check "Invalid format specifier"

- **What:** In validate_debug, Section 3: "Staleness check failed: Invalid format specifier".
- **Cause:** The staleness PASS line used `med:.0f` and `mx` in an f-string. DuckDB can return numeric types (e.g. Decimal) that don't support `.0f` or trigger format errors when mixed with strings.
- **Fix (applied):** In `pipeline/validate_debug.py`, coerce to float before formatting: `med_f = float(med) if med is not None else None`, same for `mx_f`, then format with `f"{med_f:.0f}"` and `f"{mx_f:.0f}"` (or "N/A" when None).

---

## Bad assumptions (fix test or document)

### 3. AAPL ret_12m null for first 252 rows: FAIL (e.g. 77/252 null)

- **What:** validate_debug expects the first 252 AAPL rows (by date) to have **all** null `ret_12m`; run may report e.g. 77/252 null.
- **Cause:** The assumption holds only when the pipeline's **first trading date** is the ticker's first ever trading day. With `DATE_START = 2000-01-01`, AAPL already has 252+ trading days of history before that, so the first 252 rows in the grid can have non-null `ret_12m`. The check is intentionally not added to the failures list in code.
- **Fix:** Treat as bad assumption. Options: remove this check; make it conditional (e.g. only when DATE_START equals ticker's first trading day); or document only. No code change required for pipeline correctness.

---

### 4. HTZ survivorship / HTZ after 2020-05-22 / HTZ in_universe after delist

- **What:** 09_validation: "Delisted HTZ (2020-05-22): FAIL"; validate_debug: "HTZ after 2020-05-22: FAIL", "HTZ in_universe after delist: FAIL". Diagnostic may show HTZ universe date range **min=2021-11-09** (790 rows).
- **Cause:** **HTZ in our filings/dataset is post-bankruptcy only.** Hertz re-listed after bankruptcy (trading from ~2021-11); the data source (e.g. Sharadar) only has HTZ from re-listing onward. So the universe and master contain no rows in the delist window [2020-05-22, 2020-08-01]. The test assumed "all HTZ rows with date >= 2020-05-22 should have fwd_delisted=1", but every HTZ row we have is **post re-listing** and correctly has fwd_delisted=0 and in_universe=TRUE.
- **Fix (applied):** In `pipeline/validate_debug.py` and `pipeline/09_validation.py`, the HTZ delist check is conditional: only assert fwd_delisted / in_universe when there is at least one HTZ row in the delist window. If there are zero such rows (HTZ in dataset is post-bankruptcy only), skip the check and report that HTZ data is post-bankruptcy only.

---

### 5. Sector relative all NaN (pe_vs_sector 100% null)

- **What:** 09_validation Section 5: "Sector relative (median for valuation, mean for diff): NaN"; 05_sector_relative log: "pe_vs_sector null rate for tickers with sector: 100.0%".
- **Cause:** In `pipeline/05_sector_relative.py`, sector medians are computed per (date, famaindustry) with **HAVING COUNT(*) >= 5**. With only 11 debug tickers spread across sectors, no (date, famaindustry) has 5+ tickers, so sector_medians is empty and all pe_vs_sector (and related) are null.
- **Fix:** Bad assumption in debug. Options: (a) In DEBUG, lower the minimum count (e.g. 2) for sector_medians so debug gets some values; (b) Skip sector-relative median checks in validate_debug when ticker count is small; (c) Document only: "In debug with 11 tickers, sector relative is expected to be null." No code change in this remediation pass; document only.

---

## Other run notes

- **"Universe has only 50236 rows - expected millions for full history"** — Expected when `DEBUG = True` (11 tickers × ~6k dates).
- **MON pcf_pit 0%** — Data coverage for that ticker; FLAG in coverage matrix is informational.
- **atr_14d_normalized median WARN (expect &lt; 0.10)** — Separate tuning; not a pipeline bug.
- **scalemarketcap distinct values: 1** — In debug, universe may have a single tier; expected.

For general validation failure investigation, see [validation_issues_remediation.md](validation_issues_remediation.md).
