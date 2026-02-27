# Validation checks

Reference for `pipeline/08_validation.py` and `notebooks/01_validate_pipeline.ipynb`. Each section describes what the check does, why it matters, how to interpret results, and expected pass criteria.

**Report output:** Running the script saves all output to `outputs/validation_report.md` with a timestamp. Use this for audits, comparisons across pipeline runs, or sharing results.

**When checks fail:** See [validation_issues_remediation.md](validation_issues_remediation.md) for investigation steps and SQL snippets. Fix priority order: (1) quality_null 100% → (2) scalemarketcap not categorical → (3) yield curve 2019 sign → (4) pe_pit vs DAILY.pe correlation → (5) HTZ delist → (6) pcf_pit vs pe_pit correlation. Section numbers below match `08_validation.py` section order.

## 1. PIT Integrity (Most Critical)

**What:** Verifies that no fundamental data uses information available only after the simulation date. Checks `datekey <= date` on the full `fundamental_pit` table, reports NULL datekey rate, samples AAPL for quality-metric change frequency, and audits staleness distribution.

**Why:** Lookahead bias is the most dangerous failure mode. A single row with `datekey > date` means the model could have trained on future information, invalidating backtests and live performance.

**How to interpret:**
- **NULL datekey rate:** Expect ~5–10% (tickers with no filings yet at history start). If >30%, the ASOF join logic may be wrong.
- **Violations:** Must be zero. Any violation fails the assertion.
- **ncfo_r2_adjusted changes/year:** Should be ~4 for AAPL (quarterly filings).
- **days_since_filing:** Median ~45–90 days, max ~180. Rows >365 days stale should be a small fraction.

**Expected:** Zero violations; staleness within expected ranges.

---

## 2. Survivorship Bias (Second Most Critical)

**What:** Confirms delisted tickers (LEH, HTZ) appear in the universe up to their delist date with correct `fwd_delisted_30d` on the last row; checks `fwd_delisted_90d` fraction; reports ticker date ranges.

**Why:** Excluding delisted/bankrupt companies biases returns upward. The model must see the full historical universe including failures.

**How to interpret:**
- **LEH/HTZ:** Must PASS. LEH last date ~2008-09-12; `fwd_delisted_30d` must be True on that row.
- **fwd_delisted_90d True:** Expect ~2–5%. Too low = delistings not captured; too high = logic error.
- **Ticker ranges:** Review for tickers with gaps (delisted then relisted under same ticker).

**Expected:** LEH and HTZ pass; fwd_delisted_90d in 2–5%.

---

## 3. Duplicate Rows (Critical)

**What:** Asserts zero duplicate `(ticker, date)` rows in the master table.

**Why:** Duplicates break joins, double-count rows, and corrupt model training.

**How to interpret:** Count must be 0. Any duplicate fails the assertion.

**Expected:** 0 duplicates.

---

## 4. Distribution Sanity

**What:** Checks valuation ratios (pe_pit, pb_pit, pcf_pit, evebitda_pit) in plausible ranges; ret_12m distribution; extreme returns vs ACTIONS splits; vol_20d; ncfo_r2_adjusted.

**Why:** Catches data errors (wrong units, bad joins, unadjusted splits) before they poison the model.

**How to interpret:**
- **Valuation ratios:** pe 0–500, pb 0–50, pcf 0–200, evebitda 0–100. Flag high % outside range.
- **ret_12m > 5:** Possible for penny stocks; not necessarily an error. Cross-reference with ACTIONS splits—if ret_12m ≈ split ratio, closeadj may not be adjusted.
- **vol_20d > 5:** Data error (500% annualized vol).
- **ncfo_r2_adjusted:** Should be in [0, 1], median ~0.5–0.7.

**Expected:** Most values in range; no suspicious split alignment.

---

## 5. Sector Relative Sanity

**What:** Valuation ratios (pe_vs_sector, pb_vs_sector, pcf_vs_sector) median ≈ 1.0; quality diffs (roic_vs_sector, ret_3m_vs_sector) mean ≈ 0.

**Why:** By construction, company/sector_median should have median ~1.0. Mean is skewed by extremes—use median for valuation.

**How to interpret:** Median of ratio columns ~1.0; mean of diff columns ~0.

**Expected:** Medians within ~0.2 of 1.0; means within ~0.05 of 0.

---

## 6. Macro Features Temporal Sanity

**What:** Yield curve inverts Aug–Oct 2019; VIX spikes in March 2020 vs 2019; SPY regime = 0 during COVID crash; CPI YoY elevated in 2022 vs 2019.

**Why:** Anchors macro features to known historical events. Failures indicate wrong series, dates, or joins.

**How to interpret:** Each assertion must pass. Yield curve < 0 in 2019; VIX Mar 2020 > 2× 2019; CPI 2022 > 2× 2019.

**Expected:** All assertions pass.

---

## 7. Insider Signal Sanity

**What:** Insider buy activity rate (expect 5–15%); explicit warning if <1% (securityadcode filter likely wrong); insider buy rate by scalemarketcap (Nano/Micro > Large/Mega).

**Why:** SF2 securityadcode filter was historically wrong—P transactions are mostly NULL/DA. <1% buy activity suggests the fix wasn’t applied.

**How to interpret:**
- **<1%:** Re-run 06 after removing securityadcode filter.
- **5–15%:** OK.
- **By size:** Nano/Micro caps should have higher insider buy rates.

**Expected:** 5–15% buy activity; size gradient present.

---

## 8. Cross-Feature Consistency

**What:** pcf_pit vs pe_pit correlation (expect 0.3–0.7); pe_pit vs DAILY.pe when DAILY exists (correlation >0.95, median ratio ~1.0).

**Why:** Perfect correlation = formula error. Large deviation from DAILY.pe = shareswa × closeadj ≠ Sharadar marketcap.

**How to interpret:** Correlation and ratio within expected ranges.

**Expected:** pcf/pe correlation 0.3–0.7; pe_pit/DAILY.pe correlation >0.95, ratio ~1.0.

---

## 9. Temporal Consistency

**What:** vol_20d should not jump >200% overnight outside crisis (Mar 2020, Mar 2008); pcf_pit jumps >50% should only occur right after filing date.

**Why:** Abnormal overnight jumps suggest wrong ASOF join or data errors.

**How to interpret:** Spikes outside known crisis periods are suspicious. pcf_pit jumps should align with days_since_filing reset.

**Expected:** Few spikes; those present during crisis or at filing dates.

---

## 10. Null Rate Audit

**What:** Null rate per feature, sorted descending; flag features >80% null.

**Why:** High null rates may indicate broken joins or wrong filters.

**How to interpret:**
- **>80% null:** Investigate.
- **Expected:** Quality metrics (ncfo_r2_10y etc.) high null (10y history); valuation 30–40%; price <5%; macro 0%.

**Expected:** No unexpected >80% null; known high-null features documented.

---

## 11. Null Rate by Year

**What:** Null rates for pcf_pit, ncfo_r2_adjusted, inst_shrunits, yield_curve by year.

**Why:** Catches data sourcing gaps (e.g., SF3A only from 2013; ncfo needs 10y history before 2003).

**How to interpret:**
- **inst_shrunits:** ~100% null before 2013.
- **ncfo_r2_adjusted:** High null before 2003.
- **yield_curve:** 0% null throughout (FRED covers full history).

**Expected:** Patterns match data availability.

---

## 12. Universe Composition Over Time

**What:** Unique tickers per year (in_universe = True).

**Why:** Universe should grow from ~3000 (2000) to ~5000+ (2020s). Big drops or flat counts indicate bugs.

**How to interpret:** Steady growth; no sudden drops; no flat counts (wrong filter).

**Expected:** ~3000 in 2000; ~5000+ in 2020s.

---

## 13. Restatement Coverage (Raw SF1)

**What:** For GE, UAA, NFLX, TSLA: multiple ARQ rows per reportperiod = restatement history. Single row = only original filing.

**Why:** Documents whether Sharadar stores restatements. Use AR dimensions only for PIT.

**How to interpret:** Multiple filings per period = restatement history present. Single = no amendments in data.

**Expected:** Informational; no assertion.
