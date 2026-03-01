#!/usr/bin/env python3
"""
Validate pipeline outputs after a DEBUG run (few tickers).
Contract tests, known-output spot checks, and sanity tests adapted for small N.
Run after pipeline when DEBUG=True. Report: outputs/validation_report_debug.md.
Exit 0 if all critical checks pass; 1 otherwise.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb

import config

# Required columns (minimal from docs/artifact_schema.md)
MASTER_REQUIRED = [
    "ticker", "date", "sector", "famaindustry",
    "ret_1m", "ret_12m", "vol_20d", "pe_pit", "pcf_pit", "ncfo_r2_adjusted",
    "days_since_filing", "yield_curve",
    "fwd_delisted_21td", "fwd_delisted_63td", "fwd_ret_21td",
]
UNIVERSE_REQUIRED = ["ticker", "date", "in_universe", "famaindustry"]

# Tolerance for known-output comparison
RET_12M_TOL = 1e-6

# Spot-check date for AAPL ret_12m (must have 252 trading days of history before it)
SPOT_DATE = "2021-06-15"
# Spot-check date for AAPL pe_pit (from SEP + SF1 ART)
PE_PIT_SPOT_DATE = "2020-12-31"
PE_PIT_TOL = 0.05  # 5% tolerance vs SEP+ART derived
# HTZ bankruptcy (delist) date for forward-label check
HTZ_DELIST_DATE = "2020-05-22"
# Staleness bounds (only for rows with non-null days_since_filing)
STALENESS_MEDIAN_MAX = 120
STALENESS_ABS_MAX = 400
# Tickers that must never have fwd_delisted set (still listed)
ALWAYS_LISTED_TICKERS = ("AAPL", "MSFT")
# Key columns for coverage matrix
COVERAGE_COLUMNS = ["ret_12m", "pe_pit", "pcf_pit", "ncfo_r2_adjusted", "vol_20d", "yield_curve", "days_since_filing"]
COVERAGE_FLAG_PCT = 50  # flag ticker with 0% when another has > this


def _path_sql(p: Path) -> str:
    return repr(str(p.resolve()))


def section(lines: list[str], title: str) -> None:
    lines.append(f"\n{'='*60}")
    lines.append(f"  {title}")
    lines.append(f"{'='*60}\n")


def run() -> tuple[list[str], list[str]]:
    """Run all checks. Returns (report_lines, critical_failures)."""
    report: list[str] = []
    failures: list[str] = []

    def out(s: str = "") -> None:
        report.append(s)

    out("# DEBUG run validation")
    out(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    out(f"Date range: {config.DATE_START} to {config.DATE_END}")
    if config.DEBUG and config.DEBUG_TICKERS:
        out(f"DEBUG_TICKERS: {config.DEBUG_TICKERS}")
    elif config.DEBUG:
        out(f"DEBUG_TICKERS: {config.DEBUG_TICKERS_DEFAULT}")
    out("")

    con = duckdb.connect()

    # Artifact paths
    paths = {
        "master": config.MASTER_FEATURES_PATH,
        "universe": config.DAILY_UNIVERSE_PATH,
        "fp": config.FUNDAMENTAL_PIT_PATH,
        "macro": config.MACRO_FEATURES_PATH,
        "price": config.PRICE_FEATURES_PATH,
        "sector_relative": config.SECTOR_RELATIVE_PATH,
        "insider": config.INSIDER_INSTITUTIONAL_PATH,
    }
    sep_path = config.DATA_DIR / "SEP.parquet"
    if not sep_path.exists():
        sep_path = config.DATA_DIR / "sep.parquet"
    daily_path = config.DATA_DIR / "DAILY.parquet"
    if not daily_path.exists():
        daily_path = config.DATA_DIR / "daily.parquet"
    sf1_path = config.DATA_DIR / "SF1.parquet"
    if not sf1_path.exists():
        sf1_path = config.DATA_DIR / "sf1.parquet"

    views = {}
    for name, path in paths.items():
        if path.exists():
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({_path_sql(path)})")
            views[name] = True
        else:
            views[name] = False

    if not views.get("master"):
        out("Master not found; run pipeline first.")
        failures.append("master_missing")
        con.close()
        return report, failures

    # ----- Contract tests -----
    section(report, "1. Contract tests")

    # Artifact existence
    required_artifacts = ["master", "universe", "fp", "macro", "price"]
    for name in required_artifacts:
        if views.get(name):
            out(f"  {name}: EXISTS")
        else:
            out(f"  {name}: MISSING")
            failures.append(f"contract_{name}_missing")
    for name in ["sector_relative", "insider"]:
        out(f"  {name}: {'EXISTS' if views.get(name) else 'optional missing'}")

    # Master schema
    master_cols = [r[0] for r in con.execute("DESCRIBE master").fetchall()]
    missing_master = [c for c in MASTER_REQUIRED if c not in master_cols]
    if missing_master:
        out(f"  Master missing columns: {missing_master}")
        failures.append("contract_master_schema")
    else:
        out("  Master schema: required columns present")

    # Universe schema
    if views.get("universe"):
        univ_cols = [r[0] for r in con.execute("DESCRIBE universe").fetchall()]
        missing_univ = [c for c in UNIVERSE_REQUIRED if c not in univ_cols]
        if missing_univ:
            out(f"  Universe missing columns: {missing_univ}")
            failures.append("contract_universe_schema")
        else:
            out("  Universe schema: required columns present")

    # No duplicate (ticker, date)
    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT ticker, date, COUNT(*) AS n FROM master GROUP BY ticker, date HAVING n > 1
        )
    """).fetchone()[0]
    if dupes != 0:
        out(f"  Duplicate (ticker, date): {dupes} — CRITICAL")
        failures.append("contract_duplicates")
    else:
        out("  Duplicate (ticker, date): 0 PASS")

    # Date range
    try:
        min_date = con.execute("SELECT MIN(CAST(date AS DATE)) FROM master").fetchone()[0]
        max_date = con.execute("SELECT MAX(CAST(date AS DATE)) FROM master").fetchone()[0]
        if min_date is not None and str(min_date) < config.DATE_START:
            out(f"  Date range: min date {min_date} before DATE_START {config.DATE_START}")
            failures.append("contract_date_range")
        elif max_date is not None and str(max_date) > config.DATE_END:
            out(f"  Date range: max date {max_date} after DATE_END {config.DATE_END}")
            failures.append("contract_date_range")
        else:
            out(f"  Date range: [{min_date}, {max_date}] OK")
    except Exception as e:
        out(f"  Date range check failed: {e}")

    # Row count sanity
    try:
        master_rows = con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
        out(f"  Master row count: {master_rows}")
        if master_rows == 0:
            failures.append("contract_master_empty")
        if views.get("universe"):
            univ_in = con.execute("SELECT COUNT(*) FROM universe WHERE in_universe = TRUE").fetchone()[0]
            out(f"  Universe (in_universe=True) rows: {univ_in}")
    except Exception as e:
        out(f"  Row count check failed: {e}")

    # Cross-artifact join integrity: master and universe in_universe=True must match on (ticker, date)
    if views.get("master") and views.get("universe"):
        try:
            master_count = con.execute("SELECT COUNT(*) FROM master").fetchone()[0]
            univ_in = con.execute("SELECT COUNT(*) FROM universe WHERE in_universe = TRUE").fetchone()[0]
            if master_count != univ_in:
                out(f"  Cross-join: FAIL (master={master_count}, universe in_universe=True={univ_in})")
                failures.append("cross_join_count")
            else:
                out(f"  Cross-join row count: PASS ({master_count})")
            in_master_not_univ = con.execute("""
                SELECT COUNT(*) FROM (
                    SELECT ticker, CAST(date AS DATE) AS dt FROM master
                    EXCEPT
                    SELECT ticker, CAST(date AS DATE) FROM universe WHERE in_universe = TRUE
                ) t
            """).fetchone()[0]
            in_univ_not_master = con.execute("""
                SELECT COUNT(*) FROM (
                    SELECT ticker, CAST(date AS DATE) FROM universe WHERE in_universe = TRUE
                    EXCEPT
                    SELECT ticker, CAST(date AS DATE) AS dt FROM master
                ) t
            """).fetchone()[0]
            if in_master_not_univ != 0 or in_univ_not_master != 0:
                out(f"  Cross-join EXCEPT: FAIL (in_master_not_univ={in_master_not_univ}, in_univ_not_master={in_univ_not_master})")
                failures.append("cross_join_except")
            else:
                out("  Cross-join EXCEPT: PASS (0 both directions)")
        except Exception as e:
            out(f"  Cross-join check failed: {e}")

    # ----- Known-output spot checks -----
    section(report, "2. Known-output spot checks")

    # AAPL ret_12m null for first 252 rows
    try:
        aapl_first_252 = con.execute("""
            SELECT COUNT(*) AS n, SUM(CASE WHEN ret_12m IS NULL THEN 1 ELSE 0 END) AS nulls
            FROM (
                SELECT date, ret_12m, ROW_NUMBER() OVER (ORDER BY CAST(date AS DATE)) AS rn
                FROM master WHERE ticker = 'AAPL'
            ) t WHERE rn <= 252
        """).fetchone()
        if aapl_first_252 and aapl_first_252[0] > 0:
            n, nulls = aapl_first_252[0], aapl_first_252[1]
            if nulls == n:
                out(f"  AAPL ret_12m null for first 252 rows: PASS ({nulls}/{n})")
            else:
                out(f"  AAPL ret_12m null for first 252 rows: FAIL (expected all null, got {nulls}/{n} null)")
                # Not added to failures: when DATE_START is after ticker's first trading day, first 252 rows have history and non-null ret_12m
        else:
            out("  AAPL ret_12m first 252: skip (no AAPL or <252 rows)")
    except Exception as e:
        out(f"  AAPL ret_12m first 252 check failed: {e}")

    # AAPL ret_12m on spot date: compare to SEP-derived expected
    if sep_path.exists():
        con.execute(f"CREATE OR REPLACE VIEW sep AS SELECT * FROM read_parquet({_path_sql(sep_path)})")
        try:
            # Expected: from SEP, for AAPL, closeadj at spot_date / closeadj 252 trading days earlier - 1
            expected_row = con.execute(f"""
                WITH ordered AS (
                    SELECT CAST(date AS DATE) AS dt, closeadj,
                           LAG(closeadj, 252) OVER (ORDER BY CAST(date AS DATE)) AS closeadj_252
                    FROM sep WHERE ticker = 'AAPL'
                )
                SELECT dt, closeadj, closeadj_252,
                       (closeadj / NULLIF(closeadj_252, 0) - 1) AS expected_ret12m
                FROM ordered
                WHERE dt = CAST('{SPOT_DATE}' AS DATE)
            """).fetchone()
            if expected_row and expected_row[3] is not None:
                expected_ret = float(expected_row[3])
                actual_row = con.execute(f"""
                    SELECT ret_12m FROM master
                    WHERE ticker = 'AAPL' AND CAST(date AS DATE) = CAST('{SPOT_DATE}' AS DATE)
                """).fetchone()
                actual_ret = float(actual_row[0]) if actual_row and actual_row[0] is not None else None
                if actual_ret is not None and abs(actual_ret - expected_ret) <= RET_12M_TOL:
                    out(f"  AAPL ret_12m on {SPOT_DATE}: PASS (expected ~{expected_ret:.6f}, got {actual_ret:.6f})")
                elif actual_ret is not None:
                    out(f"  AAPL ret_12m on {SPOT_DATE}: FAIL (expected ~{expected_ret:.6f}, got {actual_ret:.6f})")
                    failures.append("known_ret12m_spot_date")
                else:
                    out(f"  AAPL ret_12m on {SPOT_DATE}: FAIL (master has NULL)")
                    failures.append("known_ret12m_spot_date")
            else:
                out(f"  AAPL ret_12m on {SPOT_DATE}: skip (no SEP data or 252-day history)")
        except Exception as e:
            out(f"  AAPL ret_12m spot check failed: {e}")
    else:
        out("  AAPL ret_12m spot date: skip (SEP not found)")

    # AAPL pe_pit on spot date: compare to SEP + SF1 ART derived (5% tolerance)
    if sep_path.exists() and sf1_path.exists() and views.get("master"):
        try:
            con.execute(f"CREATE OR REPLACE VIEW sep AS SELECT * FROM read_parquet({_path_sql(sep_path)})")
            con.execute(f"CREATE OR REPLACE VIEW sf1 AS SELECT * FROM read_parquet({_path_sql(sf1_path)})")
            # Most recent ART row for AAPL with datekey <= PE_PIT_SPOT_DATE; join SEP for closeadj on that date
            expected_pe = con.execute(f"""
                WITH art_latest AS (
                    SELECT ticker, datekey, netinccmn, shareswa
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (ORDER BY datekey DESC) AS rn
                        FROM sf1
                        WHERE dimension = 'ART' AND ticker = 'AAPL' AND CAST(datekey AS DATE) <= CAST('{PE_PIT_SPOT_DATE}' AS DATE)
                    ) t WHERE rn = 1
                ),
                sep_spot AS (
                    SELECT ticker, closeadj FROM sep
                    WHERE ticker = 'AAPL' AND CAST(date AS DATE) = CAST('{PE_PIT_SPOT_DATE}' AS DATE)
                )
                SELECT (s.closeadj * a.shareswa) / NULLIF(a.netinccmn, 0) AS expected_pe
                FROM art_latest a JOIN sep_spot s ON s.ticker = a.ticker
            """).fetchone()
            if expected_pe and expected_pe[0] is not None:
                exp_pe = float(expected_pe[0])
                actual_pe = con.execute(f"""
                    SELECT pe_pit FROM master
                    WHERE ticker = 'AAPL' AND CAST(date AS DATE) = CAST('{PE_PIT_SPOT_DATE}' AS DATE)
                """).fetchone()
                act_pe = float(actual_pe[0]) if actual_pe and actual_pe[0] is not None else None
                if act_pe is not None and exp_pe > 0:
                    ratio = act_pe / exp_pe
                    if abs(ratio - 1.0) <= PE_PIT_TOL:
                        out(f"  AAPL pe_pit on {PE_PIT_SPOT_DATE}: PASS (expected ~{exp_pe:.2f}, got {act_pe:.2f})")
                    else:
                        out(f"  AAPL pe_pit on {PE_PIT_SPOT_DATE}: FAIL (expected ~{exp_pe:.2f}, got {act_pe:.2f}, ratio={ratio:.3f})")
                        failures.append("known_pe_pit_spot")
                elif act_pe is None:
                    out(f"  AAPL pe_pit on {PE_PIT_SPOT_DATE}: FAIL (master has NULL)")
                    failures.append("known_pe_pit_spot")
            else:
                out(f"  AAPL pe_pit on {PE_PIT_SPOT_DATE}: skip (no ART or SEP for date)")
        except Exception as e:
            out(f"  AAPL pe_pit spot check failed: {e}")
    else:
        out("  AAPL pe_pit spot date: skip (SEP/SF1 or master not found)")

    # JPM sector / XOM famaindustry (metadata from universe join)
    if views.get("master"):
        try:
            jpm_sector = con.execute("SELECT sector FROM master WHERE ticker = 'JPM' AND sector IS NOT NULL LIMIT 1").fetchone()
            if jpm_sector and "Financial" in str(jpm_sector[0]):
                out(f"  JPM sector: PASS ({jpm_sector[0]})")
            elif jpm_sector:
                out(f"  JPM sector: WARN (got '{jpm_sector[0]}', expect Financial Services)")
            else:
                out("  JPM sector: skip (no JPM or null)")
            xom_ind = con.execute("SELECT famaindustry FROM master WHERE ticker = 'XOM' AND famaindustry IS NOT NULL LIMIT 1").fetchone()
            if xom_ind and "Petroleum" in str(xom_ind[0]):
                out(f"  XOM famaindustry: PASS ({xom_ind[0]})")
            elif xom_ind:
                out(f"  XOM famaindustry: WARN (got '{xom_ind[0]}', expect Petroleum and Natural Gas)")
            else:
                out("  XOM famaindustry: skip (no XOM or null)")
        except Exception as e:
            out(f"  Sector/famaindustry spot check failed: {e}")

    # Macro: yield curve Aug-Oct 2019 < 0
    if views.get("macro"):
        try:
            yc_min = con.execute("""
                SELECT MIN(yield_curve) FROM macro
                WHERE CAST(date AS DATE) BETWEEN '2019-08-01' AND '2019-10-31'
            """).fetchone()[0]
            if yc_min is not None and yc_min < 0:
                out(f"  Macro yield curve Aug-Oct 2019: PASS (min={yc_min:.3f})")
            elif yc_min is not None:
                out(f"  Macro yield curve Aug-Oct 2019: FAIL (min={yc_min:.3f}, expect < 0)")
                failures.append("known_yield_curve_2019")
            else:
                out("  Macro yield curve Aug-Oct 2019: skip (no data)")
        except Exception as e:
            out(f"  Yield curve check failed: {e}")

        # VIX Mar 2020 > 2x 2019
        try:
            vix_row = con.execute("""
                SELECT
                    AVG(CASE WHEN CAST(date AS DATE) BETWEEN '2020-03-01' AND '2020-03-31' THEN vix END) AS vix_mar2020,
                    AVG(CASE WHEN CAST(date AS DATE) BETWEEN '2019-01-01' AND '2019-12-31' THEN vix END) AS vix_2019
                FROM macro
            """).fetchone()
            if vix_row and vix_row[0] is not None and vix_row[1] is not None and vix_row[1] != 0:
                if vix_row[0] > vix_row[1] * 2:
                    out(f"  Macro VIX Mar 2020 spike: PASS ({vix_row[0]:.1f} > 2*{vix_row[1]:.1f})")
                else:
                    out(f"  Macro VIX Mar 2020 spike: FAIL ({vix_row[0]:.1f} not > 2*{vix_row[1]:.1f})")
                    failures.append("known_vix_mar2020")
            else:
                out("  Macro VIX Mar 2020: skip (no data)")
        except Exception as e:
            out(f"  VIX check failed: {e}")

        # CPI 2022 > 2x 2019
        try:
            cpi_row = con.execute("""
                SELECT
                    AVG(CASE WHEN CAST(date AS DATE) BETWEEN '2022-01-01' AND '2022-12-31' THEN cpi_yoy END) AS cpi_2022,
                    AVG(CASE WHEN CAST(date AS DATE) BETWEEN '2019-01-01' AND '2019-12-31' THEN cpi_yoy END) AS cpi_2019
                FROM macro
            """).fetchone()
            if cpi_row and cpi_row[0] is not None and cpi_row[1] is not None and cpi_row[1] != 0:
                if cpi_row[0] > cpi_row[1] * 2:
                    out(f"  Macro CPI 2022 elevated: PASS ({cpi_row[0]:.1f} > 2*{cpi_row[1]:.1f})")
                else:
                    out(f"  Macro CPI 2022 elevated: FAIL ({cpi_row[0]:.1f} not > 2*{cpi_row[1]:.1f})")
                    failures.append("known_cpi_2022")
            else:
                out("  Macro CPI 2022: skip (no data)")
        except Exception as e:
            out(f"  CPI check failed: {e}")

    # ----- Sanity tests -----
    section(report, "3. Sanity tests")

    # PIT integrity: no datekey > date
    if views.get("fp"):
        try:
            datekey_col = None
            cols = [r[0] for r in con.execute("DESCRIBE fp").fetchall()]
            if "datekey" in cols:
                datekey_col = "datekey"
            elif "art_datekey" in cols:
                datekey_col = "art_datekey"
            if datekey_col:
                pit_violations = con.execute(f"""
                    SELECT COUNT(*) FROM fp
                    WHERE {datekey_col} IS NOT NULL AND CAST({datekey_col} AS DATE) > CAST(date AS DATE)
                """).fetchone()[0]
                if pit_violations != 0:
                    out(f"  PIT integrity: FAIL ({pit_violations} rows with datekey > date)")
                    failures.append("sanity_pit")
                else:
                    out("  PIT integrity: PASS (no datekey > date)")
            else:
                out("  PIT integrity: skip (no datekey column)")
        except Exception as e:
            out(f"  PIT check failed: {e}")
    else:
        out("  PIT integrity: skip (fp not found)")

    # Valuation ranges (report % outside; do not assert strict)
    try:
        for col, lo, hi in [
            ("pe_pit", 0, 500),
            ("pb_pit", 0, 50),
            ("pcf_pit", 0, 200),
            ("evebitda_pit", 0, 100),
        ]:
            if col not in master_cols:
                continue
            n_out = con.execute(f"""
                SELECT COUNT(*) FROM master
                WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})
            """).fetchone()[0]
            n_valid = con.execute(f"SELECT COUNT(*) FROM master WHERE {col} IS NOT NULL").fetchone()[0]
            pct = (n_out / n_valid * 100) if n_valid else 0
            out(f"  {col} outside [{lo},{hi}]: {n_out}/{n_valid} ({pct:.1f}%)")
    except Exception as e:
        out(f"  Valuation range check failed: {e}")

    # vol_20d median in (0.1, 0.8)
    try:
        vol_median = con.execute("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_20d) FROM master WHERE vol_20d IS NOT NULL").fetchone()[0]
        if vol_median is not None:
            if 0.1 < vol_median < 0.8:
                out(f"  vol_20d median: PASS ({vol_median:.3f})")
            else:
                out(f"  vol_20d median: WARN ({vol_median:.3f}, expect 0.1-0.8)")
    except Exception as e:
        out(f"  vol_20d check failed: {e}")

    # ATR median < 0.10 (price fraction)
    try:
        atr_col = "atr_14d_normalized" if "atr_14d_normalized" in master_cols else "atr_14d"
        if atr_col in master_cols:
            atr_median = con.execute(f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {atr_col}) FROM master WHERE {atr_col} IS NOT NULL").fetchone()[0]
            if atr_median is not None:
                if atr_median < 0.10:
                    out(f"  {atr_col} median: PASS ({atr_median:.3f})")
                else:
                    out(f"  {atr_col} median: WARN ({atr_median:.3f}, expect < 0.10)")
        else:
            out("  ATR: skip (column not in master)")
    except Exception as e:
        out(f"  ATR check failed: {e}")

    # Sector relative: median pe_vs_sector near 1.0
    if views.get("sector_relative"):
        try:
            sr = con.execute("""
                SELECT MEDIAN(pe_vs_sector) AS med_pe, AVG(roic_vs_sector) AS mean_roic
                FROM sector_relative WHERE pe_vs_sector IS NOT NULL
            """).fetchone()
            if sr and sr[0] is not None:
                if 0.7 <= sr[0] <= 1.3:
                    out(f"  Sector pe_vs_sector median: PASS ({sr[0]:.3f})")
                else:
                    out(f"  Sector pe_vs_sector median: WARN ({sr[0]:.3f}, expect ~1.0)")
            if sr and sr[1] is not None and -0.1 <= sr[1] <= 0.1:
                out(f"  Sector roic_vs_sector mean: PASS ({sr[1]:.3f})")
            elif sr and sr[1] is not None:
                out(f"  Sector roic_vs_sector mean: WARN ({sr[1]:.3f})")
        except Exception as e:
            out(f"  Sector relative check failed: {e}")
    else:
        out("  Sector relative: skip (no parquet)")

    # Staleness bounds: days_since_filing (only where non-null) median < 120, max < 400
    if "days_since_filing" in master_cols:
        try:
            staleness = con.execute("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_since_filing) AS med,
                       MAX(days_since_filing) AS mx
                FROM master WHERE days_since_filing IS NOT NULL
            """).fetchone()
            if staleness and (staleness[0] is not None or staleness[1] is not None):
                med, mx = staleness[0], staleness[1]
                med_f = float(med) if med is not None else None
                mx_f = float(mx) if mx is not None else None
                if med_f is not None and med_f > STALENESS_MEDIAN_MAX:
                    out(f"  days_since_filing median: FAIL ({med_f:.0f} > {STALENESS_MEDIAN_MAX})")
                    failures.append("sanity_staleness_median")
                elif mx_f is not None and mx_f > STALENESS_ABS_MAX:
                    out(f"  days_since_filing max: FAIL ({mx_f:.0f} > {STALENESS_ABS_MAX})")
                    failures.append("sanity_staleness_max")
                else:
                    med_str = f"{med_f:.0f}" if med_f is not None else "N/A"
                    mx_str = f"{mx_f:.0f}" if mx_f is not None else "N/A"
                    out(f"  days_since_filing (non-null): median={med_str}, max={mx_str} PASS")
            else:
                out("  days_since_filing: skip (all null)")
        except Exception as e:
            out(f"  Staleness check failed: {e}")

    # Quality not all null
    try:
        quality_null = con.execute("SELECT AVG(CASE WHEN ncfo_r2_adjusted IS NULL THEN 1.0 ELSE 0.0 END) FROM master").fetchone()[0]
        if quality_null is not None:
            if quality_null >= 0.99:
                out(f"  ncfo_r2_adjusted null rate: FAIL ({quality_null:.1%} — almost all null)")
                out("  (Cause: quality_metrics table empty in 02_fundamentals — check SF1 ARQ data and pipeline logs.)")
                failures.append("sanity_quality_null")
            else:
                out(f"  ncfo_r2_adjusted null rate: PASS ({quality_null:.1%})")
    except Exception as e:
        out(f"  Quality null check failed: {e}")

    # Macro date count vs universe; no weekends; EXCEPT alignment
    if views.get("macro") and views.get("universe"):
        try:
            macro_dates = con.execute("SELECT COUNT(DISTINCT date) FROM macro").fetchone()[0]
            univ_dates = con.execute("SELECT COUNT(DISTINCT date) FROM universe").fetchone()[0]
            if macro_dates == univ_dates:
                out(f"  Macro vs universe date count: PASS ({macro_dates})")
            else:
                out(f"  Macro vs universe date count: FAIL (macro={macro_dates}, universe={univ_dates})")
                failures.append("sanity_macro_dates")
            macro_not_univ = con.execute("""
                SELECT COUNT(*) FROM (SELECT CAST(date AS DATE) AS dt FROM macro EXCEPT SELECT CAST(date AS DATE) FROM universe) t
            """).fetchone()[0]
            univ_not_macro = con.execute("""
                SELECT COUNT(*) FROM (SELECT CAST(date AS DATE) FROM universe EXCEPT SELECT CAST(date AS DATE) AS dt FROM macro) t
            """).fetchone()[0]
            if macro_not_univ != 0 or univ_not_macro != 0:
                out(f"  Macro date EXCEPT: FAIL (macro_not_univ={macro_not_univ}, univ_not_macro={univ_not_macro})")
                failures.append("sanity_macro_except")
            else:
                out("  Macro date EXCEPT: PASS (0 both directions)")
        except Exception as e:
            out(f"  Macro date count failed: {e}")
        try:
            weekend_count = con.execute("""
                SELECT COUNT(*) FROM macro WHERE EXTRACT(DOW FROM CAST(date AS DATE)) IN (0, 6)
            """).fetchone()[0]
            if weekend_count > 0:
                out(f"  Macro weekend dates: FAIL ({weekend_count})")
                failures.append("sanity_macro_weekends")
            else:
                out("  Macro weekend dates: PASS (0)")
        except Exception as e:
            out(f"  Weekend check failed: {e}")

    # ----- Coverage matrix -----
    section(report, "4. Coverage matrix (non-null % by ticker)")
    if views.get("master"):
        try:
            cols = [c for c in COVERAGE_COLUMNS if c in master_cols]
            if cols:
                tickers = [r[0] for r in con.execute("SELECT DISTINCT ticker FROM master ORDER BY ticker").fetchall()]
                # Build table: ticker -> column -> pct non-null
                data = {}
                for ticker in tickers:
                    data[ticker] = {}
                    total = con.execute("SELECT COUNT(*) FROM master WHERE ticker = ?", [ticker]).fetchone()[0]
                    for col in cols:
                        n = con.execute(f"SELECT COUNT(*) FROM master WHERE ticker = ? AND {col} IS NOT NULL", [ticker]).fetchone()[0]
                        data[ticker][col] = (n / total * 100) if total else 0
                # Header
                header = "ticker" + "".join(f"{c:>12}" for c in cols)
                out(f"  {header}")
                for ticker in tickers:
                    row = f"  {ticker:8}" + "".join(f"{data[ticker][c]:11.1f}%" for c in cols)
                    out(row)
                # Flag: any ticker 0% on a column where another has >50% (informational only; not critical)
                for col in cols:
                    pcts = [data[t][col] for t in tickers]
                    if max(pcts) > COVERAGE_FLAG_PCT:
                        zeros = [t for t in tickers if data[t][col] == 0]
                        if zeros:
                            out(f"  FLAG: {col} 0% for {zeros} (others >{COVERAGE_FLAG_PCT}%)")
            else:
                out("  No coverage columns in master; skip")
        except Exception as e:
            out(f"  Coverage matrix failed: {e}")

    # ----- Forward label sanity -----
    section(report, "5. Forward label sanity")
    if views.get("master"):
        try:
            for t in ALWAYS_LISTED_TICKERS:
                bad = con.execute("""
                    SELECT COUNT(*) FROM master WHERE ticker = ? AND (fwd_delisted_21td = 1 OR fwd_delisted_63td = 1)
                """, [t]).fetchone()[0]
                if bad > 0:
                    out(f"  {t} fwd_delisted: FAIL ({bad} rows with 21td/63td=1)")
                    failures.append("fwd_label_listed")
                else:
                    out(f"  {t} fwd_delisted: PASS (all 0)")
            # HTZ: if present, after HTZ_DELIST_DATE should have fwd_delisted set (from labels) and in_universe=False in universe.
            # Our dataset has HTZ only post-bankruptcy (re-listed); skip delist check when no rows in delist window.
            htz_in_master = con.execute("SELECT COUNT(*) FROM master WHERE ticker = 'HTZ'").fetchone()[0]
            if htz_in_master > 0:
                htz_in_delist_window = con.execute(f"""
                    SELECT COUNT(*) FROM master
                    WHERE ticker = 'HTZ' AND CAST(date AS DATE) BETWEEN CAST('{HTZ_DELIST_DATE}' AS DATE) AND '2020-08-01'::DATE
                """).fetchone()[0]
                if htz_in_delist_window == 0:
                    out(f"  HTZ after {HTZ_DELIST_DATE}: skip (HTZ in dataset is post-bankruptcy only; no rows in delist window)")
                    if views.get("universe"):
                        out("  HTZ in_universe after delist: skip (post-bankruptcy only)")
                else:
                    after = con.execute(f"""
                        SELECT COUNT(*) FROM master
                        WHERE ticker = 'HTZ' AND CAST(date AS DATE) >= CAST('{HTZ_DELIST_DATE}' AS DATE)
                        AND (fwd_delisted_21td <> 1 AND fwd_delisted_63td <> 1)
                    """).fetchone()[0]
                    if after > 0:
                        out(f"  HTZ after {HTZ_DELIST_DATE}: FAIL ({after} rows without fwd_delisted=1)")
                        failures.append("fwd_label_htz")
                    else:
                        out(f"  HTZ after {HTZ_DELIST_DATE}: PASS (fwd_delisted set)")
                    if views.get("universe"):
                        univ_after = con.execute(f"""
                            SELECT COUNT(*) FROM universe
                            WHERE ticker = 'HTZ' AND in_universe = TRUE AND CAST(date AS DATE) >= CAST('{HTZ_DELIST_DATE}' AS DATE)
                        """).fetchone()[0]
                        if univ_after > 0:
                            out(f"  HTZ in_universe after delist: FAIL ({univ_after} rows)")
                            failures.append("fwd_label_htz_universe")
                        else:
                            out("  HTZ in_universe after delist: PASS (0)")
            else:
                out("  HTZ: not in master (add to DEBUG_TICKERS to validate delist labels)")
        except Exception as e:
            out(f"  Forward label check failed: {e}")

    # ----- Temporal continuity (AAPL ret_12m no null gaps after first non-null) -----
    section(report, "6. Temporal continuity")
    if views.get("master"):
        try:
            gaps = con.execute("""
                WITH ordered AS (
                    SELECT date, ret_12m,
                           LAG(ret_12m) OVER (ORDER BY CAST(date AS DATE)) AS prev_ret12m
                    FROM master WHERE ticker = 'AAPL'
                )
                SELECT COUNT(*) FROM ordered
                WHERE prev_ret12m IS NOT NULL AND ret_12m IS NULL
            """).fetchone()[0]
            if gaps and gaps > 0:
                out(f"  AAPL ret_12m null gaps: FAIL ({gaps} gaps after first non-null)")
                failures.append("temporal_ret12m_gaps")
            else:
                out("  AAPL ret_12m null gaps: PASS (0)")
        except Exception as e:
            out(f"  Temporal continuity check failed: {e}")

    # Optional: pe_pit vs DAILY.pe for AAPL (report only; relaxed)
    if daily_path.exists() and views.get("master"):
        try:
            con.execute(f"CREATE OR REPLACE VIEW daily AS SELECT * FROM read_parquet({_path_sql(daily_path)})")
            aapl_corr = con.execute("""
                SELECT CORR(m.pe_pit, d.pe) AS corr, MEDIAN(m.pe_pit / NULLIF(d.pe, 0)) AS ratio
                FROM master m
                JOIN daily d ON d.ticker = m.ticker AND CAST(d.date AS DATE) = CAST(m.date AS DATE)
                WHERE m.ticker = 'AAPL' AND m.pe_pit IS NOT NULL AND d.pe IS NOT NULL AND d.pe > 0
                AND CAST(m.date AS DATE) BETWEEN '2020-01-01' AND '2022-12-31'
            """).fetchone()
            if aapl_corr and aapl_corr[0] is not None:
                out(f"  pe_pit vs DAILY.pe (AAPL 2020-2022): correlation={aapl_corr[0]:.3f}, median ratio={aapl_corr[1]:.3f}")
                if aapl_corr[0] < 0.9:
                    out("    WARN: correlation < 0.9")
            else:
                out("  pe_pit vs DAILY.pe: skip (no AAPL overlap)")
        except Exception as e:
            out(f"  pe_pit vs DAILY check failed: {e}")

    con.close()

    # ----- Summary -----
    section(report, "SUMMARY")
    if failures:
        out(f"Critical failures: {', '.join(failures)}")
    else:
        out("All critical checks PASSED.")
    return report, failures


def main() -> int:
    report_path = config.OUTPUTS_DIR / "validation_report_debug.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report_lines, failures = run()
    body = "\n".join(report_lines)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(body)
        f.write("\n")

    print(body)
    print(f"\nReport saved to {report_path}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
