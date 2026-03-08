#!/usr/bin/env python3
"""
PIT fundamental features: ARQ path (rolling quality metrics) + ART path (TTM snapshot
and valuation from current price). No mixed-dimension ASOF.
Output: outputs/features/fundamental_pit.parquet
"""
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb

from config import (
    apply_duckdb_limits,
    DATA_DIR,
    DATE_END,
    DATE_START,
    DAILY_UNIVERSE_PATH,
    FUNDAMENTAL_PIT_PATH,
)

from pipeline.fundamental_quality import compute_quality_metrics_table

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Output schema: match fundamental_pit view column order and 07_merge expectations
# Valuation ratios use shareswa (TTM weighted avg); sharesbas could be used for more current count.
FUNDAMENTAL_PIT_SCHEMA = [
    ("ticker", "VARCHAR"),
    ("date", "DATE"),
    ("art_datekey", "DATE"),
    ("arq_datekey", "DATE"),
    ("datekey", "DATE"),
    ("days_since_filing", "INTEGER"),
    ("quarters_stale", "DOUBLE"),
    ("days_since_art", "INTEGER"),
    ("days_since_arq", "INTEGER"),
    ("days_since_filing_max", "INTEGER"),
    ("ncfo_r2_5y", "DOUBLE"),
    ("ncfo_r2_10y", "DOUBLE"),
    ("ncfo_cagr_5y", "DOUBLE"),
    ("ncfo_cagr_10y", "DOUBLE"),
    ("ncfo_pct_positive", "DOUBLE"),
    ("ncfo_r2_adjusted", "DOUBLE"),
    ("fcf_r2_5y", "DOUBLE"),
    ("fcf_r2_10y", "DOUBLE"),
    ("fcf_cagr_5y", "DOUBLE"),
    ("fcf_cagr_10y", "DOUBLE"),
    ("fcf_pct_positive", "DOUBLE"),
    ("fcf_r2_adjusted", "DOUBLE"),
    ("fcf_ncfo_r2_delta", "DOUBLE"),
    ("ncfo_r2_adjusted_arcsinh", "DOUBLE"),
    ("fcf_r2_adjusted_arcsinh", "DOUBLE"),
    ("roic_level", "DOUBLE"),
    ("roic_slope_3y", "DOUBLE"),
    ("grossmargin_slope", "DOUBLE"),
    ("fcf_conversion", "DOUBLE"),
    ("accrual_ratio", "DOUBLE"),
    ("sbc_pct_revenue", "DOUBLE"),
    ("capex_intensity", "DOUBLE"),
    ("net_debt_trend", "DOUBLE"),
    ("dilution_rate", "DOUBLE"),
    ("fcf_recon_ttm", "DOUBLE"),
    ("pcf_pit", "DOUBLE"),
    ("pfcf_pit", "DOUBLE"),
    ("ncfo_to_revenue", "DOUBLE"),
    ("fcf_to_revenue", "DOUBLE"),
    ("pe_pit", "DOUBLE"),
    ("pb_pit", "DOUBLE"),
    ("ps_pit", "DOUBLE"),
    ("evebitda_pit", "DOUBLE"),
    ("dividend_yield", "DOUBLE"),
    ("roe", "DOUBLE"),
    ("pretax_margin", "DOUBLE"),
    ("current_ratio", "DOUBLE"),
    ("debt_to_equity", "DOUBLE"),
    ("liabilities_to_assets", "DOUBLE"),
    ("payout_ratio", "DOUBLE"),
    ("earnings_growth_yoy", "DOUBLE"),
]


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"


def _write_empty_fundamental_pit() -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([
        ("ticker", pa.string()), ("date", pa.date32()), ("art_datekey", pa.date32()), ("arq_datekey", pa.date32()),
        ("datekey", pa.date32()), ("days_since_filing", pa.int32()), ("quarters_stale", pa.float64()),
        ("days_since_art", pa.int32()), ("days_since_arq", pa.int32()), ("days_since_filing_max", pa.int32()),
        ("ncfo_r2_5y", pa.float64()), ("ncfo_r2_10y", pa.float64()), ("ncfo_cagr_5y", pa.float64()), ("ncfo_cagr_10y", pa.float64()),
        ("ncfo_pct_positive", pa.float64()), ("ncfo_r2_adjusted", pa.float64()),
        ("fcf_r2_5y", pa.float64()), ("fcf_r2_10y", pa.float64()), ("fcf_cagr_5y", pa.float64()), ("fcf_cagr_10y", pa.float64()),
        ("fcf_pct_positive", pa.float64()), ("fcf_r2_adjusted", pa.float64()), ("fcf_ncfo_r2_delta", pa.float64()),
        ("ncfo_r2_adjusted_arcsinh", pa.float64()), ("fcf_r2_adjusted_arcsinh", pa.float64()),
        ("roic_level", pa.float64()), ("roic_slope_3y", pa.float64()), ("grossmargin_slope", pa.float64()),
        ("fcf_conversion", pa.float64()), ("accrual_ratio", pa.float64()), ("sbc_pct_revenue", pa.float64()),
        ("capex_intensity", pa.float64()), ("net_debt_trend", pa.float64()), ("dilution_rate", pa.float64()),
        ("fcf_recon_ttm", pa.float64()), ("pcf_pit", pa.float64()), ("pfcf_pit", pa.float64()),
        ("ncfo_to_revenue", pa.float64()), ("fcf_to_revenue", pa.float64()),
        ("pe_pit", pa.float64()), ("pb_pit", pa.float64()), ("ps_pit", pa.float64()), ("evebitda_pit", pa.float64()),
        ("dividend_yield", pa.float64()), ("roe", pa.float64()), ("pretax_margin", pa.float64()),
        ("current_ratio", pa.float64()), ("debt_to_equity", pa.float64()), ("liabilities_to_assets", pa.float64()),
        ("payout_ratio", pa.float64()), ("earnings_growth_yoy", pa.float64()),
    ])
    tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
    pq.write_table(tbl, FUNDAMENTAL_PIT_PATH)
    log.info("Wrote empty %s", FUNDAMENTAL_PIT_PATH)


def main() -> None:
    FUNDAMENTAL_PIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    apply_duckdb_limits(con)

    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    if not DAILY_UNIVERSE_PATH.exists():
        log.warning("Universe not found; run 01_universe.py first. Writing empty fundamental_pit.")
        _write_empty_fundamental_pit()
        return

    con.execute(f"CREATE OR REPLACE VIEW grid AS SELECT ticker, date FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})")

    sf1_path = _parquet("SF1")
    sep_path = _parquet("SEP")
    if not sf1_path.exists():
        log.warning("SF1 not found; writing empty fundamental_pit.parquet with schema")
        _write_empty_fundamental_pit()
        return

    con.execute(
        f"CREATE OR REPLACE VIEW sf1 AS SELECT * FROM read_parquet({_path_sql(sf1_path)}) WHERE ticker IN (SELECT DISTINCT ticker FROM grid)"
    )

    # ARQ PIT view: for inspection/debugging only; name signals it is not used by pipeline.
    # Quality metrics are computed by compute_quality_metrics_table with its own PIT handling.
    con.execute(
        """
        CREATE OR REPLACE VIEW arq_pit_reference AS
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, reportperiod ORDER BY datekey DESC) AS rn
            FROM sf1 WHERE dimension = 'ARQ'
        )
        SELECT * FROM ranked WHERE rn = 1
        """
    )

    # Step 2: Python pass — rolling quality metrics per (ticker, datekey)
    ticker_set = set(
        row[0] for row in con.execute("SELECT DISTINCT ticker FROM grid").fetchall()
    )
    log.info("Computing quality metrics from ARQ (vintage loop)...")
    t0 = time.time()
    quality_df = compute_quality_metrics_table(
        con, DATE_START, DATE_END, universe_tickers=ticker_set
    )
    log.info("quality metrics took %.1fs", time.time() - t0)
    log.info("quality_df.shape: %s", quality_df.shape)
    if not quality_df.empty and "ncfo_r2_adjusted" in quality_df.columns:
        sample = quality_df[["ticker", "datekey", "ncfo_r2_adjusted"]].dropna(subset=["ncfo_r2_adjusted"]).head(5)
        if len(sample) > 0:
            log.info("quality_df ncfo_r2_adjusted sample:\n%s", sample.to_string())
    if quality_df.empty:
        log.warning(
            "Quality metrics table is empty (no rows from compute_quality_metrics_table). "
            "ncfo_r2_adjusted and other quality columns will be all null in fundamental_pit and master. "
            "Check logs above for ARQ pull failure or no ARQ data in SF1."
        )
        con.execute(
            """
            CREATE OR REPLACE VIEW quality_metrics AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS datekey,
                   CAST(NULL AS DOUBLE) AS ncfo_r2_5y, CAST(NULL AS DOUBLE) AS ncfo_r2_10y,
                   CAST(NULL AS DOUBLE) AS ncfo_cagr_5y, CAST(NULL AS DOUBLE) AS ncfo_cagr_10y,
                   CAST(NULL AS DOUBLE) AS ncfo_pct_positive, CAST(NULL AS DOUBLE) AS ncfo_r2_adjusted,
                   CAST(NULL AS DOUBLE) AS fcf_r2_5y, CAST(NULL AS DOUBLE) AS fcf_r2_10y,
                   CAST(NULL AS DOUBLE) AS fcf_cagr_5y, CAST(NULL AS DOUBLE) AS fcf_cagr_10y,
                   CAST(NULL AS DOUBLE) AS fcf_pct_positive, CAST(NULL AS DOUBLE) AS fcf_r2_adjusted,
                   CAST(NULL AS DOUBLE) AS fcf_ncfo_r2_delta,
                   CAST(NULL AS DOUBLE) AS ncfo_r2_adjusted_arcsinh, CAST(NULL AS DOUBLE) AS fcf_r2_adjusted_arcsinh,
                   CAST(NULL AS DOUBLE) AS roic_level, CAST(NULL AS DOUBLE) AS roic_slope_3y,
                   CAST(NULL AS DOUBLE) AS grossmargin_slope, CAST(NULL AS DOUBLE) AS net_debt_trend,
                   CAST(NULL AS DOUBLE) AS dilution_rate
            WHERE 1=0
            """
        )
    else:
        con.register("quality_metrics_df", quality_df)
        con.execute("CREATE OR REPLACE VIEW quality_metrics AS SELECT * FROM quality_metrics_df")

    # ART: TTM snapshot only (no ARQ/ARY)
    con.execute("CREATE OR REPLACE VIEW art AS SELECT * FROM sf1 WHERE dimension = 'ART'")

    if sep_path.exists():
        con.execute(f"CREATE OR REPLACE VIEW sep AS SELECT * FROM read_parquet({_path_sql(sep_path)})")
    else:
        con.execute(
            "CREATE OR REPLACE VIEW sep AS SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date, CAST(NULL AS DOUBLE) AS closeadj WHERE 1=0"
        )

    # Step 3: ART path — single pass over grid with both current and prior-year ASOF joins
    # PIT: a_prior must use g.date - 1 year (NOT g.date) so we get prior-year TTM earnings; otherwise earnings_growth_yoy would be wrong.
    con.execute(
        """
        CREATE OR REPLACE VIEW art_snapshot_combined AS
        SELECT g.ticker, g.date,
               a.datekey_date AS art_datekey,
               a.netinccmn, a.ncfo, a.fcf AS fcf_sharadar, a.capex,
               a.ncfo + a.capex AS fcf_recon,
               a.assets, a.revenueusd, a.revenue, a.sbcomp,
               a.equity, a.debt, a.cashnequsd, a.ebitda, a.shareswa,
               a.divyield, a.dps, a.roe, a.ebt, a.currentratio, a.liabilities, a.epsdil,
               s.closeadj,
               a_prior.epsdil AS epsdil_prior
        FROM grid g
        ASOF LEFT JOIN (
            SELECT *, CAST(datekey AS DATE) AS datekey_date
            FROM art
            ORDER BY datekey_date
        ) a ON a.ticker = g.ticker AND a.datekey_date <= g.date
        ASOF LEFT JOIN (
            SELECT ticker, CAST(datekey AS DATE) AS datekey_date, epsdil
            FROM art
            ORDER BY datekey_date
        ) a_prior ON a_prior.ticker = g.ticker AND a_prior.datekey_date <= g.date - INTERVAL '1 year'
        LEFT JOIN sep s ON s.ticker = g.ticker AND s.date = g.date
        """
    )

    # Step 4: Merge — grid + ASOF quality_metrics + art_snapshot_combined (one join; no ORDER BY — parquet doesn't preserve order)
    # datekey = latest of art_datekey and arq_datekey; staleness from that. FCF from fcf_recon.
    con.execute(
        """
        CREATE OR REPLACE VIEW fundamental_pit AS
        SELECT
            g.ticker,
            g.date,
            art.art_datekey,
            q.datekey_date AS arq_datekey,
            CASE WHEN art.art_datekey IS NULL AND q.datekey_date IS NULL THEN NULL
                 ELSE GREATEST(COALESCE(art.art_datekey, '1900-01-01'::DATE), COALESCE(q.datekey_date, '1900-01-01'::DATE)) END AS datekey,
            CASE WHEN art.art_datekey IS NULL AND q.datekey_date IS NULL THEN NULL
                 ELSE DATEDIFF('day', GREATEST(COALESCE(art.art_datekey, '1900-01-01'::DATE), COALESCE(q.datekey_date, '1900-01-01'::DATE)), g.date)::INTEGER END AS days_since_filing,
            CASE WHEN art.art_datekey IS NULL AND q.datekey_date IS NULL THEN NULL
                 ELSE DATEDIFF('day', GREATEST(COALESCE(art.art_datekey, '1900-01-01'::DATE), COALESCE(q.datekey_date, '1900-01-01'::DATE)), g.date) / 91.0 END AS quarters_stale,
            CASE WHEN art.art_datekey IS NOT NULL THEN DATEDIFF('day', art.art_datekey, g.date)::INTEGER ELSE NULL END AS days_since_art,
            CASE WHEN q.datekey_date IS NOT NULL THEN DATEDIFF('day', q.datekey_date, g.date)::INTEGER ELSE NULL END AS days_since_arq,
            CASE WHEN art.art_datekey IS NULL AND q.datekey_date IS NULL THEN NULL
                 ELSE GREATEST(COALESCE(DATEDIFF('day', art.art_datekey, g.date), 0), COALESCE(DATEDIFF('day', q.datekey_date, g.date), 0))::INTEGER END AS days_since_filing_max,
            q.ncfo_r2_5y,
            q.ncfo_r2_10y,
            q.ncfo_cagr_5y,
            q.ncfo_cagr_10y,
            q.ncfo_pct_positive,
            q.ncfo_r2_adjusted,
            q.fcf_r2_5y,
            q.fcf_r2_10y,
            q.fcf_cagr_5y,
            q.fcf_cagr_10y,
            q.fcf_pct_positive,
            q.fcf_r2_adjusted,
            q.fcf_ncfo_r2_delta,
            q.ncfo_r2_adjusted_arcsinh,
            q.fcf_r2_adjusted_arcsinh,
            q.roic_level,
            q.roic_slope_3y,
            q.grossmargin_slope,
            CASE WHEN art.netinccmn IS NOT NULL AND art.netinccmn <> 0 THEN (art.ncfo + art.capex) / art.netinccmn ELSE NULL END AS fcf_conversion,
            CASE WHEN art.assets IS NOT NULL AND art.assets <> 0 THEN (art.netinccmn - art.ncfo) / art.assets ELSE NULL END AS accrual_ratio,
            -- revenueusd preferred for consistency with USD market cap; for non-USD filers uses period exchange rate
            CASE WHEN COALESCE(art.revenueusd, art.revenue) > 0 THEN art.sbcomp / COALESCE(art.revenueusd, art.revenue) ELSE NULL END AS sbc_pct_revenue,
            ABS(art.capex) / NULLIF(COALESCE(art.revenueusd, art.revenue), 0) AS capex_intensity,
            q.net_debt_trend,
            q.dilution_rate,
            art.ncfo + art.capex AS fcf_recon_ttm,
            CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL AND art.ncfo > 0
                 THEN (art.closeadj * art.shareswa) / NULLIF(art.ncfo, 0) ELSE NULL END AS pcf_pit,
            CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL AND (art.ncfo + art.capex) <> 0
                 THEN (art.closeadj * art.shareswa) / (art.ncfo + art.capex) ELSE NULL END AS pfcf_pit,
            art.ncfo / NULLIF(COALESCE(art.revenueusd, art.revenue), 0) AS ncfo_to_revenue,
            (art.ncfo + art.capex) / NULLIF(COALESCE(art.revenueusd, art.revenue), 0) AS fcf_to_revenue,
            CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL AND art.netinccmn > 0
                 THEN (art.closeadj * art.shareswa) / NULLIF(art.netinccmn, 0) ELSE NULL END AS pe_pit,
            CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL AND art.equity > 0
                 THEN (art.closeadj * art.shareswa) / NULLIF(art.equity, 0) ELSE NULL END AS pb_pit,
            CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL
                 THEN (art.closeadj * art.shareswa) / NULLIF(COALESCE(art.revenueusd, art.revenue), 0) ELSE NULL END AS ps_pit,
            CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL AND art.ebitda IS NOT NULL AND art.ebitda > 0
                 THEN ((art.closeadj * art.shareswa) + COALESCE(art.debt, 0) - COALESCE(art.cashnequsd, 0)) / art.ebitda ELSE NULL END AS evebitda_pit,
            COALESCE(art.divyield, art.dps / NULLIF(art.closeadj, 0)) AS dividend_yield,
            COALESCE(art.roe, art.netinccmn / NULLIF(art.equity, 0)) AS roe,
            art.ebt / NULLIF(COALESCE(art.revenueusd, art.revenue), 0) AS pretax_margin,
            art.currentratio AS current_ratio,
            art.debt / NULLIF(art.equity, 0) AS debt_to_equity,
            art.liabilities / NULLIF(art.assets, 0) AS liabilities_to_assets,
            art.dps / NULLIF(art.epsdil, 0) AS payout_ratio,
            CASE WHEN art.epsdil IS NOT NULL AND art.epsdil_prior IS NOT NULL AND art.epsdil_prior <> 0
                 THEN (art.epsdil - art.epsdil_prior) / NULLIF(art.epsdil_prior, 0) ELSE NULL END AS earnings_growth_yoy
        FROM grid g
        ASOF LEFT JOIN (
            SELECT *, CAST(datekey AS DATE) AS datekey_date
            FROM quality_metrics
            ORDER BY datekey_date
        ) q ON q.ticker = g.ticker AND q.datekey_date <= g.date
        LEFT JOIN art_snapshot_combined art ON art.ticker = g.ticker AND art.date = g.date
        """
    )

    try:
        # Get schema from view before writing so we don't re-read the parquet for verification
        actual = set(
            con.execute("SELECT * FROM fundamental_pit LIMIT 0").df().columns
        )
        expected = {col for col, _ in FUNDAMENTAL_PIT_SCHEMA}
        missing = expected - actual
        extra = actual - expected
        if missing:
            log.error("Schema missing columns: %s", missing)
        if extra:
            log.warning("Schema has extra columns: %s", extra)
        if not missing and not extra:
            log.info("Schema matches exactly")
        con.execute(f"COPY (SELECT * FROM fundamental_pit) TO {_path_sql(FUNDAMENTAL_PIT_PATH)} (FORMAT PARQUET)")
        log.info("Wrote %s", FUNDAMENTAL_PIT_PATH)
    except Exception as e:
        log.error("Write failed: %s. Writing empty output.", e)
        _write_empty_fundamental_pit()
    con.close()


if __name__ == "__main__":
    main()
