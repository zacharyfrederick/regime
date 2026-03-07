#!/usr/bin/env python3
"""
Lightweight pipeline for experiment 0004: one script from raw data to master.

Reimplements universe (with event handling), PIT fundamentals (pcf_pit, fcf_r2_10y,
fcf_pct_positive), and forward labels (21td with terminal price) in a single file.
Reads: TICKERS, ACTIONS, SEP, DAILY, SF1. Writes: master_features.parquet
(month-end rows, 0004 columns only). Quality metrics come from
pipeline.fundamental_quality.compute_quality_metrics_table (called, not inlined).

Usage: python scripts/build_0004_dataset.py

PIT and bias (see docs/event_handling.md):
- Universe: removal_per_ticker excludes mergerfrom; universe_core keeps ticker
  only when removal_date IS NULL OR removal_date > c.date (no survivorship bias).
- Fundamentals: ART and quality_metrics joined with ASOF datekey_date <= g.date
  (no look-ahead). pcf_pit uses closeadj at g.date.
- Labels: forward returns use last SEP closeadj when delist within 21td; mergerfrom
  excluded from fwd_delisted flag only (terminal price still used for return).
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
    MASTER_FEATURES_PATH,
    DEBUG,
    DEBUG_TICKERS,
    DEBUG_TICKERS_DEFAULT,
)

# Match 01_universe
SEP_ACTIVITY_LOOKBACK_DAYS = 14

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"


def _path_sql(p: Path) -> str:
    return repr(str(p.resolve()))


def main() -> None:
    log.info("Lightweight 0004 pipeline: raw data -> master_features.parquet")
    MASTER_FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)

    tickers_path = _parquet("TICKERS")
    actions_path = _parquet("ACTIONS")
    sep_path = _parquet("SEP")
    daily_path = _parquet("DAILY")
    sf1_path = _parquet("SF1")

    missing = [n for n, p in [
        ("TICKERS", tickers_path),
        ("ACTIONS", actions_path),
        ("SEP", sep_path),
        ("SF1", sf1_path),
    ] if not p.exists()]
    if missing:
        log.error("Missing required inputs: %s. Need parquets in data/.", ", ".join(missing))
        sys.exit(1)

    con = duckdb.connect()
    apply_duckdb_limits(con)

    # -------------------------------------------------------------------------
    # Step 1: Universe (from 01_universe)
    # -------------------------------------------------------------------------
    log.info("Step 1: Universe")
    # TICKERS: SF1 only (tickers_base so DEBUG can replace tickers without self-reference)
    con.execute(f"""
        CREATE OR REPLACE VIEW tickers_base AS
        SELECT * FROM read_parquet({_path_sql(tickers_path)})
        WHERE "table" = 'SF1'
          AND ticker IS NOT NULL AND TRIM(COALESCE(ticker, '')) <> ''
    """)
    con.execute("CREATE OR REPLACE VIEW tickers AS SELECT * FROM tickers_base")
    # ACTIONS, SEP (sep_base so DEBUG can replace sep_full)
    con.execute(f"CREATE OR REPLACE VIEW actions AS SELECT * FROM read_parquet({_path_sql(actions_path)})")
    con.execute(f"""
        CREATE OR REPLACE VIEW sep_base AS
        SELECT ticker, CAST(date AS DATE) AS date, closeadj
        FROM read_parquet({_path_sql(sep_path)})
    """)
    con.execute("CREATE OR REPLACE VIEW sep_full AS SELECT * FROM sep_base")

    # Debug: limit to a few tickers for fast runs (same pattern as 01_universe)
    if DEBUG and tickers_path.exists():
        effective_tickers = DEBUG_TICKERS or DEBUG_TICKERS_DEFAULT
        tickers_list = ",".join(repr(t) for t in effective_tickers)
        con.execute(f"CREATE OR REPLACE VIEW tickers AS SELECT * FROM tickers_base WHERE ticker IN ({tickers_list})")
        con.execute(f"CREATE OR REPLACE VIEW sep_full AS SELECT * FROM sep_base WHERE ticker IN ({tickers_list})")
        log.info("DEBUG: limiting to %d tickers: %s", len(effective_tickers), ", ".join(effective_tickers))

    con.execute(
        "CREATE OR REPLACE TABLE actions_clean AS "
        "SELECT ticker, LOWER(TRIM(action)) AS action, CAST(date AS DATE) AS date FROM actions"
    )
    con.execute(
        "CREATE OR REPLACE TABLE sep_clean AS SELECT ticker, date FROM sep_full"
    )

    # Terminal events
    con.execute("""
        CREATE OR REPLACE VIEW delist_dates AS
        SELECT ticker, date AS event_date FROM actions_clean WHERE action = 'delisted'
    """)
    con.execute("""
        CREATE OR REPLACE VIEW delist_reasons AS
        SELECT ticker, date AS event_date, action FROM actions_clean
        WHERE action IN ('acquisitionby','bankruptcyliquidation','regulatorydelisting','voluntarydelisting','mergerfrom')
    """)
    con.execute("""
        CREATE OR REPLACE VIEW resolved_delists_raw AS
        SELECT d.ticker, d.event_date, COALESCE(r.action, 'unknown') AS delist_type
        FROM delist_dates d
        LEFT JOIN delist_reasons r ON r.ticker = d.ticker AND r.event_date = d.event_date
    """)
    con.execute("""
        CREATE OR REPLACE VIEW renames_near_delist AS
        SELECT ticker, date AS rename_date FROM actions_clean WHERE action = 'tickerchangefrom'
    """)
    con.execute("""
        CREATE OR REPLACE VIEW terminal_events_resolved AS
        SELECT r.ticker, r.event_date, r.delist_type
        FROM resolved_delists_raw r
        LEFT JOIN renames_near_delist tc ON tc.ticker = r.ticker
            AND ABS(DATEDIFF('day', tc.rename_date, r.event_date)) <= 5
        WHERE tc.rename_date IS NULL
    """)
    con.execute("""
        CREATE OR REPLACE VIEW removal_per_ticker AS
        SELECT ticker, MIN(event_date) AS removal_date
        FROM terminal_events_resolved WHERE delist_type <> 'mergerfrom'
        GROUP BY ticker
    """)

    # Universe core
    con.execute(f"""
        CREATE OR REPLACE VIEW sep_in_range AS
        SELECT ticker, date FROM sep_clean
        WHERE date BETWEEN ('{DATE_START}'::DATE - INTERVAL '{SEP_ACTIVITY_LOOKBACK_DAYS} days') AND '{DATE_END}'::DATE
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW candidate_from_sep AS
        SELECT DISTINCT ticker, date FROM sep_in_range
        WHERE date BETWEEN '{DATE_START}'::DATE AND '{DATE_END}'::DATE
    """)
    con.execute("""
        CREATE OR REPLACE VIEW universe_core AS
        SELECT c.ticker, c.date,
               CAST(t.firstpricedate AS DATE) AS firstpricedate,
               CAST(t.lastpricedate AS DATE) AS lastpricedate,
               t.sector, t.famaindustry, r.removal_date
        FROM candidate_from_sep c
        INNER JOIN tickers t ON t.ticker = c.ticker
        LEFT JOIN removal_per_ticker r ON r.ticker = c.ticker
        WHERE t.firstpricedate IS NOT NULL AND CAST(t.firstpricedate AS DATE) <= c.date
          AND (t.lastpricedate IS NULL OR CAST(t.lastpricedate AS DATE) >= c.date)
          AND (r.removal_date IS NULL OR r.removal_date > c.date)
    """)
    con.execute("""
        CREATE OR REPLACE VIEW daily_universe_base AS
        SELECT u.ticker, u.date, u.sector, u.famaindustry
        FROM universe_core u
    """)
    if daily_path.exists():
        try:
            con.execute(f"CREATE OR REPLACE VIEW daily AS SELECT * FROM read_parquet({_path_sql(daily_path)})")
            con.execute("""
                CREATE OR REPLACE VIEW daily_universe AS
                SELECT b.ticker, b.date, b.sector, b.famaindustry,
                       dy.marketcap::BIGINT AS marketcap_daily
                FROM daily_universe_base b
                LEFT JOIN daily dy ON dy.ticker = b.ticker AND dy.date = b.date
            """)
        except Exception as e:
            log.warning("DAILY join failed: %s; marketcap_daily will be NULL", e)
            con.execute("""
                CREATE OR REPLACE VIEW daily_universe AS
                SELECT *, CAST(NULL AS BIGINT) AS marketcap_daily FROM daily_universe_base
            """)
    else:
        con.execute("""
            CREATE OR REPLACE VIEW daily_universe AS
            SELECT *, CAST(NULL AS BIGINT) AS marketcap_daily FROM daily_universe_base
        """)

    con.execute("CREATE OR REPLACE VIEW grid AS SELECT ticker, date FROM daily_universe")
    con.execute("""
        CREATE OR REPLACE VIEW grid_with_meta AS
        SELECT ticker, date, sector, marketcap_daily FROM daily_universe
    """)
    n_universe = con.execute("SELECT COUNT(*) FROM daily_universe").fetchone()[0]
    log.info("Universe: %d rows", n_universe)
    if n_universe == 0:
        log.warning("Universe empty; writing empty master")
        _write_empty_master(con)
        con.close()
        return

    # -------------------------------------------------------------------------
    # Step 2: Fundamentals for 0004 (from 02_fundamentals + fundamental_quality)
    # -------------------------------------------------------------------------
    log.info("Step 2: Fundamentals (pcf_pit, fcf_r2_10y, fcf_pct_positive)")
    con.execute(f"""
        CREATE OR REPLACE VIEW sf1 AS
        SELECT * FROM read_parquet({_path_sql(sf1_path)})
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
    """)
    ticker_set = set(row[0] for row in con.execute("SELECT DISTINCT ticker FROM grid").fetchall())
    from pipeline.fundamental_quality import compute_quality_metrics_table
    t0 = time.time()
    quality_df = compute_quality_metrics_table(con, DATE_START, DATE_END, universe_tickers=ticker_set)
    log.info("Quality metrics took %.1fs", time.time() - t0)
    if quality_df.empty:
        con.execute("""
            CREATE OR REPLACE VIEW quality_metrics AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS datekey,
                   CAST(NULL AS DOUBLE) AS fcf_r2_10y, CAST(NULL AS DOUBLE) AS fcf_pct_positive,
                   CAST(NULL AS DATE) AS datekey_date
            WHERE 1=0
        """)
    else:
        con.register("quality_metrics_df", quality_df)
        con.execute("""
            CREATE OR REPLACE VIEW quality_metrics AS
            SELECT *, CAST(datekey AS DATE) AS datekey_date FROM quality_metrics_df
        """)

    con.execute("CREATE OR REPLACE VIEW art AS SELECT * FROM sf1 WHERE dimension = 'ART'")
    # SEP for ART: grid tickers, date range for fundamentals
    con.execute(f"""
        CREATE OR REPLACE VIEW sep_fund AS
        SELECT * FROM sep_full
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
          AND date BETWEEN '{DATE_START}'::DATE AND '{DATE_END}'::DATE
    """)
    con.execute("""
        CREATE OR REPLACE VIEW art_snapshot_combined AS
        SELECT g.ticker, g.date,
               a.datekey_date AS art_datekey,
               a.ncfo, a.capex, a.shareswa, s.closeadj
        FROM grid g
        ASOF LEFT JOIN (
            SELECT *, CAST(datekey AS DATE) AS datekey_date
            FROM art ORDER BY datekey_date
        ) a ON a.ticker = g.ticker AND a.datekey_date <= g.date
        LEFT JOIN sep_fund s ON s.ticker = g.ticker AND s.date = g.date
    """)
    con.execute("""
        CREATE OR REPLACE VIEW fund_0004 AS
        SELECT g.ticker, g.date,
               CASE WHEN art.shareswa IS NOT NULL AND art.shareswa > 0 AND art.closeadj IS NOT NULL AND art.ncfo > 0
                    THEN (art.closeadj * art.shareswa) / NULLIF(art.ncfo, 0) ELSE NULL END AS pcf_pit,
               q.fcf_r2_10y,
               q.fcf_pct_positive
        FROM grid g
        ASOF LEFT JOIN (
            SELECT ticker, datekey_date, fcf_r2_10y, fcf_pct_positive FROM quality_metrics ORDER BY datekey_date
        ) q ON q.ticker = g.ticker AND q.datekey_date <= g.date
        LEFT JOIN art_snapshot_combined art ON art.ticker = g.ticker AND art.date = g.date
    """)

    # -------------------------------------------------------------------------
    # Step 3: Forward labels 21td only (from 07_labels)
    # -------------------------------------------------------------------------
    log.info("Step 3: Forward labels (21td, terminal price)")
    con.execute(f"""
        CREATE OR REPLACE VIEW sep_labels AS
        SELECT * FROM sep_full
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
          AND date BETWEEN (CAST('{DATE_START}' AS DATE) - INTERVAL '1 day')
                       AND (CAST('{DATE_END}' AS DATE) + INTERVAL '400 days')
    """)
    # Labels-step terminal-event views use _lbl suffix to avoid overwriting Step 1
    # views (delist_dates, terminal_events_resolved, etc.) that grid depends on.
    con.execute("""
        CREATE OR REPLACE VIEW actions_labels AS
        SELECT * FROM actions WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
    """)
    con.execute("""
        CREATE OR REPLACE VIEW delist_dates_lbl AS
        SELECT ticker, CAST(date AS DATE) AS event_date FROM actions_labels
        WHERE LOWER(TRIM(action)) = 'delisted'
    """)
    con.execute("""
        CREATE OR REPLACE VIEW delist_reasons_lbl AS
        SELECT ticker, CAST(date AS DATE) AS event_date, LOWER(TRIM(action)) AS action
        FROM actions_labels
        WHERE LOWER(TRIM(action)) IN ('acquisitionby','bankruptcyliquidation','regulatorydelisting','voluntarydelisting','mergerfrom')
    """)
    con.execute("""
        CREATE OR REPLACE VIEW resolved_delists_lbl AS
        SELECT d.ticker, d.event_date, COALESCE(r.action, 'unknown') AS delist_type
        FROM delist_dates_lbl d
        LEFT JOIN delist_reasons_lbl r ON r.ticker = d.ticker AND r.event_date = d.event_date
    """)
    con.execute("""
        CREATE OR REPLACE VIEW renames_near_delist_lbl AS
        SELECT ticker, CAST(date AS DATE) AS rename_date FROM actions_labels
        WHERE LOWER(TRIM(action)) = 'tickerchangefrom'
    """)
    con.execute("""
        CREATE OR REPLACE VIEW terminal_events_lbl AS
        WITH base AS (
            SELECT r.ticker, r.event_date, r.delist_type
            FROM resolved_delists_lbl r
            LEFT JOIN renames_near_delist_lbl tc ON tc.ticker = r.ticker
                AND ABS(DATEDIFF('day', tc.rename_date, r.event_date)) <= 5
            WHERE tc.rename_date IS NULL
        ),
        ranked AS (
            SELECT ticker, event_date, delist_type,
                   ROW_NUMBER() OVER (PARTITION BY ticker, event_date
                       ORDER BY CASE delist_type WHEN 'acquisitionby' THEN 1 WHEN 'bankruptcyliquidation' THEN 2
                                WHEN 'voluntarydelisting' THEN 3 WHEN 'regulatorydelisting' THEN 4 WHEN 'mergerfrom' THEN 5 ELSE 6 END) AS rn
            FROM base
        )
        SELECT ticker, event_date, delist_type FROM ranked WHERE rn = 1
    """)
    con.execute("""
        CREATE OR REPLACE TABLE sep_ranked AS
        SELECT ticker, date, closeadj,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn
        FROM sep_labels WHERE closeadj IS NOT NULL AND closeadj > 0
    """)
    con.execute("""
        CREATE OR REPLACE VIEW sep_max_rn AS
        SELECT ticker, MAX(rn) AS max_rn FROM sep_ranked GROUP BY ticker
    """)

    N = 21
    con.execute(f"""
        CREATE OR REPLACE VIEW grid_cur AS
        SELECT g.ticker, g.date, s.closeadj AS price_t, s.rn AS rn_t
        FROM grid g
        INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.date = g.date
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW fwd_N AS
        SELECT g.ticker, g.date, g.price_t, g.rn_t,
               f.date AS date_n, f.closeadj AS price_n, f.rn AS rn_n
        FROM grid_cur g
        LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.rn = g.rn_t + {N}
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW terminal_row AS
        SELECT m.ticker, g.date, s.date AS term_date, s.closeadj AS term_closeadj, s.rn AS term_rn
        FROM fwd_N g
        INNER JOIN sep_max_rn m ON m.ticker = g.ticker
        INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.rn = m.max_rn
        WHERE g.price_n IS NULL AND g.rn_t < m.max_rn
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW last_day AS
        SELECT g.ticker, g.date FROM fwd_N g
        INNER JOIN sep_max_rn m ON m.ticker = g.ticker
        WHERE g.price_n IS NULL AND g.rn_t >= m.max_rn
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW labels_N_computed AS
        SELECT g.ticker, g.date,
               CASE WHEN g.price_n IS NOT NULL THEN (g.price_n / g.price_t) - 1.0
                    WHEN t.term_closeadj IS NOT NULL THEN (t.term_closeadj / g.price_t) - 1.0
                    ELSE NULL END AS fwd_ret_21td,
               CASE WHEN g.price_n IS NOT NULL THEN {N}
                    WHEN t.term_rn IS NOT NULL THEN (t.term_rn - g.rn_t)::INTEGER ELSE NULL END AS fwd_holding_days_21td,
               CASE WHEN g.price_n IS NOT NULL THEN FALSE
                    WHEN t.term_closeadj IS NOT NULL THEN CASE WHEN e.delist_type IS NULL THEN NULL ELSE e.delist_type <> 'mergerfrom' END
                    WHEN ld.ticker IS NOT NULL THEN NULL ELSE NULL END AS fwd_delisted_21td
        FROM fwd_N g
        LEFT JOIN terminal_row t ON t.ticker = g.ticker AND t.date = g.date
        LEFT JOIN last_day ld ON ld.ticker = g.ticker AND ld.date = g.date
        LEFT JOIN terminal_events_lbl e ON e.ticker = t.ticker AND e.event_date = t.term_date
    """)
    con.execute("""
        CREATE OR REPLACE VIEW labels_21td AS
        SELECT g.ticker, g.date, l.fwd_ret_21td, l.fwd_holding_days_21td, l.fwd_delisted_21td
        FROM grid g
        LEFT JOIN labels_N_computed l ON l.ticker = g.ticker AND l.date = g.date
    """)

    # -------------------------------------------------------------------------
    # Step 4: Month-end merge and write
    # -------------------------------------------------------------------------
    log.info("Step 4: Month-end merge and write")
    con.execute("""
        CREATE OR REPLACE VIEW rebal_dates AS
        SELECT MAX(date) AS date FROM daily_universe
        GROUP BY DATE_TRUNC('month', date)
    """)
    con.execute("""
        CREATE OR REPLACE VIEW universe_rebal AS
        SELECT u.ticker, u.date, u.sector, u.marketcap_daily
        FROM grid_with_meta u
        INNER JOIN rebal_dates r ON u.date = r.date
    """)
    con.execute("""
        CREATE OR REPLACE VIEW master_0004 AS
        SELECT u.date, u.ticker, u.sector, u.marketcap_daily,
               f.pcf_pit, f.fcf_r2_10y, f.fcf_pct_positive,
               CAST(NULL AS DOUBLE) AS vix,
               l.fwd_ret_21td, l.fwd_holding_days_21td, l.fwd_delisted_21td
        FROM universe_rebal u
        LEFT JOIN fund_0004 f ON f.ticker = u.ticker AND f.date = u.date
        LEFT JOIN labels_21td l ON l.ticker = u.ticker AND l.date = u.date
        ORDER BY u.date, u.ticker
    """)

    out_path = _path_sql(MASTER_FEATURES_PATH)
    con.execute(f"COPY (SELECT * FROM master_0004) TO {out_path} (FORMAT PARQUET)")
    log.info("Wrote %s", MASTER_FEATURES_PATH)

    row = con.execute("""
        SELECT COUNT(*) AS n, COUNT(DISTINCT date) AS nd, MIN(date) AS dmin, MAX(date) AS dmax
        FROM master_0004
    """).fetchone()
    log.info("0004 dataset: %d rows, %d rebalance dates, %s to %s", row[0], row[1], row[2], row[3])
    con.close()


def _write_empty_master(con) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([
        ("date", pa.date32()), ("ticker", pa.string()), ("sector", pa.string()),
        ("marketcap_daily", pa.int64()), ("pcf_pit", pa.float64()), ("fcf_r2_10y", pa.float64()),
        ("fcf_pct_positive", pa.float64()), ("vix", pa.float64()),
        ("fwd_ret_21td", pa.float64()), ("fwd_holding_days_21td", pa.int64()), ("fwd_delisted_21td", pa.bool_()),
    ])
    tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
    pq.write_table(tbl, MASTER_FEATURES_PATH)
    log.info("Wrote empty %s", MASTER_FEATURES_PATH)


if __name__ == "__main__":
    main()
