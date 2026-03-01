#!/usr/bin/env python3
"""
Forward return labels with terminal-event handling.

Produces outputs/labels/forward_labels.parquet: trading-day-aligned forward returns
and delist flags so returns reflect actual economic outcomes when a terminal event
occurs within the horizon. Grid = all (ticker, date) from universe parquet (labels
decoupled from in_universe filter). Terminal price = last SEP closeadj for all event
types. mergerfrom is excluded from fwd_delisted flag and from triggering terminal
logic (informational only).

Consumes: universe (daily_universe.parquet), SEP, ACTIONS.
Run after 01_universe. Output: FORWARD_LABELS_PATH.
"""
from __future__ import annotations

import logging
import sys
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
    FORWARD_LABELS_PATH,
    LABELS_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

HORIZONS_TD = (21, 63, 126, 252)
# Temp parquet stem for per-horizon files (under LABELS_DIR); cleaned up after final write.
_LABELS_TEMP_STEM = "_labels_"


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"


def main() -> None:
    log.info("Building forward labels (07_labels)")
    FORWARD_LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    apply_duckdb_limits(con)

    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    # Grid: all (ticker, date) from universe parquet
    if not DAILY_UNIVERSE_PATH.exists():
        log.warning("Universe not found; run 01_universe.py first. Writing empty labels.")
        con.execute(
            "CREATE OR REPLACE VIEW grid AS SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date WHERE 1=0"
        )
    else:
        con.execute(
            f"CREATE OR REPLACE VIEW grid AS SELECT ticker, CAST(date AS DATE) AS date FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})"
        )
        n_grid = con.execute("SELECT COUNT(*) FROM grid").fetchone()[0]
        log.info("Grid: %d rows from universe", n_grid)

    actions_path = _parquet("ACTIONS")
    sep_path = _parquet("SEP")
    if not actions_path.exists() or not sep_path.exists():
        log.warning("ACTIONS or SEP not found; writing empty labels.")
        _write_empty_labels(con)
        con.close()
        return

    con.execute(f"CREATE OR REPLACE VIEW actions_raw AS SELECT * FROM read_parquet({_path_sql(actions_path)})")
    con.execute(
        f"CREATE OR REPLACE VIEW sep_raw AS SELECT ticker, CAST(date AS DATE) AS date, closeadj FROM read_parquet({_path_sql(sep_path)})"
    )
    # Filter to grid tickers only (when DEBUG is on, universe has few tickers — same as 03_price_features / 01_universe)
    con.execute(
        """
        CREATE OR REPLACE VIEW actions AS
        SELECT * FROM actions_raw
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
        """
    )
    # SEP: grid tickers + date range covering grid and forward 252 td for terminal/forward lookups
    con.execute(
        f"""
        CREATE OR REPLACE VIEW sep AS
        SELECT * FROM sep_raw
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
          AND date BETWEEN (CAST('{DATE_START}' AS DATE) - INTERVAL '1 day')
                       AND (CAST('{DATE_END}' AS DATE) + INTERVAL '400 days')
        """
    )
    log.info("Filtered SEP and ACTIONS to grid tickers (same as other pipeline steps)")

    # Resolved terminal events (same logic as 01_universe): one row per (ticker, event_date), delist_type; exclude renames
    con.execute(
        """
        CREATE OR REPLACE VIEW delist_dates AS
        SELECT ticker, CAST(date AS DATE) AS event_date
        FROM actions
        WHERE LOWER(TRIM(action)) = 'delisted'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW delist_reasons AS
        SELECT ticker, CAST(date AS DATE) AS event_date, LOWER(TRIM(action)) AS action
        FROM actions
        WHERE LOWER(TRIM(action)) IN ('acquisitionby','bankruptcyliquidation','regulatorydelisting','voluntarydelisting','mergerfrom')
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW resolved_delists_raw AS
        SELECT d.ticker, d.event_date, COALESCE(r.action, 'unknown') AS delist_type
        FROM delist_dates d
        LEFT JOIN delist_reasons r ON r.ticker = d.ticker AND r.event_date = d.event_date
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW renames_near_delist AS
        SELECT ticker, CAST(date AS DATE) AS rename_date
        FROM actions
        WHERE LOWER(TRIM(action)) = 'tickerchangefrom'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW terminal_events_resolved AS
        SELECT r.ticker, r.event_date, r.delist_type
        FROM resolved_delists_raw r
        LEFT JOIN renames_near_delist tc ON tc.ticker = r.ticker
            AND ABS(DATEDIFF('day', tc.rename_date, r.event_date)) <= 5
        WHERE tc.rename_date IS NULL
        """
    )

    # SEP with trading-day rank per ticker (for forward lookups). Materialize as TABLE so ROW_NUMBER() runs once.
    con.execute(
        """
        CREATE OR REPLACE TABLE sep_ranked AS
        SELECT ticker, date, closeadj,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn
        FROM sep
        WHERE closeadj IS NOT NULL AND closeadj > 0
        """
    )
    # Max rank per ticker (last trading day)
    con.execute(
        """
        CREATE OR REPLACE VIEW sep_max_rn AS
        SELECT ticker, MAX(rn) AS max_rn
        FROM sep_ranked
        GROUP BY ticker
        """
    )

    # Build labels per horizon: write each to a temp parquet, then join from parquets to limit peak memory.
    n_grid = con.execute("SELECT COUNT(*) FROM grid").fetchone()[0]
    if n_grid == 0:
        _write_empty_labels(con)
        con.close()
        return

    temp_paths: list[Path] = []
    for N in HORIZONS_TD:
        # For each (ticker, date) in grid: get cur price and rn; get N-th forward row or terminal row
        con.execute(f"""
        CREATE OR REPLACE VIEW grid_cur AS
        SELECT g.ticker, g.date,
               s.closeadj AS price_t,
               s.rn AS rn_t
        FROM grid g
        INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.date = g.date
        """)
        # Forward N-th trading day (may be null if terminated before N)
        con.execute(f"""
        CREATE OR REPLACE VIEW fwd_N AS
        SELECT g.ticker, g.date,
               g.price_t,
               g.rn_t,
               f.date AS date_n,
               f.closeadj AS price_n,
               f.rn AS rn_n
        FROM grid_cur g
        LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.rn = g.rn_t + {N}
        """)
        # Terminal row when price_n is null: last available forward price
        con.execute(f"""
        CREATE OR REPLACE VIEW terminal_row AS
        SELECT m.ticker, g.date,
               s.date AS term_date,
               s.closeadj AS term_closeadj,
               s.rn AS term_rn
        FROM fwd_N g
        INNER JOIN sep_max_rn m ON m.ticker = g.ticker
        INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.rn = m.max_rn
        WHERE g.price_n IS NULL AND g.rn_t < m.max_rn
        """)
        # Last-day case: no forward prices (rn_t = max_rn)
        con.execute(f"""
        CREATE OR REPLACE VIEW last_day AS
        SELECT g.ticker, g.date
        FROM fwd_N g
        INNER JOIN sep_max_rn m ON m.ticker = g.ticker
        WHERE g.price_n IS NULL AND g.rn_t >= m.max_rn
        """)

        # Assemble horizon N: fwd_ret, fwd_holding_days, fwd_delisted, fwd_delist_type (exclude mergerfrom from flag)
        # Tail (no forward data): NULLs — we don't know return/delist. Terminal with no action: fwd_delisted NULL.
        con.execute(f"""
        CREATE OR REPLACE VIEW labels_N AS
        SELECT
            g.ticker,
            g.date,
            CASE
                WHEN g.price_n IS NOT NULL THEN (g.price_n / g.price_t) - 1.0
                WHEN t.term_closeadj IS NOT NULL THEN (t.term_closeadj / g.price_t) - 1.0
                ELSE NULL
            END AS fwd_ret,
            CASE
                WHEN g.price_n IS NOT NULL THEN {N}
                WHEN t.term_rn IS NOT NULL THEN (t.term_rn - g.rn_t)::INTEGER
                ELSE NULL
            END AS fwd_holding_days,
            CASE
                WHEN g.price_n IS NOT NULL THEN FALSE
                WHEN t.term_closeadj IS NOT NULL THEN CASE WHEN e.delist_type IS NULL THEN NULL ELSE e.delist_type <> 'mergerfrom' END
                WHEN ld.ticker IS NOT NULL THEN NULL
                ELSE NULL
            END AS fwd_delisted,
            CASE
                WHEN g.price_n IS NOT NULL THEN CAST(NULL AS VARCHAR)
                WHEN t.term_closeadj IS NOT NULL AND COALESCE(e.delist_type, '') <> 'mergerfrom' THEN e.delist_type
                WHEN ld.ticker IS NOT NULL AND COALESCE(e2.delist_type, '') <> 'mergerfrom' THEN e2.delist_type
                ELSE CAST(NULL AS VARCHAR)
            END AS fwd_delist_type
        FROM fwd_N g
        LEFT JOIN terminal_row t ON t.ticker = g.ticker AND t.date = g.date
        LEFT JOIN last_day ld ON ld.ticker = g.ticker AND ld.date = g.date
        LEFT JOIN terminal_events_resolved e ON e.ticker = t.ticker AND e.event_date = t.term_date
        LEFT JOIN terminal_events_resolved e2 ON e2.ticker = ld.ticker AND e2.event_date = ld.date
        """)

        # One row per grid row: grid LEFT JOIN labels_N
        con.execute(f"""
        CREATE OR REPLACE VIEW labels_{N}td AS
        SELECT g.ticker, g.date,
               l.fwd_ret AS fwd_ret_{N}td,
               l.fwd_holding_days AS fwd_holding_days_{N}td,
               l.fwd_delisted AS fwd_delisted_{N}td,
               l.fwd_delist_type AS fwd_delist_type_{N}td
        FROM grid g
        LEFT JOIN labels_N l ON l.ticker = g.ticker AND l.date = g.date
        """)

        temp_path = LABELS_DIR / f"{_LABELS_TEMP_STEM}{N}td.parquet"
        temp_paths.append(temp_path)
        con.execute(f"COPY (SELECT * FROM labels_{N}td) TO {_path_sql(temp_path)} (FORMAT PARQUET)")
        log.info("Wrote %s", temp_path.name)

        # Drop per-horizon views so the engine does not retain them for the next N
        con.execute(f"DROP VIEW IF EXISTS labels_{N}td")
        con.execute("DROP VIEW IF EXISTS labels_N")
        con.execute("DROP VIEW IF EXISTS last_day")
        con.execute("DROP VIEW IF EXISTS terminal_row")
        con.execute("DROP VIEW IF EXISTS fwd_N")
        con.execute("DROP VIEW IF EXISTS grid_cur")

    # Final join: grid + four temp parquets -> one wide table
    l21_path = LABELS_DIR / f"{_LABELS_TEMP_STEM}21td.parquet"
    l63_path = LABELS_DIR / f"{_LABELS_TEMP_STEM}63td.parquet"
    l126_path = LABELS_DIR / f"{_LABELS_TEMP_STEM}126td.parquet"
    l252_path = LABELS_DIR / f"{_LABELS_TEMP_STEM}252td.parquet"
    try:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW forward_labels AS
            SELECT
                g.ticker,
                g.date,
                l21.fwd_ret_21td, l21.fwd_holding_days_21td, l21.fwd_delisted_21td, l21.fwd_delist_type_21td,
                l63.fwd_ret_63td, l63.fwd_holding_days_63td, l63.fwd_delisted_63td, l63.fwd_delist_type_63td,
                l126.fwd_ret_126td, l126.fwd_holding_days_126td, l126.fwd_delisted_126td, l126.fwd_delist_type_126td,
                l252.fwd_ret_252td, l252.fwd_holding_days_252td, l252.fwd_delisted_252td, l252.fwd_delist_type_252td
            FROM grid g
            LEFT JOIN read_parquet({_path_sql(l21_path)}) l21 ON l21.ticker = g.ticker AND l21.date = g.date
            LEFT JOIN read_parquet({_path_sql(l63_path)}) l63 ON l63.ticker = g.ticker AND l63.date = g.date
            LEFT JOIN read_parquet({_path_sql(l126_path)}) l126 ON l126.ticker = g.ticker AND l126.date = g.date
            LEFT JOIN read_parquet({_path_sql(l252_path)}) l252 ON l252.ticker = g.ticker AND l252.date = g.date
            """
        )
        con.execute(f"COPY (SELECT * FROM forward_labels) TO {_path_sql(FORWARD_LABELS_PATH)} (FORMAT PARQUET)")
        n_out = con.execute("SELECT COUNT(*) FROM forward_labels").fetchone()[0]
        log.info("Wrote %s: %d rows", FORWARD_LABELS_PATH, n_out)
        # Cleanup temp parquets after successful write
        for p in temp_paths:
            if p.exists():
                p.unlink()
                log.debug("Removed temp %s", p.name)
    finally:
        con.close()


def _write_empty_labels(con: duckdb.DuckDBPyConnection) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        ("ticker", pa.string()),
        ("date", pa.date32()),
        ("fwd_ret_21td", pa.float64()),
        ("fwd_holding_days_21td", pa.int64()),
        ("fwd_delisted_21td", pa.bool_()),
        ("fwd_delist_type_21td", pa.string()),
        ("fwd_ret_63td", pa.float64()),
        ("fwd_holding_days_63td", pa.int64()),
        ("fwd_delisted_63td", pa.bool_()),
        ("fwd_delist_type_63td", pa.string()),
        ("fwd_ret_126td", pa.float64()),
        ("fwd_holding_days_126td", pa.int64()),
        ("fwd_delisted_126td", pa.bool_()),
        ("fwd_delist_type_126td", pa.string()),
        ("fwd_ret_252td", pa.float64()),
        ("fwd_holding_days_252td", pa.int64()),
        ("fwd_delisted_252td", pa.bool_()),
        ("fwd_delist_type_252td", pa.string()),
    ])
    tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
    pq.write_table(tbl, FORWARD_LABELS_PATH)
    log.info("Wrote empty %s", FORWARD_LABELS_PATH)


if __name__ == "__main__":
    main()
