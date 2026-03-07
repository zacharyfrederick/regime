#!/usr/bin/env python3
"""
Build daily universe: active tickers per date + forward event flags.
Consumes: TICKERS, ACTIONS, SEP (and optionally DAILY) from data/.
Output: outputs/universe/daily_universe.parquet
"""
import logging
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
from tqdm import tqdm

from config import (
    apply_duckdb_limits,
    DATA_DIR,
    DATE_END,
    DATE_START,
    DAILY_UNIVERSE_PATH,
    DEBUG,
    DEBUG_TICKERS,
    DEBUG_TICKERS_DEFAULT,
    SEP_LOOKBACK_DAYS,
)

# Calendar-day lookback for "had recent price" (SEP activity). Keep 14 to match prior behavior.
SEP_ACTIVITY_LOOKBACK_DAYS = 14

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Parquet paths: Sharadar exports often use uppercase table names
def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    if not p.exists():
        alt = DATA_DIR / f"{name.lower()}.parquet"
        if alt.exists():
            return alt
    return p


def main() -> None:
    tickers_path = _parquet("TICKERS")
    actions_path = _parquet("ACTIONS")
    sep_path = _parquet("SEP")
    daily_path = _parquet("DAILY")

    steps = [
        "register tables",
        "trading_dates",
        "removal_per_ticker",
        "sep_in_range",
        "candidate_from_sep",
        "universe_core",
        "forward event views",
        "daily_universe_base",
        "daily_universe (+ marketcap_daily)",
        "write parquet",
    ]
    pbar = tqdm(total=len(steps), desc="01_universe", unit="step", leave=True)

    for path in (tickers_path, actions_path, sep_path):
        if not path.exists():
            log.warning("Missing %s; create or symlink parquet under data/", path)
            # Still create output dir and write empty/schema-only if desired
    DAILY_UNIVERSE_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    apply_duckdb_limits(con)

    # Register parquet (DuckDB does not allow prepared params in read_parquet; use literal path)
    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    def register(name: str, path: Path) -> None:
        if path.exists():
            if name == "tickers":
                # Use tickers_base so DEBUG can replace "tickers" with a filtered copy without self-reference.
                # Restrict to common stock: US exchanges, domestic common share classes, USD currency, no ticker with '.' (e.g. BRK.A).
                con.execute(
                    f"""
                    CREATE OR REPLACE VIEW tickers_base AS
                    SELECT * FROM read_parquet({_path_sql(path)})
                    WHERE "table" = 'SF1'
                      AND ticker IS NOT NULL
                      AND TRIM(COALESCE(ticker, '')) <> ''
                      AND exchange IN ('NYSE', 'NASDAQ', 'NYSEMKT')
                      AND category IN ('Domestic Common Stock Primary Class', 'Domestic Common Stock', 'Domestic Common Stock Secondary Class')
                      AND ticker NOT LIKE '%.%'
                      AND UPPER(COALESCE(currency, '')) = 'USD'
                    """
                )
                con.execute("CREATE OR REPLACE VIEW tickers AS SELECT * FROM tickers_base")
                n = con.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
                u = con.execute("SELECT COUNT(DISTINCT ticker) FROM tickers").fetchone()[0]
                log.info("TICKERS (SF1 only): %d rows, %d unique tickers", n, u)
                if n != u:
                    log.error("Still have duplicates after SF1 filter - investigate")
            elif name == "actions":
                con.execute(
                    f"CREATE OR REPLACE VIEW actions AS SELECT * FROM read_parquet({_path_sql(path)})"
                )
            elif name == "sep":
                # Use sep_base so DEBUG can replace "sep" with a filtered copy without self-reference
                con.execute(
                    f"CREATE OR REPLACE VIEW sep_base AS SELECT * FROM read_parquet({_path_sql(path)})"
                )
                con.execute("CREATE OR REPLACE VIEW sep AS SELECT * FROM sep_base")
        else:
            # Create empty view with expected columns so script can run and write empty output
            log.warning("%s not found; using empty view", path)
            if name == "tickers":
                con.execute(
                    "CREATE OR REPLACE VIEW tickers_base AS SELECT CAST(NULL AS VARCHAR) AS ticker, "
                    "CAST(NULL AS DATE) AS firstpricedate, CAST(NULL AS DATE) AS lastpricedate, "
                    "CAST(NULL AS VARCHAR) AS sector, CAST(NULL AS VARCHAR) AS famaindustry"
                )
                con.execute("CREATE OR REPLACE VIEW tickers AS SELECT * FROM tickers_base")
            elif name == "actions":
                con.execute(
                    "CREATE OR REPLACE VIEW actions AS SELECT CAST(NULL AS VARCHAR) AS ticker, "
                    "CAST(NULL AS VARCHAR) AS action, CAST(NULL AS DATE) AS date"
                )
            elif name == "sep":
                con.execute(
                    "CREATE OR REPLACE VIEW sep_base AS SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date"
                )
                con.execute("CREATE OR REPLACE VIEW sep AS SELECT * FROM sep_base")

    register("tickers", tickers_path)
    register("actions", actions_path)
    register("sep", sep_path)
    if daily_path.exists():
        con.execute(f"CREATE OR REPLACE VIEW daily AS SELECT * FROM read_parquet({_path_sql(daily_path)})")
    pbar.set_postfix_str(steps[0])
    pbar.update(1)

    # Debug: limit universe to a few tickers for fast runs (only when TICKERS data exists).
    if DEBUG and tickers_path.exists():
        effective_tickers = DEBUG_TICKERS or DEBUG_TICKERS_DEFAULT
        tickers_list = ",".join(repr(t) for t in effective_tickers)
        con.execute(
            f"CREATE OR REPLACE VIEW tickers AS SELECT * FROM tickers_base WHERE ticker IN ({tickers_list})"
        )
        # Also limit SEP so sep_in_range and candidate_from_sep only process debug tickers (avoids full-SEP scan)
        con.execute(
            f"CREATE OR REPLACE VIEW sep AS SELECT * FROM sep_base WHERE ticker IN ({tickers_list})"
        )
        log.info("DEBUG: limiting universe to %d tickers: %s", len(effective_tickers), ", ".join(effective_tickers))

    # Materialize cleaned tables once so downstream views avoid repeated CAST/LOWER(TRIM)
    con.execute(
        "CREATE OR REPLACE TABLE actions_clean AS SELECT ticker, LOWER(TRIM(action)) AS action, CAST(date AS DATE) AS date FROM actions"
    )
    con.execute(
        "CREATE OR REPLACE TABLE sep_clean AS SELECT ticker, CAST(date AS DATE) AS date FROM sep"
    )

    # Trading dates in range from SEP
    con.execute(
        f"""
        CREATE OR REPLACE VIEW trading_dates AS
        SELECT DISTINCT date FROM sep_clean
        WHERE date BETWEEN '{DATE_START}'::DATE AND '{DATE_END}'::DATE
        ORDER BY date
        """
    )
    pbar.set_postfix_str(steps[1])
    pbar.update(1)
    # If SEP is empty, trading_dates will be empty
    dates_table = "trading_dates"
    try:
        n_dates = con.execute("SELECT COUNT(*) FROM trading_dates").fetchone()[0]
        if n_dates == 0:
            log.warning("No trading dates in SEP for range %s–%s; output will be empty", DATE_START, DATE_END)
    except Exception:
        pass

    # Resolved terminal events: one row per (ticker, event_date) with delist_type from companion row.
    # Exclude renames (tickerchangefrom within ±5 days). mergerfrom included for reporting but excluded from flag logic below.
    # Use actions_clean so LOWER(TRIM(action)) and CAST(date) are done once.
    con.execute(
        """
        CREATE OR REPLACE VIEW delist_dates AS
        SELECT ticker, date AS event_date
        FROM actions_clean
        WHERE action = 'delisted'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW delist_reasons AS
        SELECT ticker, date AS event_date, action
        FROM actions_clean
        WHERE action IN ('acquisitionby','bankruptcyliquidation','regulatorydelisting','voluntarydelisting','mergerfrom')
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
        SELECT ticker, date AS rename_date
        FROM actions_clean
        WHERE action = 'tickerchangefrom'
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
    # Removal: do not use mergerfrom (ticker recycling breaks date semantics).
    con.execute(
        """
        CREATE OR REPLACE VIEW removal_per_ticker AS
        SELECT ticker, MIN(event_date) AS removal_date
        FROM terminal_events_resolved
        WHERE delist_type <> 'mergerfrom'
        GROUP BY ticker
        """
    )
    pbar.set_postfix_str(steps[2])
    pbar.update(1)

    # Restrict SEP to date range + lookback once (sep_clean already has date cast)
    con.execute(
        f"""
        CREATE OR REPLACE VIEW sep_in_range AS
        SELECT ticker, date
        FROM sep_clean
        WHERE date BETWEEN ('{DATE_START}'::DATE - INTERVAL '{SEP_ACTIVITY_LOOKBACK_DAYS} days') AND '{DATE_END}'::DATE
        """
    )
    pbar.set_postfix_str(steps[3])
    pbar.update(1)

    # Candidate (ticker, date): every row in sep_in_range in pipeline date range (EXISTS was redundant: row satisfies itself).
    # DISTINCT preserves original behavior if SEP has duplicate (ticker, date) rows.
    con.execute(
        f"""
        CREATE OR REPLACE VIEW candidate_from_sep AS
        SELECT DISTINCT ticker, date
        FROM sep_in_range
        WHERE date BETWEEN '{DATE_START}'::DATE AND '{DATE_END}'::DATE
        """
    )
    pbar.set_postfix_str(steps[4])
    pbar.update(1)

    # Join to TICKERS and removal_per_ticker: same semantics as before (firstpricedate/lastpricedate/removal_date)
    con.execute(
        """
        CREATE OR REPLACE VIEW universe_core AS
        SELECT c.ticker, c.date,
               CAST(t.firstpricedate AS DATE) AS firstpricedate,
               CAST(t.lastpricedate AS DATE) AS lastpricedate,
               t.sector, t.famaindustry,
               r.removal_date
        FROM candidate_from_sep c
        INNER JOIN tickers t ON t.ticker = c.ticker
        LEFT JOIN removal_per_ticker r ON r.ticker = c.ticker
        WHERE t.firstpricedate IS NOT NULL AND CAST(t.firstpricedate AS DATE) <= c.date
          AND (t.lastpricedate IS NULL OR CAST(t.lastpricedate AS DATE) >= c.date)
          AND (r.removal_date IS NULL OR r.removal_date > c.date)
        """
    )
    pbar.set_postfix_str(steps[5])
    pbar.update(1)

    con.execute(
        """
        CREATE OR REPLACE VIEW spinoff_60 AS
        SELECT ticker, date AS action_date
        FROM actions_clean
        WHERE action = 'spinoff'
        """
    )
    pbar.set_postfix_str(steps[6])
    pbar.update(1)

    # Build base view once: universe_core + days_listed + fwd_spinoff_60d (no marketcap).
    # fwd_spinoff_60d via LEFT JOIN + BOOL_OR to avoid per-row correlated EXISTS.
    con.execute(
        """
        CREATE OR REPLACE VIEW daily_universe_base AS
        SELECT
            u.ticker,
            u.date,
            TRUE AS in_universe,
            DATEDIFF('day', u.firstpricedate, u.date)::INTEGER AS days_listed,
            COALESCE(BOOL_OR(s.ticker IS NOT NULL), FALSE) AS fwd_spinoff_60d,
            u.sector,
            u.famaindustry
        FROM universe_core u
        LEFT JOIN spinoff_60 s
            ON s.ticker = u.ticker
            AND s.action_date > u.date
            AND s.action_date <= u.date + INTERVAL '60 days'
        GROUP BY u.ticker, u.date, u.firstpricedate, u.sector, u.famaindustry
        """
    )
    pbar.set_postfix_str(steps[7])
    pbar.update(1)

    # Add marketcap_daily from DAILY when available; explicit fallback on failure
    if daily_path.exists():
        try:
            con.execute(
                """
                CREATE OR REPLACE VIEW daily_universe AS
                SELECT
                    b.*,
                    dy.marketcap::BIGINT AS marketcap_daily,
                    CASE
                        WHEN dy.marketcap::BIGINT IS NULL THEN NULL
                        WHEN dy.marketcap::BIGINT < 50000000 THEN 1
                        WHEN dy.marketcap::BIGINT < 300000000 THEN 2
                        WHEN dy.marketcap::BIGINT < 2000000000 THEN 3
                        WHEN dy.marketcap::BIGINT < 10000000000 THEN 4
                        WHEN dy.marketcap::BIGINT < 200000000000 THEN 5
                        ELSE 6
                    END AS scalemarketcap
                FROM daily_universe_base b
                LEFT JOIN daily dy ON dy.ticker = b.ticker AND dy.date = b.date
                """
            )
            log.info("daily_universe: joined with DAILY marketcap_daily")
        except Exception as e:
            log.warning("Could not join DAILY for marketcap_daily: %s", e)
            con.execute(
                """
                CREATE OR REPLACE VIEW daily_universe AS
                SELECT *, CAST(NULL AS BIGINT) AS marketcap_daily, CAST(NULL AS INTEGER) AS scalemarketcap FROM daily_universe_base
                """
            )
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW daily_universe AS
            SELECT *, CAST(NULL AS BIGINT) AS marketcap_daily, CAST(NULL AS INTEGER) AS scalemarketcap FROM daily_universe_base
            """
        )
    pbar.set_postfix_str(steps[8])
    pbar.update(1)

    n = con.execute("SELECT COUNT(*) FROM daily_universe").fetchone()[0]
    con.execute(
        f"COPY (SELECT * FROM daily_universe) TO {_path_sql(DAILY_UNIVERSE_PATH)} (FORMAT PARQUET)"
    )
    pbar.set_postfix_str(steps[9])
    pbar.update(1)
    pbar.close()

    log.info("Wrote %s: %d rows", DAILY_UNIVERSE_PATH, n)
    if n == 0:
        log.error("Universe is empty - check TICKERS and SEP data")
    elif n < 100_000:
        log.warning("Universe has only %d rows - expected millions for full history", n)
    con.close()


if __name__ == "__main__":
    main()
