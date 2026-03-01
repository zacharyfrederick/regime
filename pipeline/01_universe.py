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
        "candidate_ticker_dates",
        "universe_core",
        "forward event views",
        "daily_universe_base",
        "daily_universe (+ marketcap_daily)",
        "daily_universe_ranked",
        "write parquet",
    ]
    pbar = tqdm(total=len(steps), desc="01_universe", unit="step", leave=True)

    for path in (tickers_path, actions_path, sep_path):
        if not path.exists():
            log.warning("Missing %s; create or symlink parquet under data/", path)
            # Still create output dir and write empty/schema-only if desired
    DAILY_UNIVERSE_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    apply_duckdb_limits(con)

    # Register parquet (DuckDB does not allow prepared params in read_parquet; use literal path)
    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    def register(name: str, path: Path) -> None:
        if path.exists():
            if name == "tickers":
                # Use tickers_base so DEBUG can replace "tickers" with a filtered copy without self-reference
                con.execute(
                    f"""
                    CREATE OR REPLACE VIEW tickers_base AS
                    SELECT * FROM read_parquet({_path_sql(path)})
                    WHERE "table" = 'SF1'
                      AND ticker IS NOT NULL
                      AND TRIM(COALESCE(ticker, '')) <> ''
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
                con.execute(
                    f"CREATE OR REPLACE VIEW sep AS SELECT * FROM read_parquet({_path_sql(path)})"
                )
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
                    "CREATE OR REPLACE VIEW sep AS SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date"
                )

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
        log.info("DEBUG: limiting universe to %d tickers: %s", len(effective_tickers), ", ".join(effective_tickers))

    # Trading dates in range from SEP (cast date to DATE in case parquet has VARCHAR)
    con.execute(
        f"""
        CREATE OR REPLACE VIEW trading_dates AS
        SELECT DISTINCT CAST(date AS DATE) AS date FROM sep
        WHERE CAST(date AS DATE) BETWEEN '{DATE_START}'::DATE AND '{DATE_END}'::DATE
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

    # Active universe: ticker-date where firstpricedate <= date, (lastpricedate >= date OR lastpricedate IS NULL),
    # and removal_date > date (or no removal), and has SEP activity in lookback window
    con.execute(
        """
        CREATE OR REPLACE VIEW candidate_ticker_dates AS
        SELECT t.ticker, d.date,
               CAST(t.firstpricedate AS DATE) AS firstpricedate,
               CAST(t.lastpricedate AS DATE) AS lastpricedate,
               t.sector, t.famaindustry,
               r.removal_date
        FROM tickers t
        CROSS JOIN trading_dates d
        LEFT JOIN removal_per_ticker r ON r.ticker = t.ticker
        WHERE t.firstpricedate IS NOT NULL AND CAST(t.firstpricedate AS DATE) <= d.date
          AND (t.lastpricedate IS NULL OR CAST(t.lastpricedate AS DATE) >= d.date)
          AND (r.removal_date IS NULL OR r.removal_date > d.date)
        """
    )
    pbar.set_postfix_str(steps[3])
    pbar.update(1)

    # Require at least one SEP row in 14-day lookback so the ticker had recent price data on that date
    con.execute(
        """
        CREATE OR REPLACE VIEW universe_core AS
        SELECT c.ticker, c.date, c.firstpricedate, c.lastpricedate, c.sector, c.famaindustry, c.removal_date
        FROM candidate_ticker_dates c
        WHERE EXISTS (
            SELECT 1 FROM sep s
            WHERE s.ticker = c.ticker AND CAST(s.date AS DATE) <= c.date AND CAST(s.date AS DATE) >= c.date - INTERVAL '14 days'
        )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW spinoff_60 AS
        SELECT a.ticker, CAST(a.date AS DATE) AS action_date
        FROM actions a
        WHERE LOWER(TRIM(a.action)) = 'spinoff'
        """
    )
    pbar.set_postfix_str(steps[5])
    pbar.update(1)

    # Build base view once: universe_core + days_listed + fwd_spinoff_60d (no marketcap).
    # Delist/acquired flags live in forward_labels (06_5_labels), not in universe.
    con.execute(
        """
        CREATE OR REPLACE VIEW daily_universe_base AS
        SELECT
            u.ticker,
            u.date,
            TRUE AS in_universe,
            DATEDIFF('day', u.firstpricedate, u.date)::INTEGER AS days_listed,
            EXISTS (
                SELECT 1 FROM spinoff_60 s
                WHERE s.ticker = u.ticker AND s.action_date > u.date AND s.action_date <= u.date + INTERVAL '60 days'
            ) AS fwd_spinoff_60d,
            u.sector,
            u.famaindustry
        FROM universe_core u
        """
    )
    pbar.set_postfix_str(steps[6])
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
    pbar.set_postfix_str(steps[7])
    pbar.update(1)

    # Annual market cap rank (year-end) for Dreman top-1500 universe filter
    con.execute(
        """
        CREATE OR REPLACE VIEW daily_universe_ranked AS
        WITH year_ends AS (
            SELECT
                EXTRACT(year FROM date) AS year,
                MAX(date) AS last_trading_day
            FROM daily_universe
            GROUP BY EXTRACT(year FROM date)
        ),
        annual_ranks AS (
            SELECT
                u.ticker,
                ye.year,
                ye.last_trading_day,
                ROW_NUMBER() OVER (
                    PARTITION BY ye.last_trading_day
                    ORDER BY u.marketcap_daily DESC NULLS LAST
                ) AS marketcap_rank_annual
            FROM daily_universe u
            INNER JOIN year_ends ye ON u.date = ye.last_trading_day
            WHERE u.marketcap_daily > 0
        )
        SELECT
            u.*,
            ar.marketcap_rank_annual
        FROM daily_universe u
        LEFT JOIN annual_ranks ar
            ON ar.ticker = u.ticker AND ar.year = EXTRACT(year FROM u.date)
        """
    )
    pbar.set_postfix_str(steps[8])
    pbar.update(1)

    con.execute(
        f"COPY (SELECT * FROM daily_universe_ranked) TO {_path_sql(DAILY_UNIVERSE_PATH)} (FORMAT PARQUET)"
    )
    pbar.set_postfix_str(steps[9])
    pbar.update(1)
    pbar.close()

    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})"
    ).fetchone()[0]
    log.info("Wrote %s: %d rows", DAILY_UNIVERSE_PATH, n)
    if n == 0:
        log.error("Universe is empty - check TICKERS and SEP data")
    elif n < 100_000:
        log.warning("Universe has only %d rows - expected millions for full history", n)
    con.close()


if __name__ == "__main__":
    main()
