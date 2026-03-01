#!/usr/bin/env python3
"""
Price-derived features from SEP: returns, volatility, volume ratio, 52w range,
ATR, MA cross, momentum skew. Requires SEP with high, low, volume, closeadj.
Output: outputs/features/price_features.parquet
"""
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
    PRICE_FEATURES_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Max lookback for price features (ret_12m, 52w range). Used to restrict SEP date range.
LOOKBACK_DAYS = 252


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"


def main() -> None:
    log.info("Building price features (SEP -> returns, vol, 52w, ATR, MA cross, momentum skew)")
    PRICE_FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    apply_duckdb_limits(con)

    if not DAILY_UNIVERSE_PATH.exists():
        log.warning("Universe not found; run 01_universe.py first.")
        con.execute("CREATE OR REPLACE VIEW grid AS SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date WHERE 1=0")
    else:
        _path_sql = lambda p: repr(str(Path(p).resolve()))
        con.execute(f"CREATE OR REPLACE VIEW grid AS SELECT ticker, date FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})")
        log.info("Loaded grid from %s", DAILY_UNIVERSE_PATH)

    sep_path = _parquet("SEP")
    if not sep_path.exists():
        log.warning("SEP not found; writing empty price_features.parquet")
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("ticker", pa.string()), ("date", pa.date32()),
            ("ret_1m", pa.float64()), ("ret_3m", pa.float64()), ("ret_6m", pa.float64()), ("ret_12m", pa.float64()),
            ("vol_20d", pa.float64()), ("vol_60d", pa.float64()), ("vol_ratio", pa.float64()),
            ("volume_ratio_1m", pa.float64()), ("pct_52w_range", pa.float64()),
            ("atr_14d", pa.float64()), ("ma50_cross", pa.float64()), ("ma200_cross", pa.float64()),
            ("momentum_skew_60d", pa.float64()),
        ])
        tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
        pq.write_table(tbl, PRICE_FEATURES_PATH)
        log.info("Wrote empty %s", PRICE_FEATURES_PATH)
        return

    _path_sql = lambda p: repr(str(Path(p).resolve()))
    con.execute(f"CREATE OR REPLACE VIEW sep AS SELECT * FROM read_parquet({_path_sql(sep_path)})")
    log.info("Loaded SEP from %s", sep_path)
    # Filter SEP to grid tickers and date range with lookback so feature computation runs only on universe (or debug) tickers.
    con.execute(
        f"""
        CREATE OR REPLACE VIEW sep_filtered AS
        SELECT * FROM sep
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
          AND CAST(date AS DATE) BETWEEN (CAST('{DATE_START}' AS DATE) - INTERVAL '{LOOKBACK_DAYS} days') AND CAST('{DATE_END}' AS DATE)
        """
    )
    # Three CTEs: daily = per-row expressions (no nested windows); base = window aggregates over daily_ret etc.; derived = ratios and skewness.
    log.info("Building price_features_full (daily -> base -> derived CTEs)")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW price_features_full AS
        WITH daily AS (
            SELECT ticker, date, closeadj, volume, high, low,
                   closeadj / LAG(closeadj, 1) OVER w - 1 AS daily_ret,
                   closeadj / LAG(closeadj, 21) OVER w - 1 AS ret_1m,
                   closeadj / LAG(closeadj, 63) OVER w - 1 AS ret_3m,
                   closeadj / LAG(closeadj, 126) OVER w - 1 AS ret_6m,
                   closeadj / LAG(closeadj, 252) OVER w - 1 AS ret_12m,
                   high - low AS hl_range,
                   ABS(high - LAG(closeadj, 1) OVER w) AS hc_range,
                   ABS(low - LAG(closeadj, 1) OVER w) AS lc_range
            FROM sep_filtered
            WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ),
        base AS (
            SELECT *,
                   STDDEV(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_20d,
                   STDDEV(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_60d,
                   AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_20d_avg,
                   AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS vol_60d_avg,
                   MIN(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS low_52w,
                   MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52w,
                   AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
                   AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                   AVG(GREATEST(hl_range, hc_range, lc_range)) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) / NULLIF(closeadj, 0) AS atr_14d
            FROM daily
        ),
        derived AS (
            SELECT *,
                   vol_20d / NULLIF(vol_60d, 0) AS vol_ratio,
                   vol_20d_avg / NULLIF(vol_60d_avg, 0) AS volume_ratio_1m,
                   (closeadj - low_52w) / NULLIF(high_52w - low_52w, 0) AS pct_52w_range,
                   (closeadj - ma50) / NULLIF(ma50, 0) AS ma50_cross,
                   (closeadj - ma200) / NULLIF(ma200, 0) AS ma200_cross,
                   SKEWNESS(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS momentum_skew_60d
            FROM base
        )
        SELECT ticker, date,
               ret_1m, ret_3m, ret_6m, ret_12m,
               vol_20d, vol_60d, vol_ratio,
               volume_ratio_1m, pct_52w_range,
               atr_14d, ma50_cross, ma200_cross,
               momentum_skew_60d
        FROM derived
        WHERE CAST(date AS DATE) BETWEEN '{DATE_START}'::DATE AND '{DATE_END}'::DATE
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW price_features AS
        SELECT g.ticker, g.date,
               p.ret_1m, p.ret_3m, p.ret_6m, p.ret_12m,
               p.vol_20d, p.vol_60d, p.vol_ratio,
               p.volume_ratio_1m, p.pct_52w_range,
               p.atr_14d, p.ma50_cross, p.ma200_cross,
               p.momentum_skew_60d
        FROM grid g
        LEFT JOIN price_features_full p ON p.ticker = g.ticker AND p.date = g.date
        """
    )
    n_rows = con.execute("SELECT COUNT(*) FROM price_features").fetchone()[0]
    log.info("Writing %s rows to %s", n_rows, PRICE_FEATURES_PATH)
    con.execute(f"COPY (SELECT * FROM price_features) TO {_path_sql(PRICE_FEATURES_PATH)} (FORMAT PARQUET)")
    log.info("Wrote %s", PRICE_FEATURES_PATH)
    con.close()


if __name__ == "__main__":
    main()
