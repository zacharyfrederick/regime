#!/usr/bin/env python3
"""
Macro features: FRED series + derived (yield curve, VIX change, real rate, cpi_yoy, etc.) + SPY regime from SFP.
Expects parquet per series under config.FRED_DIR from 00_fetch_fred (vix.parquet, yield_curve.parquet, etc.).
Output: outputs/features/macro_features.parquet (date-level only).
"""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd

from config import (
    apply_duckdb_limits,
    DATA_DIR,
    DATE_END,
    DATE_START,
    DAILY_UNIVERSE_PATH,
    FRED_DIR,
    MACRO_FEATURES_PATH,
    SPY_TICKER,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Parquet stems written by 00_fetch_fred; column name in file matches stem.
FRED_SERIES_STEMS = [
    "yield_curve", "hy_spread", "ig_spread", "vix", "nfci", "fed_funds", "cpi", "treasury_10y",
]


def main() -> None:
    MACRO_FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    apply_duckdb_limits(con)

    if not FRED_DIR.exists():
        FRED_DIR.mkdir(parents=True, exist_ok=True)
    # Deterministic order: sort by name so date grid and joins are reproducible.
    fred_files = sorted(FRED_DIR.glob("*.parquet"), key=lambda p: p.name)
    if not fred_files:
        log.warning("No FRED parquet in %s; writing empty macro_features.parquet", FRED_DIR)
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("date", pa.date32()),
            ("yield_curve", pa.float64()), ("hy_spread", pa.float64()), ("ig_spread", pa.float64()),
            ("vix", pa.float64()), ("vix_change_20d", pa.float64()), ("nfci", pa.float64()),
            ("real_rate", pa.float64()), ("fed_funds", pa.float64()), ("cpi_yoy", pa.float64()),
            ("spy_regime_ma", pa.float64()),
            ("spy_ret_1m", pa.float64()), ("spy_ret_3m", pa.float64()),
            ("spy_ret_6m", pa.float64()), ("spy_ret_12m", pa.float64()),
        ])
        tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
        pq.write_table(tbl, MACRO_FEATURES_PATH)
        log.info("Wrote empty %s", MACRO_FEATURES_PATH)
        return

    def path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    # Date grid: use trading dates from universe when available so macro parquet matches universe size
    if DAILY_UNIVERSE_PATH.exists():
        con.execute(f"""
            CREATE OR REPLACE VIEW dates AS
            SELECT DISTINCT date FROM read_parquet({path_sql(DAILY_UNIVERSE_PATH)})
            ORDER BY date
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE VIEW dates AS
            SELECT unnest(generate_series(
                CAST('{DATE_START}' AS DATE),
                CAST('{DATE_END}' AS DATE),
                INTERVAL '1 day'
            )) AS date
        """)

    # Build list of (stem, path) for parquets that exist; detect PIT by presence of vintage_date
    stems_with_paths = [
        (stem, FRED_DIR / f"{stem}.parquet")
        for stem in FRED_SERIES_STEMS
        if (FRED_DIR / f"{stem}.parquet").exists()
    ]
    stems_single = []  # (stem, path): schema (date, stem) — join on date, then ffill
    stems_pit = []     # (stem, path): schema (observation_date, vintage_date, value) — effective vintage per sim date, then ffill
    for stem, path in stems_with_paths:
        df = con.execute(f"SELECT * FROM read_parquet({path_sql(path)}) LIMIT 0").df()
        col_names = list(df.columns)
        if "vintage_date" in col_names:
            if "observation_date" in col_names and "value" in col_names:
                stems_pit.append((stem, path))
            else:
                log.warning("PIT parquet %s missing observation_date or value — skipping", path.name)
            continue
        if stem not in col_names:
            log.warning("Expected column '%s' in %s, got %s — skipping", stem, path.name, col_names)
            continue
        stems_single.append((stem, path))
    stems_with_paths = stems_single + stems_pit  # keep order for column ordering
    stems_list = [s for s, _ in stems_with_paths]
    if not stems_with_paths:
        log.warning("No matching FRED series in %s; writing empty macro_features.parquet", FRED_DIR)
        con.execute(f"""
            CREATE OR REPLACE VIEW macro_features AS
            SELECT d.date,
                   CAST(NULL AS DOUBLE) AS yield_curve, CAST(NULL AS DOUBLE) AS hy_spread,
                   CAST(NULL AS DOUBLE) AS ig_spread, CAST(NULL AS DOUBLE) AS vix,
                   CAST(NULL AS DOUBLE) AS vix_change_20d, CAST(NULL AS DOUBLE) AS nfci,
                   CAST(NULL AS DOUBLE) AS real_rate, CAST(NULL AS DOUBLE) AS fed_funds,
                   CAST(NULL AS DOUBLE) AS cpi_yoy,
                   CAST(NULL AS DOUBLE) AS spy_regime_ma,
                   CAST(NULL AS DOUBLE) AS spy_ret_1m, CAST(NULL AS DOUBLE) AS spy_ret_3m,
                   CAST(NULL AS DOUBLE) AS spy_ret_6m, CAST(NULL AS DOUBLE) AS spy_ret_12m
            FROM dates d
            ORDER BY d.date
        """)
        con.execute(f"COPY (SELECT * FROM macro_features) TO {path_sql(MACRO_FEATURES_PATH)} (FORMAT PARQUET)")
        log.info("Wrote empty %s", MACRO_FEATURES_PATH)
        return

    # Register single-vintage parquets as views (date + one value column named by stem). PIT parquets become pit_{stem} views via ASOF SQL above.
    for stem, path in stems_single:
        con.execute(f"CREATE OR REPLACE VIEW fred_{stem} AS SELECT * FROM read_parquet({path_sql(path)})")

    # PIT stems: resolve in DuckDB with ASOF (effective vintage per sim date, then effective observation); one view per stem
    for stem, path in stems_pit:
        con.execute(f"""
            CREATE OR REPLACE VIEW pit_raw_{stem} AS
            SELECT observation_date, vintage_date, value
            FROM read_parquet({path_sql(path)})
        """)
        con.execute(f"""
            CREATE OR REPLACE VIEW pit_{stem} AS
            WITH effective_vintage AS (
                SELECT d.date AS sim_date, MAX(p.vintage_date) AS eff_vintage
                FROM dates d
                JOIN pit_raw_{stem} p ON p.vintage_date <= d.date
                GROUP BY d.date
            ),
            effective_obs AS (
                SELECT ev.sim_date, ev.eff_vintage, MAX(p.observation_date) AS eff_obs
                FROM effective_vintage ev
                JOIN pit_raw_{stem} p ON p.vintage_date = ev.eff_vintage
                    AND p.observation_date <= ev.sim_date
                GROUP BY ev.sim_date, ev.eff_vintage
            ),
            resolved AS (
                SELECT eo.sim_date AS date, MAX(p.value) AS {stem}
                FROM effective_obs eo
                JOIN pit_raw_{stem} p ON p.vintage_date = eo.eff_vintage
                    AND p.observation_date = eo.eff_obs
                GROUP BY eo.sim_date
            )
            SELECT d.date, r.{stem}
            FROM dates d
            LEFT JOIN resolved r ON r.date = d.date
        """)

    # Join clauses: single-vintage (fred_{stem} on date), then PIT (pit_{stem} views)
    pit_stems = [s for s, _ in stems_pit]
    join_parts = [f"LEFT JOIN fred_{stem} {stem} ON {stem}.date = d.date" for stem, _ in stems_single]
    join_parts += [f"LEFT JOIN pit_{stem} {stem} ON {stem}.date = d.date" for stem in pit_stems]
    select_parts = [f"{stem}.{stem} AS {stem}" for stem, _ in stems_single] + [f"{stem}.{stem} AS {stem}" for stem in pit_stems]
    from_clause = " FROM dates d " + " ".join(join_parts)

    con.execute(f"""
        CREATE OR REPLACE VIEW raw_base AS
        SELECT d.date, {', '.join(select_parts)}
        {from_clause}
    """)

    # base: forward-fill monthly series to daily (LAST_VALUE IGNORE NULLS)
    ffill_cols = ", ".join(
        f"LAST_VALUE(r.{stem} IGNORE NULLS) OVER (ORDER BY r.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {stem}"
        for stem, _ in stems_with_paths
    )
    con.execute(f"""
        CREATE OR REPLACE VIEW base AS
        SELECT r.date, {ffill_cols}
        FROM raw_base r
    """)

    # base2: add vix_change_20d (LAG 20 rows), cpi_yoy (LAG 252 rows ≈ 1y on trading-day grid, in percent)
    has_vix = "vix" in stems_list
    has_cpi = "cpi" in stems_list
    has_treasury = "treasury_10y" in stems_list

    derived = []
    if has_vix:
        derived.append("base.vix - LAG(base.vix, 20) OVER (ORDER BY base.date) AS vix_change_20d")
    else:
        derived.append("CAST(NULL AS DOUBLE) AS vix_change_20d")
    if has_cpi:
        # cpi_yoy: LAG 252 rows = ~1 year on trading-day grid (consistent with VIX 20d and SPY 12m)
        derived.append("(base.cpi - LAG(base.cpi, 252) OVER (ORDER BY base.date)) / NULLIF(LAG(base.cpi, 252) OVER (ORDER BY base.date), 0) * 100 AS cpi_yoy")
    else:
        derived.append("CAST(NULL AS DOUBLE) AS cpi_yoy")

    con.execute(f"""
        CREATE OR REPLACE VIEW base2 AS
        SELECT base.*, {derived[0]}, {derived[1]}
        FROM base
    """)

    # base3: add real_rate = treasury_10y - cpi_yoy (both in percent)
    if has_treasury and has_cpi:
        real_rate_expr = "base2.treasury_10y - base2.cpi_yoy AS real_rate"
    elif has_treasury:
        real_rate_expr = "base2.treasury_10y AS real_rate"
    else:
        real_rate_expr = "CAST(NULL AS DOUBLE) AS real_rate"
    con.execute(f"""
        CREATE OR REPLACE VIEW base3 AS
        SELECT base2.*, {real_rate_expr}
        FROM base2
    """)

    # SPY regime and 12m return from SFP (fund/ETF prices only)
    sfp_path = DATA_DIR / "SFP.parquet"
    if not sfp_path.exists():
        sfp_path = DATA_DIR / "sfp.parquet"
    has_spy = sfp_path.exists()
    if has_spy:
        con.execute(f"""
            CREATE OR REPLACE VIEW spy_raw AS
            WITH spy AS (
                SELECT date, closeadj,
                       closeadj / LAG(closeadj, 21)  OVER (ORDER BY date) - 1 AS spy_ret_1m,
                       closeadj / LAG(closeadj, 63)  OVER (ORDER BY date) - 1 AS spy_ret_3m,
                       closeadj / LAG(closeadj, 126) OVER (ORDER BY date) - 1 AS spy_ret_6m,
                       closeadj / LAG(closeadj, 252) OVER (ORDER BY date) - 1 AS spy_ret_12m,
                       AVG(closeadj) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200
                FROM read_parquet({path_sql(sfp_path)})
                WHERE ticker = '{SPY_TICKER}'
            )
            SELECT date,
                   spy_ret_1m, spy_ret_3m, spy_ret_6m, spy_ret_12m,
                   CASE WHEN closeadj > ma200 THEN 1.0 ELSE 0.0 END AS spy_regime_ma
            FROM spy
        """)

    # Output schema: date, yield_curve, ..., spy_regime_ma, spy_ret_12m (explicit aliases)
    final_cols = [
        "m.date AS date",
        "m.yield_curve AS yield_curve" if "yield_curve" in stems_list else "CAST(NULL AS DOUBLE) AS yield_curve",
        "m.hy_spread AS hy_spread" if "hy_spread" in stems_list else "CAST(NULL AS DOUBLE) AS hy_spread",
        "m.ig_spread AS ig_spread" if "ig_spread" in stems_list else "CAST(NULL AS DOUBLE) AS ig_spread",
        "m.vix AS vix" if has_vix else "CAST(NULL AS DOUBLE) AS vix",
        "m.vix_change_20d AS vix_change_20d",
        "m.nfci AS nfci" if "nfci" in stems_list else "CAST(NULL AS DOUBLE) AS nfci",
        "m.real_rate AS real_rate",
        "m.fed_funds AS fed_funds" if "fed_funds" in stems_list else "CAST(NULL AS DOUBLE) AS fed_funds",
        "m.cpi_yoy AS cpi_yoy",
        "s.spy_regime_ma AS spy_regime_ma" if has_spy else "CAST(NULL AS DOUBLE) AS spy_regime_ma",
        "s.spy_ret_1m AS spy_ret_1m" if has_spy else "CAST(NULL AS DOUBLE) AS spy_ret_1m",
        "s.spy_ret_3m AS spy_ret_3m" if has_spy else "CAST(NULL AS DOUBLE) AS spy_ret_3m",
        "s.spy_ret_6m AS spy_ret_6m" if has_spy else "CAST(NULL AS DOUBLE) AS spy_ret_6m",
        "s.spy_ret_12m AS spy_ret_12m" if has_spy else "CAST(NULL AS DOUBLE) AS spy_ret_12m",
    ]
    macro_from = "FROM base3 m" + (" LEFT JOIN spy_raw s ON s.date = m.date" if has_spy else "")
    con.execute(f"""
        CREATE OR REPLACE VIEW macro_features AS
        SELECT {', '.join(final_cols)}
        {macro_from}
    """)
    con.execute(f"COPY (SELECT * FROM macro_features) TO {path_sql(MACRO_FEATURES_PATH)} (FORMAT PARQUET)")
    log.info("Wrote %s", MACRO_FEATURES_PATH)
    con.close()


if __name__ == "__main__":
    main()
