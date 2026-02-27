#!/usr/bin/env python3
"""
Macro features: FRED series + derived (yield curve, VIX change, real rate, cpi_yoy, etc.) + SPY regime.
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
    DATA_DIR,
    DATE_END,
    DATE_START,
    DAILY_UNIVERSE_PATH,
    FRED_DIR,
    MACRO_FEATURES_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Parquet stems written by 00_fetch_fred; column name in file matches stem.
FRED_SERIES_STEMS = [
    "yield_curve", "hy_spread", "ig_spread", "vix", "nfci", "fed_funds", "cpi", "treasury_10y",
]


def main() -> None:
    MACRO_FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")

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
            ("spy_regime_ma", pa.float64()), ("spy_ret_12m", pa.float64()),
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
                   CAST(NULL AS DOUBLE) AS spy_regime_ma, CAST(NULL AS DOUBLE) AS spy_ret_12m
            FROM dates d
            ORDER BY d.date
        """)
        con.execute(f"COPY (SELECT * FROM macro_features) TO {path_sql(MACRO_FEATURES_PATH)} (FORMAT PARQUET)")
        log.info("Wrote empty %s", MACRO_FEATURES_PATH)
        return

    # Register single-vintage parquets as views (date + one value column named by stem). PIT parquets are handled via pit_dfs.
    for stem, path in stems_single:
        con.execute(f"CREATE OR REPLACE VIEW fred_{stem} AS SELECT * FROM read_parquet({path_sql(path)})")

    # raw_base: dates LEFT JOIN each FRED series (sparse for monthly series)
    # Build (date, value) per PIT stem: for each sim date D, effective_vintage = max(vintage_date <= D), ffill that vintage to daily, value at D
    pit_dfs: dict[str, pd.DataFrame] = {}
    if stems_pit:
        dates_df = con.execute("SELECT date FROM dates ORDER BY date").df()
        sim_dates = pd.to_datetime(dates_df["date"]).dt.date.tolist()
        for stem, path in stems_pit:
            pit = pd.read_parquet(path)
            if pit.empty or "vintage_date" not in pit.columns:
                pit_dfs[stem] = pd.DataFrame({"date": sim_dates, stem: [None] * len(sim_dates)})
                continue
            pit["observation_date"] = pd.to_datetime(pit["observation_date"]).dt.date
            pit["vintage_date"] = pd.to_datetime(pit["vintage_date"]).dt.date
            vintage_dates = sorted(pit["vintage_date"].unique())
            row_list = []
            for d in sim_dates:
                v_leq = [v for v in vintage_dates if v <= d]
                if not v_leq:
                    row_list.append({"date": d, stem: None})
                    continue
                eff_v = max(v_leq)
                sub = pit.loc[pit["vintage_date"] == eff_v, ["observation_date", "value"]].drop_duplicates(subset=["observation_date"], keep="last").sort_values("observation_date")
                if sub.empty:
                    row_list.append({"date": d, stem: None})
                    continue
                sub = sub.set_index("observation_date")
                sub.index = pd.to_datetime(sub.index)
                day_range = pd.date_range(sub.index.min(), pd.Timestamp(d), freq="D")
                sub = sub.reindex(day_range).ffill()
                if len(sub) == 0:
                    row_list.append({"date": d, stem: None})
                    continue
                # Value at sim date d = last forward-filled value up to d
                sub_trunc = sub.loc[sub.index <= pd.Timestamp(d)]
                val = sub_trunc.iloc[-1].iloc[0] if len(sub_trunc) else None
                row_list.append({"date": d, stem: float(val) if pd.notna(val) else None})
            pit_dfs[stem] = pd.DataFrame(row_list)

    # Join clauses: single-vintage (fred_{stem} on date), then PIT (pit_dfs)
    join_parts = []
    for stem, _ in stems_single:
        join_parts.append(f"LEFT JOIN fred_{stem} {stem} ON {stem}.date = d.date")
    for stem in pit_dfs:
        join_parts.append(f"LEFT JOIN pit_{stem} {stem} ON {stem}.date = d.date")
    select_parts = [f"{stem}.{stem} AS {stem}" for stem, _ in stems_single] + [f"{stem}.{stem} AS {stem}" for stem in pit_dfs]
    from_clause = " FROM dates d " + " ".join(join_parts)

    for stem, df in pit_dfs.items():
        con.register(f"pit_{stem}", df)

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

    # base2: add vix_change_20d (LAG 20), cpi_yoy (LAG 365 for YoY, in percent)
    has_vix = "vix" in stems_list
    has_cpi = "cpi" in stems_list
    has_treasury = "treasury_10y" in stems_list

    derived = []
    if has_vix:
        derived.append("base.vix - LAG(base.vix, 20) OVER (ORDER BY base.date) AS vix_change_20d")
    else:
        derived.append("CAST(NULL AS DOUBLE) AS vix_change_20d")
    if has_cpi:
        # cpi_yoy: LAG 365 days on forward-filled daily CPI
        # Approximate YoY — 365 calendar days back, not exact 12-month period
        # Sufficient for macro regime signal; not for official inflation reporting
        derived.append("(base.cpi - LAG(base.cpi, 365) OVER (ORDER BY base.date)) / NULLIF(LAG(base.cpi, 365) OVER (ORDER BY base.date), 0) * 100 AS cpi_yoy")
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

    # SPY regime and 12m return from SEP (if available)
    sep_path = DATA_DIR / "SEP.parquet"
    if not sep_path.exists():
        sep_path = DATA_DIR / "sep.parquet"
    has_spy = sep_path.exists()
    if has_spy:
        con.execute(f"""
            CREATE OR REPLACE VIEW spy_raw AS
            WITH spy AS (
                SELECT date, closeadj,
                       closeadj / LAG(closeadj, 252) OVER (ORDER BY date) - 1 AS spy_ret_12m,
                       AVG(closeadj) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200
                FROM read_parquet({path_sql(sep_path)})
                WHERE ticker = 'SPY'
            )
            SELECT date,
                   spy_ret_12m,
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
        "s.spy_ret_12m AS spy_ret_12m" if has_spy else "CAST(NULL AS DOUBLE) AS spy_ret_12m",
    ]
    macro_from = "FROM base3 m" + (" LEFT JOIN spy_raw s ON s.date = m.date" if has_spy else "")
    con.execute(f"""
        CREATE OR REPLACE VIEW macro_features AS
        SELECT {', '.join(final_cols)}
        {macro_from}
        ORDER BY m.date
    """)
    con.execute(f"COPY (SELECT * FROM macro_features) TO {path_sql(MACRO_FEATURES_PATH)} (FORMAT PARQUET)")
    log.info("Wrote %s", MACRO_FEATURES_PATH)
    con.close()


if __name__ == "__main__":
    main()
