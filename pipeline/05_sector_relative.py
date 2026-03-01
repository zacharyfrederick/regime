#!/usr/bin/env python3
"""
Sector-relative features: valuation and performance vs sector median.
Consumes: daily_universe, DAILY (optional), fundamental_pit, price_features.
Output: outputs/features/sector_relative.parquet
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
    DAILY_UNIVERSE_PATH,
    FUNDAMENTAL_PIT_PATH,
    PRICE_FEATURES_PATH,
    SECTOR_RELATIVE_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"


def main() -> None:
    SECTOR_RELATIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    apply_duckdb_limits(con)

    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    if not DAILY_UNIVERSE_PATH.exists():
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("ticker", pa.string()), ("date", pa.date32()),
            ("pe_vs_sector", pa.float64()), ("pb_vs_sector", pa.float64()),
            ("ps_vs_sector", pa.float64()), ("pcf_vs_sector", pa.float64()),
            ("evebitda_vs_sector", pa.float64()), ("roic_vs_sector", pa.float64()),
            ("liabilities_to_assets_vs_sector", pa.float64()),
            ("ret_3m_vs_sector", pa.float64()), ("vol_vs_sector", pa.float64()),
            ("ret_3m_rank_sector", pa.float64()),
        ])
        tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
        pq.write_table(tbl, SECTOR_RELATIVE_PATH)
        log.info("Wrote empty %s", SECTOR_RELATIVE_PATH)
        return

    con.execute(f"CREATE OR REPLACE VIEW universe AS SELECT * FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})")

    if FUNDAMENTAL_PIT_PATH.exists():
        con.execute(f"CREATE OR REPLACE VIEW fundamental_pit AS SELECT * FROM read_parquet({_path_sql(FUNDAMENTAL_PIT_PATH)})")
    else:
        log.warning("%s not found; using empty view.", FUNDAMENTAL_PIT_PATH)
        con.execute(
            """
            CREATE OR REPLACE VIEW fundamental_pit AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date,
                   CAST(NULL AS DOUBLE) AS pe_pit, CAST(NULL AS DOUBLE) AS pb_pit,
                   CAST(NULL AS DOUBLE) AS ps_pit, CAST(NULL AS DOUBLE) AS evebitda_pit,
                   CAST(NULL AS DOUBLE) AS pcf_pit, CAST(NULL AS DOUBLE) AS roic_level,
                   CAST(NULL AS DOUBLE) AS liabilities_to_assets
            FROM universe WHERE 1=0
            """
        )

    if PRICE_FEATURES_PATH.exists():
        con.execute(f"CREATE OR REPLACE VIEW price_features AS SELECT * FROM read_parquet({_path_sql(PRICE_FEATURES_PATH)})")
    else:
        log.warning("%s not found; using empty view.", PRICE_FEATURES_PATH)
        con.execute(
            """
            CREATE OR REPLACE VIEW price_features AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date,
                   CAST(NULL AS DOUBLE) AS ret_3m, CAST(NULL AS DOUBLE) AS vol_20d
            FROM universe WHERE 1=0
            """
        )

    daily_path = _parquet("DAILY")
    if daily_path.exists():
        con.execute(f"CREATE OR REPLACE VIEW daily_metrics AS SELECT * FROM read_parquet({_path_sql(daily_path)})")
        log.info("Loaded DAILY from %s", daily_path)
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW daily_metrics AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date,
                   CAST(NULL AS DOUBLE) AS pe, CAST(NULL AS DOUBLE) AS pb,
                   CAST(NULL AS DOUBLE) AS ps, CAST(NULL AS DOUBLE) AS evebitda
            FROM universe WHERE 1=0
            """
        )

    con.execute(
        """
        CREATE OR REPLACE VIEW sector_relative AS
        WITH combined AS (
            SELECT
                u.ticker,
                u.date,
                u.famaindustry,
                COALESCE(d.pe, f.pe_pit) AS pe,
                COALESCE(d.pb, f.pb_pit) AS pb,
                COALESCE(d.ps, f.ps_pit) AS ps,
                COALESCE(d.evebitda, f.evebitda_pit) AS evebitda,
                f.pcf_pit,
                f.roic_level,
                f.liabilities_to_assets,
                p.ret_3m,
                p.vol_20d
            FROM universe u
            LEFT JOIN daily_metrics d ON d.ticker = u.ticker AND d.date = u.date
            LEFT JOIN fundamental_pit f ON f.ticker = u.ticker AND f.date = u.date
            LEFT JOIN price_features p ON p.ticker = u.ticker AND p.date = u.date
        ),
        sector_medians AS (
            SELECT
                date,
                famaindustry,
                MEDIAN(pe) AS sector_median_pe,
                MEDIAN(pb) AS sector_median_pb,
                MEDIAN(ps) AS sector_median_ps,
                MEDIAN(pcf_pit) AS sector_median_pcf,
                MEDIAN(evebitda) AS sector_median_evebitda,
                MEDIAN(roic_level) AS sector_median_roic,
                MEDIAN(liabilities_to_assets) AS sector_median_liabilities_to_assets,
                MEDIAN(ret_3m) AS sector_median_ret_3m,
                MEDIAN(vol_20d) AS sector_median_vol
            FROM combined
            WHERE famaindustry IS NOT NULL
            GROUP BY date, famaindustry
            HAVING COUNT(*) >= 5
        )
        SELECT
            c.ticker,
            c.date,
            CASE WHEN s.sector_median_pe > 0 THEN c.pe / s.sector_median_pe ELSE NULL END AS pe_vs_sector,
            CASE WHEN s.sector_median_pb > 0 THEN c.pb / s.sector_median_pb ELSE NULL END AS pb_vs_sector,
            CASE WHEN s.sector_median_ps > 0 THEN c.ps / s.sector_median_ps ELSE NULL END AS ps_vs_sector,
            CASE WHEN s.sector_median_pcf > 0 THEN c.pcf_pit / s.sector_median_pcf ELSE NULL END AS pcf_vs_sector,
            CASE WHEN s.sector_median_evebitda > 0 THEN c.evebitda / s.sector_median_evebitda ELSE NULL END AS evebitda_vs_sector,
            c.roic_level - s.sector_median_roic AS roic_vs_sector,
            CASE WHEN s.sector_median_liabilities_to_assets IS NOT NULL AND s.sector_median_liabilities_to_assets > 0
                 THEN c.liabilities_to_assets / s.sector_median_liabilities_to_assets ELSE NULL END AS liabilities_to_assets_vs_sector,
            c.ret_3m - s.sector_median_ret_3m AS ret_3m_vs_sector,
            c.vol_20d - s.sector_median_vol AS vol_vs_sector,
            PERCENT_RANK() OVER (PARTITION BY c.date, c.famaindustry ORDER BY c.ret_3m NULLS LAST) AS ret_3m_rank_sector
        FROM combined c
        LEFT JOIN sector_medians s ON s.date = c.date AND s.famaindustry = c.famaindustry
        """
    )
    try:
        con.execute(f"COPY (SELECT * FROM sector_relative) TO {_path_sql(SECTOR_RELATIVE_PATH)} (FORMAT PARQUET)")
    except Exception as e:
        log.error("COPY sector_relative failed: %s", e)
        raise

    log.info("Wrote %s", SECTOR_RELATIVE_PATH)

    # Validation
    import pandas as pd
    sector = pd.read_parquet(SECTOR_RELATIVE_PATH)
    pe_vals = sector["pe_vs_sector"].dropna()
    if len(pe_vals) > 0:
        pe_mean = pe_vals.mean()
        assert abs(pe_mean - 1.0) < 0.3, f"pe_vs_sector should average near 1.0, got {pe_mean}"
    roic_vals = sector["roic_vs_sector"].dropna()
    if len(roic_vals) > 0:
        roic_mean = roic_vals.mean()
        assert abs(roic_mean) < 0.05, f"roic_vs_sector should average near 0, got {roic_mean}"
    universe_df = pd.read_parquet(DAILY_UNIVERSE_PATH)
    has_sector = universe_df[universe_df["famaindustry"].notna()]
    merged = has_sector.merge(sector, on=["ticker", "date"])
    null_pct = merged["pe_vs_sector"].isna().mean()
    log.info("pe_vs_sector null rate for tickers with sector: %.1f%%", null_pct * 100)

    con.close()


if __name__ == "__main__":
    main()
