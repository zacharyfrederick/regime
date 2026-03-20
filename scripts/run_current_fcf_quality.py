#!/usr/bin/env python3
"""
Current-market FCF quality: score fcf_quality_rank factor on latest MRY data.
Universe: TICKERS (01_universe filters) + isdelisted = 'N'.
Output: ticker names to console (top N by composite score).
Uses DuckDB for universe and SF1 MRY load (same patterns as pipeline 01_universe, 02_fundamentals).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd

from config import DATA_DIR, apply_duckdb_limits
from pipeline.fundamental_quality import (
    ary_records_to_annual_list,
    compute_quality_metrics_for_ticker,
)
from walkforward.strategies import build_rank_composite_score

# Same factor as experiments/walk_forward.py (fcf_quality_rank)
FCF_QUALITY_COMPONENTS = [
    ("fcf_r2_10y", 0.5),
    ("fcf_cagr_10y", 0.3),
    ("ncfo_r2_10y", 0.2),
]
TOP_N = 50
MRY_LOOKBACK_YEARS = 11


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    if p.exists():
        return p
    return DATA_DIR / f"{name.lower()}.parquet"


def _path_sql(p: Path) -> str:
    """Literal path for DuckDB read_parquet (no prepared params)."""
    return repr(str(p.resolve()))


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Use lowercase column names for consistent access."""
    df = df.copy()
    df.columns = [c.strip().lower() if isinstance(c, str) else c for c in df.columns]
    return df


def get_universe_tickers(con: duckdb.DuckDBPyConnection) -> list[str]:
    """TICKERS with 01_universe tickers_base filters + isdelisted = 'N' (same SQL logic as pipeline)."""
    path = _parquet("TICKERS")
    if not path.exists():
        raise FileNotFoundError(f"TICKERS not found under {DATA_DIR}")
    # Mirror 01_universe tickers_base WHERE + current filter isdelisted = 'N'
    con.execute(
        f"""
        CREATE OR REPLACE VIEW current_universe AS
        SELECT DISTINCT ticker
        FROM read_parquet({_path_sql(path)})
        WHERE "table" = 'SF1'
          AND ticker IS NOT NULL
          AND TRIM(COALESCE(ticker, '')) <> ''
          AND exchange IN ('NYSE', 'NASDAQ', 'NYSEMKT')
          AND category IN (
              'Domestic Common Stock Primary Class',
              'Domestic Common Stock',
              'Domestic Common Stock Secondary Class'
          )
          AND ticker NOT LIKE '%.%'
          AND UPPER(COALESCE(currency, '')) = 'USD'
          AND UPPER(COALESCE(isdelisted, 'N')) = 'N'
          AND (sector IS NULL OR sector NOT IN ('Financial Services', 'Real Estate'))
        """
    )
    out = con.execute("SELECT ticker FROM current_universe ORDER BY ticker").fetchall()
    tickers = [str(r[0]).strip() for r in out]
    return tickers


def mry_df_to_annual_list(mry: pd.DataFrame) -> list[dict]:
    """Convert MRY DataFrame to ary_records dict and use pipeline's ary_records_to_annual_list."""
    if mry is None or mry.empty:
        return []
    df = mry.copy()
    # Sharadar parquet may use "report period" or "reportperiod"
    for c in df.columns:
        if str(c).strip().lower() == "report period":
            df = df.rename(columns={c: "reportperiod"})
            break
    df["reportperiod"] = pd.to_datetime(df["reportperiod"], errors="coerce")
    df = df.dropna(subset=["reportperiod"]).sort_values("reportperiod")
    # Same shape as pipeline: reportperiod -> {ncfo, capex, roic, sharesbas} for ary_records_to_annual_list
    ary_records = {}
    for _, r in df.iterrows():
        period = r["reportperiod"]
        ary_records[period] = {
            "ncfo": r.get("ncfo"),
            "capex": r.get("capex"),
            "roic": r.get("roic"),
            "sharesbas": r.get("sharesbas"),
        }
    return ary_records_to_annual_list(ary_records)


def load_mry_for_universe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load SF1 dimension=MRY for current_universe (last MRY_LOOKBACK_YEARS). Same column set as pipeline ARY pull."""
    path = _parquet("SF1")
    if not path.exists():
        raise FileNotFoundError(f"SF1 not found under {DATA_DIR}")
    # DuckDB pushdown: dimension, ticker, reportperiod filter; columns match fundamental_quality ARY query
    # Cast reportperiod to DATE in case parquet has it as VARCHAR
    sql = f"""
        SELECT ticker, reportperiod, ncfo, capex, roic, sharesbas
        FROM read_parquet({_path_sql(path)})
        WHERE dimension = 'MRY'
          AND ticker IN (SELECT ticker FROM current_universe)
          AND CAST(reportperiod AS DATE) >= current_date - INTERVAL '{MRY_LOOKBACK_YEARS} years'
        ORDER BY ticker, reportperiod
    """
    sf1 = con.execute(sql).df()
    if sf1.empty:
        return sf1
    sf1 = _normalize_columns(sf1)
    if "report period" in sf1.columns and "reportperiod" not in sf1.columns:
        sf1 = sf1.rename(columns={"report period": "reportperiod"})

    return sf1


def main() -> int:
    top_n = TOP_N
    if len(sys.argv) > 1:
        try:
            top_n = int(sys.argv[1])
        except ValueError:
            pass

    con = duckdb.connect()
    apply_duckdb_limits(con)

    tickers = get_universe_tickers(con)
    if not tickers:
        con.close()
        print("No universe tickers (check TICKERS parquet and filters).", file=sys.stderr)
        return 1

    try:
        sf1 = load_mry_for_universe(con)
    except Exception as e:
        con.close()
        print(f"Failed to load SF1 MRY: {e}", file=sys.stderr)
        return 1

    rows = []
    for ticker in tickers:
        mry = sf1[sf1["ticker"].astype(str) == ticker] if not sf1.empty else pd.DataFrame()
        if mry.empty:
            continue
        annual_list = mry_df_to_annual_list(mry)
        if not annual_list:
            continue
        metrics = compute_quality_metrics_for_ticker(annual_list, None)
        fcf_r2 = metrics.get("fcf_r2_10y")
        fcf_cagr = metrics.get("fcf_cagr_10y")
        ncfo_r2 = metrics.get("ncfo_r2_10y")
        if fcf_r2 is None and fcf_cagr is None and ncfo_r2 is None:
            continue
        rows.append({
            "ticker": ticker,
            "fcf_r2_10y": fcf_r2,
            "fcf_cagr_10y": fcf_cagr,
            "ncfo_r2_10y": ncfo_r2,
        })

    con.close()

    if not rows:
        print("No tickers with quality metrics.", file=sys.stderr)
        return 1

    df = pd.DataFrame(rows)
    # Single cross-section: same date for all so groupby("date").rank works
    as_of = pd.Timestamp.now().normalize()
    df["date"] = as_of
    # Drop rows missing any of the three factors so rank is comparable
    if df.empty:
        print("No tickers with all three factors.", file=sys.stderr)
        return 1

    score = build_rank_composite_score(df, FCF_QUALITY_COMPONENTS)
    df = df.assign(score=score.values)
    df = df.sort_values("score", ascending=False).head(top_n)

    for t in df["ticker"]:
        print(f"{t}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
