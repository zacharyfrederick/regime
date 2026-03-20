#!/usr/bin/env python3
"""
Build P/FCF 3-year and 5-year quantile valuation from existing master parquet.
Reads master_features.parquet, restricts to month-ends, computes trailing
percentile rank of pfcf_pit (0 = cheap, 1 = expensive). No pipeline re-run needed.
Output: outputs/features/pfcf_quantile_valuation.parquet
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

import config
from walkforward.folds import get_rebal_dates


# Minimum non-null observations in window to assign a quantile
MIN_OBS_3Y = 12
MIN_OBS_5Y = 24
LOOKBACK_3Y = 36
LOOKBACK_5Y = 60


def _percentile_rank(current: float, window: np.ndarray) -> float | None:
    """Percentile rank of current in window, in [0, 1]. Returns None if window too small or current is NaN."""
    if np.isnan(current) or current is None:
        return None
    valid = window[~np.isnan(window)]
    if len(valid) < 2:
        return None
    # rank: 0 = smallest, 1 = largest; (rank - 1) / (n - 1) gives [0, 1]
    le = (valid <= current).sum()
    lt = (valid < current).sum()
    n = len(valid)
    # use midpoint of strict and weak rank for ties
    rank = (le + lt) / 2.0
    return float((rank - 0.5) / (n - 1)) if n > 1 else None


def main() -> None:
    master_path = config.MASTER_FEATURES_PATH
    if not master_path.exists():
        print(f"Master not found: {master_path}. Run pipeline 08_merge first.")
        sys.exit(1)

    out_path = config.PFCF_QUANTILE_VALUATION_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = __import__("duckdb").connect()
    # Month-ends from master (same logic as get_rebal_dates)
    month_ends = get_rebal_dates(conn, str(master_path), config.DATE_START, freq="month")
    conn.close()

    # Load master at month-ends only: ticker, date, pfcf_pit
    df = pd.read_parquet(
        master_path,
        columns=["ticker", "date", "pfcf_pit"],
    )
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    rebal_dates = pd.to_datetime(month_ends).normalize()
    df = df[df["date"].isin(rebal_dates)].copy()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Rolling percentile rank per ticker
    pfcf_3y = []
    pfcf_5y = []
    for ticker, grp in df.groupby("ticker", sort=False):
        dates = grp["date"].values
        vals = grp["pfcf_pit"].values.astype(np.float64)
        n = len(dates)
        q3 = np.full(n, np.nan, dtype=np.float64)
        q5 = np.full(n, np.nan, dtype=np.float64)
        for i in range(n):
            # Trailing 36 month-ends (inclusive of current)
            start_3 = max(0, i - LOOKBACK_3Y + 1)
            window_3 = vals[start_3 : i + 1]
            if np.sum(~np.isnan(window_3)) >= MIN_OBS_3Y:
                q3[i] = _percentile_rank(vals[i], window_3)
            # Trailing 60 month-ends
            start_5 = max(0, i - LOOKBACK_5Y + 1)
            window_5 = vals[start_5 : i + 1]
            if np.sum(~np.isnan(window_5)) >= MIN_OBS_5Y:
                q5[i] = _percentile_rank(vals[i], window_5)
        pfcf_3y.append(q3)
        pfcf_5y.append(q5)

    df["pfcf_3y_quantile"] = np.concatenate(pfcf_3y)
    df["pfcf_5y_quantile"] = np.concatenate(pfcf_5y)

    out = df[["ticker", "date", "pfcf_3y_quantile", "pfcf_5y_quantile"]].copy()
    out.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} ({len(out)} rows, {out['ticker'].nunique()} tickers)")


if __name__ == "__main__":
    main()
