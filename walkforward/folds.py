"""
Rebal date loading and fold generation for walk-forward validation.
"""
from __future__ import annotations

import pandas as pd


def get_rebal_dates(
    conn,
    parquet_path: str,
    data_start: str,
    freq: str = "month",
) -> list:
    """
    Get rebalance dates (month-end or week-end) from parquet.
    Returns list of dates. Caller should register as DataFrame for load_fold, e.g.:
      conn.register("month_ends", pd.DataFrame({"rebal_date": dates}))
    """
    if freq == "month":
        df = conn.execute(
            """
            SELECT MAX(date) AS rebal_date
            FROM read_parquet(?)
            WHERE date >= ?
            GROUP BY year(date), month(date)
            ORDER BY rebal_date
            """,
            [parquet_path, data_start],
        ).df()
    elif freq == "week":
        df = conn.execute(
            """
            SELECT MAX(date) AS rebal_date
            FROM read_parquet(?)
            WHERE date >= ?
            GROUP BY year(date), date_part('week', date)
            ORDER BY rebal_date
            """,
            [parquet_path, data_start],
        ).df()
    else:
        raise ValueError(f"freq must be 'month' or 'week', got {freq!r}")
    return df["rebal_date"].tolist()


def generate_folds(
    rebal_dates: list,
    is_periods: int,
    oos_periods: int,
    embargo_periods: int,
    min_oos_periods: int = 12,
):
    """
    Rolling window folds. Embargo periods are skipped between IS end and OOS start.
    Skip a fold if its OOS would have fewer than min_oos_periods.
    Returns list of (is_start, is_end, oos_start, oos_end).
    """
    folds = []
    i = 0
    while True:
        oos_start_idx = i + is_periods + embargo_periods
        if oos_start_idx >= len(rebal_dates):
            break
        is_start = rebal_dates[i]
        is_end = rebal_dates[i + is_periods - 1]
        oos_start = rebal_dates[oos_start_idx]
        oos_end_idx = min(
            oos_start_idx + oos_periods - 1,
            len(rebal_dates) - 1,
        )
        n_oos = oos_end_idx - oos_start_idx + 1
        if n_oos >= min_oos_periods:
            oos_end = rebal_dates[oos_end_idx]
            folds.append((is_start, is_end, oos_start, oos_end))
        if oos_end_idx >= len(rebal_dates) - 1:
            break
        i += oos_periods
    return folds
