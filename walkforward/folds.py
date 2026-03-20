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
    step: int = 1,
) -> list:
    """
    Get rebalance dates from parquet.

    - month: last available date per calendar month
    - week: last available date per ISO week
    - day: every distinct trading date (panel row date), optionally thinned

    ``step``: if > 1, keep every step-th date from the ordered list (reduces load for freq=day).

    Register for loaders, e.g.:
      conn.register("rebal_dates", pd.DataFrame({"rebal_date": dates}))
    """
    if freq == "month":
        df = conn.execute(
            """
            SELECT MAX(CAST(date AS DATE)) AS rebal_date
            FROM read_parquet(?)
            WHERE CAST(date AS DATE) >= CAST(? AS DATE)
            GROUP BY year(CAST(date AS DATE)), month(CAST(date AS DATE))
            ORDER BY rebal_date
            """,
            [parquet_path, data_start],
        ).df()
    elif freq == "week":
        df = conn.execute(
            """
            SELECT MAX(CAST(date AS DATE)) AS rebal_date
            FROM read_parquet(?)
            WHERE CAST(date AS DATE) >= CAST(? AS DATE)
            GROUP BY year(CAST(date AS DATE)), date_part('week', CAST(date AS DATE))
            ORDER BY rebal_date
            """,
            [parquet_path, data_start],
        ).df()
    elif freq == "day":
        df = conn.execute(
            """
            SELECT DISTINCT CAST(date AS DATE) AS rebal_date
            FROM read_parquet(?)
            WHERE CAST(date AS DATE) >= CAST(? AS DATE)
            ORDER BY rebal_date
            """,
            [parquet_path, data_start],
        ).df()
    else:
        raise ValueError(f"freq must be 'month', 'week', or 'day', got {freq!r}")
    dates = df["rebal_date"].tolist()
    if step > 1:
        dates = dates[::step]
    return dates


def generate_folds(
    rebal_dates: list,
    is_periods: int,
    oos_periods: int,
    embargo_periods: int,
    min_oos_periods: int = 12,
):
    """
    Rolling window folds over an ordered rebal calendar.

    ``is_periods`` / ``oos_periods`` / ``embargo_periods`` / ``min_oos_periods`` are counts of
    **rebalance steps** (month-ends, week-ends, or trading days), depending on how
    ``rebal_dates`` was built (see ``get_rebal_dates``).

    Embargo: that many rebal steps are skipped between IS end and OOS start.
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
