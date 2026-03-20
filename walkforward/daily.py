"""
Daily (and explicit calendar) rebalance helpers for walk-forward.

``get_rebal_dates(..., freq="day")`` in ``folds.py`` builds one rebal per distinct
panel date. This module documents that contract and provides thin wrappers +
annualization helpers for experiments.
"""
from __future__ import annotations

from walkforward.folds import generate_folds, get_rebal_dates

__all__ = [
    "get_trading_rebal_dates",
    "generate_folds_daily",
    "periods_per_year_for_freq",
    "DEFAULT_DAILY_FOLD_PERIODS",
]


# Default fold sizes when each rebal step is one trading day (tune to taste).
DEFAULT_DAILY_FOLD_PERIODS = {
    "is_periods": 504,
    "oos_periods": 252,
    "embargo_periods": 10,
    "min_oos_periods": 63,
}

# Default fold sizes when each rebal step is one week-end bucket from the panel.
DEFAULT_WEEK_FOLD_PERIODS = {
    "is_periods": 104,
    "oos_periods": 52,
    "embargo_periods": 2,
    "min_oos_periods": 26,
}


def get_trading_rebal_dates(
    conn,
    parquet_path: str,
    data_start: str,
    *,
    step: int = 1,
) -> list:
    """Distinct trading dates in the panel on or after ``data_start``; optional ``step`` subsampling."""
    return get_rebal_dates(conn, parquet_path, data_start, freq="day", step=step)


def generate_folds_daily(
    rebal_dates: list,
    is_trading_days: int,
    oos_trading_days: int,
    embargo_trading_days: int,
    min_oos_trading_days: int | None = None,
):
    """Rolling folds; arguments are counts of **trading-day** rebal steps (see ``generate_folds``)."""
    mo = min_oos_trading_days
    if mo is None:
        mo = DEFAULT_DAILY_FOLD_PERIODS["min_oos_periods"]
    return generate_folds(
        rebal_dates,
        is_trading_days,
        oos_trading_days,
        embargo_trading_days,
        mo,
    )


def periods_per_year_for_freq(freq: str) -> float:
    """Annualization factor for per-rebal-period returns (Sharpe, vol, etc.)."""
    if freq == "month":
        return 12.0
    if freq == "week":
        return 52.0
    if freq == "day":
        return 252.0
    raise ValueError(f"freq must be 'month', 'week', or 'day', got {freq!r}")
