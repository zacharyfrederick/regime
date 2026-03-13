"""Unit tests for walkforward.folds."""
import pandas as pd
import pytest

from walkforward.folds import generate_folds


def test_generate_folds_respects_embargo():
    """Gap between IS end and OOS start should equal embargo periods."""
    # 24 rebal dates, is_periods=4, oos_periods=4, embargo_periods=2, min_oos=2
    rebal_dates = pd.date_range("2000-01-31", periods=24, freq="ME").tolist()
    folds = generate_folds(
        rebal_dates,
        is_periods=4,
        oos_periods=4,
        embargo_periods=2,
        min_oos_periods=2,
    )
    assert len(folds) >= 1
    is_end, oos_start = folds[0][1], folds[0][2]
    # Index of is_end + 1 + 2 (embargo) should be index of oos_start
    idx_is_end = rebal_dates.index(is_end)
    idx_oos_start = rebal_dates.index(oos_start)
    assert idx_oos_start - idx_is_end == 1 + 2  # 1 step + 2 embargo


def test_generate_folds_min_oos():
    """Folds with OOS shorter than min_oos_periods are skipped."""
    # Only 3 rebal dates for OOS possible; min_oos_periods=5 => fold skipped
    rebal_dates = pd.date_range("2000-01-31", periods=15, freq="ME").tolist()
    folds = generate_folds(
        rebal_dates,
        is_periods=2,
        oos_periods=10,  # would extend past list
        embargo_periods=1,
        min_oos_periods=5,
    )
    # First fold: oos_end_idx = min(2+1+10-1, 14) = 12, n_oos = 12 - 3 + 1 = 10 >= 5, so included
    # After first fold i += oos_periods => i=10, oos_start_idx = 10+2+1 = 13, oos_end_idx = min(13+10-1, 14)=14, n_oos=2 < 5 => skip
    assert len(folds) >= 1
    # If we make min_oos very large, we may get zero folds
    folds_strict = generate_folds(
        rebal_dates,
        is_periods=2,
        oos_periods=4,
        embargo_periods=1,
        min_oos_periods=100,
    )
    assert len(folds_strict) == 0


def test_generate_folds_count():
    """For a fixed list and params, expect a known number of folds."""
    # 38 months: is=12, oos=12, embargo=2. First fold: OOS 14..25 (12 periods). Then i+=12 => 12,
    # second fold: OOS start 26, end min(26+11, 37)=37, n_oos=12 => included. So 2 folds.
    rebal_dates = pd.date_range("2000-01-31", periods=38, freq="ME").tolist()
    folds = generate_folds(
        rebal_dates,
        is_periods=12,
        oos_periods=12,
        embargo_periods=2,
        min_oos_periods=12,
    )
    assert len(folds) == 2
    assert folds[0][0] == rebal_dates[0]
    assert folds[0][1] == rebal_dates[11]
    assert folds[0][2] == rebal_dates[14]
    assert folds[0][3] == rebal_dates[25]
    assert folds[1][2] == rebal_dates[26]
    assert folds[1][3] == rebal_dates[37]
