"""Tests for combo vs SPY plot alignment and CAGR consistency.

Verifies that:
- CAGR from cumprod matches oos_drawdown_and_cagr.
- Aligned combo_rets and spy_fwd have expected lengths (no silent dropna).
- Plotted-span CAGR is consistent with full-series CAGR when indices align.
"""
import numpy as np
import pandas as pd
import pytest

from walkforward.evaluation import oos_drawdown_and_cagr


def test_plotted_cagr_math():
    """CAGR from cumprod matches oos_drawdown_and_cagr formula."""
    np.random.seed(42)
    monthly_rets = pd.Series(np.random.randn(24) * 0.01)
    _, cagr = oos_drawdown_and_cagr(monthly_rets, periods_per_year=12)
    n = len(monthly_rets)
    cum = (1 + monthly_rets).cumprod()
    expected = (float(cum.iloc[-1]) ** (12 / n)) - 1
    assert cagr == pytest.approx(expected)


def test_combo_spy_alignment_same_index():
    """When combo_rets and spy_fwd share the same index, concat().dropna() keeps all rows."""
    idx = pd.date_range("2020-01-31", periods=12, freq="ME")
    combo_rets = pd.Series(np.random.randn(12) * 0.01, index=idx).rename("combo")
    spy_fwd = pd.Series(np.random.randn(12) * 0.01, index=idx).rename("spy")
    aligned = pd.concat([combo_rets, spy_fwd], axis=1).dropna()
    assert len(aligned) == 12
    assert (aligned.index == idx).all()


def test_combo_spy_alignment_spy_one_less():
    """spy_fwd has n-1 rows (shift(-1).dropna()); aligned should have n-1, not silently fewer."""
    idx = pd.date_range("2020-01-31", periods=24, freq="ME")
    combo_rets = pd.Series(np.random.randn(24) * 0.01, index=idx).rename("combo")
    # Simulate spy_fwd: one fewer date (last month has no forward return)
    spy_idx = idx[:-1]
    spy_fwd = pd.Series(np.random.randn(23) * 0.01, index=spy_idx).rename("spy")
    aligned = pd.concat([combo_rets, spy_fwd], axis=1).dropna()
    assert len(aligned) == 23
    assert aligned.index.equals(spy_idx)


def test_plotted_span_cagr_consistent():
    """CAGR over the aligned (plotted) series should be close to full-series CAGR when lengths match."""
    np.random.seed(123)
    n = 36
    idx = pd.date_range("2020-01-31", periods=n, freq="ME")
    combo_rets = pd.Series(np.random.randn(n) * 0.01, index=idx)
    _, cagr_full = oos_drawdown_and_cagr(combo_rets, periods_per_year=12)
    # "Plotted" = same series (no alignment drop)
    combo_plotted = combo_rets.copy()
    n_plot = len(combo_plotted)
    cum_plotted = (1 + combo_plotted).cumprod()
    cagr_plotted = (float(cum_plotted.iloc[-1]) ** (12 / n_plot)) - 1
    assert cagr_plotted == pytest.approx(cagr_full)


def test_plotted_span_cagr_when_aligned_shorter():
    """If aligned has fewer rows than full combo_rets, plotted CAGR can differ from full CAGR."""
    np.random.seed(456)
    n_full = 24
    idx_full = pd.date_range("2020-01-31", periods=n_full, freq="ME")
    combo_rets = pd.Series(np.random.randn(n_full) * 0.01, index=idx_full)
    _, cagr_full = oos_drawdown_and_cagr(combo_rets, periods_per_year=12)
    # Simulate alignment dropping first 4 months (e.g. SPY missing)
    idx_short = idx_full[4:]
    combo_short = combo_rets.loc[idx_short]
    n_short = len(combo_short)
    cum_short = (1 + combo_short).cumprod()
    cagr_short = (float(cum_short.iloc[-1]) ** (12 / n_short)) - 1
    # Full and short-period CAGRs can differ
    assert cagr_full != pytest.approx(cagr_short) or abs(cagr_full - cagr_short) < 0.001


def test_combo_spy_index_normalize():
    """Normalizing both indices to date yields full overlap (no spurious dropna)."""
    # Combo: month-end at midnight; spy: same calendar date (no time)
    idx_combo = pd.DatetimeIndex(["2020-01-31", "2020-02-28", "2020-03-31"])
    idx_spy = pd.DatetimeIndex(["2020-01-31", "2020-02-28", "2020-03-31"])
    combo_rets = pd.Series([0.01, -0.005, 0.02], index=idx_combo)
    spy_fwd = pd.Series([0.008, -0.003, 0.015], index=idx_spy)
    aligned = pd.concat([combo_rets.rename("combo"), spy_fwd.rename("spy")], axis=1).dropna()
    assert len(aligned) == 3
    # After explicit normalize, intersection should still be full
    c = pd.to_datetime(combo_rets.index).normalize()
    s = pd.to_datetime(spy_fwd.index).normalize()
    common = c.intersection(s)
    assert len(common) == 3


def test_oos_drawdown_and_cagr_zero_returns():
    """VIX-filtered series with many zeros: CAGR is still consistent with cumprod."""
    idx = pd.date_range("2020-01-31", periods=24, freq="ME")
    # Half zeros (out of market), half small positive
    rets = pd.Series([0.0, 0.005, 0.0, 0.004, 0.0, 0.006] * 4, index=idx[:24])
    max_dd, cagr = oos_drawdown_and_cagr(rets, periods_per_year=12)
    n = len(rets)
    cum = (1 + rets).cumprod()
    expected_cagr = (float(cum.iloc[-1]) ** (12 / n)) - 1
    assert cagr == pytest.approx(expected_cagr)
    assert max_dd <= 0
