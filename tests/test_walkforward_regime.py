"""Unit tests for walkforward.regime."""
import numpy as np
import pandas as pd
import pytest

from walkforward.evaluation import oos_drawdown_and_cagr
from walkforward.regime import RegimeFilterResult, apply_vix_regime_filter


def test_apply_vix_regime_filter_basic():
    """Filtered is 0 where VIX >= threshold, equals raw where VIX < 35; in_market and pct_in_market correct."""
    idx = pd.date_range("2020-01-31", periods=4, freq="ME")
    oos_rets = pd.Series([0.01, -0.02, 0.015, 0.02], index=idx)
    vix_aligned = pd.Series([20.0, 40.0, 25.0, 45.0], index=idx)

    result = apply_vix_regime_filter(
        oos_rets, vix_aligned, vix_threshold=35, periods_per_year=12
    )

    assert isinstance(result, RegimeFilterResult)
    assert result.in_market.iloc[0]
    assert not result.in_market.iloc[1]
    assert result.in_market.iloc[2]
    assert not result.in_market.iloc[3]
    assert result.oos_rets_filtered.iloc[0] == pytest.approx(0.01)
    assert result.oos_rets_filtered.iloc[1] == pytest.approx(0.0)
    assert result.oos_rets_filtered.iloc[2] == pytest.approx(0.015)
    assert result.oos_rets_filtered.iloc[3] == pytest.approx(0.0)
    assert result.pct_in_market == pytest.approx(50.0)

    # Filtered stats should match oos_drawdown_and_cagr on the filtered series
    expected_dd, expected_cagr = oos_drawdown_and_cagr(
        result.oos_rets_filtered, periods_per_year=12
    )
    assert result.max_dd_filtered == pytest.approx(expected_dd)
    assert result.cagr_filtered == pytest.approx(expected_cagr)

    if result.oos_rets_filtered.std() > 0:
        expected_sharpe = (
            result.oos_rets_filtered.mean()
            / result.oos_rets_filtered.std()
            * (12 ** 0.5)
        )
        assert result.sharpe_filtered == pytest.approx(expected_sharpe)


def test_apply_vix_regime_filter_all_above_threshold():
    """All VIX above threshold: filtered all 0, pct_in_market 0."""
    idx = pd.date_range("2020-01-31", periods=2, freq="ME")
    oos_rets = pd.Series([0.01, -0.02], index=idx)
    vix_aligned = pd.Series([40.0, 50.0], index=idx)

    result = apply_vix_regime_filter(
        oos_rets, vix_aligned, vix_threshold=35, periods_per_year=12
    )

    assert (result.oos_rets_filtered == 0).all()
    assert result.pct_in_market == pytest.approx(0.0)
    assert not result.in_market.any()
    assert np.isnan(result.sharpe_filtered) or result.sharpe_filtered == 0


def test_apply_vix_regime_filter_all_below_threshold():
    """All VIX below threshold: filtered equals raw, pct_in_market 100."""
    idx = pd.date_range("2020-01-31", periods=2, freq="ME")
    oos_rets = pd.Series([0.01, -0.02], index=idx)
    vix_aligned = pd.Series([20.0, 25.0], index=idx)

    result = apply_vix_regime_filter(
        oos_rets, vix_aligned, vix_threshold=35, periods_per_year=12
    )

    pd.testing.assert_series_equal(result.oos_rets_filtered, oos_rets)
    assert result.pct_in_market == pytest.approx(100.0)
    assert result.in_market.all()


def test_regime_filter_result_contract():
    """RegimeFilterResult has expected attributes."""
    idx = pd.date_range("2020-01-31", periods=1, freq="ME")
    oos_rets = pd.Series([0.01], index=idx)
    vix_aligned = pd.Series([20.0], index=idx)
    result = apply_vix_regime_filter(oos_rets, vix_aligned)

    expected_attrs = (
        "oos_rets_filtered",
        "in_market",
        "sharpe_filtered",
        "max_dd_filtered",
        "cagr_filtered",
        "pct_in_market",
        "vix_mean_in",
        "vix_mean_out",
    )
    for attr in expected_attrs:
        assert hasattr(result, attr), f"Missing attribute: {attr}"
