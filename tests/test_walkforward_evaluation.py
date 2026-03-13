"""Unit tests for walkforward.evaluation."""
import numpy as np
import pandas as pd
import pytest

from walkforward.evaluation import (
    _weights_exponential,
    _weights_rank,
    _weights_zscore,
    compute_turnover,
    evaluate_fold,
    oos_drawdown_and_cagr,
)


def test_evaluate_fold_returns_dict():
    """evaluate_fold returns dict with sharpe, hit_rate, monthly_rets, selected_by_date."""
    # Two dates, few rows each
    df = pd.DataFrame({
        "date": ["2020-01-31"] * 5 + ["2020-02-28"] * 5,
        "ticker": [f"A{i}" for i in range(5)] + [f"B{i}" for i in range(5)],
        "fwd_ret_21td": [0.1, 0.2, -0.1, 0.0, 0.05] + [0.02, 0.03, -0.02, 0.01, 0.04],
    })
    preds = np.array([1, 2, 0, 0.5, 0.3] + [2, 1, 0, 0.5, 0.3])  # top 2 per date: A1,A0 and B0,B1
    out = evaluate_fold(
        df,
        preds,
        ret_col="fwd_ret_21td",
        top_n=2,
        periods_per_year=12,
    )
    assert "sharpe" in out
    assert "hit_rate" in out
    assert "monthly_rets" in out
    assert "selected_by_date" in out
    assert hasattr(out["monthly_rets"], "index")
    assert len(out["monthly_rets"]) == 2
    assert len(out["selected_by_date"]) == 2
    # Top 2 by pred for first date: A1 (0.2), A0 (0.1) => mean 0.15
    assert out["monthly_rets"].iloc[0] == pytest.approx(0.15)
    first_date = out["monthly_rets"].index[0]
    assert out["selected_by_date"][first_date] == {"A0", "A1"}


def test_compute_turnover():
    """Turnover for two dates with known overlap matches hand-computed value."""
    # Date1: {A,B}, Date2: {A,B} => overlap 2, union 2 => 1 - 2/2 = 0
    selected = {
        pd.Timestamp("2020-01-31"): {"A", "B"},
        pd.Timestamp("2020-02-28"): {"A", "B"},
    }
    assert compute_turnover(selected) == pytest.approx(0.0)
    # Date1: {A,B}, Date2: {C,D} => overlap 0, union 4 => 1 - 0/4 = 1
    selected_full_churn = {
        pd.Timestamp("2020-01-31"): {"A", "B"},
        pd.Timestamp("2020-02-28"): {"C", "D"},
    }
    assert compute_turnover(selected_full_churn) == pytest.approx(1.0)
    # Single date => no consecutive pair
    assert np.isnan(compute_turnover({pd.Timestamp("2020-01-31"): {"A", "B"}}))


def test_weights_rank():
    """_weights_rank: w_i ∝ (N - rank + 1), sum to 1."""
    w = _weights_rank(3, np.array([1.0, 2.0, 3.0]))
    assert w.shape == (3,)
    assert np.sum(w) == pytest.approx(1.0)
    # Proportional to (3, 2, 1) => 3:2:1 ratio
    assert w[0] > w[1] > w[2]
    assert w[0] / w[2] == pytest.approx(3.0)


def test_weights_exponential():
    """_weights_exponential: shape (3), sum 1, strictly increasing with score."""
    w = _weights_exponential(np.array([0.0, 1.0, 2.0]))
    assert w.shape == (3,)
    assert np.sum(w) == pytest.approx(1.0)
    assert w[2] > w[1] > w[0]


def test_weights_zscore():
    """_weights_zscore: shape (3), sum 1; constant input gives equal weights."""
    w = _weights_zscore(np.array([1.0, 2.0, 3.0]))
    assert w.shape == (3,)
    assert np.sum(w) == pytest.approx(1.0)
    assert w[2] > w[1] > w[0]
    w_const = _weights_zscore(np.array([5.0, 5.0, 5.0]))
    assert np.allclose(w_const, 1.0 / 3.0)


def test_evaluate_fold_weight_schemes():
    """evaluate_fold with rank vs equal gives different period return when returns differ."""
    df = pd.DataFrame({
        "date": ["2020-01-31"] * 5,
        "ticker": [f"A{i}" for i in range(5)],
        "fwd_ret_21td": [0.2, 0.1, -0.1, 0.0, 0.05],  # best pred gets 0.2, 2nd gets 0.1, 3rd gets -0.1
    })
    preds = np.array([3, 2, 0, 0.5, 1])  # rank 1=A0(0.2), 2=A4(0.05), 3=A1(0.1)
    out_equal = evaluate_fold(
        df, preds, ret_col="fwd_ret_21td", top_n=3, periods_per_year=12, weight_scheme="equal"
    )
    out_rank = evaluate_fold(
        df, preds, ret_col="fwd_ret_21td", top_n=3, periods_per_year=12, weight_scheme="rank"
    )
    assert set(out_equal.keys()) == set(out_rank.keys())
    # Equal: mean(0.2, 0.05, 0.1) = 0.1166...
    # Rank: more weight on A0 (best) => higher period return
    assert out_equal["monthly_rets"].iloc[0] == pytest.approx((0.2 + 0.05 + 0.1) / 3)
    assert out_rank["monthly_rets"].iloc[0] != pytest.approx(out_equal["monthly_rets"].iloc[0])
    assert out_rank["monthly_rets"].iloc[0] >= -0.1
    assert out_rank["monthly_rets"].iloc[0] <= 0.2
    # Exponential and zscore also produce valid period returns
    out_exp = evaluate_fold(df, preds, ret_col="fwd_ret_21td", top_n=3, periods_per_year=12, weight_scheme="exponential")
    out_z = evaluate_fold(df, preds, ret_col="fwd_ret_21td", top_n=3, periods_per_year=12, weight_scheme="zscore")
    assert -0.1 <= out_exp["monthly_rets"].iloc[0] <= 0.2
    assert -0.1 <= out_z["monthly_rets"].iloc[0] <= 0.2


def test_oos_drawdown_and_cagr():
    """Known return series produces expected max drawdown and CAGR."""
    # 3 periods: +10%, -20%, +15% => cum 1.1, 0.88, 1.012; run_max 1.1, 1.1, 1.012; dd 0, -0.2, 0
    rets = pd.Series([0.1, -0.2, 0.15], index=pd.DatetimeIndex(["2020-01-31", "2020-02-28", "2020-03-31"]))
    max_dd, cagr = oos_drawdown_and_cagr(rets, periods_per_year=12)
    assert max_dd == pytest.approx(-0.2)
    # CAGR = (1.012)^(12/3) - 1 = 1.012^4 - 1
    assert cagr == pytest.approx(1.012 ** 4 - 1)
