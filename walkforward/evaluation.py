"""
Portfolio evaluation and metrics for walk-forward validation.
"""
from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

WeightScheme = Literal["equal", "rank", "exponential", "zscore", "inv_vol", "signal_inv_vol"]

# Signal type for signal_inv_vol weighting: which score-based component to combine with 1/vol.
SignalInvVolSignal = Literal["rank", "exponential", "zscore"]

# Column name expected in fold data for inv_vol / signal_inv_vol (point-in-time volatility, e.g. annualized).
INV_VOL_COL = "vol_21d"


def _weights_rank(N: int, rank: np.ndarray) -> np.ndarray:
    """w_i ∝ (N - rank + 1), rank 1 = best. Normalized to sum 1."""
    w = (N - rank + 1).astype(float)
    s = w.sum()
    return w / s if s > 0 else np.full_like(w, 1.0 / len(w))


def _weights_exponential(score: np.ndarray) -> np.ndarray:
    """w_i ∝ exp(score). Subtract max for numerical stability; normalized to sum 1."""
    s = score.astype(float)
    s = s - np.nanmax(s)
    w = np.exp(np.clip(s, -500, 500))
    total = w.sum()
    return w / total if total > 0 else np.full_like(w, 1.0 / len(w))


def _weights_zscore(score: np.ndarray) -> np.ndarray:
    """w_i ∝ zscore(score), shifted so all positive. Normalized to sum 1."""
    s = score.astype(float)
    mu, std = np.nanmean(s), np.nanstd(s)
    if std == 0 or np.isnan(std):
        return np.full_like(s, 1.0 / len(s))
    z = (s - mu) / std
    w = z - np.nanmin(z) + 1e-8
    total = w.sum()
    return w / total if total > 0 else np.full_like(w, 1.0 / len(w))


def _weights_inv_vol(sigma: np.ndarray, floor: float = 1e-8) -> np.ndarray:
    """Inverse volatility: w_i = (1/sigma_i) / sum(1/sigma_j). sigma floored to avoid div by zero."""
    s = np.asarray(sigma, dtype=float)
    s = np.where(np.isnan(s) | (s <= 0), floor, s)
    inv = 1.0 / s
    total = inv.sum()
    return inv / total if total > 0 else np.full_like(inv, 1.0 / len(inv))


def _weights_signal_inv_vol(
    signal: np.ndarray,
    sigma: np.ndarray,
    floor: float = 1e-8,
) -> np.ndarray:
    """w_i ∝ (signal_i * (1/sigma_i)); normalized. sigma floored to avoid div by zero."""
    sig = np.asarray(signal, dtype=float)
    sig = np.where(np.isnan(sig) | (sig < 0), 0.0, sig)
    s = np.asarray(sigma, dtype=float)
    s = np.where(np.isnan(s) | (s <= 0), floor, s)
    raw = sig * (1.0 / s)
    total = raw.sum()
    return raw / total if total > 0 else np.full_like(raw, 1.0 / len(raw))


def _period_returns_weighted(
    d: pd.DataFrame,
    ret_col: str,
    weight_scheme: WeightScheme,
    signal_inv_vol_signal: SignalInvVolSignal = "rank",
) -> pd.Series:
    """Among selected rows in d, compute weights per (date,) and return weighted period return series.
    signal_inv_vol_signal: used only when weight_scheme='signal_inv_vol' ('rank' | 'exponential' | 'zscore').
    """
    if weight_scheme == "equal":
        return d.groupby("date").apply(
            lambda g: (g[ret_col] * np.ones(len(g)) / len(g)).sum(),
            include_groups=False,
        ).rename("port_ret")
    out = []
    for date, g in d.groupby("date"):
        rets = g[ret_col].values
        n = len(rets)
        if n == 0:
            continue
        if weight_scheme == "rank":
            rank = g["rank"].values.astype(float)
            w = _weights_rank(n, rank)
        elif weight_scheme == "exponential":
            w = _weights_exponential(g["pred"].values)
        elif weight_scheme == "zscore":
            w = _weights_zscore(g["pred"].values)
        elif weight_scheme == "inv_vol":
            if INV_VOL_COL not in g.columns:
                w = np.ones(n) / n
            else:
                w = _weights_inv_vol(g[INV_VOL_COL].values)
        elif weight_scheme == "signal_inv_vol":
            if INV_VOL_COL not in g.columns:
                w = np.ones(n) / n
            else:
                if signal_inv_vol_signal == "rank":
                    signal = (n - g["rank"].values.astype(float) + 1)
                elif signal_inv_vol_signal == "exponential":
                    s = g["pred"].values.astype(float) - np.nanmax(g["pred"].values)
                    signal = np.exp(np.clip(s, -500, 500))
                else:
                    s = g["pred"].values.astype(float)
                    mu, std = np.nanmean(s), np.nanstd(s)
                    if std == 0 or np.isnan(std):
                        signal = np.ones(n)
                    else:
                        z = (s - mu) / std
                        signal = z - np.nanmin(z) + 1e-8
                w = _weights_signal_inv_vol(signal, g[INV_VOL_COL].values)
        else:
            w = np.ones(n) / n
        out.append((date, float(np.dot(w, rets))))
    return pd.Series(dict(out)).rename("port_ret").sort_index()


def evaluate_fold(
    df: pd.DataFrame,
    preds,
    ret_col: str = "fwd_ret_21td",
    top_n: int = 50,
    periods_per_year: float = 12,
    weight_scheme: WeightScheme = "equal",
    signal_inv_vol_signal: SignalInvVolSignal = "rank",
):
    """
    Rank by pred within each date; select top top_n names; compute period return by weight_scheme.
    weight_scheme: 'equal' | 'rank' | 'exponential' | 'zscore' | 'inv_vol' | 'signal_inv_vol'.
    For 'inv_vol' / 'signal_inv_vol', selected rows must have column INV_VOL_COL (vol_21d); else equal weight.
    signal_inv_vol_signal: for 'signal_inv_vol' only, which signal to use ('rank' | 'exponential' | 'zscore').
    Returns dict with sharpe, hit_rate, monthly_rets, selected_by_date, etc.
    """
    d = df.copy()
    preds = np.asarray(preds).ravel()
    assert len(preds) == len(d), f"preds length {len(preds)} != df length {len(d)}"
    d["pred"] = preds
    d["rank"] = d.groupby("date")["pred"].rank(ascending=False)
    d["selected"] = d["rank"] <= top_n
    sel = d.loc[d["selected"]]
    if weight_scheme == "equal":
        period_rets = sel.groupby("date")[ret_col].mean().rename("port_ret")
    elif weight_scheme in ("inv_vol", "signal_inv_vol") and INV_VOL_COL not in sel.columns:
        period_rets = sel.groupby("date")[ret_col].mean().rename("port_ret")
    else:
        period_rets = _period_returns_weighted(
            sel, ret_col, weight_scheme, signal_inv_vol_signal=signal_inv_vol_signal
        )
    selected_by_date = d.loc[d["selected"]].groupby("date")["ticker"].apply(set).to_dict()
    if period_rets.empty or period_rets.std() == 0:
        sharpe = np.nan
    else:
        sharpe = period_rets.mean() / period_rets.std() * (periods_per_year ** 0.5)
    return {
        "sharpe": sharpe,
        "hit_rate": (period_rets > 0).mean() if len(period_rets) else np.nan,
        "avg_period_ret": period_rets.mean() if len(period_rets) else np.nan,
        "worst_period": period_rets.min() if len(period_rets) else np.nan,
        "n_periods": len(period_rets),
        "monthly_rets": period_rets,
        "selected_by_date": selected_by_date,
        "avg_universe_size": d.groupby("date").size().mean() if "date" in d.columns else np.nan,
        "avg_selected_count": d.loc[d["selected"]].groupby("date").size().mean() if d["selected"].any() else 0,
    }


def evaluate_equal_weight(
    df: pd.DataFrame,
    ret_col: str = "fwd_ret_21td",
    periods_per_year: float = 12,
):
    """
    Screen-only: equal-weight all names that pass the screen each period. Same metric shape as evaluate_fold.
    """
    period_rets = df.groupby("date")[ret_col].mean().sort_index()
    if period_rets.empty or period_rets.std() == 0:
        sharpe = np.nan
    else:
        sharpe = period_rets.mean() / period_rets.std() * (periods_per_year ** 0.5)
    n = len(period_rets)
    return {
        "sharpe": sharpe,
        "hit_rate": (period_rets > 0).mean() if n else np.nan,
        "avg_period_ret": period_rets.mean() if n else np.nan,
        "worst_period": period_rets.min() if n else np.nan,
        "n_periods": n,
        "monthly_rets": period_rets,
        "selected_by_date": {},
        "avg_universe_size": df.groupby("date").size().mean() if "date" in df.columns else np.nan,
        "avg_selected_count": df.groupby("date").size().mean() if "date" in df.columns else np.nan,
    }


def compute_turnover(selected_by_date: dict) -> float:
    """Average turnover: 1 - |prev & curr| / |prev | curr| per consecutive period pair."""
    dates = sorted(selected_by_date.keys())
    turns = []
    for i in range(1, len(dates)):
        prev, curr = set(selected_by_date[dates[i - 1]]), set(selected_by_date[dates[i]])
        if len(prev) == 0:
            continue
        union = len(prev | curr)
        turns.append(1 - len(prev & curr) / union if union else 0)
    return float(np.mean(turns)) if turns else np.nan


def evaluate_fold_long_short(
    df: pd.DataFrame,
    preds,
    ret_col: str = "fwd_ret_21td",
    top_n: int = 20,
    periods_per_year: float = 12,
    scale_gross_to_one: bool = True,
):
    """
    Long top_n by score, short bottom top_n. If scale_gross_to_one, port_ret = 0.5*long - 0.5*short.
    """
    d = df.copy()
    preds = np.asarray(preds).ravel()
    assert len(preds) == len(d), f"preds length {len(preds)} != df length {len(d)}"
    # NaN predictions are excluded from selection (rank NaN => never in top_n or bottom top_n)
    d["pred"] = preds
    d["rank_desc"] = d.groupby("date")["pred"].rank(method="first", ascending=False)
    d["rank_asc"] = d.groupby("date")["pred"].rank(method="first", ascending=True)
    d["is_long"] = d["rank_desc"] <= top_n
    d["is_short"] = d["rank_asc"] <= top_n
    long_rets = d.loc[d["is_long"]].groupby("date")[ret_col].mean().rename("long_ret")
    short_rets = d.loc[d["is_short"]].groupby("date")[ret_col].mean().rename("short_ret")
    period_rets = pd.concat([long_rets, short_rets], axis=1).dropna()
    if scale_gross_to_one:
        period_rets["port_ret"] = 0.5 * period_rets["long_ret"] - 0.5 * period_rets["short_ret"]
    else:
        period_rets["port_ret"] = period_rets["long_ret"] - period_rets["short_ret"]
    monthly_rets = period_rets["port_ret"]
    long_selected_by_date = d.loc[d["is_long"]].groupby("date")["ticker"].apply(set).to_dict()
    short_selected_by_date = d.loc[d["is_short"]].groupby("date")["ticker"].apply(set).to_dict()
    if monthly_rets.empty or monthly_rets.std() == 0:
        sharpe = np.nan
    else:
        sharpe = monthly_rets.mean() / monthly_rets.std() * (periods_per_year ** 0.5)
    return {
        "sharpe": sharpe,
        "hit_rate": (monthly_rets > 0).mean() if len(monthly_rets) else np.nan,
        "avg_period_ret": monthly_rets.mean() if len(monthly_rets) else np.nan,
        "worst_period": monthly_rets.min() if len(monthly_rets) else np.nan,
        "n_periods": len(monthly_rets),
        "monthly_rets": monthly_rets,
        "long_selected_by_date": long_selected_by_date,
        "short_selected_by_date": short_selected_by_date,
        "avg_universe_size": d.groupby("date").size().mean() if "date" in d.columns else np.nan,
        "avg_long_count": d.loc[d["is_long"]].groupby("date").size().mean() if d["is_long"].any() else 0,
        "avg_short_count": d.loc[d["is_short"]].groupby("date").size().mean() if d["is_short"].any() else 0,
    }


def compute_turnover_long_short(
    long_selected_by_date: dict,
    short_selected_by_date: dict,
) -> dict:
    out = {}
    dates = sorted(set(long_selected_by_date.keys()) & set(short_selected_by_date.keys()))
    long_turns = []
    short_turns = []
    for i in range(1, len(dates)):
        prev_long = set(long_selected_by_date[dates[i - 1]])
        curr_long = set(long_selected_by_date[dates[i]])
        prev_short = set(short_selected_by_date[dates[i - 1]])
        curr_short = set(short_selected_by_date[dates[i]])
        long_union = len(prev_long | curr_long)
        short_union = len(prev_short | curr_short)
        if long_union:
            long_turns.append(1 - len(prev_long & curr_long) / long_union)
        if short_union:
            short_turns.append(1 - len(prev_short & curr_short) / short_union)
    out["long_turnover"] = float(np.mean(long_turns)) if long_turns else np.nan
    out["short_turnover"] = float(np.mean(short_turns)) if short_turns else np.nan
    out["turnover"] = float(np.nanmean([out["long_turnover"], out["short_turnover"]]))
    return out


def oos_drawdown_and_cagr(
    monthly_rets: pd.Series,
    periods_per_year: float = 12,
) -> tuple[float, float]:
    """From stitched period returns: max drawdown and CAGR."""
    if monthly_rets is None or len(monthly_rets) == 0 or monthly_rets.std() == 0:
        return np.nan, np.nan
    cum = (1 + monthly_rets).cumprod()
    run_max = cum.cummax()
    dd = (cum - run_max) / run_max
    max_dd = float(dd.min())
    n = len(monthly_rets)
    cagr = (float(cum.iloc[-1]) ** (periods_per_year / n)) - 1 if n and cum.iloc[-1] > 0 else np.nan
    return max_dd, cagr


def random_top_n_mean_by_date(
    df: pd.DataFrame,
    ret_col: str,
    top_n: int,
    rng: np.random.Generator,
) -> pd.Series:
    """Random shuffle per date, take first top_n, return mean return per date."""
    x = df[["date", ret_col]].copy()
    x["rand"] = rng.random(len(x))
    x = x.sort_values(["date", "rand"])
    x["rank"] = x.groupby("date").cumcount()
    return (
        x.loc[x["rank"] < top_n]
        .groupby("date")[ret_col]
        .mean()
        .rename("port_ret")
    )
