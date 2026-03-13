"""
Composite scoring and strategy runner for walk-forward validation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from walkforward.encode import encode_fold
from walkforward.evaluation import (
    compute_turnover,
    compute_turnover_long_short,
    evaluate_equal_weight,
    evaluate_fold,
    evaluate_fold_long_short,
    oos_drawdown_and_cagr,
)
from walkforward.labels import add_target_xs, add_top_bucket_label


def build_composite_score(
    df: pd.DataFrame,
    components: list[tuple[str, float, bool]],
) -> pd.Series:
    """
    components: list of (col_name, weight, ascending). ascending=True means low is good (e.g. pe).
    """
    d = df.copy()
    pieces = []
    for col, weight, ascending in components:
        if col not in d.columns:
            pieces.append(pd.Series(0.0, index=d.index))
            continue
        z = d.groupby("date")[col].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() > 0 else x * 0
        )
        z = z.replace([np.inf, -np.inf], np.nan)
        if ascending:
            z = -z
        pieces.append(weight * z)
    return pd.concat(pieces, axis=1).sum(axis=1, min_count=1)


def build_rank_composite_score(
    df: pd.DataFrame,
    components: list[tuple[str, float]],
) -> pd.Series:
    """components: list of (col_name, weight). Score = sum(weight * pct_rank) per date. Higher = better."""
    pieces = []
    for col, weight in components:
        if col not in df.columns:
            pieces.append(pd.Series(0.0, index=df.index))
            continue
        r = df.groupby("date")[col].rank(pct=True, method="average")
        pieces.append(weight * r)
    return pd.concat(pieces, axis=1).sum(axis=1, min_count=1)


def make_single_feature_scorer(feature_col: str, ascending: bool = False):
    """Rank by raw feature. ascending=False => high is good; ascending=True => low is good (e.g. pe)."""

    def scorer(is_df, oos_df):
        fill_val = -np.inf if not ascending else np.inf
        is_s = (
            is_df[feature_col].fillna(fill_val).values
            if feature_col in is_df.columns
            else np.zeros(len(is_df))
        )
        oos_s = (
            oos_df[feature_col].fillna(fill_val).values
            if feature_col in oos_df.columns
            else np.zeros(len(oos_df))
        )
        if ascending:
            is_s, oos_s = -np.asarray(is_s, float), -np.asarray(oos_s, float)
        return is_s, oos_s

    return scorer


def make_composite_scorer(components: list[tuple[str, float, bool]]):
    def scorer(is_df, oos_df):
        is_score = build_composite_score(is_df, components)
        oos_score = build_composite_score(oos_df, components)
        return is_score.values, oos_score.values

    return scorer


def make_rank_composite_scorer(components: list[tuple[str, float]]):
    """Scorer from weighted sum of cross-sectional percentile ranks."""

    def scorer(is_df, oos_df):
        is_score = build_rank_composite_score(is_df, components)
        oos_score = build_rank_composite_score(oos_df, components)
        return is_score.values, oos_score.values

    return scorer


def build_fold_cache(
    folds: list,
    conn,
    load_fn,
    ret_col: str = "fwd_ret_21td",
    eval_dropna_subset: tuple[str, ...] = ("fwd_ret_21td", "fcf_r2_10y"),
    time_feature: str = "month_of_year",
) -> list[dict]:
    """
    Load and preprocess each fold once. load_fn(is_start, is_end, oos_start, oos_end) -> DataFrame.
    Returns list of dicts with eval_is, eval_oos, fold_tuple.
    """
    cache = []
    for is_start, is_end, oos_start, oos_end in folds:
        df = load_fn(is_start, is_end, oos_start, oos_end)
        df = add_target_xs(df, ret_col=ret_col)
        df = add_top_bucket_label(df, ret_col=ret_col)
        is_df = df[df["fold"] == "is"].copy()
        oos_df = df[df["fold"] == "oos"].copy()
        encode_fold(is_df, oos_df, time_feature=time_feature)
        eval_is = is_df.dropna(subset=list(eval_dropna_subset)).copy()
        eval_oos = oos_df.dropna(subset=list(eval_dropna_subset)).copy()
        cache.append({
            "eval_is": eval_is,
            "eval_oos": eval_oos,
            "fold_tuple": (is_start, is_end, oos_start, oos_end),
        })
    return cache


def run_strategy_over_cached_folds(
    fold_cache: list[dict],
    strategy_name: str,
    score_fn,
    top_n: int = 50,
    ret_col: str = "fwd_ret_21td",
    periods_per_year: float = 12,
    use_equal_weight: bool = False,
    weight_scheme: str = "equal",
    portfolio_mode: str = "long_only",
) -> tuple[pd.DataFrame, pd.Series, float, float, float]:
    """
    Run a strategy over precomputed fold_cache. portfolio_mode: 'long_only' or 'long_short'.
    Returns (summary_df, all_oos_returns, full_sharpe, max_dd, cagr).
    """
    rows = []
    stitched_oos = []
    for fold_idx, fold_data in enumerate(fold_cache):
        eval_is = fold_data["eval_is"]
        eval_oos = fold_data["eval_oos"]
        if use_equal_weight:
            is_metrics = evaluate_equal_weight(
                eval_is, ret_col=ret_col, periods_per_year=periods_per_year
            )
            oos_metrics = evaluate_equal_weight(
                eval_oos, ret_col=ret_col, periods_per_year=periods_per_year
            )
            turnover = long_turnover = short_turnover = np.nan
        else:
            is_scores, oos_scores = score_fn(eval_is, eval_oos)
            if portfolio_mode == "long_only":
                is_metrics = evaluate_fold(
                    eval_is, is_scores, ret_col=ret_col, top_n=top_n,
                    periods_per_year=periods_per_year, weight_scheme=weight_scheme,
                )
                oos_metrics = evaluate_fold(
                    eval_oos, oos_scores, ret_col=ret_col, top_n=top_n,
                    periods_per_year=periods_per_year, weight_scheme=weight_scheme,
                )
                turnover = compute_turnover(oos_metrics["selected_by_date"])
                long_turnover = short_turnover = np.nan
            elif portfolio_mode == "long_short":
                is_metrics = evaluate_fold_long_short(
                    eval_is, is_scores, ret_col=ret_col, top_n=top_n,
                    periods_per_year=periods_per_year,
                )
                oos_metrics = evaluate_fold_long_short(
                    eval_oos, oos_scores, ret_col=ret_col, top_n=top_n,
                    periods_per_year=periods_per_year,
                )
                turns = compute_turnover_long_short(
                    oos_metrics["long_selected_by_date"],
                    oos_metrics["short_selected_by_date"],
                )
                turnover = turns["turnover"]
                long_turnover = turns["long_turnover"]
                short_turnover = turns["short_turnover"]
            else:
                raise ValueError(f"Unknown portfolio_mode: {portfolio_mode}")
        rows.append({
            "strategy": strategy_name,
            "fold": fold_idx + 1,
            "is_sharpe": is_metrics["sharpe"],
            "oos_sharpe": oos_metrics["sharpe"],
            "is_hit_rate": is_metrics["hit_rate"],
            "oos_hit_rate": oos_metrics["hit_rate"],
            "turnover": turnover,
            "long_turnover": long_turnover,
            "short_turnover": short_turnover,
            "avg_universe_size": oos_metrics.get("avg_universe_size", np.nan),
            "avg_selected_count": oos_metrics.get("avg_selected_count", np.nan),
        })
        stitched_oos.append(oos_metrics["monthly_rets"])
    summary = pd.DataFrame(rows)
    all_oos = pd.concat(stitched_oos).sort_index()
    full_sharpe = (
        (all_oos.mean() / all_oos.std() * (periods_per_year ** 0.5))
        if len(all_oos) and all_oos.std() > 0
        else np.nan
    )
    max_dd, cagr = oos_drawdown_and_cagr(all_oos, periods_per_year=periods_per_year)
    return summary, all_oos, full_sharpe, max_dd, cagr
