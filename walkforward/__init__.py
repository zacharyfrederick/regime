"""
Walk-forward validation module. Re-expose public API.
"""
from walkforward.daily import (
    DEFAULT_DAILY_FOLD_PERIODS,
    DEFAULT_WEEK_FOLD_PERIODS,
    generate_folds_daily,
    get_trading_rebal_dates,
    periods_per_year_for_freq,
)
from walkforward.folds import generate_folds, get_rebal_dates
from walkforward.load import load_fold, load_fold_monthly
from walkforward.labels import add_target_xs, add_top_bucket_label
from walkforward.encode import encode_fold
from walkforward.evaluation import (
    compute_turnover,
    compute_turnover_long_short,
    evaluate_equal_weight,
    evaluate_fold,
    evaluate_fold_long_short,
    oos_drawdown_and_cagr,
    random_top_n_mean_by_date,
)
from walkforward.strategies import (
    build_composite_score,
    build_rank_composite_score,
    build_fold_cache,
    run_strategy_over_cached_folds,
    make_single_feature_scorer,
    make_composite_scorer,
    make_rank_composite_scorer,
)
from walkforward.regime import (
    RegimeFilterResult,
    apply_vix_regime_filter,
    load_vix_and_apply_regime_filter,
    print_regime_backtest,
    plot_raw_vs_filtered,
)

__all__ = [
    "DEFAULT_DAILY_FOLD_PERIODS",
    "DEFAULT_WEEK_FOLD_PERIODS",
    "generate_folds_daily",
    "get_trading_rebal_dates",
    "periods_per_year_for_freq",
    "get_rebal_dates",
    "generate_folds",
    "load_fold",
    "load_fold_monthly",
    "add_top_bucket_label",
    "add_target_xs",
    "encode_fold",
    "evaluate_fold",
    "evaluate_equal_weight",
    "compute_turnover",
    "evaluate_fold_long_short",
    "compute_turnover_long_short",
    "oos_drawdown_and_cagr",
    "random_top_n_mean_by_date",
    "build_composite_score",
    "build_rank_composite_score",
    "build_fold_cache",
    "run_strategy_over_cached_folds",
    "make_single_feature_scorer",
    "make_composite_scorer",
    "make_rank_composite_scorer",
    "RegimeFilterResult",
    "apply_vix_regime_filter",
    "load_vix_and_apply_regime_filter",
    "print_regime_backtest",
    "plot_raw_vs_filtered",
]
