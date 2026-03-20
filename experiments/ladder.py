# Config: month-end rebalance, 21td forward returns, progressive baseline ladder
import sys
sys.path.append("../")
sys.path.append("/Users/zacharyfrederick/regime/")
import config
import duckdb
import numpy as np
import pandas as pd

from walkforward import (
    get_rebal_dates,
    generate_folds,
    load_fold_monthly,
    evaluate_fold,
    oos_drawdown_and_cagr,
    random_top_n_mean_by_date,
    build_fold_cache,
    run_strategy_over_cached_folds,
    make_single_feature_scorer,
    make_composite_scorer,
    make_rank_composite_scorer,
    load_vix_and_apply_regime_filter,
    print_regime_backtest,
    plot_raw_vs_filtered,
)

PARQUET_PATH = str(config.MASTER_FEATURES_PATH)
DATA_START = "2000-01-01"
IS_MONTHS = 84     # ~7 years
OOS_MONTHS = 48    # ~4 years
EMBARGO_MONTHS = 2  # ~2 months
MIN_OOS_MONTHS = 12  # skip fold if OOS would be shorter
TOP_N = 50  # select top N by prediction for portfolio (fixed count)
PERIODS_PER_YEAR = 12  # monthly rebalance
FINAL_TEST_MONTHS = 60  # 5-year final test: train before embargo, test on last 5 years
EXCLUDE = ["ticker", "date", "sector", "famaindustry", "fwd_ret_21td", "label", "fold", "spy_regime_score", "label_top", "target_xs"]

# Core features for Tier 1-3 and Tier 4 (same set); extended for optional Tier 4 run
CORE_FEATURES = ["ret_12m", "ret_6m", "ncfo_r2_10y", "fcf_cagr_5y", "pe_vs_sector", "vol_vs_sector"]
TOP_25_FEATURES = [
    "vix", "nfci", "yield_curve", "hy_spread", "vix_change_20d", "real_rate", "payout_ratio",
    "inst_shrholders", "inst_shrvalue", "vol_vs_sector", "ret_12m", "current_ratio", "ret_3m",
    "ret_1m", "inst_shrunits", "ret_6m", "dividend_yield", "capex_intensity", "accrual_ratio",
    "pe_vs_sector", "pb_pit", "pretax_margin", "pcf_pit", "ps_pit", "ps_vs_sector",
]

# %%
# DuckDB connection and month-ends from actual trading dates (last trading day per month)
conn = duckdb.connect()
month_ends = get_rebal_dates(conn, PARQUET_PATH, DATA_START, freq="month")
conn.register("month_ends", pd.DataFrame({"rebal_date": month_ends}))
print(f"Month-ends: {len(month_ends)} from {month_ends[0]} to {month_ends[-1]}")

# %%
folds = generate_folds(month_ends, IS_MONTHS, OOS_MONTHS, EMBARGO_MONTHS, MIN_OOS_MONTHS)
for idx, (is_start, is_end, oos_start, oos_end) in enumerate(folds):
    print(f"Fold {idx+1}: IS {is_start.date()} → {is_end.date()} | OOS {oos_start.date()} → {oos_end.date()}")

# %%
# Load one fold: from walkforward.load_fold_monthly. Used by build_fold_cache and feature list.
def load_fold(is_start, is_end, oos_start, oos_end):
    return load_fold_monthly(conn, is_start, is_end, oos_start, oos_end, PARQUET_PATH)

load_fn = lambda is_s, is_e, oos_s, oos_e: load_fold_monthly(conn, is_s, is_e, oos_s, oos_e, PARQUET_PATH)

# %%
# Feature list (common panel): all numeric from load_fold + encoded; build once from first fold
_df0 = load_fold(*folds[0])
base_feature_cols = [c for c in _df0.columns if c not in EXCLUDE]
feature_cols = base_feature_cols + ["sector_enc", "famaindustry_enc", "month_of_year"]
# For models that use only core subset (Tier 3/4 core)
feature_cols_core = [c for c in CORE_FEATURES if c in base_feature_cols] + ["sector_enc", "famaindustry_enc", "month_of_year"]
feature_cols_top25 = [c for c in TOP_25_FEATURES if c in base_feature_cols] + ["sector_enc", "famaindustry_enc", "month_of_year"]
print(f"Full feature count: {len(feature_cols)}; core: {len(feature_cols_core)}; top25: {len(feature_cols_top25)}")


# %%
# Run bucket monotonicity on one fold's OOS (representative)
MOMENTUM_QUALITY_COMPONENTS = [
    ("ret_12m", 0.4, False), ("ret_6m", 0.2, False), ("ncfo_r2_10y", 0.2, False),
    ("ncfo_cagr_5y", 0.1, False), ("pe_vs_sector", 0.1, True),
]

# Run full strategy ladder (Tier 0 -> Tier 4)
ladder_results = []
strategies = [
    ("screen_only", None, True),   # (name, score_fn, use_equal_weight)
    # ("ret_12m", make_single_feature_scorer("ret_12m", ascending=False), False),
    # ("ret_6m", make_single_feature_scorer("ret_6m", ascending=False), False),
    # ("ncfo_r2_10y", make_single_feature_scorer("ncfo_r2_10y", ascending=False), False),
    # ("ncfo_r2_5y", make_single_feature_scorer("ncfo_r2_5y", ascending=False), False),
    # ("fcf_r2_10y", make_single_feature_scorer("fcf_r2_10y", ascending=False), False),
    # ("fcf_r2_5y", make_single_feature_scorer("fcf_r2_5y", ascending=False), False),
    # ("pe_vs_sector", make_single_feature_scorer("pe_vs_sector", ascending=True), False),
    # ("vol_vs_sector", make_single_feature_scorer("vol_vs_sector", ascending=False), False),
    # ("ncfo_cagr_5y", make_single_feature_scorer("ncfo_cagr_5y", ascending=False), False),
    ("fcf_cagr_5y", make_single_feature_scorer("fcf_cagr_5y", ascending=False), False),
    ("fcf_cagr_10y", make_single_feature_scorer("fcf_cagr_10y", ascending=False), False),
    ("ncfo_r2_adjusted_arcsinh", make_single_feature_scorer("ncfo_r2_adjusted_arcsinh", ascending=False), False),
    ("fcf_r2_adjusted_arcsinh", make_single_feature_scorer("fcf_r2_adjusted_arcsinh", ascending=False), False),
    ("momentum_quality_composite", make_composite_scorer(MOMENTUM_QUALITY_COMPONENTS), False),
    ("fcf_quality_rank", make_rank_composite_scorer([
         ("fcf_r2_10y", 0.5),
         ("fcf_cagr_10y", 0.3),
         ("ncfo_r2_10y", 0.2),
     ]), False),
    ("institutional_flow", make_rank_composite_scorer([
    ("inst_shrunits_chg_qoq", 0.4),
    ("inst_shrholders_chg_qoq", 0.3),
    ("inst_put_call_ratio", -0.3),
]), False),

("insider_flow", make_rank_composite_scorer([
    ("insider_net_ratio_90d", 0.5),
    ("insider_buy_count_90d", 0.3),
    ("insider_officer_buy_90d", 0.2),
]), False),

("industry_momentum", make_rank_composite_scorer([
    ("ret_3m_rank_sector", 0.5),
    ("ret_3m_vs_sector", 0.3),
    ("pct_52w_range", 0.2),
]), False),

("earnings_quality", make_rank_composite_scorer([
    ("accrual_ratio", -0.5),
    ("earnings_growth_yoy", 0.3),
    ("grossmargin_slope", 0.2),
]), False),

    ("value_rank_avg", make_rank_composite_scorer([
        ("pe_vs_sector", 0.2),
        ("pb_vs_sector", 0.2),
        ("ps_vs_sector", 0.2),
        ("pcf_vs_sector", 0.2),
        ("evebitda_vs_sector", 0.2),
    ]), False),
]
fold_cache = build_fold_cache(folds, conn, load_fn, ret_col="fwd_ret_21td", time_feature="month_of_year")
for name, score_fn, use_eq in strategies:
    if use_eq:
        summary, all_oos, full_sharpe, max_dd, cagr = run_strategy_over_cached_folds(
            fold_cache, name, None, top_n=TOP_N, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=True
        )
    else:
        summary, all_oos, full_sharpe, max_dd, cagr = run_strategy_over_cached_folds(
            fold_cache, name, score_fn, top_n=TOP_N, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False
        )
    mean_oos_sharpe = summary["oos_sharpe"].mean()
    mean_turnover = summary["turnover"].mean()
    ladder_results.append({
        "strategy": name,
        "mean_oos_sharpe": mean_oos_sharpe,
        "full_oos_sharpe": full_sharpe,
        "oos_hit_rate": summary["oos_hit_rate"].mean(),
        "turnover": mean_turnover,
        "max_drawdown": max_dd,
        "cagr": cagr,
    })

# Market baseline: SPY monthly returns from SFP (same OOS calendar)
sfp_path = config.DATA_DIR / "SFP.parquet"
assert sfp_path.exists(), "SFP.parquet not found"
spy_df = conn.execute("""
    SELECT date, closeadj
    FROM read_parquet(?)
    WHERE ticker = ?
    ORDER BY date
""", [str(sfp_path), config.SPY_TICKER]).df()
assert not spy_df.empty, "SPY not found in SFP"
spy_df["date"] = pd.to_datetime(spy_df["date"]).dt.normalize()
oos_first, oos_last = folds[0][2], folds[-1][3]
oos_dates = sorted([d for d in month_ends if oos_first <= d <= oos_last])
rebal_df = pd.DataFrame({"rebal_date": oos_dates})
rebal_df["rebal_date"] = pd.to_datetime(rebal_df["rebal_date"])
spy_aligned = pd.merge_asof(
    rebal_df.sort_values("rebal_date"),
    spy_df.sort_values("date"),
    left_on="rebal_date", right_on="date", direction="backward"
)
spy_aligned = spy_aligned.dropna(subset=["closeadj"]).drop_duplicates("rebal_date")
spy_px = spy_aligned.set_index("rebal_date")["closeadj"].sort_index()
spy_rets = spy_px.pct_change().dropna()
assert len(spy_rets) > 0 and spy_rets.std() > 0, "SPY returns empty or constant"
# Forward one-period SPY return by rebal date (for correlation/beta vs strategy; strategy uses fwd_ret_21td)
spy_fwd_rets = (spy_px.shift(-1) / spy_px - 1).dropna()
spy_fwd_rets.name = "spy_fwd"
spy_sharpe = spy_rets.mean() / spy_rets.std() * (PERIODS_PER_YEAR ** 0.5)
spy_max_dd, spy_cagr = oos_drawdown_and_cagr(spy_rets, periods_per_year=PERIODS_PER_YEAR)
ladder_results.append({
    "strategy": "market_spy",
    "mean_oos_sharpe": spy_sharpe,
    "full_oos_sharpe": spy_sharpe,
    "oos_hit_rate": (spy_rets > 0).mean(),
    "turnover": np.nan,
    "max_drawdown": spy_max_dd,
    "cagr": spy_cagr,
})

# Universe baseline: random TOP_N names per month, multiple seeds (uses cached folds + vectorized sampling)
RANDOM_SEEDS = 5
random_sharpes, random_cagrs, random_dds = [], [], []
for seed in range(RANDOM_SEEDS):
    rng = np.random.default_rng(seed)
    stitched = []
    for fold_data in fold_cache:
        eval_oos = fold_data["eval_oos"]
        period_rets = random_top_n_mean_by_date(eval_oos, "fwd_ret_21td", TOP_N, rng)
        stitched.append(period_rets)
    all_oos_r = pd.concat(stitched).sort_index()
    if len(all_oos_r) > 0 and all_oos_r.std() > 0:
        random_sharpes.append(all_oos_r.mean() / all_oos_r.std() * (PERIODS_PER_YEAR ** 0.5))
        dd, cagr = oos_drawdown_and_cagr(all_oos_r)
        random_cagrs.append(cagr)
        random_dds.append(dd)
assert random_sharpes, "Random baseline produced no results"
ladder_results.append({
    "strategy": "universe_random",
    "mean_oos_sharpe": np.mean(random_sharpes),
    "full_oos_sharpe": np.mean(random_sharpes),
    "full_oos_sharpe_std": np.std(random_sharpes),
    "oos_hit_rate": np.nan,
    "turnover": np.nan,
    "max_drawdown": np.mean(random_dds),
    "cagr": np.mean(random_cagrs),
})

ladder_df = pd.DataFrame(ladder_results)
print(ladder_df.sort_values("mean_oos_sharpe", ascending=False))