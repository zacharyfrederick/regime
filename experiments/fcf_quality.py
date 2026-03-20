# %%
# Config: month-end rebalance, 21td forward returns, progressive baseline ladder
import sys
sys.path.append("../")
sys.path.append("/Users/zacharyfrederick/regime/")
import config
import numpy as np
import pandas as pd
import duckdb

from walkforward import (
    get_rebal_dates,
    generate_folds,
    load_fold_monthly,
    evaluate_fold,
    oos_drawdown_and_cagr,
    build_fold_cache,
    plot_raw_vs_filtered,
    run_strategy_over_cached_folds,
    make_rank_composite_scorer,
    load_vix_and_apply_regime_filter,
    print_regime_backtest,
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

sfp_path = config.DATA_DIR / "SFP.parquet"


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

fold_cache = build_fold_cache(folds, conn, load_fn, ret_col="fwd_ret_21td", time_feature="month_of_year")

# %%
# Feature list (common panel): all numeric from load_fold + encoded; build once from first fold
_df0 = load_fold(*folds[0])
base_feature_cols = [c for c in _df0.columns if c not in EXCLUDE]
feature_cols = base_feature_cols + ["sector_enc", "famaindustry_enc", "month_of_year"]

# %% [markdown]
# # 21td quality + momentum with regime filter (VIX < 35)
# 
# Same strategy for full OOS; sit in cash when VIX >= 35. VIX from SFP.

# %%
# 21td quality + momentum with regime filter: full OOS, then apply VIX < 35 filter
score_fn = make_rank_composite_scorer([
        ("fcf_r2_10y", 0.5),
         ("fcf_cagr_10y", 0.3),
         ("ncfo_r2_10y", 0.2),
     ])

_, oos_rets_raw, sharpe_raw, dd_raw, cagr_raw = run_strategy_over_cached_folds(
    fold_cache, "fcf_quality_momentum_overlay", score_fn,
    top_n=50, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False
)

sfp_path = config.DATA_DIR / "SFP.parquet"
filtered = load_vix_and_apply_regime_filter(
    oos_rets_raw, conn, sfp_path,
    vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
)
print_regime_backtest("fcf_quality_momentum_overlay", sharpe_raw, dd_raw, cagr_raw, filtered)

# SPY forward returns over same OOS dates for chart comparison
rebal_dates = oos_rets_raw.index.unique().sort_values()
rebal_df = pd.DataFrame({
    "rebal_date": pd.to_datetime(rebal_dates).astype("datetime64[ns]"),
})
spy_df = conn.execute(
    "SELECT date, closeadj FROM read_parquet(?) WHERE ticker = ? ORDER BY date",
    [str(sfp_path), config.SPY_TICKER],
).df()
spy_df["date"] = pd.to_datetime(spy_df["date"]).dt.normalize().astype("datetime64[ns]")
spy_aligned = pd.merge_asof(
    rebal_df.sort_values("rebal_date"),
    spy_df.sort_values("date"),
    left_on="rebal_date", right_on="date", direction="backward",
).dropna(subset=["closeadj"]).drop_duplicates("rebal_date")
spy_px = spy_aligned.set_index("rebal_date")["closeadj"].sort_index()
spy_fwd = (spy_px.shift(-1) / spy_px - 1).reindex(rebal_dates).dropna()
oos_rets_spy = spy_fwd.align(oos_rets_raw, join="inner")[0]

plot_raw_vs_filtered(
    oos_rets_raw, filtered.oos_rets_filtered,
    "21td quality + momentum: raw vs VIX < 35",
    raw_label="fcf_quality_momentum_overlay (raw)",
    filtered_label="fcf_quality_momentum_overlay (regime filter)",
    oos_rets_spy=oos_rets_spy,
)