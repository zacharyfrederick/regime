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

#Diversification test: quality (portfolio long/short) vs momentum (industry-feature composite, long-only); then combo metrics
# Quality sleeve = long top N by FCF quality, short bottom N (no SPY hedge). Momentum = rank composite of sector-relative features.
quality_scorer = make_rank_composite_scorer([
    ("fcf_r2_10y", 0.47),
    ("fcf_cagr_10y", 0.29),
    ("fcf_r2_5y", 0.12),
    ("fcf_cagr_5y", 0.12),
])
earnings_quality_scorer = make_rank_composite_scorer([
    ("accrual_ratio", -0.5),
    ("earnings_growth_yoy", 0.3),
    ("grossmargin_slope", 0.2),
])

insider_scorer = make_rank_composite_scorer([
    ("insider_net_ratio_90d", 0.5),
    ("insider_buy_count_90d", 0.3),
    ("insider_officer_buy_90d", 0.2),
])

_, quality_rets, quality_sharpe_raw, quality_dd_raw, quality_cagr_raw = run_strategy_over_cached_folds(
    fold_cache, "quality_sleeve_ls", quality_scorer,
    top_n=TOP_N, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False,
    portfolio_mode="long_short",
)
_, earnings_rets, earnings_sharpe_raw, earnings_dd_raw, earnings_cagr_raw = run_strategy_over_cached_folds(
    fold_cache, "earnings_quality_ls", earnings_quality_scorer,
    top_n=TOP_N, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False,
    portfolio_mode="long_short",
)
_, insider_rets, insider_sharpe_raw, insider_dd_raw, insider_cagr_raw = run_strategy_over_cached_folds(
    fold_cache, "insider_ls", insider_scorer,
    top_n=TOP_N, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False,
)

# VIX < 35 regime filter (same as elsewhere): zero returns when VIX >= 35
filtered_quality = load_vix_and_apply_regime_filter(
    quality_rets, conn, sfp_path,
    vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
)
filtered_earnings = load_vix_and_apply_regime_filter(
    earnings_rets, conn, sfp_path,
    vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
)
filtered_insider = load_vix_and_apply_regime_filter(
    insider_rets, conn, sfp_path,
    vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
)
print_regime_backtest("quality_sleeve_ls", quality_sharpe_raw, quality_dd_raw, quality_cagr_raw, filtered_quality)
print_regime_backtest("earnings_quality_ls", earnings_sharpe_raw, earnings_dd_raw, earnings_cagr_raw, filtered_earnings)
print_regime_backtest("insider_ls", insider_sharpe_raw, insider_dd_raw, insider_cagr_raw, filtered_insider)

quality_rets_f = filtered_quality.oos_rets_filtered.rename("quality")
earnings_rets_f = filtered_earnings.oos_rets_filtered.rename("earnings_quality")
insider_rets_f = filtered_insider.oos_rets_filtered.rename("insider")

aligned = pd.concat(
    [quality_rets_f, earnings_rets_f, insider_rets_f],
    axis=1,
).dropna()
print("\nDiversification test (VIX < 35): quality, earnings quality, insider (all portfolio long/short)")
print("Typical in equities: corr ≈ 0.2–0.4; if so, combining improves Sharpe.")
print(aligned.corr())

# Annualized volatility per sleeve (filtered)
def _ann_vol(s):
    return s.std() * (PERIODS_PER_YEAR ** 0.5) if len(s) and s.std() > 0 else np.nan
quality_ann_vol = _ann_vol(aligned["quality"])
earnings_ann_vol = _ann_vol(aligned["earnings_quality"])
insider_ann_vol = _ann_vol(aligned["insider"])
print("\nSleeves (VIX < 35) — annualized vol:")
print(f"  quality:         {quality_ann_vol:.2%}")
print(f"  earnings_qual:  {earnings_ann_vol:.2%}")
print(f"  insider:        {insider_ann_vol:.2%}")

# Combined returns 1/3 each (filtered); Sharpe, drawdown, CAGR, vol
combo_rets = (
    (.45) * aligned["quality"]
    + (.35) * aligned["earnings_quality"]
    + (.2) * aligned["insider"]
).rename("combo")
combo_sharpe = combo_rets.mean() / combo_rets.std() * (PERIODS_PER_YEAR ** 0.5) if combo_rets.std() > 0 else np.nan
combo_max_dd, combo_cagr = oos_drawdown_and_cagr(combo_rets, periods_per_year=PERIODS_PER_YEAR)
combo_ann_vol = _ann_vol(combo_rets)
print("\nCombined (1/3 quality + 1/3 earnings quality + 1/3 insider, VIX < 35):")
print(f"  Sharpe:   {combo_sharpe:.3f}")
print(f"  Ann Vol:  {combo_ann_vol:.2%}")
print(f"  Max DD:   {combo_max_dd:.2%}")
print(f"  CAGR:     {combo_cagr:.2%}")


print("Combo hit rate (VIX < 35):", (combo_rets > 0).mean())

# Plot combo vs SPY (cumulative, same OOS calendar; strategy uses forward returns so align SPY forward)
oos_first, oos_last = folds[0][2], folds[-1][3]
oos_dates = sorted([d for d in month_ends if oos_first <= d <= oos_last])
rebal_df = pd.DataFrame({"rebal_date": oos_dates})
rebal_df["rebal_date"] = pd.to_datetime(rebal_df["rebal_date"])
spy_df = conn.execute(
    "SELECT date, closeadj FROM read_parquet(?) WHERE ticker = ? ORDER BY date",
    [str(sfp_path), config.SPY_TICKER],
).df()
spy_df["date"] = pd.to_datetime(spy_df["date"]).dt.normalize()
spy_aligned = pd.merge_asof(
    rebal_df.sort_values("rebal_date"),
    spy_df.sort_values("date"),
    left_on="rebal_date", right_on="date", direction="backward",
)
spy_aligned = spy_aligned.dropna(subset=["closeadj"]).drop_duplicates("rebal_date")
spy_px = spy_aligned.set_index("rebal_date")["closeadj"].sort_index()
spy_fwd = (spy_px.shift(-1) / spy_px - 1).dropna()
spy_fwd.name = "spy"
combo_vs_spy = pd.concat([combo_rets.rename("combo"), spy_fwd], axis=1).dropna()
cum_combo = (1 + combo_vs_spy["combo"]).cumprod()
cum_spy = (1 + combo_vs_spy["spy"]).cumprod()

import matplotlib.pyplot as plt
plt.figure(figsize=(10, 5))
plt.plot(cum_combo.index, cum_combo.values, label="Combo (VIX < 35)")
plt.plot(cum_spy.index, cum_spy.values, label="SPY")
plt.ylabel("Cumulative return (1 = 100%)")
plt.xlabel("Date")
plt.title("Combo vs SPY — cumulative (OOS, forward returns)")
plt.legend()
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}"))
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
