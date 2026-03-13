# %%
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

# Diversification test: quality (portfolio long/short) vs momentum (industry-feature composite, long-only); then combo metrics
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

# %% [markdown]
# # 21td quality + momentum with regime filter (VIX < 35)
# 
# Same strategy for full OOS; sit in cash when VIX >= 35. VIX from SFP.

exit()
# %%
# 21td quality + momentum with regime filter: full OOS, then apply VIX < 35 filter
score_fn = make_rank_composite_scorer([
         ("fcf_r2_10y", 0.7),
         ("fcf_cagr_10y", 0.3),
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
plot_raw_vs_filtered(
    oos_rets_raw, filtered.oos_rets_filtered,
    "21td quality + momentum: raw vs VIX < 35",
    raw_label="fcf_quality_momentum_overlay (raw)",
    filtered_label="fcf_quality_momentum_overlay (regime filter)",
)

# %%
# Diagnostics: monthly OOS counts (for report); ladder already has per-strategy performance
counts_list = []
for fold_data in fold_cache:
    eval_oos = fold_data["eval_oos"]
    monthly_counts = eval_oos.groupby("date").size()
    counts_list.append(monthly_counts)
counts_all = pd.concat(counts_list)
diagnostics = {
    "monthly_counts_describe": counts_all.describe().to_string(),
    "pct_months_lte_top_n": (counts_all <= TOP_N).mean(),
    "top_n_note": None,
}
if diagnostics["pct_months_lte_top_n"] >= 0.8:
    diagnostics["top_n_note"] = (
        "Most OOS months have universe size <= TOP_N; strategies effectively hold the full universe. "
        "Consider lowering TOP_N (e.g. 20 or 50) for backtest so that selection matters."
    )


# %%
# fcf_r2_adjusted_arcsinh decile analysis — same OOS as ladder (apples to apples Sharpe)
# Uses fold_cache OOS panel; rank by factor within each date, assign deciles, then Sharpe per decile.
FACTOR_COL = "fcf_r2_adjusted_arcsinh"
oos_panel = pd.concat([fold_data["eval_oos"] for fold_data in fold_cache], ignore_index=True)
oos_panel = oos_panel.dropna(subset=[FACTOR_COL, "fwd_ret_21td"])
oos_panel["decile"] = oos_panel.groupby("date")[FACTOR_COL].transform(
    lambda x: pd.qcut(x.rank(method="first"), 10, labels=np.arange(1, 11), duplicates="drop")
)
decile_rets = oos_panel.groupby(["date", "decile"])["fwd_ret_21td"].mean().unstack(level="decile")
sharpes = {}
for d in decile_rets.columns:
    s = decile_rets[d].dropna()
    if len(s) > 0 and s.std() > 0:
        sharpes[d] = s.mean() / s.std() * (PERIODS_PER_YEAR ** 0.5)
    else:
        sharpes[d] = np.nan
mean_rets = decile_rets.mean() * PERIODS_PER_YEAR  # annualized mean return per decile
decile_summary = pd.DataFrame({
    "decile": list(sharpes.keys()),
    "ann_mean_ret": [mean_rets.get(d, np.nan) for d in sharpes.keys()],
    "oos_sharpe": list(sharpes.values()),
})
decile_summary = decile_summary.sort_values("decile")
print(f"OOS period: {decile_rets.index.min()} to {decile_rets.index.max()} ({len(decile_rets)} months) — same as fcf_r2_adjusted_arcsinh ladder.")
print(decile_summary.to_string(index=False))
decile_summary

# %%
# Decile cumulative returns (same OOS as above)
import matplotlib.pyplot as plt
cum = (1 + decile_rets).cumprod()
cum.plot(title="fcf_r2_adjusted_arcsinh decile cumulative returns (OOS)", figsize=(10, 5), legend=True)
plt.ylabel("Cumulative return (1 = 100%)")
plt.xlabel("Date")
plt.gca().legend(title="Decile", bbox_to_anchor=(1.02, 1), loc="upper left")
plt.tight_layout()
plt.show()

# %% [markdown]
# # Find best N

# %%
# Test how top N impacts Sharpe (same OOS, same strategy: fcf_quality_rank)
TOP_N_LIST = [10, 20, 30, 50, 100]
score_fn = make_rank_composite_scorer([("fcf_r2_10y", 0.5), ("fcf_cagr_10y", 0.3), ("ncfo_r2_10y", 0.2)])
sharpe_by_n = []
for n in TOP_N_LIST:
    _, _, full_sharpe, _, _ = run_strategy_over_cached_folds(
        fold_cache, f"fcf_quality_rank_n{n}", score_fn, top_n=n, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False
    )
    sharpe_by_n.append(full_sharpe)
import matplotlib.pyplot as plt
plt.figure(figsize=(7, 4))
plt.plot(TOP_N_LIST, sharpe_by_n, marker="o")
plt.xlabel("Top N")
plt.ylabel("Full OOS Sharpe")
plt.title("fcf_quality_rank: Sharpe vs portfolio size (top N)")
plt.xticks(TOP_N_LIST)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
pd.DataFrame({"top_n": TOP_N_LIST, "full_oos_sharpe": sharpe_by_n})

# %% [markdown]
# # Find best horizon 

# %%
# Horizon test: rebalance every N td, hold N td, evaluate with fwd_ret_{N}td.
# Same fold logic as main ladder: for N=21 use folds from generate_folds; for 63/126/252 use period-based folds with truncated last OOS.
import matplotlib.pyplot as plt

HORIZONS_TD = (5, 10, 21, 63, 126, 252)
PERIODS_CONFIG = {5: (352, 201, 4), 10: (176, 100, 2), 21: (84, 48, 2), 63: (28, 16, 1), 126: (14, 8, 1), 252: (7, 4, 1)}
# Min OOS periods to keep a fold (allow truncated last fold, same idea as MIN_OOS_MONTHS for 21d)
MIN_OOS_PERIODS = {5: 24, 10: 12, 21: 12, 63: 4, 126: 4, 252: 2}

all_dates = conn.execute(
    "SELECT DISTINCT date FROM read_parquet(?) ORDER BY date",
    [PARQUET_PATH]
).df()["date"].tolist()

score_fn = make_rank_composite_scorer([("fcf_r2_10y", 0.5), ("fcf_cagr_10y", 0.3), ("ncfo_r2_10y", 0.2)])
top_n = TOP_N

def load_horizon_fold(conn, path, is_dates, oos_dates):
    combined = list(is_dates) + list(oos_dates)
    conn.register("_rebal_dates", pd.DataFrame({"date": combined}))
    df = conn.execute("""
        SELECT
            f.ticker, f.date, f.fcf_r2_10y, f.fcf_cagr_10y, f.ncfo_r2_10y,
            f.fwd_ret_5td, f.fwd_ret_10td, f.fwd_ret_21td, f.fwd_ret_63td, f.fwd_ret_126td, f.fwd_ret_252td
        FROM read_parquet(?) f
        INNER JOIN _rebal_dates r ON f.date = r.date
        WHERE f.marketcap_daily IS NOT NULL
          AND f.marketcap_daily >= 1000
    """, [path]).df()
    is_set = set(is_dates)
    df["fold"] = np.where(df["date"].isin(is_set), "is", "oos")
    return df

def generate_folds_from_rebal_dates(rebal_dates, is_periods, oos_periods, embargo_periods, min_oos_periods):
    """Same logic as generate_folds but in period indices; allows truncated last OOS."""
    fold_tuples = []
    i = 0
    while True:
        oos_start_idx = i + is_periods + embargo_periods
        if oos_start_idx >= len(rebal_dates):
            break
        is_end_idx = i + is_periods - 1
        oos_end_idx = min(oos_start_idx + oos_periods - 1, len(rebal_dates) - 1)
        n_oos = oos_end_idx - oos_start_idx + 1
        if n_oos >= min_oos_periods:
            fold_tuples.append((i, is_end_idx, oos_start_idx, oos_end_idx))
        if oos_end_idx >= len(rebal_dates) - 1:
            break
        i += oos_periods
    return fold_tuples

def run_horizon_folds(rebal_dates, N, path):
    IS_P, OOS_P, EMB_P = PERIODS_CONFIG[N]
    min_oos = MIN_OOS_PERIODS[N]
    ret_col = f"fwd_ret_{N}td"
    stitched = []

    for (is_lo, is_hi, oos_lo, oos_hi) in generate_folds_from_rebal_dates(rebal_dates, IS_P, OOS_P, EMB_P, min_oos):
        is_dates = rebal_dates[is_lo : is_hi + 1]
        oos_dates = rebal_dates[oos_lo : oos_hi + 1]

        df = load_horizon_fold(conn, path, is_dates, oos_dates)
        df = df.dropna(subset=["fcf_r2_10y", "fcf_cagr_10y", "ncfo_r2_10y", ret_col])

        is_df = df[df["fold"] == "is"].copy()
        oos_df = df[df["fold"] == "oos"].copy()

        if is_df.empty or oos_df.empty:
            continue

        _, oos_scores = score_fn(is_df, oos_df)
        oos_df["pred"] = oos_scores
        oos_df["rank"] = oos_df.groupby("date")["pred"].rank(method="first", ascending=False)
        oos_df["selected"] = oos_df["rank"] <= top_n

        period_rets = (
            oos_df.loc[oos_df["selected"]]
            .groupby("date")[ret_col]
            .mean()
        )

        if not period_rets.empty:
            stitched.append(period_rets)

    if not stitched:
        return pd.Series(dtype=float)

    return pd.concat(stitched).sort_index()

def run_horizon_21d_from_fold_cache(fold_cache, score_fn, top_n):
    """N=21: use same folds, universe, and evaluate_fold as ladder so 21d Sharpe matches exactly."""
    stitched = []
    for fold_data in fold_cache:
        eval_is = fold_data["eval_is"]
        eval_oos = fold_data["eval_oos"]
        _, oos_scores = score_fn(eval_is, eval_oos)
        oos_metrics = evaluate_fold(eval_oos, oos_scores, top_n=top_n, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR)
        period_rets = oos_metrics["monthly_rets"]
        if not period_rets.empty:
            stitched.append(period_rets)
    if not stitched:
        return pd.Series(dtype=float)
    return pd.concat(stitched).sort_index()

results = []

for N in HORIZONS_TD:
    if N == 21:
        oos_rets_list = [run_horizon_21d_from_fold_cache(fold_cache, score_fn, top_n)]
    else:
        offsets = (0, N // 3, 2 * N // 3)
        rebal_schedules = [
            [all_dates[o + i * N] for i in range((len(all_dates) - o) // N)]
            for o in offsets
        ]
        oos_rets_list = [run_horizon_folds(rebal_dates, N, PARQUET_PATH) for rebal_dates in rebal_schedules]

    sharpes_n = []
    cagrs_n = []

    for oos_rets in oos_rets_list:
        if len(oos_rets) == 0 or oos_rets.std() == 0:
            continue
        periods_per_year = 252 / N
        sharpe = oos_rets.mean() / oos_rets.std() * (periods_per_year ** 0.5)
        cum = (1 + oos_rets).cumprod()
        cagr = cum.iloc[-1] ** (periods_per_year / len(oos_rets)) - 1
        sharpes_n.append(sharpe)
        cagrs_n.append(cagr)

    results.append({
        "horizon_td": N,
        "mean_oos_sharpe": np.mean(sharpes_n) if sharpes_n else np.nan,
        "std_oos_sharpe": np.std(sharpes_n) if sharpes_n else np.nan,
        "mean_cagr": np.mean(cagrs_n) if cagrs_n else np.nan,
        "n_offsets": len(sharpes_n),
    })

res_df = pd.DataFrame(results)
print("Horizon test (fcf_quality_rank, top N=%d): rebalance every N td, hold N td, annualize sqrt(252/N)" % top_n)
print(res_df.to_string(index=False))

plt.figure(figsize=(7, 4))
plt.bar(res_df["horizon_td"].astype(str), res_df["mean_oos_sharpe"])
plt.xlabel("Horizon (trading days)")
plt.ylabel("Mean OOS Sharpe")
plt.title("fcf_quality_rank: Sharpe by holding horizon")
plt.tight_layout()
plt.show()
res_df




# %% [markdown]
# ### Current holdings: tickers the model is buying
# 
# OOS selections from the same strategy (fcf_quality_momentum_overlay). Shows ticker, composite score, and sector when available.

# %%
# Reuse same strategy as regime filter: get OOS holdings (ticker, score, sector) per rebalance date
score_fn_holdings = make_rank_composite_scorer([
    ("fcf_r2_10y", 0.40), ("fcf_cagr_10y", 0.25), ("fcf_r2_5y", 0.10), ("fcf_cagr_5y", 0.1),
    ("ret_12m", 0.1), ("ret_6m", 0.1),
])
holdings_rows = []
for fold_data in fold_cache:
    eval_is, eval_oos = fold_data["eval_is"], fold_data["eval_oos"]
    _, oos_scores = score_fn_holdings(eval_is, eval_oos)
    d = eval_oos.copy()
    d["score"] = oos_scores
    d["rank"] = d.groupby("date")["score"].rank(ascending=False)
    d["selected"] = d["rank"] <= TOP_N
    sel = d.loc[d["selected"]].copy()
    cols = ["date", "ticker", "score", "rank"]
    if "sector" in sel.columns:
        cols.append("sector")
    if "famaindustry" in sel.columns:
        cols.append("famaindustry")
    holdings_rows.append(sel[[c for c in cols if c in sel.columns]])
holdings_df = pd.concat(holdings_rows, ignore_index=True)
holdings_df["date"] = pd.to_datetime(holdings_df["date"])
holdings_df = holdings_df.sort_values(["date", "rank"])

# %%
# Sector breakdown (current holdings) — latest rebalance date only
import matplotlib.pyplot as plt
assert "sector" in holdings_df.columns, "holdings_df has no sector column"
last_date = holdings_df["date"].max()
subset = holdings_df[holdings_df["date"] == last_date]
sector_counts = subset["sector"].fillna("Unknown").value_counts()
plt.figure(figsize=(8, 6))
plt.pie(sector_counts, labels=sector_counts.index, autopct="%1.1f%%", startangle=90)
plt.title(f"Sector breakdown (current holdings, {last_date.strftime('%Y-%m-%d')})")
plt.tight_layout()
plt.show()

# %% [markdown]
# # Is it just momentum?

# %%
# 21td quality + momentum with regime filter: full OOS, then apply VIX < 35 filter (top_n=30, quality-heavy)
score_fn = make_rank_composite_scorer([
    ("fcf_r2_10y", 0.60), ("fcf_cagr_10y", 0.10), ("fcf_r2_5y", 0.1), ("fcf_cagr_5y", 0.20),
    ("ret_12m", 0.00), ("ret_6m", 0.00),
])
_, oos_rets_raw, sharpe_raw, dd_raw, cagr_raw = run_strategy_over_cached_folds(
    fold_cache, "fcf_quality_momentum_overlay", score_fn,
    top_n=30, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False
)

filtered = load_vix_and_apply_regime_filter(
    oos_rets_raw, conn, sfp_path,
    vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
)
print_regime_backtest("fcf_quality_momentum_overlay", sharpe_raw, dd_raw, cagr_raw, filtered)
plot_raw_vs_filtered(
    oos_rets_raw, filtered.oos_rets_filtered,
    "21td quality + momentum: raw vs VIX < 35",
    raw_label="fcf_quality_momentum_overlay (raw)",
    filtered_label="fcf_quality_momentum_overlay (regime filter)",
)

# %% [markdown]
# Weight scheme comparison (quality + momentum overlay)
#
# Same strategy as the regime backtest above; applies same VIX < 35 regime filter. Compares equal vs rank vs exponential vs z-score weighting on filtered OOS metrics.

# %%
# Compare weighting schemes on quality + momentum overlay (with regime filter)
score_fn_w = make_rank_composite_scorer([
    ("fcf_r2_10y", 0.40), ("fcf_cagr_10y", 0.25), ("fcf_r2_5y", 0.10), ("fcf_cagr_5y", 0.1),
    ("ret_12m", 0.1), ("ret_6m", 0.1),
])
sfp_path = config.DATA_DIR / "SFP.parquet"
weight_scheme_results = []
for weight_scheme in ["equal", "rank", "exponential", "zscore"]:
    _, oos_rets, _, _, _ = run_strategy_over_cached_folds(
        fold_cache, "fcf_quality_momentum_overlay", score_fn_w,
        top_n=50, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR,
        use_equal_weight=False, weight_scheme=weight_scheme,
    )
    filtered = load_vix_and_apply_regime_filter(
        oos_rets, conn, sfp_path,
        vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
    )
    ann_vol = filtered.oos_rets_filtered.std() * (PERIODS_PER_YEAR ** 0.5) if len(filtered.oos_rets_filtered) and filtered.oos_rets_filtered.std() > 0 else np.nan
    weight_scheme_results.append({
        "weight_scheme": weight_scheme,
        "sharpe": filtered.sharpe_filtered,
        "ann_vol": ann_vol,
        "max_dd": filtered.max_dd_filtered,
        "cagr": filtered.cagr_filtered,
    })
weight_scheme_df = pd.DataFrame(weight_scheme_results)
print(weight_scheme_df.to_string(index=False))


# %% [markdown]
# Long/short vs market (same strategy, VIX < 35)
#
# Long top-N portfolio, short SPY; regime filter applied to the long/short series.
# Two variants: (1) long strategy minus 1× SPY (not beta-neutral); (2) beta-neutral = long - β× SPY.
# Caveat: CAGR on long/short is less economically intuitive (capital usage / gross exposure convention);
# Sharpe and drawdown are the more important metrics here.

# %%
# Strategy returns are forward-looking (fwd_ret_21td at date t = return from t to next period).
# SPY must use forward returns by rebal date for same-period correlation/beta.
score_fn = make_rank_composite_scorer([
    ("fcf_r2_10y", 0.40), ("fcf_cagr_10y", 0.25), ("fcf_r2_5y", 0.10), ("fcf_cagr_5y", 0.1),
    ("ret_12m", 0.1), ("ret_6m", 0.1),
])
_, oos_rets_raw, sharpe_raw, dd_raw, cagr_raw = run_strategy_over_cached_folds(
    fold_cache, "fcf_quality_momentum_overlay", score_fn,
    top_n=50, ret_col="fwd_ret_21td", periods_per_year=PERIODS_PER_YEAR, use_equal_weight=False
)

# Diagnostic: backward- vs forward-labeled SPY (same-date alignment)
spy_back = spy_rets.rename("spy_back")
spy_fwd = spy_fwd_rets.copy()
aligned_back = pd.concat([oos_rets_raw.rename("strategy"), spy_back], axis=1).dropna()
aligned_fwd = pd.concat([oos_rets_raw.rename("strategy"), spy_fwd], axis=1).dropna()

def _beta_of(a, x, y):
    return a[x].cov(a[y]) / a[y].var()

print("Backward-labeled SPY (strategy[t] = fwd from t, spy[t] = return into t — mismatch):")
print("  corr:", aligned_back["strategy"].corr(aligned_back["spy_back"]))
print("  beta:", _beta_of(aligned_back, "strategy", "spy_back"))
print("\nForward-labeled SPY (same-period alignment):")
print("  corr:", aligned_fwd["strategy"].corr(aligned_fwd["spy_fwd"]))
print("  beta:", _beta_of(aligned_fwd, "strategy", "spy_fwd"))

# Sanity: correlation vs universe average forward return
universe_fwd = pd.concat(
    [fd["eval_oos"].groupby("date")["fwd_ret_21td"].mean() for fd in fold_cache]
).sort_index()
aligned_uni = pd.concat(
    [oos_rets_raw.rename("strategy"), universe_fwd.rename("universe_fwd")],
    axis=1,
).dropna()
print("\nVs universe forward return:")
print("  corr:", aligned_uni["strategy"].corr(aligned_uni["universe_fwd"]))
print("  beta:", aligned_uni["strategy"].cov(aligned_uni["universe_fwd"]) / aligned_uni["universe_fwd"].var())

# Use forward-aligned series for long/short and regression
aligned = aligned_fwd
beta = _beta_of(aligned, "strategy", "spy_fwd")
corr = aligned["strategy"].corr(aligned["spy_fwd"])
print("\nForward-aligned beta: %.3f" % beta)
print("Forward-aligned corr: %.3f" % corr)

long_short_1x = aligned["strategy"] - aligned["spy_fwd"]
long_short_beta = aligned["strategy"] - beta * aligned["spy_fwd"]

def _regime_filtered_metrics(series, conn, sfp_path):
    f = load_vix_and_apply_regime_filter(
        series, conn, sfp_path,
        vix_threshold=35, periods_per_year=PERIODS_PER_YEAR,
    )
    rets_f = f.oos_rets_filtered
    ann_vol = (
        rets_f.std() * (PERIODS_PER_YEAR ** 0.5)
        if len(rets_f) > 0 and rets_f.std() > 0
        else np.nan
    )
    return {"sharpe": f.sharpe_filtered, "ann_vol": ann_vol, "max_dd": f.max_dd_filtered, "cagr": f.cagr_filtered}

ls_results = []
for label, rets in [("long_minus_1x_spy", long_short_1x), ("beta_neutral", long_short_beta)]:
    m = _regime_filtered_metrics(rets, conn, sfp_path)
    ls_results.append({"variant": label, **m})
print("Long/short vs market (regime filtered):")
print(pd.DataFrame(ls_results).to_string(index=False))

import statsmodels.api as sm

X = sm.add_constant(aligned["spy_fwd"])
model = sm.OLS(aligned["strategy"], X).fit()

print("regression summary (strategy on forward SPY):")
print(model.summary())

# %% [markdown]
# OOS cross-sectional IC (information coefficient) by month
#
# Corr(score, fwd_ret) per rebalance date. Spearman = rank IC (natural for rank composite). Mean IC, IR, hit rate, and rolling IC plot.

# %%
def compute_ic_by_date(
    df: pd.DataFrame,
    score_col: str,
    ret_col: str,
    method: str = "spearman",
    min_names: int = 20,
) -> pd.Series:
    """Cross-sectional IC per date. method: 'spearman' or 'pearson'."""
    out = {}
    for dt, g in df.groupby("date"):
        g = g[[score_col, ret_col]].dropna()
        if len(g) < min_names:
            continue
        if g[score_col].nunique() < 2 or g[ret_col].nunique() < 2:
            continue
        out[dt] = g[score_col].corr(g[ret_col], method=method)
    return pd.Series(out).sort_index()


def compute_oos_ic_over_folds(
    fold_cache,
    score_fn,
    ret_col: str = "fwd_ret_21td",
    method: str = "spearman",
    min_names: int = 20,
) -> pd.Series:
    """Stitched OOS IC series across folds."""
    ic_list = []
    for fold_data in fold_cache:
        eval_is = fold_data["eval_is"]
        eval_oos = fold_data["eval_oos"].copy()
        _, oos_scores = score_fn(eval_is, eval_oos)
        eval_oos["score"] = oos_scores
        ic_fold = compute_ic_by_date(
            eval_oos, score_col="score", ret_col=ret_col,
            method=method, min_names=min_names,
        )
        ic_list.append(ic_fold)
    if not ic_list:
        return pd.Series(dtype=float)
    return pd.concat(ic_list).sort_index()


score_fn_ic = make_rank_composite_scorer([
    ("fcf_r2_10y", 0.40), ("fcf_cagr_10y", 0.25), ("fcf_r2_5y", 0.10), ("fcf_cagr_5y", 0.10),
    ("ret_12m", 0.10), ("ret_6m", 0.10),
])
ic_s = compute_oos_ic_over_folds(
    fold_cache, score_fn_ic, ret_col="fwd_ret_21td", method="spearman", min_names=20,
)
print("Mean IC: %.4f" % ic_s.mean())
print("IC std: %.4f" % ic_s.std())
print("IC IR: %.4f" % (ic_s.mean() / ic_s.std() if ic_s.std() > 0 else np.nan))
print("Positive IC hit rate: %.2f%%" % (100 * (ic_s > 0).mean()))
print("Num months:", len(ic_s))

ic_summary = {
    "mean_ic": ic_s.mean(),
    "median_ic": ic_s.median(),
    "std_ic": ic_s.std(),
    "ic_ir": ic_s.mean() / ic_s.std() if ic_s.std() > 0 else np.nan,
    "t_stat": ic_s.mean() / (ic_s.std(ddof=1) / np.sqrt(len(ic_s))) if len(ic_s) > 1 and ic_s.std(ddof=1) > 0 else np.nan,
    "hit_rate": (ic_s > 0).mean(),
    "n_months": len(ic_s),
}
print(pd.DataFrame([ic_summary]).to_string(index=False))

import matplotlib.pyplot as plt
plt.figure(figsize=(10, 4))
ic_s.plot()
plt.axhline(0, color="black", linewidth=1)
plt.title("Monthly OOS IC")
plt.ylabel("IC")
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 4))
ic_s.rolling(12).mean().plot()
plt.axhline(0, color="black", linewidth=1)
plt.title("12-month rolling mean IC")
plt.ylabel("Rolling IC")
plt.tight_layout()
plt.show()

# %%
# Write full run output to a single file (so you can reference the run outside the notebook)
from datetime import datetime

report_path = config.OUTPUTS_DIR / "walk_forward_monthly_run.md"
report_path = report_path.resolve()
report_path.parent.mkdir(parents=True, exist_ok=True)

lines = [
    "# Walk-forward validation (monthly) — run output",
    "",
    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "",
    "## Config",
    f"- Data: {PARQUET_PATH}",
    f"- DATA_START: {DATA_START}",
    f"- IS_MONTHS: {IS_MONTHS}, OOS_MONTHS: {OOS_MONTHS}, EMBARGO_MONTHS: {EMBARGO_MONTHS}",
    f"- TOP_N: {TOP_N}, PERIODS_PER_YEAR: {PERIODS_PER_YEAR}",
    f"- FINAL_TEST_MONTHS: {FINAL_TEST_MONTHS}",
    f"- Month-ends: {len(month_ends)} ({month_ends[0].date() if len(month_ends) else 'N/A'} to {month_ends[-1].date() if len(month_ends) else 'N/A'})",
    "",
    "## Folds",
]
for i, (is_start, is_end, oos_start, oos_end) in enumerate(folds, 1):
    lines.append(f"- Fold {i}: IS {is_start.date()} → {is_end.date()}  |  OOS {oos_start.date()} → {oos_end.date()}")

lines.extend([
    "",
    "## Ladder results (strategies by full OOS Sharpe)",
    "",
    ladder_df.sort_values("full_oos_sharpe", ascending=False).to_markdown(index=False) if hasattr(ladder_df, "to_markdown") else ladder_df.sort_values("full_oos_sharpe", ascending=False).to_string(),
    "",
])

# Diagnostics section (from diagnostics cell; run diagnostics cell first to populate)
if "diagnostics" in dir():
    lines.extend(["", "## Diagnostics", ""])
    lines.append("### Monthly OOS candidate counts")
    lines.append(diagnostics["monthly_counts_describe"])
    lines.append("")
    lines.append(f"Fraction of months with universe size <= TOP_N ({TOP_N}): {diagnostics['pct_months_lte_top_n']:.2%}")
    if diagnostics.get("top_n_note"):
        lines.append("")
        lines.append(diagnostics["top_n_note"])
    lines.append("")

if "full_sharpe_f" in dir() and full_sharpe_f is not None:
    lines.extend([
        "## Final test (ridge_core, last 5y OOS)",
        f"- IS: {is_start.date()} → {is_end.date()}",
        f"- OOS: {oos_start.date()} → {oos_end.date()} ({FINAL_TEST_MONTHS} months)",
        f"- OOS Sharpe: {full_sharpe_f:.3f}",
        f"- CAGR: {cagr_f:.2%}",
        f"- Max drawdown: {max_dd_f:.2%}",
        "",
    ])

lines.append("---")
lines.append("")
report_content = "\n".join(lines)
report_path.write_text(report_content, encoding="utf-8")
assert report_path.exists(), "Report file was not written"
print(f"Run report written to file: {report_path}")
print(f"  Size: {report_path.stat().st_size} bytes")


