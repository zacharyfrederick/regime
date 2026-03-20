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
    oos_drawdown_and_cagr,
    build_fold_cache,
    plot_raw_vs_filtered,
    run_strategy_over_cached_folds,
    make_rank_composite_scorer,
    load_vix_and_apply_regime_filter,
    print_regime_backtest,
)
from walkforward.evaluation import INV_VOL_COL

PARQUET_PATH = str(config.MASTER_FEATURES_PATH)
DATA_START = "2000-01-01"
IS_MONTHS = 84  # ~7 years
OOS_MONTHS = 48  # ~4 years
EMBARGO_MONTHS = 2  # ~2 months
MIN_OOS_MONTHS = 12  # skip fold if OOS would be shorter
TOP_N = 50  # select top N by prediction for portfolio (fixed count)
PERIODS_PER_YEAR = 12  # monthly rebalance
VOL_LOOKBACK_DAYS = 21  # trading days for rolling volatility (inv_vol weighting)
VOL_BUFFER_DAYS = 63  # calendar days before first rebal date so vol is available
FINAL_TEST_MONTHS = 60  # 5-year final test: train before embargo, test on last 5 years
EXCLUDE = [
    "ticker",
    "date",
    "sector",
    "famaindustry",
    "fwd_ret_21td",
    "label",
    "fold",
    "spy_regime_score",
    "label_top",
    "target_xs",
]

# Core features for Tier 1-3 and Tier 4 (same set); extended for optional Tier 4 run
CORE_FEATURES = [
    "ret_12m",
    "ret_6m",
    "ncfo_r2_10y",
    "fcf_cagr_5y",
    "pe_vs_sector",
    "vol_vs_sector",
]
TOP_25_FEATURES = [
    "vix",
    "nfci",
    "yield_curve",
    "hy_spread",
    "vix_change_20d",
    "real_rate",
    "payout_ratio",
    "inst_shrholders",
    "inst_shrvalue",
    "vol_vs_sector",
    "ret_12m",
    "current_ratio",
    "ret_3m",
    "ret_1m",
    "inst_shrunits",
    "ret_6m",
    "dividend_yield",
    "capex_intensity",
    "accrual_ratio",
    "pe_vs_sector",
    "pb_pit",
    "pretax_margin",
    "pcf_pit",
    "ps_pit",
    "ps_vs_sector",
]

sfp_path = config.DATA_DIR / "SFP.parquet"

# %% -------------------------------------------------------------------------
# Extension-only run: P/FCF decile analysis (temporary — remove exit() to run full script)
# -------------------------------------------------------------------------
conn = duckdb.connect()
month_ends = get_rebal_dates(conn, PARQUET_PATH, DATA_START, freq="month")
conn.register("month_ends", pd.DataFrame({"rebal_date": month_ends}))
folds = generate_folds(
    month_ends, IS_MONTHS, OOS_MONTHS, EMBARGO_MONTHS, MIN_OOS_MONTHS
)
load_fn = lambda is_s, is_e, oos_s, oos_e: load_fold_monthly(
    conn, is_s, is_e, oos_s, oos_e, PARQUET_PATH
)
fold_cache = build_fold_cache(
    folds, conn, load_fn, ret_col="fwd_ret_21td", time_feature="month_of_year"
)


def _sep_path():
    p = config.DATA_DIR / "SEP.parquet"
    return p if p.exists() else config.DATA_DIR / "sep.parquet"


rebal_min, rebal_max = min(month_ends), max(month_ends)
date_min = pd.Timestamp(rebal_min) - pd.Timedelta(days=VOL_BUFFER_DAYS)
date_max = pd.Timestamp(rebal_max)
universe_df = conn.execute(
    "SELECT DISTINCT ticker FROM read_parquet(?) WHERE date >= ? AND date <= ?",
    [PARQUET_PATH, date_min.date(), date_max.date()],
).df()
universe_tickers = universe_df["ticker"].tolist()
if not universe_tickers:
    vol_df = pd.DataFrame(columns=["ticker", "date", INV_VOL_COL])
else:
    sep_path = _sep_path()
    if not sep_path.exists():
        vol_df = pd.DataFrame(columns=["ticker", "date", INV_VOL_COL])
    else:
        sep = conn.execute(
            """SELECT ticker, CAST(date AS DATE) AS date, closeadj FROM read_parquet(?)
               WHERE CAST(date AS DATE) >= ? AND CAST(date AS DATE) <= ? ORDER BY ticker, date""",
            [str(sep_path.resolve()), date_min.date(), date_max.date()],
        ).df()
        sep = sep[sep["ticker"].isin(universe_tickers)]
        if sep.empty:
            vol_df = pd.DataFrame(columns=["ticker", "date", INV_VOL_COL])
        else:
            sep["date"] = pd.to_datetime(sep["date"]).dt.normalize()
            sep = sep.sort_values(["ticker", "date"])
            sep["daily_ret"] = sep.groupby("ticker")["closeadj"].pct_change()
            sep["vol_raw"] = sep.groupby("ticker")["daily_ret"].transform(
                lambda x: x.rolling(
                    VOL_LOOKBACK_DAYS, min_periods=VOL_LOOKBACK_DAYS
                ).std()
            )
            sep[INV_VOL_COL] = sep["vol_raw"] * np.sqrt(252)
            vol_df = (
                sep[["ticker", "date", INV_VOL_COL]]
                .dropna(subset=[INV_VOL_COL])
                .drop_duplicates(subset=["ticker", "date"])
                .copy()
            )
for fold_data in fold_cache:
    for key in ("eval_is", "eval_oos"):
        df = fold_data[key]
        if vol_df.empty:
            fold_data[key] = df.assign(**{INV_VOL_COL: np.nan})
        else:
            merged = df[["ticker", "date"]].merge(
                vol_df, on=["ticker", "date"], how="left", suffixes=("", "_vol")
            )
            fold_data[key] = df.assign(**{INV_VOL_COL: merged[INV_VOL_COL].values})
MOM_12_1_COL = "mom_12_1"
for fold_data in fold_cache:
    for key in ("eval_is", "eval_oos"):
        df = fold_data[key]
        fold_data[key] = df.assign(
            **{
                MOM_12_1_COL: (
                    (df["ret_12m"] - df["ret_1m"])
                    if "ret_12m" in df.columns and "ret_1m" in df.columns
                    else np.nan
                )
            }
        )
score_fn = make_rank_composite_scorer(
    [
        ("fcf_r2_10y", 0.5),
        ("fcf_cagr_10y", 0.3),
        ("ncfo_r2_10y", 0.2),
        ("accrual_ratio", -0.5),
        ("earnings_growth_yoy", 0.3),
        ("grossmargin_slope", 0.2),
        ("insider_net_ratio_90d", 0.5),
        ("insider_buy_count_90d", 0.3),
        ("insider_officer_buy_90d", 0.2),
        (MOM_12_1_COL, 0.3),
    ]
)
PFCF_DECILE_COL = "pfcf_decile"
N_QUINTILES = 5  # P/FCF 5y valuation quintiles (1=cheapest, 5=most expensive)


def _pfcf_value_for_decile(df: pd.DataFrame) -> pd.Series:
    """Prefer 5y quantile, then 3y, then raw P/FCF. Lower = cheaper."""
    if "pfcf_5y_quantile" in df.columns:
        return df["pfcf_5y_quantile"]
    if "pfcf_3y_quantile" in df.columns:
        return df["pfcf_3y_quantile"]
    if "pfcf_pit" in df.columns:
        return df["pfcf_pit"]
    return pd.Series(np.nan, index=df.index)


def run_pfcf_decile_analysis(
    fold_cache,
    score_fn,
    top_n=50,
    ret_col="fwd_ret_21td",
    n_deciles=10,
    conn=None,
    sfp_path=None,
    vix_threshold=35,
):
    from walkforward.evaluation import oos_drawdown_and_cagr

    decile_rets = {d: [] for d in range(1, n_deciles + 1)}
    for fold_data in fold_cache:
        eval_oos = fold_data["eval_oos"]
        if eval_oos.empty:
            continue
        is_scores, oos_scores = score_fn(fold_data["eval_is"], eval_oos)
        preds = np.asarray(oos_scores).ravel()
        if len(preds) != len(eval_oos):
            continue
        d = eval_oos.copy()
        d["pred"] = preds
        d["rank"] = d.groupby("date")["pred"].rank(ascending=False)
        d["selected"] = d["rank"] <= top_n
        sel = d.loc[d["selected"]].copy()
        if sel.empty:
            continue
        pfcf = _pfcf_value_for_decile(sel)
        sel = sel.assign(_pfcf_val=pfcf.values)
        sel["_pfcf_rank"] = sel.groupby("date")["_pfcf_val"].rank(
            ascending=True, method="first"
        )
        n_per_date = sel.groupby("date").size()
        sel["_n"] = sel["date"].map(n_per_date)
        decile_raw = (
            np.ceil(sel["_pfcf_rank"] * n_deciles / sel["_n"].clip(lower=1))
        ).clip(upper=n_deciles)
        sel[PFCF_DECILE_COL] = decile_raw.fillna(0).astype(int)
        for dec in range(1, n_deciles + 1):
            sub = sel[sel[PFCF_DECILE_COL] == dec]
            if sub.empty:
                continue
            decile_rets[dec].append(sub.groupby("date")[ret_col].mean())
    # Stitch series per decile and optionally apply regime filter
    decile_series = {}
    for dec in range(1, n_deciles + 1):
        if not decile_rets[dec]:
            decile_series[dec] = pd.Series(dtype=float)
            continue
        s = pd.concat(decile_rets[dec], axis=0).sort_index()
        decile_series[dec] = s[~s.index.duplicated(keep="first")]
    vix_monthly = None
    if conn is not None and sfp_path is not None and sfp_path.exists():
        vix_df = conn.execute(
            "SELECT date, closeadj FROM read_parquet(?) WHERE ticker = ? ORDER BY date",
            [str(sfp_path), "^VIX"],
        ).df()
        if not vix_df.empty:
            vix_df["date"] = pd.to_datetime(vix_df["date"]).dt.normalize()
            vix_df = vix_df.set_index("date").sort_index()
            vix_monthly = vix_df["closeadj"].resample("ME").last().dropna()
    rows = []
    for dec in range(1, n_deciles + 1):
        series = decile_series[dec].copy()
        n_periods = len(series)
        if n_periods == 0:
            rows.append({"decile": dec, "sharpe": np.nan, "max_dd": np.nan, "cagr": np.nan, "n_periods": 0})
            continue
        if vix_monthly is not None and not vix_monthly.empty:
            month_ends = series.index + pd.offsets.MonthEnd(0)
            vix_aligned = vix_monthly.reindex(month_ends)
            vix_aligned.index = series.index
            in_market = (vix_aligned < vix_threshold) & vix_aligned.notna()
            series.loc[~in_market] = 0.0
        if series.std() == 0:
            rows.append({"decile": dec, "sharpe": np.nan, "max_dd": np.nan, "cagr": np.nan, "n_periods": n_periods})
            continue
        sharpe = float(series.mean() / series.std() * (PERIODS_PER_YEAR**0.5))
        max_dd, cagr = oos_drawdown_and_cagr(series, periods_per_year=PERIODS_PER_YEAR)
        rows.append({"decile": dec, "sharpe": sharpe, "max_dd": max_dd, "cagr": cagr, "n_periods": n_periods})
    return pd.DataFrame(rows)


sfp_path = config.DATA_DIR / "SFP.parquet"
pfcf_decile_stats = run_pfcf_decile_analysis(
    fold_cache,
    score_fn,
    top_n=200,
    ret_col="fwd_ret_21td",
    n_deciles=N_QUINTILES,
    conn=conn,
    sfp_path=sfp_path,
    vix_threshold=35,
)
print(
    "P/FCF 5y valuation quintile analysis (signal_inv_vol selections, top 200, equal-weight within quintile; regime filter VIX < 35)"
)
print("-" * 60)
out = pfcf_decile_stats.rename(columns={"decile": "quintile"})
print(out.to_string(index=False))
print("-" * 60)
exit()

# %%
# DuckDB connection and month-ends from actual trading dates (last trading day per month)
conn = duckdb.connect()
month_ends = get_rebal_dates(conn, PARQUET_PATH, DATA_START, freq="month")
conn.register("month_ends", pd.DataFrame({"rebal_date": month_ends}))
print(f"Month-ends: {len(month_ends)} from {month_ends[0]} to {month_ends[-1]}")

# %%
folds = generate_folds(
    month_ends, IS_MONTHS, OOS_MONTHS, EMBARGO_MONTHS, MIN_OOS_MONTHS
)
for idx, (is_start, is_end, oos_start, oos_end) in enumerate(folds):
    print(
        f"Fold {idx+1}: IS {is_start.date()} → {is_end.date()} | OOS {oos_start.date()} → {oos_end.date()}"
    )


# %%
# Load one fold: from walkforward.load_fold_monthly. Used by build_fold_cache and feature list.
def load_fold(is_start, is_end, oos_start, oos_end):
    return load_fold_monthly(conn, is_start, is_end, oos_start, oos_end, PARQUET_PATH)


load_fn = lambda is_s, is_e, oos_s, oos_e: load_fold_monthly(
    conn, is_s, is_e, oos_s, oos_e, PARQUET_PATH
)

fold_cache = build_fold_cache(
    folds, conn, load_fn, ret_col="fwd_ret_21td", time_feature="month_of_year"
)


# %%
# Point-in-time volatility from SEP for inv_vol weighting: universe + date range with buffer
def _sep_path():
    p = config.DATA_DIR / "SEP.parquet"
    return p if p.exists() else config.DATA_DIR / "sep.parquet"


rebal_min, rebal_max = min(month_ends), max(month_ends)
date_min = pd.Timestamp(rebal_min) - pd.Timedelta(days=VOL_BUFFER_DAYS)
date_max = pd.Timestamp(rebal_max)
universe_df = conn.execute(
    "SELECT DISTINCT ticker FROM read_parquet(?) WHERE date >= ? AND date <= ?",
    [PARQUET_PATH, date_min.date(), date_max.date()],
).df()
universe_tickers = universe_df["ticker"].tolist()
if not universe_tickers:
    vol_df = pd.DataFrame(columns=["ticker", "date", INV_VOL_COL])
else:
    sep_path = _sep_path()
    if not sep_path.exists():
        vol_df = pd.DataFrame(columns=["ticker", "date", INV_VOL_COL])
    else:
        sep = conn.execute(
            """
            SELECT ticker, CAST(date AS DATE) AS date, closeadj
            FROM read_parquet(?)
            WHERE CAST(date AS DATE) >= ? AND CAST(date AS DATE) <= ?
            ORDER BY ticker, date
            """,
            [str(sep_path.resolve()), date_min.date(), date_max.date()],
        ).df()
        sep = sep[sep["ticker"].isin(universe_tickers)]
        if sep.empty:
            vol_df = pd.DataFrame(columns=["ticker", "date", INV_VOL_COL])
        else:
            sep["date"] = pd.to_datetime(sep["date"]).dt.normalize()
            sep = sep.sort_values(["ticker", "date"])
            sep["daily_ret"] = sep.groupby("ticker")["closeadj"].pct_change()
            sep["vol_raw"] = sep.groupby("ticker")["daily_ret"].transform(
                lambda x: x.rolling(
                    VOL_LOOKBACK_DAYS, min_periods=VOL_LOOKBACK_DAYS
                ).std()
            )
            sep[INV_VOL_COL] = sep["vol_raw"] * np.sqrt(252)
            vol_df = (
                sep[["ticker", "date", INV_VOL_COL]]
                .dropna(subset=[INV_VOL_COL])
                .drop_duplicates(subset=["ticker", "date"])
                .copy()
            )
# Join vol into each fold's eval_is and eval_oos
for fold_data in fold_cache:
    for key in ("eval_is", "eval_oos"):
        df = fold_data[key]
        if vol_df.empty:
            df[INV_VOL_COL] = np.nan
        else:
            merged = df[["ticker", "date"]].merge(
                vol_df, on=["ticker", "date"], how="left", suffixes=("", "_vol")
            )
            fold_data[key] = df.assign(**{INV_VOL_COL: merged[INV_VOL_COL].values})

# %%
# 12-1 momentum: skip last month to reduce short-term reversal (ret_12m - ret_1m)
MOM_12_1_COL = "mom_12_1"
for fold_data in fold_cache:
    for key in ("eval_is", "eval_oos"):
        df = fold_data[key]
        if "ret_12m" in df.columns and "ret_1m" in df.columns:
            fold_data[key] = df.assign(**{MOM_12_1_COL: df["ret_12m"] - df["ret_1m"]})
        else:
            fold_data[key] = df.assign(**{MOM_12_1_COL: np.nan})

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
# Composite: quality + insider + 12-1 momentum (mom_12_1 = ret_12m - ret_1m), weights in proportion
score_fn = make_rank_composite_scorer(
    [
        ("fcf_r2_10y", 0.5),
        ("fcf_cagr_10y", 0.3),
        ("ncfo_r2_10y", 0.2),
        ("accrual_ratio", -0.5),
        ("earnings_growth_yoy", 0.3),
        ("grossmargin_slope", 0.2),
        ("insider_net_ratio_90d", 0.5),
        ("insider_buy_count_90d", 0.3),
        ("insider_officer_buy_90d", 0.2),
        (MOM_12_1_COL, 0.3),
    ]
)

_, oos_rets_raw, sharpe_raw, dd_raw, cagr_raw = run_strategy_over_cached_folds(
    fold_cache,
    "fcf_quality_momentum_overlay",
    score_fn,
    top_n=50,
    ret_col="fwd_ret_21td",
    periods_per_year=PERIODS_PER_YEAR,
    use_equal_weight=False,
    weight_scheme="equal",
)

VIX_THRESHOLD_SWEEP_STEP = 5  # 2.5 or 5 for threshold sweep (15 -> 60)

sfp_path = config.DATA_DIR / "SFP.parquet"
filtered = load_vix_and_apply_regime_filter(
    oos_rets_raw,
    conn,
    sfp_path,
    vix_threshold=35,
    periods_per_year=PERIODS_PER_YEAR,
)
print_regime_backtest(
    "fcf_quality_momentum_overlay", sharpe_raw, dd_raw, cagr_raw, filtered
)

# SPY forward returns over same OOS dates for chart comparison
rebal_dates = oos_rets_raw.index.unique().sort_values()
rebal_df = pd.DataFrame(
    {
        "rebal_date": pd.to_datetime(rebal_dates).astype("datetime64[ns]"),
    }
)
spy_df = conn.execute(
    "SELECT date, closeadj FROM read_parquet(?) WHERE ticker = ? ORDER BY date",
    [str(sfp_path), config.SPY_TICKER],
).df()
spy_df["date"] = pd.to_datetime(spy_df["date"]).dt.normalize().astype("datetime64[ns]")
spy_aligned = (
    pd.merge_asof(
        rebal_df.sort_values("rebal_date"),
        spy_df.sort_values("date"),
        left_on="rebal_date",
        right_on="date",
        direction="backward",
    )
    .dropna(subset=["closeadj"])
    .drop_duplicates("rebal_date")
)
spy_px = spy_aligned.set_index("rebal_date")["closeadj"].sort_index()
spy_fwd = (spy_px.shift(-1) / spy_px - 1).reindex(rebal_dates).dropna()
oos_rets_spy = spy_fwd.align(oos_rets_raw, join="inner")[0]

plot_raw_vs_filtered(
    oos_rets_raw,
    filtered.oos_rets_filtered,
    "21td quality + momentum: raw vs VIX < 35",
    raw_label="fcf_quality_momentum_overlay (raw)",
    filtered_label="fcf_quality_momentum_overlay (regime filter)",
    oos_rets_spy=oos_rets_spy,
)

# %%
# VIX threshold sweep: 15 -> 60 (step 2.5 or 5), printed to terminal
_thresholds = np.arange(15, 60 + 1e-9, VIX_THRESHOLD_SWEEP_STEP)
print("VIX threshold sweep (equal-weight strategy): in market when VIX < threshold")
print("-" * 72)
print(f"{'VIX <':>6}  {'Sharpe':>8}  {'MaxDD%':>8}  {'CAGR%':>8}  {'% in mkt':>10}")
print("-" * 72)
for thresh in _thresholds:
    res = load_vix_and_apply_regime_filter(
        oos_rets_raw,
        conn,
        sfp_path,
        vix_threshold=float(thresh),
        periods_per_year=PERIODS_PER_YEAR,
    )
    print(
        f"{thresh:>6.1f}  {res.sharpe_filtered:>8.3f}  {res.max_dd_filtered*100:>8.1f}  "
        f"{res.cagr_filtered*100:>8.1f}  {res.pct_in_market:>10.1f}"
    )
print("-" * 72)

# %%
# Inverse-volatility weighted portfolio (same strategy, weight_scheme="inv_vol")
_, oos_rets_inv_vol, sharpe_inv_vol, dd_inv_vol, cagr_inv_vol = (
    run_strategy_over_cached_folds(
        fold_cache,
        "fcf_quality_momentum_overlay_inv_vol",
        score_fn,
        top_n=50,
        ret_col="fwd_ret_21td",
        periods_per_year=PERIODS_PER_YEAR,
        use_equal_weight=False,
        weight_scheme="inv_vol",
    )
)
filtered_inv_vol = load_vix_and_apply_regime_filter(
    oos_rets_inv_vol,
    conn,
    sfp_path,
    vix_threshold=35,
    periods_per_year=PERIODS_PER_YEAR,
)
print_regime_backtest(
    "fcf_quality_momentum_overlay_inv_vol",
    sharpe_inv_vol,
    dd_inv_vol,
    cagr_inv_vol,
    filtered_inv_vol,
)
oos_rets_spy_inv = spy_fwd.align(oos_rets_inv_vol, join="inner")[0]
plot_raw_vs_filtered(
    oos_rets_inv_vol,
    filtered_inv_vol.oos_rets_filtered,
    "21td quality + momentum (inv_vol): raw vs VIX < 35",
    raw_label="fcf_quality_momentum_overlay_inv_vol (raw)",
    filtered_label="fcf_quality_momentum_overlay_inv_vol (regime filter)",
    oos_rets_spy=oos_rets_spy_inv,
)

# %%
# Signal / vol weighted: stronger score gets more weight, risk-scaled by 1/vol (default signal="rank")
# Set signal_inv_vol_signal="exponential" or "zscore" to try other signal components.
_, oos_rets_sig_vol, sharpe_sig_vol, dd_sig_vol, cagr_sig_vol = (
    run_strategy_over_cached_folds(
        fold_cache,
        "fcf_quality_momentum_overlay_signal_inv_vol",
        score_fn,
        top_n=50,
        ret_col="fwd_ret_21td",
        periods_per_year=PERIODS_PER_YEAR,
        use_equal_weight=False,
        weight_scheme="signal_inv_vol",
        signal_inv_vol_signal="rank",
    )
)
filtered_sig_vol = load_vix_and_apply_regime_filter(
    oos_rets_sig_vol,
    conn,
    sfp_path,
    vix_threshold=35,
    periods_per_year=PERIODS_PER_YEAR,
)
print_regime_backtest(
    "fcf_quality_momentum_overlay_signal_inv_vol",
    sharpe_sig_vol,
    dd_sig_vol,
    cagr_sig_vol,
    filtered_sig_vol,
)
oos_rets_spy_sig = spy_fwd.align(oos_rets_sig_vol, join="inner")[0]
plot_raw_vs_filtered(
    oos_rets_sig_vol,
    filtered_sig_vol.oos_rets_filtered,
    "21td quality + momentum (signal/vol, rank): raw vs VIX < 35",
    raw_label="fcf_quality_momentum_overlay_signal_inv_vol (raw)",
    filtered_label="fcf_quality_momentum_overlay_signal_inv_vol (regime filter)",
    oos_rets_spy=oos_rets_spy_sig,
)
