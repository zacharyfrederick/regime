#!/usr/bin/env python3
"""
Quality + momentum: walk-forward signal validation (daily rebalancing).

Steps:
1. Rank by fcf_r2_10y (quality).
2. Keep top decile by quality.
3. Within that group, rank by ret_1m (prior month return).
4. Long top decile of prior month returns (winners). Long-only.

Label: 10d forward return (fwd_ret_10td). Each observation = 10d forward return
of the long portfolio. Observations are overlapping (one per rebalance date);
Sharpe and max drawdown are not valid for this series.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
import duckdb
import numpy as np
import pandas as pd

from walkforward import generate_folds

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
PARQUET_PATH = str(config.MASTER_FEATURES_PATH)
DATA_START = "2000-01-01"

IS_DAYS = 252 * 7       # ~7 years in-sample
OOS_DAYS = 252 * 4      # ~4 years out-of-sample
EMBARGO_DAYS = 10       # ~2 weeks between IS end and OOS start
MIN_OOS_DAYS = 252      # skip fold if OOS would be shorter than 1 year

PERIODS_PER_YEAR = 252  # for SPY (daily)

UNIVERSE_WHERE = "AND (f.marketcap_daily IS NOT NULL AND f.marketcap_daily >= 1000)"

# Quality + momentum: top decile by fcf_r2_10y, then long top decile by prior month return
QUALITY_TOP_PCT = 0.10  # top 10% (top decile) by fcf_r2_10y

# -----------------------------------------------------------------------------
# Daily rebalance dates (all trading days in parquet from DATA_START)
# -----------------------------------------------------------------------------
conn = duckdb.connect()
day_ends = conn.execute(
    """
    SELECT DISTINCT date AS rebal_date
    FROM read_parquet(?)
    WHERE date >= ?
    ORDER BY rebal_date
    """,
    [PARQUET_PATH, DATA_START],
).df()["rebal_date"].tolist()
conn.register("day_ends", pd.DataFrame({"rebal_date": day_ends}))
print(f"Rebal dates (daily): {len(day_ends)} from {day_ends[0]} to {day_ends[-1]}")

# -----------------------------------------------------------------------------
# Folds
# -----------------------------------------------------------------------------
folds = generate_folds(day_ends, IS_DAYS, OOS_DAYS, EMBARGO_DAYS, MIN_OOS_DAYS)
for idx, (is_start, is_end, oos_start, oos_end) in enumerate(folds):
    print(f"Fold {idx+1}: IS {is_start.date()} → {is_end.date()} | OOS {oos_start.date()} → {oos_end.date()}")

# -----------------------------------------------------------------------------
# Minimal fold loader: ticker, date, ret_1m, fwd_ret_10td, fcf_r2_10y, fold
# -----------------------------------------------------------------------------
def load_fold_minimal(is_start, is_end, oos_start, oos_end):
    return conn.execute(
        """
        WITH rebal_dates AS (
            SELECT rebal_date FROM day_ends
            WHERE (rebal_date >= ? AND rebal_date <= ?)
               OR (rebal_date >= ? AND rebal_date <= ?)
        ),
        universe AS (
            SELECT f.ticker, f.date, f.ret_1m, f.fwd_ret_10td, f.fcf_r2_10y, f.marketcap_daily
            FROM read_parquet(?) f
            INNER JOIN rebal_dates r ON f.date = r.rebal_date
            WHERE 1=1
            """ + UNIVERSE_WHERE + """
        )
        SELECT ticker, date, ret_1m, fwd_ret_10td, fcf_r2_10y,
               CASE WHEN date <= ? THEN 'is' ELSE 'oos' END AS fold
        FROM universe
        """,
        [is_start, is_end, oos_start, oos_end, PARQUET_PATH, is_end],
    ).df()


# -----------------------------------------------------------------------------
# Strategy: top decile quality (fcf_r2_10y) then long top decile by prior month return
# -----------------------------------------------------------------------------
LABEL_COL = "fwd_ret_10td"
spread_list = []
long_ret_list = []
short_ret_list = []

for is_start, is_end, oos_start, oos_end in folds:
    df = load_fold_minimal(is_start, is_end, oos_start, oos_end)
    oos = df[df["fold"] == "oos"].copy()
    oos = oos.dropna(subset=["ret_1m", LABEL_COL, "fcf_r2_10y"])
    if oos.empty:
        continue

    # Step 1: rank by fcf_r2_10y (high = good quality)
    oos["quality_rank_pct"] = oos.groupby("date")["fcf_r2_10y"].rank(pct=True)
    # Step 2: keep top decile by quality
    quality_cutoff = 1.0 - QUALITY_TOP_PCT  # top 10% => rank_pct >= 0.9
    oos_quality = oos.loc[oos["quality_rank_pct"] >= quality_cutoff].copy()
    if oos_quality.empty:
        continue
    # Step 3: within quality group, rank by prior month return (ret_1m; high = good)
    oos_quality["momentum_rank_pct"] = oos_quality.groupby("date")["ret_1m"].rank(pct=True)
    # Step 4: long top decile of prior month returns (winners)
    oos_quality["is_long"] = oos_quality["momentum_rank_pct"] > 0.9

    long_rets = oos_quality.loc[oos_quality["is_long"]].groupby("date")[LABEL_COL].mean()
    # Long-only: strategy return = long portfolio 10d return
    spread = long_rets.dropna()
    spread_list.append(spread)
    long_ret_list.append(long_rets.dropna())

def dedupe_last(s: pd.Series) -> pd.Series:
    if len(s) == 0:
        return s
    s = s.sort_index()
    return s[~s.index.duplicated(keep="last")]


if not spread_list:
    all_spread = pd.Series(dtype=float)
    all_long_ret = pd.Series(dtype=float)
    all_short_ret = pd.Series(dtype=float)
else:
    all_spread = dedupe_last(pd.concat(spread_list)) if spread_list else pd.Series(dtype=float)
    all_long_ret = dedupe_last(pd.concat(long_ret_list)) if long_ret_list else pd.Series(dtype=float)
    all_short_ret = dedupe_last(pd.concat(short_ret_list)) if short_ret_list else pd.Series(dtype=float)

# -----------------------------------------------------------------------------
# Signal validation (overlapping 10-day forward returns — one per rebalance date)
# -----------------------------------------------------------------------------
PERIODS_PER_YEAR_10D = 252 / 10  # only for rough annualized mean


def newey_west_tstat(x: pd.Series, lags: int) -> float:
    x = pd.Series(x).dropna().astype(float)
    n = len(x)
    if n < 2:
        return np.nan
    mu = x.mean()
    eps = x - mu
    gamma0 = float((eps @ eps) / n)
    if gamma0 <= 0:
        return np.nan
    var = gamma0
    for k in range(1, min(lags, n - 1) + 1):
        gamma_k = float((eps.iloc[k:].to_numpy() @ eps.iloc[:-k].to_numpy()) / n)
        weight = 1.0 - k / (lags + 1)
        var += 2.0 * weight * gamma_k
    if var <= 0:
        return np.nan
    se = np.sqrt(var / n)
    return float(mu / se) if se > 0 else np.nan


if len(all_spread) == 0 or all_spread.std(ddof=1) == 0:
    mean_10d_ret = np.nan
    t_stat = np.nan
    hit_rate = np.nan
    annualized_mean_approx = np.nan
else:
    mean_10d_ret = float(all_spread.mean())
    t_stat = newey_west_tstat(all_spread, lags=9)
    hit_rate = float((all_spread > 0).mean())
    annualized_mean_approx = float((1 + mean_10d_ret) ** PERIODS_PER_YEAR_10D - 1)

mean_long_10d = float(all_long_ret.mean()) if len(all_long_ret) else np.nan
mean_short_10d = float(all_short_ret.mean()) if len(all_short_ret) else np.nan

print("\n--- Quality + momentum (top bucket fcf_r2_10y, long top decile prior month return) ---")
print("  (Long-only; label = fwd_ret_10td; overlapping 10d forward returns — Sharpe/MaxDD not valid.)")
print(f"  Mean 10d return (long):      {mean_10d_ret:.6f}")
print(f"  Newey-West t-stat:           {t_stat:.3f}" if not np.isnan(t_stat) else "  Newey-West t-stat:           n/a")
print(f"  Hit rate (return > 0):       {hit_rate:.2%}" if not np.isnan(hit_rate) else "  Hit rate (return > 0):       n/a")
print(
    f"  Annualized mean (approx):    {annualized_mean_approx:.2%}"
    if not np.isnan(annualized_mean_approx)
    else "  Annualized mean (approx):    n/a"
)
print(f"  Mean 10d (long portfolio):   {mean_long_10d:.6f}" if not np.isnan(mean_long_10d) else "  Mean 10d (long portfolio):   n/a")
if len(all_short_ret):
    print(f"  Mean 10d (short portfolio):  {mean_short_10d:.6f}" if not np.isnan(mean_short_10d) else "  Mean 10d (short portfolio):  n/a")
print(f"  N observations:              {len(all_spread)}")

# -----------------------------------------------------------------------------
# Optional: SPY (daily returns, same OOS window — not comparable to 10d strategy return)
# -----------------------------------------------------------------------------
sfp_path = config.DATA_DIR / "SFP.parquet"
spy_sharpe = spy_max_dd = None
if sfp_path.exists() and len(folds) > 0:
    spy_df = conn.execute(
        "SELECT date, closeadj FROM read_parquet(?) WHERE ticker = ? ORDER BY date",
        [str(sfp_path), config.SPY_TICKER],
    ).df()
    if not spy_df.empty:
        spy_df["date"] = pd.to_datetime(spy_df["date"]).dt.normalize()
        oos_first, oos_last = folds[0][2], folds[-1][3]
        oos_dates = sorted([d for d in day_ends if oos_first <= d <= oos_last])
        rebal_df = pd.DataFrame({"rebal_date": pd.to_datetime(oos_dates)})
        spy_aligned = pd.merge_asof(
            rebal_df.sort_values("rebal_date"),
            spy_df.sort_values("date"),
            left_on="rebal_date", right_on="date", direction="backward",
        ).dropna(subset=["closeadj"]).drop_duplicates("rebal_date")
        spy_px = spy_aligned.set_index("rebal_date")["closeadj"].sort_index()
        spy_rets = spy_px.pct_change().dropna()
        if len(spy_rets) > 0 and spy_rets.std() > 0:
            spy_sharpe = spy_rets.mean() / spy_rets.std() * (PERIODS_PER_YEAR ** 0.5)
            cum = (1 + spy_rets).cumprod()
            run_max = cum.cummax()
            spy_max_dd = float(((cum - run_max) / run_max).min())
            print(f"\n  SPY (daily, same OOS; not comparable to 10d strategy return): Sharpe {spy_sharpe:.3f}, MaxDD {spy_max_dd:.2%}")
        else:
            spy_sharpe = spy_max_dd = None
    else:
        spy_sharpe = spy_max_dd = None
else:
    spy_sharpe = spy_max_dd = None

# -----------------------------------------------------------------------------
# Report
# -----------------------------------------------------------------------------
from datetime import datetime

report_path = config.OUTPUTS_DIR / "walk_forward_daily_run.md"
report_path = report_path.resolve()
report_path.parent.mkdir(parents=True, exist_ok=True)

lines = [
    "# Walk-forward validation (quality + momentum, long-only) — run output",
    "",
    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "",
    "## Strategy",
    "- Step 1: rank by fcf_r2_10y. Step 2: keep top decile (quality).",
    "- Step 3: within that, rank by ret_1m (prior month return). Step 4: long top decile (winners).",
    "- Label: fwd_ret_10td. Long-only; each observation = 10d forward return of long portfolio.",
    "",
    "## Config",
    f"- Data: {PARQUET_PATH}",
    f"- DATA_START: {DATA_START}",
    f"- IS_DAYS: {IS_DAYS}, OOS_DAYS: {OOS_DAYS}, EMBARGO_DAYS: {EMBARGO_DAYS}, MIN_OOS_DAYS: {MIN_OOS_DAYS}",
    f"- PERIODS_PER_YEAR: {PERIODS_PER_YEAR}",
    f"- Rebal dates: {len(day_ends)} ({day_ends[0]} to {day_ends[-1]})",
    "",
    "## Folds",
]
for i, (is_start, is_end, oos_start, oos_end) in enumerate(folds, 1):
    lines.append(f"- Fold {i}: IS {is_start.date()} → {is_end.date()}  |  OOS {oos_start.date()} → {oos_end.date()}")

lines.extend([
    "",
    "## Model (long-only; label = fwd_ret_10td; overlapping 10d forward returns)",
    "- Sharpe and max drawdown omitted (not valid for overlapping return series).",
    f"- Mean 10d return (long): {mean_10d_ret:.6f}",
    f"- Newey-West t-stat: {t_stat:.3f}" if not np.isnan(t_stat) else "- Newey-West t-stat: n/a",
    f"- Hit rate (return > 0): {hit_rate:.2%}",
    f"- Annualized mean (approx): {annualized_mean_approx:.2%}" if not np.isnan(annualized_mean_approx) else "- Annualized mean (approx): n/a",
    f"- Mean 10d (long portfolio): {mean_long_10d:.6f}",
    f"- N observations: {len(all_spread)}",
    "",
])
if spy_sharpe is not None and spy_max_dd is not None:
    lines.append(f"## SPY (daily returns, same OOS — not comparable to 10d strategy return)\n- Sharpe: {spy_sharpe:.3f}, MaxDD: {spy_max_dd:.2%}\n")
lines.append("---")

report_path.write_text("\n".join(lines), encoding="utf-8")
print(f"\nReport written to {report_path}")
