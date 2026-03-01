#!/usr/bin/env python3
"""
Experiment 0001: Plain Dreman Price-to-Cash-Flow (Quintiles)

Basic Dreman-style experiment: rank by P/CF only, no quality filters.
Dreman targets the bottom 20% on valuation (P/E, P/CF, P/B, P/D); this uses P/CF alone.

Methodology:
- Monthly rebalance (last trading day of each month)
- Top 1500 by market cap on rebalance date
- No quality filter (plain P/CF)
- Exclude Financial Services and Real Estate (P/CF distorted)
- Sort into quintiles by pcf_pit (ascending — Q1 = cheapest, Q5 = most expensive)
- Equal weight within quintile
- 21 trading day forward returns (fwd_ret_21td)
- Post-2010

Output: experiments/runs/0001_dreman_pcf_quintile/
"""
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd
import numpy as np

from config import MASTER_FEATURES_PATH

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
EXPERIMENT_ID = "0001_dreman_pcf_quintile"
DESCRIPTION = "Dreman-style P/CF quintiles only, top 1500, ex-Fin/RE, monthly"
N_QUINTILES = 5
TOP_N = 1500
HORIZON = "fwd_ret_21td"

EXCLUDE_SECTORS = ("Financial Services", "Real Estate")
DATE_START = "2000-01-01"

OUTPUT_DIR = ROOT / "experiments" / "runs" / EXPERIMENT_ID
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def sharpe_annualized(series: pd.Series) -> float:
    s = series.dropna()
    if len(s) < 2:
        return 0.0
    ann_ret = s.mean() * 12
    ann_vol = s.std() * np.sqrt(12)
    return ann_ret / ann_vol if ann_vol > 0 else 0.0


def main():
    print(f"Running experiment: {EXPERIMENT_ID}")
    print(f"Description: {DESCRIPTION}")
    print(f"Master features: {MASTER_FEATURES_PATH}")
    assert MASTER_FEATURES_PATH.exists(), f"Master features not found: {MASTER_FEATURES_PATH}"

    con = duckdb.connect()
    master_path = repr(str(MASTER_FEATURES_PATH.resolve()))
    exclude_list = ",".join(f"'{s}'" for s in EXCLUDE_SECTORS)

    print(f"\nNo quality filter (plain P/CF)")
    print(f"Excluding sectors: {', '.join(EXCLUDE_SECTORS)}")
    print(f"Date range: >= {DATE_START}")

    # -------------------------------------------------------------------
    # Main query: month-end, top 1500, P/CF only, quintile sort
    # -------------------------------------------------------------------
    df = con.execute(f"""
        WITH month_end_dates AS (
            SELECT MAX(date) AS rebal_date
            FROM read_parquet({master_path})
            GROUP BY DATE_TRUNC('month', date)
        ),
        ranked AS (
            SELECT
                m.date,
                m.ticker,
                m.pcf_pit,
                m.{HORIZON} AS fwd_ret,
                m.fwd_holding_days_21td AS fwd_holding_days,
                m.fwd_delisted_21td AS fwd_delisted,
                m.marketcap_daily,
                m.sector,
                ROW_NUMBER() OVER (PARTITION BY m.date ORDER BY m.marketcap_daily DESC) AS mktcap_rank
            FROM read_parquet({master_path}) m
            INNER JOIN month_end_dates d ON m.date = d.rebal_date
            WHERE m.pcf_pit IS NOT NULL AND m.pcf_pit > 0
              AND m.marketcap_daily IS NOT NULL
              AND m.{HORIZON} IS NOT NULL
              AND m.date >= '{DATE_START}'
              AND m.sector NOT IN ({exclude_list})
        ),
        top_n AS (
            SELECT *,
                   NTILE({N_QUINTILES}) OVER (PARTITION BY date ORDER BY pcf_pit ASC) AS pcf_quintile
            FROM ranked
            WHERE mktcap_rank <= {TOP_N}
        )
        SELECT * FROM top_n
        ORDER BY date, pcf_quintile, ticker
    """).df()

    print(f"Total stock-month observations: {len(df):,}")
    if len(df) == 0:
        print("No data. Exiting.")
        con.close()
        return

    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    n_dates = df['date'].nunique()
    print(f"Rebalance dates: {n_dates}")
    print(f"Avg stocks per quintile per date: {len(df) / n_dates / N_QUINTILES:.0f}")

    # -------------------------------------------------------------------
    # Quintile summary
    # -------------------------------------------------------------------
    quintile_summary = df.groupby('pcf_quintile').agg(
        n_obs=('fwd_ret', 'count'),
        avg_pcf=('pcf_pit', 'mean'),
        median_pcf=('pcf_pit', 'median'),
        avg_fwd_ret=('fwd_ret', 'mean'),
        median_fwd_ret=('fwd_ret', 'median'),
        std_fwd_ret=('fwd_ret', 'std'),
        pct_positive=('fwd_ret', lambda x: (x > 0).mean()),
        avg_mktcap=('marketcap_daily', 'mean'),
        pct_delisted=('fwd_delisted', 'mean'),
    ).round(4)

    q1_ret = quintile_summary.loc[1, 'avg_fwd_ret']
    q5_ret = quintile_summary.loc[N_QUINTILES, 'avg_fwd_ret']
    spread = q1_ret - q5_ret
    returns_by_q = quintile_summary['avg_fwd_ret'].values
    is_monotonic = all(returns_by_q[i] >= returns_by_q[i + 1] for i in range(len(returns_by_q) - 1))

    # -------------------------------------------------------------------
    # Time series of quintile returns
    # -------------------------------------------------------------------
    ts = df.groupby(['date', 'pcf_quintile'])['fwd_ret'].mean().unstack()
    ts.columns = [f'Q{int(c)}' for c in ts.columns]
    ts['Q1_Q5_spread'] = ts['Q1'] - ts[f'Q{N_QUINTILES}']
    ts = ts.sort_index()

    # Reindex to full calendar month-ends: missing months = 0.0 return (cash). Ensures
    # CAGR, Sharpe, and drawdowns use the real calendar timeline, not just active months.
    ts.index = pd.to_datetime(ts.index).to_period("M").to_timestamp("M")
    ts = ts[~ts.index.duplicated(keep="first")]
    all_month_ends = pd.date_range(ts.index.min(), ts.index.max(), freq="ME")
    ts = ts.reindex(all_month_ends, fill_value=0.0)

    # -------------------------------------------------------------------
    # Cumulative returns
    # -------------------------------------------------------------------
    cumulative = pd.DataFrame(index=ts.index)
    for col in [f'Q{i}' for i in range(1, N_QUINTILES + 1)]:
        cumulative[col] = (1 + ts[col]).cumprod()

    n_years = (ts.index[-1] - ts.index[0]).days / 365.25 if len(ts) > 0 else 0

    # Q1-Q5 spread stats for metadata
    spread_series = ts['Q1_Q5_spread'].dropna()
    sharpe_s = (spread_series.mean() * 12) / (spread_series.std() * np.sqrt(12)) if spread_series.std() > 0 else 0
    ts_annual = ts.copy()
    ts_annual['year'] = pd.to_datetime(ts_annual.index).year
    annual = ts_annual.groupby('year')[[f'Q1', f'Q{N_QUINTILES}']].apply(lambda x: (1 + x).prod() - 1)
    annual['Q1_wins'] = annual['Q1'] > annual[f'Q{N_QUINTILES}']
    win_rate = annual['Q1_wins'].mean()

    # -------------------------------------------------------------------
    # Save outputs
    # -------------------------------------------------------------------
    df.to_parquet(OUTPUT_DIR / "quintile_assignments.parquet", index=False)
    ts.to_parquet(OUTPUT_DIR / "quintile_returns_ts.parquet")
    cumulative.to_parquet(OUTPUT_DIR / "cumulative_returns.parquet")
    quintile_summary.to_parquet(OUTPUT_DIR / "quintile_summary.parquet")

    metadata = {
        "experiment_id": EXPERIMENT_ID,
        "description": DESCRIPTION,
        "strategy": "Dreman-style plain P/CF quintiles",
        "timestamp": datetime.now().isoformat(),
        "n_quintiles": N_QUINTILES,
        "top_n": TOP_N,
        "horizon": HORIZON,
        "exclude_sectors": list(EXCLUDE_SECTORS),
        "date_start": DATE_START,
        "date_range": f"{df['date'].min()} to {df['date'].max()}",
        "n_rebalance_dates": int(n_dates),
        "total_observations": int(len(df)),
        "avg_stocks_per_quintile": int(len(df) / n_dates / N_QUINTILES),
        "q1_avg_monthly_ret": float(q1_ret),
        f"q{N_QUINTILES}_avg_monthly_ret": float(q5_ret),
        "q1_q5_spread_monthly": float(spread),
        "q1_q5_spread_annualized": float(spread * 12),
        "is_monotonic": bool(is_monotonic),
        "q1_beats_q5_pct": float(win_rate),
        "q1_q5_spread_sharpe": float(sharpe_s),
    }
    import json
    with open(OUTPUT_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print(f"\nOutputs saved to: {OUTPUT_DIR}")
    for f in sorted(OUTPUT_DIR.iterdir()):
        print(f"  {f.name}")

    con.close()

    # Generate descriptive report (report.md + figures) and full analysis (turnover, split-sample, etc.)
    sys.path.insert(0, str(ROOT / "experiments"))
    import describe_backtest
    import analyze_backtest
    describe_backtest.run_report(OUTPUT_DIR)
    analyze_backtest.run_analysis(OUTPUT_DIR)


if __name__ == "__main__":
    main()
