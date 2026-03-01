#!/usr/bin/env python3
"""
Experiment 0000: Dreman PE Quintile Sort (Unfiltered)

Hypothesis: Bottom quintile by PE (cheapest) outperforms top quintile (most expensive)
with monotonic spread across quintiles. This validates the pipeline produces
sensible results before adding Dreman quality filters.

Methodology:
- Monthly rebalance (last trading day of each month)
- Top 1500 by market cap on rebalance date
- Sort into quintiles by PE (ascending — Q1 = cheapest)
- Equal weight within quintile
- 21 trading day forward returns (fwd_ret_21td)
- Exclude negative PE (loss-making companies)

Output: experiments/runs/0000_dreman_quintile_pe/
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
EXPERIMENT_ID = "0000_dreman_quintile_pe"
DESCRIPTION = "PE quintile sort on top 1500 by market cap, monthly rebalance, no quality filters"
N_QUINTILES = 5
TOP_N = 1500
HORIZON = "fwd_ret_21td"

OUTPUT_DIR = ROOT / "experiments" / "runs" / EXPERIMENT_ID
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    print(f"Running experiment: {EXPERIMENT_ID}")
    print(f"Description: {DESCRIPTION}")
    print(f"Master features: {MASTER_FEATURES_PATH}")
    assert MASTER_FEATURES_PATH.exists(), f"Master features not found: {MASTER_FEATURES_PATH}"

    con = duckdb.connect(":memory:")
    master_path = repr(str(MASTER_FEATURES_PATH.resolve()))

    # -------------------------------------------------------------------
    # Single query: month-end dates, top 1500, quintile sort, forward returns
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
                m.pe_pit,
                m.{HORIZON} AS fwd_ret,
                m.fwd_holding_days_21td AS fwd_holding_days,
                m.fwd_delisted_21td AS fwd_delisted,
                m.marketcap_daily,
                m.scalemarketcap,
                m.sector,
                ROW_NUMBER() OVER (PARTITION BY m.date ORDER BY m.marketcap_daily DESC) AS mktcap_rank
            FROM read_parquet({master_path}) m
            INNER JOIN month_end_dates d ON m.date = d.rebal_date
            WHERE m.pe_pit IS NOT NULL
              AND m.pe_pit > 0
              AND m.marketcap_daily IS NOT NULL
              AND m.{HORIZON} IS NOT NULL
        ),
        top_n AS (
            SELECT *,
                   NTILE({N_QUINTILES}) OVER (PARTITION BY date ORDER BY pe_pit ASC) AS pe_quintile
            FROM ranked
            WHERE mktcap_rank <= {TOP_N}
        )
        SELECT * FROM top_n
        ORDER BY date, pe_quintile, ticker
    """).df()

    print(f"\nTotal stock-month observations: {len(df):,}")
    if len(df) == 0:
        print("No data. Exiting.")
        con.close()
        return

    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Rebalance dates: {df['date'].nunique()}")
    print(f"Avg stocks per quintile per date: {len(df) / df['date'].nunique() / N_QUINTILES:.0f}")

    # -------------------------------------------------------------------
    # Quintile summary statistics
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUINTILE SUMMARY (Q1 = cheapest PE, Q5 = most expensive)")
    print("=" * 70)

    quintile_summary = df.groupby('pe_quintile').agg(
        n_obs=('fwd_ret', 'count'),
        avg_pe=('pe_pit', 'mean'),
        median_pe=('pe_pit', 'median'),
        avg_fwd_ret=('fwd_ret', 'mean'),
        median_fwd_ret=('fwd_ret', 'median'),
        std_fwd_ret=('fwd_ret', 'std'),
        pct_positive=('fwd_ret', lambda x: (x > 0).mean()),
        avg_mktcap=('marketcap_daily', 'mean'),
        pct_delisted=('fwd_delisted', 'mean'),
    ).round(4)

    print(quintile_summary.to_string())

    # Q1-Q5 spread
    q1_ret = quintile_summary.loc[1, 'avg_fwd_ret']
    q5_ret = quintile_summary.loc[N_QUINTILES, 'avg_fwd_ret']
    spread = q1_ret - q5_ret
    print(f"\nQ1 - Q5 spread (monthly): {spread:.4f} ({spread * 100:.2f}%)")
    print(f"Q1 - Q5 spread (annualized): {spread * 12:.4f} ({spread * 12 * 100:.2f}%)")

    # -------------------------------------------------------------------
    # Monotonicity check
    # -------------------------------------------------------------------
    returns_by_q = quintile_summary['avg_fwd_ret'].values
    is_monotonic = all(returns_by_q[i] >= returns_by_q[i + 1] for i in range(len(returns_by_q) - 1))
    print(f"\nMonotonic (Q1 >= Q2 >= ... >= Q5): {is_monotonic}")
    if not is_monotonic:
        print("  WARNING: Returns are not monotonically decreasing across quintiles")
        print(f"  Returns by quintile: {[f'{r:.4f}' for r in returns_by_q]}")

    # -------------------------------------------------------------------
    # Time series of quintile returns (per rebalance date)
    # -------------------------------------------------------------------
    ts = df.groupby(['date', 'pe_quintile'])['fwd_ret'].mean().unstack()
    ts.columns = [f'Q{int(c)}' for c in ts.columns]
    ts['Q1_Q5_spread'] = ts['Q1'] - ts[f'Q{N_QUINTILES}']
    ts = ts.sort_index()

    # -------------------------------------------------------------------
    # Cumulative returns per quintile
    # -------------------------------------------------------------------
    cumulative = pd.DataFrame(index=ts.index)
    for col in [f'Q{i}' for i in range(1, N_QUINTILES + 1)]:
        cumulative[col] = (1 + ts[col]).cumprod()
    cumulative['Q1_Q5_spread'] = (1 + ts['Q1_Q5_spread']).cumprod()

    n_years = (ts.index[-1] - ts.index[0]).days / 365.25 if len(ts) > 0 else 0

    print("\n" + "=" * 70)
    print("CUMULATIVE RETURNS (final values)")
    print("=" * 70)
    for col in [f'Q{i}' for i in range(1, N_QUINTILES + 1)]:
        total_ret = cumulative[col].iloc[-1] - 1
        cagr = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else 0
        print(f"  {col}: {total_ret:+.2%} total, {cagr:+.2%} CAGR")

    # -------------------------------------------------------------------
    # Risk metrics per quintile
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RISK METRICS (annualized)")
    print("=" * 70)

    for q in range(1, N_QUINTILES + 1):
        col = f'Q{q}'
        monthly_rets = ts[col]
        ann_ret = monthly_rets.mean() * 12
        ann_vol = monthly_rets.std() * np.sqrt(12)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
        cum = cumulative[col]
        max_dd = ((cum - cum.cummax()) / cum.cummax()).min()
        downside = monthly_rets[monthly_rets < 0].std() * np.sqrt(12)
        sortino = ann_ret / downside if downside > 0 else 0

        print(f"  {col}: Sharpe={sharpe:.2f}, Sortino={sortino:.2f}, "
              f"MaxDD={max_dd:.2%}, AnnRet={ann_ret:.2%}, AnnVol={ann_vol:.2%}")

    # -------------------------------------------------------------------
    # Long Q1 / Short Q5 (value spread)
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("LONG Q1 / SHORT Q5 (value spread)")
    print("=" * 70)

    spread_series = ts['Q1_Q5_spread'].dropna()
    ann_ret_s = spread_series.mean() * 12
    ann_vol_s = spread_series.std() * np.sqrt(12)
    sharpe_s = ann_ret_s / ann_vol_s if ann_vol_s > 0 else 0
    cum_spread = cumulative['Q1_Q5_spread']
    total_spread = cum_spread.iloc[-1] - 1
    cagr_spread = (1 + total_spread) ** (1 / n_years) - 1 if n_years > 0 else 0
    max_dd_spread = ((cum_spread - cum_spread.cummax()) / cum_spread.cummax()).min()

    print(f"  Q1-Q5 spread: Total return={total_spread:+.2%}, CAGR={cagr_spread:+.2%}")
    print(f"  Sharpe={sharpe_s:.2f}, AnnRet={ann_ret_s:.2%}, AnnVol={ann_vol_s:.2%}, MaxDD={max_dd_spread:.2%}")

    ts_index = pd.to_datetime(ts.index)
    period1 = (ts_index >= "2010-01-01") & (ts_index <= "2017-12-31")
    period2 = (ts_index >= "2018-01-01") & (ts_index <= "2024-12-31")

    def sharpe_ann(s):
        s = s.dropna()
        if len(s) < 2:
            return 0.0
        return (s.mean() * 12) / (s.std() * np.sqrt(12)) if s.std() > 0 else 0.0

    s1 = sharpe_ann(spread_series[period1])
    s2 = sharpe_ann(spread_series[period2])
    print(f"  Split-sample Sharpe: 2010-2017={s1:.2f}, 2018-2024={s2:.2f}")

    # -------------------------------------------------------------------
    # Consistency: Q1 beats Q5 by year (chained monthly returns)
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("CONSISTENCY: Q1 beats Q5 by year (chained monthly returns)")
    print("=" * 70)

    ts_annual = ts.copy()
    ts_annual['year'] = pd.to_datetime(ts_annual.index).year
    annual = ts_annual.groupby('year')[['Q1', f'Q{N_QUINTILES}']].apply(
        lambda x: (1 + x).prod() - 1
    )
    annual['Q1_wins'] = annual['Q1'] > annual[f'Q{N_QUINTILES}']
    annual['spread'] = annual['Q1'] - annual[f'Q{N_QUINTILES}']

    for year, row in annual.iterrows():
        marker = "✓" if row['Q1_wins'] else "✗"
        print(f"  {year}: Q1={row['Q1']:.4f}, Q{N_QUINTILES}={row[f'Q{N_QUINTILES}']:.4f}, "
              f"spread={row['spread']:+.4f} {marker}")

    win_rate = annual['Q1_wins'].mean()
    print(f"\n  Q1 beats Q5 in {annual['Q1_wins'].sum()}/{len(annual)} years ({win_rate:.0%})")

    # -------------------------------------------------------------------
    # Sector composition of Q1 vs Q5
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("SECTOR COMPOSITION (avg % of quintile)")
    print("=" * 70)

    for q in [1, N_QUINTILES]:
        q_data = df[df['pe_quintile'] == q]
        sector_pct = q_data.groupby('sector').size() / len(q_data) * 100
        sector_pct = sector_pct.sort_values(ascending=False).head(10)
        label = 'cheapest' if q == 1 else 'most expensive'
        print(f"\n  Q{q} ({label} PE):")
        for sector, pct in sector_pct.items():
            print(f"    {sector}: {pct:.1f}%")

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
        "timestamp": datetime.now().isoformat(),
        "n_quintiles": N_QUINTILES,
        "top_n": TOP_N,
        "horizon": HORIZON,
        "date_range": f"{df['date'].min()} to {df['date'].max()}",
        "n_rebalance_dates": int(df['date'].nunique()),
        "total_observations": int(len(df)),
        "q1_avg_monthly_ret": float(q1_ret),
        "q5_avg_monthly_ret": float(q5_ret),
        "q1_q5_spread_monthly": float(spread),
        "q1_q5_spread_annualized": float(spread * 12),
        "is_monotonic": bool(is_monotonic),
        "q1_beats_q5_pct": float(win_rate),
        "spread_sharpe": float(sharpe_s),
        "spread_sharpe_2010_2017": float(s1),
        "spread_sharpe_2018_2024": float(s2),
    }

    import json
    with open(OUTPUT_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print(f"\nOutputs saved to: {OUTPUT_DIR}")
    print("Files:")
    for f in sorted(OUTPUT_DIR.iterdir()):
        print(f"  {f.name}")

    con.close()


if __name__ == "__main__":
    main()