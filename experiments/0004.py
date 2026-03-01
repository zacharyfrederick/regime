#!/usr/bin/env python3
"""
Experiment 0004: PCF Decile Sort + Quality Filter

The core strategy validated across 15+ experiments. Quality filter removes
value traps; PCF decile sort identifies fair-priced quality businesses.
Q6-Q7 consistently produces the best risk-adjusted returns (Sharpe ~1.0,
MaxDD ~-20%) across split samples, quality thresholds, and VIX regimes.

Methodology:
- Monthly rebalance (last trading day of each month)
- Top 1500 by market cap on rebalance date
- Quality filter: fcf_r2_10y > 0.5 AND fcf_pct_positive >= 0.5
  (robust across NCFO/FCF and thresholds 0.5-0.7)
- Exclude Financial Services and Real Estate (PB/PCF distorted)
- Sort survivors into deciles by pcf_pit (ascending — D1 = cheapest, D10 = most expensive)
- Equal weight within decile
- 21 trading day forward returns (fwd_ret_21td)
- Post-2010 (avoids GFC regime, cleaner signal)

Key findings:
- Q6-Q7: Sharpe ~1.0-1.3, MaxDD ~-20%, CAGR ~15-17%
- Q4 beats Q1 on Sharpe in both split-sample halves
- Quality filter is the edge; valuation ranking within quality is secondary
- Signal amplified when VIX >= 15 but optimal decile doesn't shift

Output: experiments/runs/0004_pcf_quality_quintile/
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
EXPERIMENT_ID = "0004_pcf_quality_quintile"
DESCRIPTION = "PCF decile sort + quality filter (fcf_r2>0.5, fcf_pct>=0.5), top 1500, ex-Fin/RE, monthly"
N_DECILES = 10
TOP_N = 1500
HORIZON = "fwd_ret_21td"

# Quality thresholds (robust across 0.5-0.7 range)
FCF_R2_MIN = 0.5
FCF_PCT_POSITIVE_MIN = 0.5

# Sectors to exclude
EXCLUDE_SECTORS = ("Financial Services", "Real Estate")

# Date filter
DATE_START = "2010-01-01"

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

    con = duckdb.connect(":memory:")
    master_path = repr(str(MASTER_FEATURES_PATH.resolve()))

    exclude_list = ",".join(f"'{s}'" for s in EXCLUDE_SECTORS)

    print(f"\nQuality filters: fcf_r2_10y > {FCF_R2_MIN}, fcf_pct_positive >= {FCF_PCT_POSITIVE_MIN}")
    print(f"Excluding sectors: {', '.join(EXCLUDE_SECTORS)}")
    print(f"Date range: >= {DATE_START}")

    # -------------------------------------------------------------------
    # Main query: month-end, top 1500, quality filter, decile sort
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
                m.scalemarketcap,
                m.sector,
                m.fcf_r2_10y,
                m.fcf_pct_positive,
                ROW_NUMBER() OVER (PARTITION BY m.date ORDER BY m.marketcap_daily DESC) AS mktcap_rank
            FROM read_parquet({master_path}) m
            INNER JOIN month_end_dates d ON m.date = d.rebal_date
            WHERE m.fcf_r2_10y IS NOT NULL AND m.fcf_r2_10y > {FCF_R2_MIN}
              AND m.fcf_pct_positive IS NOT NULL AND m.fcf_pct_positive >= {FCF_PCT_POSITIVE_MIN}
              AND m.pcf_pit IS NOT NULL AND m.pcf_pit > 0
              AND m.marketcap_daily IS NOT NULL
              AND m.{HORIZON} IS NOT NULL
              AND m.date >= '{DATE_START}'
              AND m.sector NOT IN ({exclude_list})
        ),
        top_n AS (
            SELECT *,
                   NTILE({N_DECILES}) OVER (PARTITION BY date ORDER BY pcf_pit ASC) AS pcf_quintile
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
    print(f"Avg stocks per quintile per date: {len(df) / n_dates / N_DECILES:.0f}")

    # -------------------------------------------------------------------
    # Decile summary statistics
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"QUINTILE SUMMARY (Q1 = cheapest PCF, Q{N_DECILES} = most expensive)")
    print("=" * 70)

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

    print(quintile_summary.to_string())

    q1_ret = quintile_summary.loc[1, 'avg_fwd_ret']
    q5_ret = quintile_summary.loc[N_DECILES, 'avg_fwd_ret']
    spread = q1_ret - q5_ret
    print(f"\nQ1 - Q{N_DECILES} spread (monthly): {spread:.4f} ({spread * 100:.2f}%)")
    print(f"Q1 - Q{N_DECILES} spread (annualized): {spread * 12:.4f} ({spread * 12 * 100:.2f}%)")

    # Monotonicity
    returns_by_q = quintile_summary['avg_fwd_ret'].values
    is_monotonic = all(returns_by_q[i] >= returns_by_q[i + 1] for i in range(len(returns_by_q) - 1))
    print(f"\nMonotonic (Q1 >= Q2 >= ... >= Q{N_DECILES}): {is_monotonic}")
    if not is_monotonic:
        print("  WARNING: Returns are not monotonically decreasing across quintiles")
        print(f"  Returns by quintile: {[f'{r:.4f}' for r in returns_by_q]}")

    # -------------------------------------------------------------------
    # Time series of decile returns
    # -------------------------------------------------------------------
    ts = df.groupby(['date', 'pcf_quintile'])['fwd_ret'].mean().unstack()
    ts.columns = [f'Q{int(c)}' for c in ts.columns]
    ts['Q1_Q5_spread'] = ts['Q1'] - ts[f'Q{N_DECILES}']
    if N_DECILES >= 7:
        ts['Q4_Q5_spread'] = ts['Q4'] - ts[f'Q{N_DECILES}']
    ts = ts.sort_index()

    # -------------------------------------------------------------------
    # Cumulative returns
    # -------------------------------------------------------------------
    cumulative = pd.DataFrame(index=ts.index)
    for col in [f'Q{i}' for i in range(1, N_DECILES + 1)]:
        cumulative[col] = (1 + ts[col]).cumprod()

    n_years = (ts.index[-1] - ts.index[0]).days / 365.25 if len(ts) > 0 else 0

    print("\n" + "=" * 70)
    print("CUMULATIVE RETURNS (final values)")
    print("=" * 70)
    for col in [f'Q{i}' for i in range(1, N_DECILES + 1)]:
        total_ret = cumulative[col].iloc[-1] - 1
        cagr = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else 0
        print(f"  {col}: {total_ret:+.2%} total, {cagr:+.2%} CAGR")

    # -------------------------------------------------------------------
    # Risk metrics
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RISK METRICS (annualized)")
    print("=" * 70)

    for q in range(1, N_DECILES + 1):
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
    # Long Q1 / Short Q10 (value spread)
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"LONG Q1 / SHORT Q{N_DECILES} (value spread)")
    print("=" * 70)

    spread_series = ts['Q1_Q5_spread'].dropna()
    ann_ret_s = spread_series.mean() * 12
    ann_vol_s = spread_series.std() * np.sqrt(12)
    sharpe_s = ann_ret_s / ann_vol_s if ann_vol_s > 0 else 0
    cum_spread = (1 + spread_series).cumprod()
    total_spread = cum_spread.iloc[-1] - 1
    cagr_spread = (1 + total_spread) ** (1 / n_years) - 1 if n_years > 0 else 0
    max_dd_spread = ((cum_spread - cum_spread.cummax()) / cum_spread.cummax()).min()
    sortino_s = ann_ret_s / (spread_series[spread_series < 0].std() * np.sqrt(12)) if (spread_series < 0).any() else 0

    print(f"  Q1-Q{N_DECILES} spread: Total return={total_spread:+.2%}, CAGR={cagr_spread:+.2%}")
    print(f"  Sharpe={sharpe_s:.2f}, Sortino={sortino_s:.2f}, MaxDD={max_dd_spread:.2%}, AnnVol={ann_vol_s:.2%}")

    ts_index = pd.to_datetime(ts.index)
    period1 = (ts_index >= "2010-01-01") & (ts_index <= "2017-12-31")
    period2 = (ts_index >= "2018-01-01") & (ts_index <= "2024-12-31")
    s1 = sharpe_annualized(spread_series[period1])
    s2 = sharpe_annualized(spread_series[period2])
    print(f"  Split-sample Sharpe: 2010-2017={s1:.2f}, 2018-2024={s2:.2f}")

    # -------------------------------------------------------------------
    # Long Q4 / Short Q10 (quality at fair price) — if deciles
    # -------------------------------------------------------------------
    if N_DECILES >= 7 and 'Q4_Q5_spread' in ts.columns:
        print("\n" + "=" * 70)
        print(f"LONG Q4 / SHORT Q{N_DECILES} (quality at fair price)")
        print("=" * 70)
        q4_spread = ts['Q4_Q5_spread'].dropna()
        ann_ret_q4 = q4_spread.mean() * 12
        ann_vol_q4 = q4_spread.std() * np.sqrt(12)
        sharpe_q4 = ann_ret_q4 / ann_vol_q4 if ann_vol_q4 > 0 else 0
        cum_q4 = (1 + q4_spread).cumprod()
        total_q4 = cum_q4.iloc[-1] - 1
        cagr_q4 = (1 + total_q4) ** (1 / n_years) - 1 if n_years > 0 else 0
        max_dd_q4 = ((cum_q4 - cum_q4.cummax()) / cum_q4.cummax()).min()
        sortino_q4 = ann_ret_q4 / (q4_spread[q4_spread < 0].std() * np.sqrt(12)) if (q4_spread < 0).any() else 0

        print(f"  Q4-Q{N_DECILES} spread: Total return={total_q4:+.2%}, CAGR={cagr_q4:+.2%}")
        print(f"  Sharpe={sharpe_q4:.2f}, Sortino={sortino_q4:.2f}, MaxDD={max_dd_q4:.2%}, AnnVol={ann_vol_q4:.2%}")
        print(f"  (Q1-Q{N_DECILES} spread AnnVol={ann_vol_s:.2%} for comparison)")
        s1_q4 = sharpe_annualized(q4_spread[period1])
        s2_q4 = sharpe_annualized(q4_spread[period2])
        print(f"  Split-sample Sharpe: 2010-2017={s1_q4:.2f}, 2018-2024={s2_q4:.2f}")

    # -------------------------------------------------------------------
    # Consistency: Q1 beats Q10 by year
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"CONSISTENCY: Q1 beats Q{N_DECILES} by year (chained monthly returns)")
    print("=" * 70)

    ts_annual = ts.copy()
    ts_annual['year'] = pd.to_datetime(ts_annual.index).year
    annual = ts_annual.groupby('year')[[f'Q1', f'Q{N_DECILES}']].apply(
        lambda x: (1 + x).prod() - 1
    )
    annual['Q1_wins'] = annual['Q1'] > annual[f'Q{N_DECILES}']
    annual['spread'] = annual['Q1'] - annual[f'Q{N_DECILES}']

    for year, row in annual.iterrows():
        marker = "✓" if row['Q1_wins'] else "✗"
        print(f"  {year}: Q1={row['Q1']:.4f}, Q{N_DECILES}={row[f'Q{N_DECILES}']:.4f}, "
              f"spread={row['spread']:+.4f} {marker}")

    win_rate = annual['Q1_wins'].mean()
    print(f"\n  Q1 beats Q{N_DECILES} in {annual['Q1_wins'].sum()}/{len(annual)} years ({win_rate:.0%})")

    # -------------------------------------------------------------------
    # Split-sample: Q4 vs Q1 Sharpe
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"SPLIT-SAMPLE: Q4 vs Q1 Sharpe (2010-2017 vs 2018-2024)")
    print("=" * 70)

    q4_s1 = sharpe_annualized(ts.loc[period1, 'Q4'])
    q1_s1 = sharpe_annualized(ts.loc[period1, 'Q1'])
    q4_s2 = sharpe_annualized(ts.loc[period2, 'Q4'])
    q1_s2 = sharpe_annualized(ts.loc[period2, 'Q1'])

    print(f"  2010-2017: Q4 Sharpe={q4_s1:.2f}, Q1 Sharpe={q1_s1:.2f} → {'Q4' if q4_s1 > q1_s1 else 'Q1'} wins")
    print(f"  2018-2024: Q4 Sharpe={q4_s2:.2f}, Q1 Sharpe={q1_s2:.2f} → {'Q4' if q4_s2 > q1_s2 else 'Q1'} wins")

    if q4_s1 > q1_s1 and q4_s2 > q1_s2:
        print("\n  Conclusion: Real pattern: Q4 beats Q1 on Sharpe in both halves.")
    else:
        print("\n  Conclusion: Period-specific: Q4 does not consistently beat Q1.")

    # -------------------------------------------------------------------
    # Sector composition
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("SECTOR COMPOSITION (avg % of quintile)")
    print("=" * 70)

    for q in [1, N_DECILES]:
        q_data = df[df['pcf_quintile'] == q]
        sector_pct = q_data.groupby('sector').size() / len(q_data) * 100
        sector_pct = sector_pct.sort_values(ascending=False)
        label = 'cheapest' if q == 1 else 'most expensive'
        print(f"\n  Q{q} ({label} PCF):")
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
        "n_deciles": N_DECILES,
        "top_n": TOP_N,
        "horizon": HORIZON,
        "quality_filter": f"fcf_r2_10y > {FCF_R2_MIN}, fcf_pct_positive >= {FCF_PCT_POSITIVE_MIN}",
        "exclude_sectors": list(EXCLUDE_SECTORS),
        "date_start": DATE_START,
        "date_range": f"{df['date'].min()} to {df['date'].max()}",
        "n_rebalance_dates": int(n_dates),
        "total_observations": int(len(df)),
        "avg_stocks_per_decile": int(len(df) / n_dates / N_DECILES),
        "q1_avg_monthly_ret": float(q1_ret),
        f"q{N_DECILES}_avg_monthly_ret": float(q5_ret),
        "q1_q10_spread_monthly": float(spread),
        "q1_q10_spread_annualized": float(spread * 12),
        "is_monotonic": bool(is_monotonic),
        "q1_beats_q10_pct": float(win_rate),
        "q1_q10_spread_sharpe": float(sharpe_s),
        "split_sample_q4_sharpe_2010_2017": float(q4_s1),
        "split_sample_q4_sharpe_2018_2024": float(q4_s2),
        "split_sample_q1_sharpe_2010_2017": float(q1_s1),
        "split_sample_q1_sharpe_2018_2024": float(q1_s2),
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