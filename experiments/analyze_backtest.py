#!/usr/bin/env python3
"""
Backtest analysis: reads experiment run output and generates tables + figures.

Run after 0001.py or 0004.py. Outputs go into the same run directory (figures/, CSV).

Usage:
  python experiments/analyze_backtest.py experiments/runs/0001_dreman_pcf_quintile
  python experiments/analyze_backtest.py experiments/runs/0004_pcf_quality_quintile [--benchmark path/to/spy_returns.csv]

Artifacts:
  - performance_table.csv       Core quintile/decile: CAGR, vol, Sharpe, Sortino, max DD, win rate
  - cumulative_returns_log.png  Log-scale equity curves (optional benchmark overlay)
  - annual_returns_heatmap.png  Decile x year with color coding
  - drawdown_curves.png         Max drawdown over time per decile
  - worst_drawdowns.csv         Worst drawdown per decile with dates
  - return_distribution.png    KDE/histogram of monthly returns per decile
  - turnover_table.csv          Avg monthly turnover per decile
  - split_sample.csv            Stats for 2010-2017 vs 2018-present
  - long_short_spread.png       Cumulative long-short (e.g. Q6-Q7 minus Q10 or Q1-Q5)
  - sector_composition_*.png     Stacked area: sector mix per decile over time
  - holdings_count.png         Names per decile over time
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

# -------------------------------------------------------------------
# Load run
# -------------------------------------------------------------------

def load_run(run_dir: Path):
    run_dir = Path(run_dir)
    assignments = pd.read_parquet(run_dir / "quintile_assignments.parquet")
    ts = pd.read_parquet(run_dir / "quintile_returns_ts.parquet")
    cumulative = pd.read_parquet(run_dir / "cumulative_returns.parquet")
    summary = pd.read_parquet(run_dir / "quintile_summary.parquet")
    with open(run_dir / "metadata.json") as f:
        meta = json.load(f)
    # N = quintiles or deciles
    N = int(meta.get("n_quintiles") or meta.get("n_deciles", 5))
    qcols = [f"Q{i}" for i in range(1, N + 1)]
    return {
        "assignments": assignments,
        "ts": ts,
        "cumulative": cumulative,
        "summary": summary,
        "meta": meta,
        "N": N,
        "qcols": qcols,
        "run_dir": run_dir,
    }


def ensure_fig_dir(run_dir: Path) -> Path:
    fig_dir = run_dir / "figures"
    fig_dir.mkdir(exist_ok=True)
    return fig_dir


# -------------------------------------------------------------------
# 1) Core performance table
# -------------------------------------------------------------------

def sharpe_annualized(series: pd.Series) -> float:
    s = series.dropna()
    if len(s) < 2:
        return 0.0
    ann_ret = s.mean() * 12
    ann_vol = s.std() * np.sqrt(12)
    return ann_ret / ann_vol if ann_vol > 0 else 0.0


def performance_table(data: dict) -> pd.DataFrame:
    ts = data["ts"]
    cumulative = data["cumulative"]
    N = data["N"]
    qcols = data["qcols"]
    n_years = (ts.index[-1] - ts.index[0]).days / 365.25 if len(ts) > 0 else 0

    rows = []
    for q, col in enumerate(qcols, 1):
        monthly = ts[col]
        cum = cumulative[col]
        ann_ret = monthly.mean() * 12
        ann_vol = monthly.std() * np.sqrt(12)
        sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
        downside = monthly[monthly < 0].std() * np.sqrt(12)
        sortino = ann_ret / downside if downside > 0 else 0
        dd = (cum - cum.cummax()) / cum.cummax()
        max_dd = dd.min()
        total_ret = cum.iloc[-1] - 1
        cagr = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else 0
        win_rate = (monthly > 0).mean()
        rows.append({
            "decile": col,
            "cagr": cagr,
            "ann_vol": ann_vol,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_dd": max_dd,
            "win_rate": win_rate,
        })
    return pd.DataFrame(rows)


# -------------------------------------------------------------------
# 2) Cumulative return curves (log scale) + optional benchmark
# -------------------------------------------------------------------

def _has_mpl():
    try:
        import matplotlib.pyplot  # noqa: F401
        return True
    except ImportError:
        return False


def plot_cumulative_log(data: dict, fig_dir: Path, benchmark: pd.Series | None = None):
    import matplotlib.pyplot as plt

    ts = data["ts"]
    cumulative = data["cumulative"]
    qcols = data["qcols"]
    N = data["N"]

    fig, ax = plt.subplots(figsize=(10, 6))
    for col in qcols:
        ax.semilogy(cumulative.index, cumulative[col], label=col, alpha=0.9)
    if benchmark is not None:
        # align by index
        b = benchmark.reindex(ts.index).fillna(0)
        cum_b = (1 + b).cumprod()
        ax.semilogy(cum_b.index, cum_b.values, label="Benchmark", color="black", linestyle="--", linewidth=2)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative return (log scale)")
    ax.set_title("Cumulative returns by decile (log scale)")
    ax.legend(loc="best", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "cumulative_returns_log.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# 3) Annual returns heatmap
# -------------------------------------------------------------------

def plot_annual_heatmap(data: dict, fig_dir: Path):
    import matplotlib.pyplot as plt

    ts = data["ts"]
    qcols = data["qcols"]
    ts_annual = ts[qcols].copy()
    ts_annual["year"] = pd.to_datetime(ts_annual.index).year
    annual = ts_annual.groupby("year")[qcols].apply(lambda x: (1 + x).prod() - 1)

    fig, ax = plt.subplots(figsize=(12, max(6, annual.shape[0] * 0.35)))
    im = ax.imshow(annual.T, aspect="auto", cmap="RdYlGn", vmin=-0.3, vmax=0.5)
    ax.set_yticks(range(len(qcols)))
    ax.set_yticklabels(qcols)
    ax.set_xticks(range(len(annual.index)))
    ax.set_xticklabels(annual.index.astype(int), rotation=45)
    ax.set_xlabel("Year")
    ax.set_ylabel("Decile")
    ax.set_title("Annual returns by decile (row = decile, col = year)")
    plt.colorbar(im, ax=ax, label="Return")
    fig.tight_layout()
    fig.savefig(fig_dir / "annual_returns_heatmap.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# 4) Drawdown curves + worst drawdown table
# -------------------------------------------------------------------

def drawdown_curves_and_table(data: dict, fig_dir: Path) -> pd.DataFrame:
    import matplotlib.pyplot as plt

    cumulative = data["cumulative"]
    qcols = data["qcols"]

    fig, ax = plt.subplots(figsize=(10, 6))
    worst_rows = []
    for col in qcols:
        cum = cumulative[col]
        dd = (cum - cum.cummax()) / cum.cummax()
        ax.fill_between(dd.index, dd, 0, alpha=0.5, label=col)
        ax.plot(dd.index, dd, alpha=0.8)
        idx_min = dd.idxmin()
        worst_rows.append({
            "decile": col,
            "max_drawdown": dd.min(),
            "date_of_worst": idx_min,
        })
    ax.set_xlabel("Date")
    ax.set_ylabel("Drawdown")
    ax.set_title("Drawdown over time by decile")
    ax.legend(loc="lower left", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "drawdown_curves.png", dpi=150)
    plt.close(fig)

    worst_df = pd.DataFrame(worst_rows)
    return worst_df


# -------------------------------------------------------------------
# 5) Return distribution (KDE or histogram per decile)
# -------------------------------------------------------------------

def plot_return_distribution(data: dict, fig_dir: Path):
    import matplotlib.pyplot as plt

    ts = data["ts"]
    qcols = data["qcols"]

    nq = len(qcols)
    nrows = (nq + 1) // 2
    fig, axes = plt.subplots(nrows, 2, figsize=(10, 2.5 * nrows))
    axes_flat = np.atleast_1d(axes).flatten()
    for i, col in enumerate(qcols):
        ax = axes_flat[i]
        monthly = ts[col].dropna()
        ax.hist(monthly, bins=30, density=True, alpha=0.6, label=col, color=f"C{i}")
        monthly.plot(kind="kde", ax=ax, color=f"C{i}", linewidth=2)
        ax.axvline(0, color="gray", linestyle="--")
        ax.set_title(col)
        ax.set_xlabel("Monthly return")
    for j in range(nq, len(axes_flat)):
        axes_flat[j].set_visible(False)
    fig.suptitle("Monthly return distribution by decile")
    fig.tight_layout()
    fig.savefig(fig_dir / "return_distribution.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# 6) Turnover (from assignments: same rebalance date ordering)
# -------------------------------------------------------------------

def turnover_table(data: dict) -> pd.DataFrame:
    assignments = data["assignments"]
    qcols = data["qcols"]
    N = data["N"]
    dates = sorted(assignments["date"].unique())
    if len(dates) < 2:
        return pd.DataFrame([{"decile": c, "avg_turnover": np.nan} for c in qcols])

    # For each date t, we have quintile membership. Turnover at t = fraction of names in quintile q at t that were not in q at t-1 (or new to universe).
    # Standard: turnover_q(t) = 1 - |intersection(holdings_q(t), holdings_q(t-1))| / |holdings_q(t)|  (one-way) or symmetric.
    # Use one-way: what fraction of current names were in same quintile last month?
    turnover_by_q = {q: [] for q in range(1, N + 1)}
    for i in range(1, len(dates)):
        d_curr = dates[i]
        d_prev = dates[i - 1]
        curr = assignments[assignments["date"] == d_curr].set_index("ticker")["pcf_quintile"]
        prev = assignments[assignments["date"] == d_prev].set_index("ticker")["pcf_quintile"]
        for q in range(1, N + 1):
            tickers_curr = set(curr[curr == q].index)
            if len(tickers_curr) == 0:
                turnover_by_q[q].append(np.nan)
                continue
            same = sum(1 for t in tickers_curr if prev.get(t) == q)
            turnover_by_q[q].append(1 - same / len(tickers_curr))
    rows = []
    for q, col in enumerate(qcols, 1):
        vals = [x for x in turnover_by_q[q] if np.isfinite(x)]
        rows.append({"decile": col, "avg_turnover": np.mean(vals) if vals else np.nan})
    return pd.DataFrame(rows)


# -------------------------------------------------------------------
# 7) Split-sample validation (all deciles × multiple periods)
# -------------------------------------------------------------------

def split_sample_multi_table(data: dict, n_splits: int = 4) -> pd.DataFrame:
    """Sharpe by decile in each of n_splits equal-length sub-periods over the full data range."""
    ts = data["ts"]
    qcols = data["qcols"]
    idx = pd.to_datetime(ts.index)
    if len(idx) == 0:
        return pd.DataFrame()
    n = len(idx)
    months_per_split = max(1, n // n_splits)
    result = {}
    for k in range(n_splits):
        start_i = k * months_per_split
        end_i = (k + 1) * months_per_split if k < n_splits - 1 else n
        if start_i >= end_i:
            continue
        period_idx = (idx >= idx[start_i]) & (idx <= idx[end_i - 1])
        period_ts = ts.loc[period_idx]
        start_str = pd.Timestamp(period_ts.index.min()).strftime("%Y-%m")
        end_str = pd.Timestamp(period_ts.index.max()).strftime("%Y-%m")
        col_name = f"{start_str} to {end_str}"
        result[col_name] = {q: sharpe_annualized(period_ts[q]) for q in qcols}
    df = pd.DataFrame(result)
    df.index.name = "decile"
    return df


def plot_split_sample_heatmap(data: dict, fig_dir: Path, n_splits: int = 4) -> None:
    """Heatmap: decile × period, color = Sharpe."""
    import matplotlib.pyplot as plt
    tbl = split_sample_multi_table(data, n_splits=n_splits)
    if tbl.empty:
        return
    fig, ax = plt.subplots(figsize=(max(8, tbl.shape[1] * 1.5), max(5, tbl.shape[0] * 0.5)))
    im = ax.imshow(tbl.values, aspect="auto", cmap="RdYlGn", vmin=-0.5, vmax=1.5)
    ax.set_xticks(range(tbl.shape[1]))
    ax.set_xticklabels(tbl.columns, rotation=45, ha="right")
    ax.set_yticks(range(tbl.shape[0]))
    ax.set_yticklabels(tbl.index)
    ax.set_xlabel("Period")
    ax.set_ylabel("Decile")
    ax.set_title("Split-sample Sharpe by decile and period")
    plt.colorbar(im, ax=ax, label="Sharpe")
    fig.tight_layout()
    fig.savefig(fig_dir / "split_sample_heatmap.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# 8) Long-short spread (cumulative)
# -------------------------------------------------------------------

def plot_long_short_spread(data: dict, fig_dir: Path):
    import matplotlib.pyplot as plt

    ts = data["ts"]
    N = data["N"]
    qcols = data["qcols"]
    # Spread: Q1 - QN (value minus growth). If we have Q1_Q5_spread or Q1_Q5_spread already in ts, use it.
    spread_col = "Q1_Q5_spread" if "Q1_Q5_spread" in ts.columns else None
    if spread_col is None:
        spread_col = f"Q1_Q5_spread"
        ts = ts.copy()
        ts[spread_col] = ts["Q1"] - ts[qcols[-1]]
    spread_series = ts[spread_col].dropna()
    cum_spread = (1 + spread_series).cumprod()

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(cum_spread.index, cum_spread.values, color="green", linewidth=2, label=f"Long Q1 / Short Q{N}")
    ax.axhline(1, color="gray", linestyle="--")
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative return")
    ax.set_title(f"Long Q1 / Short Q{N} spread (cumulative)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "long_short_spread.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# 9) Sector composition over time (stacked area per decile)
# -------------------------------------------------------------------

def plot_sector_composition(data: dict, fig_dir: Path):
    import matplotlib.pyplot as plt

    assignments = data["assignments"]
    qcols = data["qcols"]
    N = data["N"]
    # For each (date, decile), pct per sector
    agg = assignments.groupby(["date", "pcf_quintile", "sector"]).size().unstack(fill_value=0)
    pct = agg.div(agg.sum(axis=1), axis=0)
    pct = pct.reset_index()
    dates = pct["date"].unique()
    sectors = [c for c in pct.columns if c not in ("date", "pcf_quintile")]

    # One figure per decile (or 2x3 for 5, 2x5 for 10)
    nrows = (N + 1) // 2
    ncols = 2
    fig, axes = plt.subplots(nrows, ncols, figsize=(12, 3 * nrows))
    axes = np.atleast_2d(axes)
    for i, q in enumerate(range(1, N + 1)):
        r, c = i // ncols, i % ncols
        ax = axes[r, c]
        sub = pct[pct["pcf_quintile"] == q]
        sub = sub.set_index("date")[sectors].sort_index()
        sub.plot(kind="area", stacked=True, ax=ax, legend=(i == 0))
        ax.set_title(f"Q{q}")
        ax.set_xlabel("")
        if i != 0 and ax.legend_ is not None:
            ax.legend_.set_visible(False)
    for i in range(N, nrows * ncols):
        r, c = i // ncols, i % ncols
        axes[r, c].set_visible(False)
    fig.suptitle("Sector composition over time by decile")
    fig.tight_layout()
    fig.savefig(fig_dir / "sector_composition.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# 10) Holdings count per decile over time
# -------------------------------------------------------------------

def plot_holdings_count(data: dict, fig_dir: Path):
    import matplotlib.pyplot as plt

    assignments = data["assignments"]
    qcols = data["qcols"]
    N = data["N"]
    counts = assignments.groupby(["date", "pcf_quintile"]).size().unstack(fill_value=0)
    counts = counts[[q for q in range(1, N + 1)]]
    counts.columns = qcols

    fig, ax = plt.subplots(figsize=(10, 4))
    for col in qcols:
        ax.plot(counts.index, counts[col], label=col, alpha=0.8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Number of names")
    ax.set_title("Holdings count per decile over time")
    ax.legend(loc="best", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "holdings_count.png", dpi=150)
    plt.close(fig)


# -------------------------------------------------------------------
# Programmatic entrypoint (for experiment scripts to call)
# -------------------------------------------------------------------

def run_analysis(run_dir: Path, benchmark_path: Path | None = None, no_figures: bool = False) -> None:
    """Run full analysis (tables + figures) for an existing run. Call from experiment scripts after saving outputs."""
    run_dir = Path(run_dir)
    if not (run_dir / "metadata.json").exists():
        raise FileNotFoundError(f"Run directory missing metadata: {run_dir}")

    data = load_run(run_dir)
    fig_dir = ensure_fig_dir(run_dir)
    qcols = data["qcols"]

    benchmark_series = None
    if benchmark_path and Path(benchmark_path).exists():
        b = pd.read_csv(benchmark_path)
        if "date" in b.columns and "return" in b.columns:
            b["date"] = pd.to_datetime(b["date"])
            benchmark_series = b.set_index("date")["return"]
        elif "date" in b.columns and "ret" in b.columns:
            b["date"] = pd.to_datetime(b["date"])
            benchmark_series = b.set_index("date")["ret"]

    perf = performance_table(data)
    perf.to_csv(run_dir / "performance_table.csv", index=False)
    print("Performance table:")
    print(perf.to_string(index=False))

    if not no_figures and _has_mpl():
        plot_cumulative_log(data, fig_dir, benchmark_series)
        plot_annual_heatmap(data, fig_dir)
        drawdown_curves_and_table(data, fig_dir)
        plot_return_distribution(data, fig_dir)
        plot_long_short_spread(data, fig_dir)
        plot_sector_composition(data, fig_dir)
        plot_holdings_count(data, fig_dir)
        plot_split_sample_heatmap(data, fig_dir, n_splits=4)
    elif not no_figures and not _has_mpl():
        print("matplotlib not found; skipping figures.")

    turnover = turnover_table(data)
    turnover.to_csv(run_dir / "turnover_table.csv", index=False)
    print("\nTurnover:")
    print(turnover.to_string(index=False))

    split = split_sample_multi_table(data, n_splits=4)
    split.to_csv(run_dir / "split_sample.csv", index=True)
    print("\nSplit-sample Sharpe (all deciles × periods):")
    print(split.round(3).to_string())

    cumulative = data["cumulative"]
    worst_rows = []
    for col in qcols:
        cum = cumulative[col]
        dd = (cum - cum.cummax()) / cum.cummax()
        worst_rows.append({"decile": col, "max_drawdown": dd.min(), "date_of_worst": dd.idxmin()})
    pd.DataFrame(worst_rows).to_csv(run_dir / "worst_drawdowns.csv", index=False)

    print(f"\nAnalysis outputs in {run_dir}")
    print(f"  CSV: performance_table.csv, turnover_table.csv, split_sample.csv, worst_drawdowns.csv")
    if not no_figures and _has_mpl():
        print(f"  Figures: {fig_dir}/")


# -------------------------------------------------------------------
# Main (CLI)
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Analyze backtest run: tables + figures")
    parser.add_argument("run_dir", type=Path, help="e.g. experiments/runs/0001_dreman_pcf_quintile")
    parser.add_argument("--benchmark", type=Path, default=None, help="Optional: CSV with date, return for overlay")
    parser.add_argument("--no-figures", action="store_true", help="Only write CSV tables, no plots")
    args = parser.parse_args()

    run_dir = args.run_dir if args.run_dir.is_absolute() else (ROOT / args.run_dir)
    run_analysis(run_dir, benchmark_path=args.benchmark, no_figures=args.no_figures)


if __name__ == "__main__":
    main()
