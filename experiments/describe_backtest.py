#!/usr/bin/env python3
"""
Descriptive backtest report: characterise what each bucket did — no spreads,
no trading logic, no benchmark. Output is a single Markdown report plus figures.

Usage:
  python experiments/describe_backtest.py experiments/runs/0001_dreman_pcf_quintile
  python experiments/describe_backtest.py experiments/runs/0004_pcf_quality_quintile

Produces:
  - report.md   Single markdown document with tables + embedded figures
  - figures/    PNGs for cumulative curves, heatmap, return dist, drawdown,
                holdings count, sector composition, rolling Sharpe

Sections in report:
  1. Per-bucket summary — CAGR, ann vol, Sharpe, Sortino, max DD, win rate
  2. Cumulative equity curves (log scale)
  3. Annual returns heatmap (bucket × year)
  4. Monthly return distributions — KDE/histogram + skewness & kurtosis table
  5. Drawdown curves per bucket
  6. Holdings count per bucket per date
  7. Sector composition per bucket (stacked area)
  8. Rolling Sharpe (36-month window) per bucket
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


# ---------------------------------------------------------------------------
# Load run
# ---------------------------------------------------------------------------

def load_run(run_dir: Path) -> dict:
    run_dir = Path(run_dir)
    assignments = pd.read_parquet(run_dir / "quintile_assignments.parquet")
    ts = pd.read_parquet(run_dir / "quintile_returns_ts.parquet")
    cumulative = pd.read_parquet(run_dir / "cumulative_returns.parquet")
    summary = pd.read_parquet(run_dir / "quintile_summary.parquet")
    with open(run_dir / "metadata.json") as f:
        meta = json.load(f)
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


def _has_mpl() -> bool:
    try:
        import matplotlib.pyplot  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# 1) Per-bucket summary table
# ---------------------------------------------------------------------------

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
    qcols = data["qcols"]
    n_years = (ts.index[-1] - ts.index[0]).days / 365.25 if len(ts) > 0 else 0

    rows = []
    for col in qcols:
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
            "bucket": col,
            "CAGR": cagr,
            "ann_vol": ann_vol,
            "Sharpe": sharpe,
            "Sortino": sortino,
            "max_dd": max_dd,
            "win_rate_pct": win_rate * 100,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 2) Cumulative equity curves (log scale)
# ---------------------------------------------------------------------------

def plot_cumulative_log(data: dict, fig_dir: Path) -> None:
    import matplotlib.pyplot as plt
    cumulative = data["cumulative"]
    qcols = data["qcols"]
    fig, ax = plt.subplots(figsize=(10, 6))
    for col in qcols:
        ax.semilogy(cumulative.index, cumulative[col], label=col, alpha=0.9)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative return (log scale)")
    ax.set_title("Cumulative returns by bucket (log scale)")
    ax.legend(loc="best", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "cumulative_returns_log.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 3) Annual returns heatmap
# ---------------------------------------------------------------------------

def plot_annual_heatmap(data: dict, fig_dir: Path) -> None:
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
    ax.set_ylabel("Bucket")
    ax.set_title("Annual returns by bucket (row = bucket, col = year)")
    plt.colorbar(im, ax=ax, label="Return")
    fig.tight_layout()
    fig.savefig(fig_dir / "annual_returns_heatmap.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 4) Monthly return distributions + skew/kurtosis
# ---------------------------------------------------------------------------

def return_distribution_stats(data: dict) -> pd.DataFrame:
    ts = data["ts"]
    qcols = data["qcols"]
    rows = []
    for col in qcols:
        monthly = ts[col].dropna()
        rows.append({
            "bucket": col,
            "skewness": float(monthly.skew()),
            "kurtosis": float(monthly.kurtosis()),
            "n_months": len(monthly),
        })
    return pd.DataFrame(rows)


def plot_return_distribution(data: dict, fig_dir: Path) -> None:
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
    fig.suptitle("Monthly return distribution by bucket")
    fig.tight_layout()
    fig.savefig(fig_dir / "return_distribution.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 5) Drawdown curves
# ---------------------------------------------------------------------------

def plot_drawdown_curves(data: dict, fig_dir: Path) -> None:
    import matplotlib.pyplot as plt
    cumulative = data["cumulative"]
    qcols = data["qcols"]
    fig, ax = plt.subplots(figsize=(10, 6))
    for col in qcols:
        cum = cumulative[col]
        dd = (cum - cum.cummax()) / cum.cummax()
        ax.fill_between(dd.index, dd, 0, alpha=0.5, label=col)
        ax.plot(dd.index, dd, alpha=0.8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Drawdown")
    ax.set_title("Drawdown over time by bucket")
    ax.legend(loc="lower left", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "drawdown_curves.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 6) Holdings count per bucket per date
# ---------------------------------------------------------------------------

def plot_holdings_count(data: dict, fig_dir: Path) -> None:
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
    ax.set_title("Holdings count per bucket over time")
    ax.legend(loc="best", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "holdings_count.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 7) Sector composition per bucket
# ---------------------------------------------------------------------------

def plot_sector_composition(data: dict, fig_dir: Path) -> None:
    import matplotlib.pyplot as plt
    assignments = data["assignments"]
    N = data["N"]
    qcols = data["qcols"]
    agg = assignments.groupby(["date", "pcf_quintile", "sector"]).size().unstack(fill_value=0)
    pct = agg.div(agg.sum(axis=1), axis=0)
    pct = pct.reset_index()
    sectors = [c for c in pct.columns if c not in ("date", "pcf_quintile")]

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
    fig.suptitle("Sector composition over time by bucket")
    fig.tight_layout()
    fig.savefig(fig_dir / "sector_composition.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 8) Rolling Sharpe (36-month)
# ---------------------------------------------------------------------------

def rolling_sharpe_series(monthly_ret: pd.Series, window: int = 36) -> pd.Series:
    ann_ret = monthly_ret.rolling(window).mean() * 12
    ann_vol = monthly_ret.rolling(window).std() * np.sqrt(12)
    return ann_ret / ann_vol.replace(0, np.nan)


def plot_rolling_sharpe(data: dict, fig_dir: Path, window: int = 36) -> None:
    import matplotlib.pyplot as plt
    ts = data["ts"]
    qcols = data["qcols"]
    fig, ax = plt.subplots(figsize=(10, 5))
    for col in qcols:
        rs = rolling_sharpe_series(ts[col], window=window)
        ax.plot(rs.index, rs.values, label=col, alpha=0.9)
    ax.axhline(0, color="gray", linestyle="--")
    ax.set_xlabel("Date")
    ax.set_ylabel("Rolling Sharpe (annualized)")
    ax.set_title(f"Rolling {window}-month Sharpe by bucket")
    ax.legend(loc="best", ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(fig_dir / "rolling_sharpe.png", dpi=150)
    plt.close(fig)


def rolling_sharpe_summary_table(data: dict, window: int = 36) -> pd.DataFrame:
    ts = data["ts"]
    qcols = data["qcols"]
    rows = []
    for col in qcols:
        rs = rolling_sharpe_series(ts[col], window=window).dropna()
        rows.append({
            "bucket": col,
            "rolling_sharpe_mean": rs.mean(),
            "rolling_sharpe_min": rs.min(),
            "rolling_sharpe_max": rs.max(),
            "rolling_sharpe_std": rs.std(),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

def df_to_markdown_table(df: pd.DataFrame, float_fmt: str = ".4f") -> str:
    """Simple dataframe to markdown table (no alignment)."""
    def _fmt(x):
        if isinstance(x, (int, np.integer)):
            return str(x)
        if isinstance(x, (float, np.floating)):
            return f"{x:{float_fmt}}"
        return str(x)

    headers = list(df.columns)
    lines = ["| " + " | ".join(str(h) for h in headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(_fmt(row[h]) for h in headers) + " |")
    return "\n".join(lines)


def write_report(data: dict, fig_dir: Path, run_dir: Path, figures_available: bool = True) -> None:
    meta = data["meta"]
    N = data["N"]
    run_id = meta.get("experiment_id", "unknown")
    description = meta.get("description", "")
    date_range = meta.get("date_range", "")

    perf = performance_table(data)
    dist_stats = return_distribution_stats(data)
    roll_summary = rolling_sharpe_summary_table(data, window=36)
    from analyze_backtest import split_sample_multi_table
    split_multi = split_sample_multi_table(data, n_splits=4).round(3)
    split_multi_for_md = split_multi.reset_index() if not split_multi.empty else pd.DataFrame()
    split_multi_text = split_multi.to_string() if not split_multi.empty else "(no data)"

    # Format perf for markdown (percentages where useful)
    perf_md = perf.copy()
    perf_md["CAGR"] = perf_md["CAGR"].map(lambda x: f"{x:.2%}")
    perf_md["ann_vol"] = perf_md["ann_vol"].map(lambda x: f"{x:.2%}")
    perf_md["max_dd"] = perf_md["max_dd"].map(lambda x: f"{x:.2%}")
    perf_md["win_rate_pct"] = perf_md["win_rate_pct"].map(lambda x: f"{x:.1f}%")
    perf_md["Sharpe"] = perf_md["Sharpe"].map(lambda x: f"{x:.2f}")
    perf_md["Sortino"] = perf_md["Sortino"].map(lambda x: f"{x:.2f}")

    rel = "figures"
    img = lambda name: f"![{name}]({rel}/{name})" if figures_available else f"*Figure: {name} (run with matplotlib to generate)*"

    sections = [
        "# Descriptive backtest report",
        "",
        f"**Run:** `{run_id}`  \n**Description:** {description}  \n**Date range:** {date_range}  \n**Buckets:** {N} (Q1 = cheapest, Q{N} = most expensive)",
        "",
        "---",
        "",
        "## 1. Per-bucket summary",
        "",
        "CAGR, annualized volatility, Sharpe, Sortino, max drawdown, and win rate (% of months with positive return).",
        "",
        df_to_markdown_table(perf_md, ".2f"),
        "",
        "---",
        "",
        "## 2. Cumulative equity curves (log scale)",
        "",
        "Whether outperformance is steady or regime-dependent.",
        "",
        img("cumulative_returns_log.png"),
        "",
        "---",
        "",
        "## 3. Annual returns heatmap",
        "",
        "Bucket × year. Are mid-buckets consistently better or lumpy?",
        "",
        img("annual_returns_heatmap.png"),
        "",
        "---",
        "",
        "## 4. Monthly return distributions",
        "",
        "KDE/histogram per bucket. Quality filter compressing the left tail in mid-buckets vs Q1 would show here. Skewness and kurtosis below.",
        "",
        img("return_distribution.png"),
        "",
        "**Skewness & kurtosis (monthly returns):**",
        "",
        df_to_markdown_table(dist_stats, ".3f"),
        "",
        "---",
        "",
        "## 5. Drawdown curves",
        "",
        "Peak-to-trough and recovery by bucket.",
        "",
        img("drawdown_curves.png"),
        "",
        "---",
        "",
        "## 6. Holdings count per bucket over time",
        "",
        "Check that no bucket is thin in certain periods (spurious results).",
        "",
        img("holdings_count.png"),
        "",
        "---",
        "",
        "## 7. Sector composition per bucket",
        "",
        "Stacked area: are certain buckets sector bets in disguise?",
        "",
        img("sector_composition.png"),
        "",
        "---",
        "",
        "## 8. Rolling Sharpe (36-month window)",
        "",
        "Signal stability over time rather than a single full-period number.",
        "",
        img("rolling_sharpe.png"),
        "",
        "**Rolling Sharpe summary:**",
        "",
        df_to_markdown_table(roll_summary, ".3f"),
        "",
        "---",
        "",
        "## 9. Split-sample (all deciles × multiple periods)",
        "",
        "Sharpe by decile in each of 4 equal-length sub-periods over the full data range.",
        "",
        img("split_sample_heatmap.png"),
        "",
        "**Sharpe by decile and period (markdown table):**",
        "",
        (df_to_markdown_table(split_multi_for_md, ".3f") if not split_multi_for_md.empty else "*No data*"),
        "",
        "**Plain text (Sharpe by decile × period):**",
        "",
        "```",
        split_multi_text,
        "```",
        "",
    ]

    report_path = run_dir / "report.md"
    report_path.write_text("\n".join(sections), encoding="utf-8")


# ---------------------------------------------------------------------------
# Programmatic entrypoint (for experiment scripts to call)
# ---------------------------------------------------------------------------

def run_report(run_dir: Path) -> None:
    """Generate report.md and figures for an existing run. Call from experiment scripts after saving outputs."""
    run_dir = Path(run_dir)
    if not (run_dir / "metadata.json").exists():
        raise FileNotFoundError(f"Run directory missing metadata: {run_dir}")

    data = load_run(run_dir)
    fig_dir = ensure_fig_dir(run_dir)

    perf = performance_table(data)
    perf.to_csv(run_dir / "performance_table.csv", index=False)
    return_distribution_stats(data).to_csv(run_dir / "return_distribution_stats.csv", index=False)
    rolling_sharpe_summary_table(data, window=36).to_csv(run_dir / "rolling_sharpe_summary.csv", index=False)

    has_mpl = _has_mpl()
    if has_mpl:
        plot_cumulative_log(data, fig_dir)
        plot_annual_heatmap(data, fig_dir)
        plot_return_distribution(data, fig_dir)
        plot_drawdown_curves(data, fig_dir)
        plot_holdings_count(data, fig_dir)
        plot_sector_composition(data, fig_dir)
        plot_rolling_sharpe(data, fig_dir, window=36)
        import analyze_backtest
        analyze_backtest.plot_split_sample_heatmap(data, fig_dir, n_splits=4)
    write_report(data, fig_dir, run_dir, figures_available=has_mpl)
    print(f"Report written: {run_dir / 'report.md'}")
    if has_mpl:
        print(f"Figures: {fig_dir}/")
    else:
        print("matplotlib not found; figures not generated. Report contains tables only.")
    # Text output: split-sample table
    from analyze_backtest import split_sample_multi_table
    split_tbl = split_sample_multi_table(data, n_splits=4).round(3)
    if not split_tbl.empty:
        print("\nSplit-sample Sharpe (all deciles × periods):")
        print(split_tbl.to_string())


# ---------------------------------------------------------------------------
# Main (CLI)
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Descriptive backtest report (tables + figures → report.md)")
    parser.add_argument("run_dir", type=Path, help="e.g. experiments/runs/0001_dreman_pcf_quintile")
    args = parser.parse_args()

    run_dir = args.run_dir if args.run_dir.is_absolute() else (ROOT / args.run_dir)
    run_report(run_dir)
    perf = performance_table(load_run(run_dir))
    print("\nPer-bucket summary:")
    print(perf.to_string(index=False))


if __name__ == "__main__":
    main()
