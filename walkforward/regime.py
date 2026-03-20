"""
VIX regime filter for walk-forward backtests: zero returns when VIX >= threshold.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from walkforward.evaluation import oos_drawdown_and_cagr


@dataclass
class RegimeFilterResult:
    """Result of applying a VIX regime filter to OOS returns."""

    oos_rets_filtered: pd.Series  # same index as input; 0 where out of market
    in_market: pd.Series  # boolean, index = oos_rets.index
    sharpe_filtered: float
    max_dd_filtered: float
    cagr_filtered: float
    pct_in_market: float  # 0-100
    vix_mean_in: float  # mean VIX when in market (for display)
    vix_mean_out: float  # mean VIX when out (for display)


def apply_vix_regime_filter(
    oos_rets: pd.Series,
    vix_aligned: pd.Series,
    *,
    vix_threshold: float = 35,
    periods_per_year: float = 12,
) -> RegimeFilterResult:
    """
    Zero out returns when vix_aligned >= vix_threshold; compute filtered stats.
    vix_aligned must have the same index as oos_rets.
    """
    in_market = (vix_aligned < vix_threshold) & vix_aligned.notna()
    oos_rets_filtered = oos_rets.copy()
    oos_rets_filtered.loc[~in_market] = 0.0

    if oos_rets_filtered.std() > 0 and len(oos_rets_filtered) > 0:
        sharpe_filtered = (
            oos_rets_filtered.mean()
            / oos_rets_filtered.std()
            * (periods_per_year ** 0.5)
        )
    else:
        sharpe_filtered = np.nan
    max_dd_filtered, cagr_filtered = oos_drawdown_and_cagr(
        oos_rets_filtered, periods_per_year=periods_per_year
    )

    n = len(in_market)
    pct_in_market = 100.0 * in_market.sum() / n if n else 0.0

    vix_mean_in = float(vix_aligned.loc[in_market].mean()) if in_market.any() else np.nan
    out_market = ~in_market & vix_aligned.notna()
    vix_mean_out = float(vix_aligned.loc[out_market].mean()) if out_market.any() else np.nan

    return RegimeFilterResult(
        oos_rets_filtered=oos_rets_filtered,
        in_market=in_market,
        sharpe_filtered=sharpe_filtered,
        max_dd_filtered=max_dd_filtered,
        cagr_filtered=cagr_filtered,
        pct_in_market=pct_in_market,
        vix_mean_in=vix_mean_in,
        vix_mean_out=vix_mean_out,
    )


def load_vix_and_apply_regime_filter(
    oos_rets: pd.Series,
    conn,
    sfp_path: Path,
    *,
    vix_ticker: str = "^VIX",
    vix_threshold: float = 35,
    periods_per_year: float = 12,
) -> RegimeFilterResult:
    """
    Load VIX from SFP, align to oos_rets (month-end), then apply regime filter.
    Raises if SFP missing or VIX empty. Calls apply_vix_regime_filter internally.
    """
    if not sfp_path.exists():
        raise FileNotFoundError(f"SFP.parquet not found: {sfp_path}")

    vix_df = conn.execute(
        """
        SELECT date, closeadj
        FROM read_parquet(?)
        WHERE ticker = ?
        ORDER BY date
        """,
        [str(sfp_path), vix_ticker],
    ).df()

    if vix_df.empty:
        raise ValueError(f"VIX not found in SFP (ticker={vix_ticker})")

    vix_df["date"] = pd.to_datetime(vix_df["date"]).dt.normalize()
    vix_df = vix_df.set_index("date").sort_index()
    vix_monthly = vix_df.resample("ME").last().dropna(subset=["closeadj"])

    if vix_monthly.empty:
        raise ValueError("VIX monthly series empty")

    month_ends = oos_rets.index + pd.offsets.MonthEnd(0)
    vix_aligned = vix_monthly["closeadj"].reindex(month_ends)
    vix_aligned.index = oos_rets.index

    return apply_vix_regime_filter(
        oos_rets,
        vix_aligned,
        vix_threshold=vix_threshold,
        periods_per_year=periods_per_year,
    )


def print_regime_backtest(
    strategy_label: str,
    sharpe_raw: float,
    dd_raw: float,
    cagr_raw: float,
    filtered: RegimeFilterResult,
) -> None:
    """Print raw vs regime-filtered stats (for script/notebook display)."""
    print("  Mean VIX when in market: %.1f  when out: %.1f" % (filtered.vix_mean_in, filtered.vix_mean_out))
    print("%s — raw vs regime filter (VIX < 35)" % strategy_label)
    print(
        "  Raw:     Sharpe %.3f  MaxDD %.1f%%  CAGR %.1f%%"
        % (sharpe_raw, dd_raw * 100, cagr_raw * 100)
    )
    print(
        "  Filtered: Sharpe %.3f  MaxDD %.1f%%  CAGR %.1f%%"
        % (filtered.sharpe_filtered, filtered.max_dd_filtered * 100, filtered.cagr_filtered * 100)
    )
    print("  %% months in market: %.1f" % filtered.pct_in_market)


def plot_raw_vs_filtered(
    oos_rets_raw: pd.Series,
    oos_rets_filtered: pd.Series,
    title: str,
    *,
    raw_label: str | None = None,
    filtered_label: str | None = None,
    oos_rets_spy: pd.Series | None = None,
) -> None:
    """Plot cumulative raw vs filtered returns (for script/notebook display).
    If oos_rets_spy is provided, plot SPY cumulative on the same axes for comparison.
    """
    import matplotlib.pyplot as plt

    cum_raw = (1 + oos_rets_raw).cumprod()
    cum_filtered = (1 + oos_rets_filtered).cumprod()
    plt.figure(figsize=(8, 4))
    plt.plot(
        cum_raw.index,
        cum_raw.values,
        label=raw_label or "raw",
        alpha=0.8,
    )
    plt.plot(
        cum_filtered.index,
        cum_filtered.values,
        label=filtered_label or "regime filter",
        alpha=0.8,
    )
    if oos_rets_spy is not None and len(oos_rets_spy) > 0:
        cum_spy = (1 + oos_rets_spy).cumprod()
        plt.plot(
            cum_spy.index,
            cum_spy.values,
            label="SPY",
            color="black",
            linestyle="--",
            linewidth=1.2,
            alpha=0.9,
        )
    plt.xlabel("Date")
    plt.ylabel("Cumulative return")
    plt.title(title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
