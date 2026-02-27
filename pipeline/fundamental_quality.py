"""
Rolling fundamental quality metrics from ARQ (annual aggregation) and ARY where needed.
Used by 02_fundamentals: NCFO R²/CAGR, FCF CAGR, ROIC slope, gross margin slope,
net debt trend, dilution rate. PIT-correct: only data with datekey <= vintage.
"""
# SHARADAR SIGN CONVENTIONS (empirically verified in 00_testing capex check)
# - capex: negative (cash outflow), e.g. AAPL 2020: capex = -7,309,000,000
# - FCF reconstruction: ncfo + capex (adding negative = subtracting |capex|)
# - Verified: ncfo + capex matches Sharadar's pre-computed fcf field exactly
# DO NOT change to ncfo - capex

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

log = logging.getLogger(__name__)

# Minimum observations for metrics
# MIN_YEARS_NCFO = 5: companies need 5 years of positive NCFO before R2 is computed;
# downstream should handle NULLs for newer companies (exclude from quality-filtered strategies, not necessarily from universe)
MIN_YEARS_NCFO = 5
MIN_YEARS_FCF = 3
MIN_YEARS_ROIC_SLOPE = 2
MIN_QUARTERS_GM = 4
MIN_QUARTERS_NET_DEBT = 4

# Years of ARQ history to load before date_start so rolling 10y metrics have enough data
ARQ_LOOKBACK_YEARS = 10


def ncfo_r2_cagr(ncfo_annual: pd.Series) -> tuple[float | None, float | None, float | None]:
    """
    OLS R² of log(ncfo) ~ time, CAGR, and fraction of years with positive NCFO.
    Returns (r_squared, cagr, pct_positive). Needs at least MIN_YEARS_NCFO positive values.
    """
    ncfo = ncfo_annual.dropna()
    if len(ncfo) < MIN_YEARS_NCFO:
        return None, None, None
    pct_positive = float((ncfo > 0).sum() / len(ncfo))
    ncfo_pos = ncfo[ncfo > 0]
    if len(ncfo_pos) < MIN_YEARS_NCFO:
        return None, None, pct_positive
    ncfo_pos = ncfo_pos.sort_index()
    y = np.log(ncfo_pos.values)
    t = np.arange(len(y))
    slope, intercept, r_value, _, _ = stats.linregress(t, y)
    r_squared = r_value ** 2
    cagr = float(np.exp(slope) - 1.0)
    return float(r_squared), cagr, pct_positive


def fcf_cagr(fcf_annual: pd.Series) -> float | None:
    """Compound annual growth rate of FCF over last 5 years (or available)."""
    fcf = fcf_annual.dropna()
    if len(fcf) < MIN_YEARS_FCF:
        return None
    fcf = fcf.sort_index()
    first_val = fcf.iloc[0]
    last_val = fcf.iloc[-1]
    n = len(fcf) - 1
    if n <= 0 or first_val <= 0:
        return None
    if last_val <= 0:
        return None
    return float((last_val / first_val) ** (1.0 / n) - 1.0)


def r2_and_pct_positive(series: pd.Series, min_points: int = 3) -> tuple[float | None, float | None]:
    """R² of log(series) ~ time and fraction of periods positive. Returns (r2, pct_positive)."""
    s = series.dropna()
    if len(s) < min_points:
        return None, None
    pct_positive = float((s > 0).sum() / len(s))
    s_pos = s[s > 0]
    if len(s_pos) < min_points:
        return None, pct_positive
    s_pos = s_pos.sort_index()
    y = np.log(s_pos.values)
    t = np.arange(len(y))
    _, _, r_value, _, _ = stats.linregress(t, y)
    return float(r_value ** 2), pct_positive


def slope_series(series: pd.Series, min_points: int = 2) -> float | None:
    """Linear regression slope of series (index order)."""
    s = series.dropna()
    if len(s) < min_points:
        return None
    s = s.sort_index()
    x = np.arange(len(s))
    slope, _, _, _, _ = stats.linregress(x, s.values)
    return float(slope)


def dilution_rate(sharesbas_annual: pd.Series) -> float | None:
    """Annualized share count growth rate over available years. Positive = dilution, negative = buybacks."""
    s = sharesbas_annual.dropna()
    s = s[s > 0]
    if len(s) < 2:
        return None
    s = s.sort_index()
    first_val = s.iloc[0]
    last_val = s.iloc[-1]
    n_years = len(s) - 1
    return float((last_val / first_val) ** (1.0 / n_years) - 1.0)


def compute_quality_metrics_for_ticker(
    annual_df: pd.DataFrame,
    quarterly_df: pd.DataFrame | None,
) -> dict[str, Any]:
    """
    Given annual aggregates (fiscal_year, ncfo_annual, fcf_recon_annual, roic_avg, etc.)
    and optional quarterly (for gross margin 8q, net debt 8q), compute all quality metrics.
    NCFO and FCF series computed in parallel; includes pct_positive, r2_adjusted, delta.
    """
    out: dict[str, Any] = {
        "ncfo_r2_10y": None,
        "ncfo_cagr_10y": None,
        "ncfo_pct_positive": None,
        "ncfo_r2_adjusted": None,
        "fcf_cagr_5y": None,
        "fcf_r2_10y": None,
        "fcf_pct_positive": None,
        "fcf_r2_adjusted": None,
        "fcf_ncfo_r2_delta": None,
        "roic_level": None,
        "roic_slope_3y": None,
        "grossmargin_slope": None,
        "net_debt_trend": None,
        "dilution_rate": None,
    }
    if annual_df is None or annual_df.empty:
        return out

    annual_df = annual_df.sort_values("fiscal_year")
    ncfo_col = "ncfo_annual" if "ncfo_annual" in annual_df.columns else "ncfo"
    if ncfo_col in annual_df.columns:
        ncfo_10 = annual_df[ncfo_col].tail(10)
        r2, cagr, pct_pos = ncfo_r2_cagr(ncfo_10)
        out["ncfo_r2_10y"] = r2
        out["ncfo_cagr_10y"] = cagr
        out["ncfo_pct_positive"] = pct_pos
        out["ncfo_r2_adjusted"] = (r2 * pct_pos) if (r2 is not None and pct_pos is not None) else None

    fcf_recon_col = "fcf_recon_annual" if "fcf_recon_annual" in annual_df.columns else None
    fcf_col = fcf_recon_col or ("fcf_annual" if "fcf_annual" in annual_df.columns else "fcf")
    if fcf_col in annual_df.columns:
        fcf_5y = annual_df[fcf_col].tail(5)
        out["fcf_cagr_5y"] = fcf_cagr(fcf_5y)
        fcf_10y = annual_df[fcf_col].tail(10)
        fcf_r2, fcf_pct = r2_and_pct_positive(fcf_10y, min_points=MIN_YEARS_FCF)
        out["fcf_r2_10y"] = fcf_r2
        out["fcf_pct_positive"] = fcf_pct
        out["fcf_r2_adjusted"] = (fcf_r2 * fcf_pct) if (fcf_r2 is not None and fcf_pct is not None) else None
        ncfo_adj = out.get("ncfo_r2_adjusted")
        fcf_adj = out["fcf_r2_adjusted"]
        if ncfo_adj is not None and fcf_adj is not None:
            out["fcf_ncfo_r2_delta"] = round(ncfo_adj - fcf_adj, 4)
        else:
            out["fcf_ncfo_r2_delta"] = None

    if "roic_avg" in annual_df.columns:
        out["roic_level"] = annual_df["roic_avg"].iloc[-1] if len(annual_df) else None
        roic_3y = annual_df["roic_avg"].tail(3)
        out["roic_slope_3y"] = slope_series(roic_3y, MIN_YEARS_ROIC_SLOPE)

    # Sharadar ARQ grossmargin: assume ratio (0-1). If your data has gross profit dollars, use gp/revenue instead.
    if quarterly_df is not None and not quarterly_df.empty and "grossmargin" in quarterly_df.columns:
        gm_8q = quarterly_df["grossmargin"].tail(8)
        out["grossmargin_slope"] = slope_series(gm_8q, MIN_QUARTERS_GM)

    if quarterly_df is not None and not quarterly_df.empty and "net_debt" in quarterly_df.columns:
        nd_8q = quarterly_df["net_debt"].tail(8)
        out["net_debt_trend"] = slope_series(nd_8q, MIN_QUARTERS_NET_DEBT)

    if "sharesbas_annual" in annual_df.columns:
        out["dilution_rate"] = dilution_rate(annual_df["sharesbas_annual"])

    return out


def compute_quality_metrics_table(
    con: Any,
    date_start: str,
    date_end: str,
    universe_tickers: set[str] | None = None,
) -> pd.DataFrame:
    """
    Build quality_metrics table: one row per (ticker, datekey) for each filing date.
    PIT-correct: per-ticker quarterly state updated only with new filings each vintage;
    DataFrame build and groupby are scoped to tickers that filed (no full-dataset
    aggregation). New filings processed via to_dict("records") to avoid iterrows() overhead.
    When universe_tickers is provided, only those tickers are loaded and computed.
    """
    try:
        # Explicit column introspection for grossmargin (avoids swallowing connection/syntax errors)
        desc = con.execute("DESCRIBE sf1").df()
        sf1_columns = desc["column_name"].tolist() if "column_name" in desc.columns else []
        has_gp = "gp" in sf1_columns
        has_grossmargin = "grossmargin" in sf1_columns
        if has_grossmargin and has_gp:
            grossmargin_expr = (
                "COALESCE("
                "CASE WHEN grossmargin BETWEEN 0 AND 2 THEN grossmargin ELSE NULL END, "
                "gp / NULLIF(revenue, 0)"
                ") AS grossmargin"
            )
        elif has_grossmargin:
            grossmargin_expr = "grossmargin"
        elif has_gp:
            grossmargin_expr = "gp / NULLIF(revenue, 0) AS grossmargin"
        else:
            grossmargin_expr = "CAST(NULL AS DOUBLE) AS grossmargin"
        ticker_filter = ""
        if universe_tickers:
            # Quote tickers for SQL; avoid empty IN
            ticker_list = ",".join(repr(t) for t in sorted(universe_tickers))
            ticker_filter = f" AND ticker IN ({ticker_list})"
        all_arq = con.execute(f"""
            SELECT
                ticker, reportperiod, datekey,
                ncfo, fcf, capex, roic, sharesbas,
                revenue, debt, cashnequsd,
                {grossmargin_expr}
            FROM sf1
            WHERE dimension = 'ARQ'
            AND CAST(datekey AS DATE) BETWEEN ('{date_start}'::DATE - INTERVAL '{ARQ_LOOKBACK_YEARS} years') AND '{date_end}'::DATE  -- lookback required for 10y rolling ncfo_r2
            {ticker_filter}
            ORDER BY ticker, datekey
        """).df()
    except Exception as e:
        log.warning("ARQ pull failed (quality_metrics will be empty; ncfo_r2_adjusted etc. will be all null): %s", e)
        return pd.DataFrame()

    if all_arq.empty:
        log.warning(
            "No ARQ rows from sf1 for date range / tickers (quality_metrics will be empty; ncfo_r2_adjusted will be all null in master). Check SF1 has dimension='ARQ' and ncfo/fcf/capex columns."
        )
        return pd.DataFrame()

    all_arq["reportperiod"] = pd.to_datetime(all_arq["reportperiod"])
    all_arq["datekey"] = pd.to_datetime(all_arq["datekey"])
    all_arq["fiscal_year"] = all_arq["reportperiod"].dt.year
    all_arq["net_debt"] = all_arq["debt"] - all_arq["cashnequsd"]

    vintages = sorted(all_arq["datekey"].unique())
    vintage_to_tickers = (
        all_arq.groupby("datekey")["ticker"]
        .apply(set)
        .to_dict()
    )

    # Per-ticker quarterly accumulation: ticker -> { reportperiod -> record }
    # Avoids full pd.DataFrame(current_pit.values()) every vintage.
    ticker_quarters: dict[str, dict[Any, dict]] = {}
    rows = []

    try:
        from tqdm import tqdm
        vintage_iter = tqdm(vintages, desc="Quality metrics", unit="vintage")
    except ImportError:
        vintage_iter = vintages

    for vintage in vintage_iter:
        new_filings = all_arq[all_arq["datekey"] == vintage]
        for record in new_filings.to_dict("records"):
            ticker = record["ticker"]
            period = record["reportperiod"]
            if ticker not in ticker_quarters:
                ticker_quarters[ticker] = {}
            existing = ticker_quarters[ticker].get(period)
            if existing is None or record["datekey"] >= existing["datekey"]:
                ticker_quarters[ticker][period] = record

        tickers_to_update = vintage_to_tickers.get(vintage, set())
        if not tickers_to_update:
            continue

        for ticker in tickers_to_update:
            if ticker not in ticker_quarters:
                continue
            ticker_quarterly = pd.DataFrame(ticker_quarters[ticker].values())
            if ticker_quarterly.empty:
                continue
            ticker_quarterly["reportperiod"] = pd.to_datetime(ticker_quarterly["reportperiod"])
            ticker_quarterly = ticker_quarterly.sort_values("reportperiod")
            ticker_quarterly["fiscal_year"] = ticker_quarterly["reportperiod"].dt.year

            ticker_annual = (
                ticker_quarterly.groupby("fiscal_year", as_index=False)
                .agg(
                    ncfo_annual=("ncfo", "sum"),
                    fcf_annual=("fcf", "sum"),
                    capex_annual=("capex", "sum"),
                    roic_avg=("roic", "mean"),
                    sharesbas_annual=("sharesbas", "max"),
                    quarters_in_year=("ncfo", "count"),
                )
                .query("quarters_in_year >= 3")
                .sort_values("fiscal_year")
            )
            if ticker_annual.empty:
                continue
            ticker_annual["fcf_recon_annual"] = (
                ticker_annual["ncfo_annual"] + ticker_annual["capex_annual"]
            )

            metrics = compute_quality_metrics_for_ticker(
                ticker_annual,
                ticker_quarterly,
            )
            # Emit only vintages inside the pipeline window; we still accumulate state for earlier vintages
            if pd.Timestamp(date_start) <= vintage <= pd.Timestamp(date_end):
                rows.append({
                    "ticker": ticker,
                    "datekey": vintage,
                    "ncfo_r2_10y": metrics["ncfo_r2_10y"],
                    "ncfo_cagr_10y": metrics["ncfo_cagr_10y"],
                    "ncfo_pct_positive": metrics.get("ncfo_pct_positive"),
                    "ncfo_r2_adjusted": metrics.get("ncfo_r2_adjusted"),
                    "fcf_cagr_5y": metrics["fcf_cagr_5y"],
                    "fcf_r2_10y": metrics.get("fcf_r2_10y"),
                    "fcf_pct_positive": metrics.get("fcf_pct_positive"),
                    "fcf_r2_adjusted": metrics.get("fcf_r2_adjusted"),
                    "fcf_ncfo_r2_delta": metrics.get("fcf_ncfo_r2_delta"),
                    "roic_level": metrics["roic_level"],
                    "roic_slope_3y": metrics["roic_slope_3y"],
                    "grossmargin_slope": metrics["grossmargin_slope"],
                    "net_debt_trend": metrics["net_debt_trend"],
                    "dilution_rate": metrics["dilution_rate"],
                })

    return pd.DataFrame(rows)


def validate_quality_sanity(
    quality_path: str = "outputs/features/fundamental_pit.parquet",
) -> None:
    """
    Sanity check after running compute_quality_metrics_table (e.g. on debug tickers AAPL, GE):
    AAPL should rank higher quality than GE (ncfo_r2_adjusted). Run after 02_fundamentals
    writes fundamental_pit.parquet.
    """
    quality = pd.read_parquet(quality_path)
    aapl = quality[quality["ticker"] == "AAPL"]["ncfo_r2_adjusted"].dropna()
    ge = quality[quality["ticker"] == "GE"]["ncfo_r2_adjusted"].dropna()
    print(f"AAPL median ncfo_r2_adjusted: {aapl.median():.3f}")  # expect > 0.80
    print(f"GE median ncfo_r2_adjusted: {ge.median():.3f}")  # expect < 0.40
    date_col = "date" if "date" in quality.columns else "datekey"
    ge_recent = quality[(quality["ticker"] == "GE") & (quality[date_col] > "2019-01-01")]
    if not ge_recent.empty:
        print(f"GE 2019+ ncfo_r2_adjusted (first): {ge_recent['ncfo_r2_adjusted'].dropna().head(1).values}")
    assert aapl.median() > ge.median(), "AAPL should rank higher quality than GE"
