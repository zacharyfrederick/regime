"""
Rolling fundamental quality metrics from ARY (annual) and ARQ (quarterly only for 8Q metrics).
Used by 02_fundamentals: NCFO R²/CAGR, FCF CAGR, ROIC slope, gross margin slope,
net debt trend, dilution rate. PIT-correct: only data with datekey <= vintage.

Annual series: ARY (Sharadar annual, excluding restatements). Pre-aggregated by fiscal year,
avoids fiscal-year-boundary bugs from grouping ARQ by reportperiod.year. Amendments (10-K/A)
handled by keeping latest datekey per (ticker, reportperiod) at each vintage.
Quarterly: ARQ only for last-8-quarters gross margin slope and net debt trend.
"""
# SHARADAR SIGN CONVENTIONS (empirically verified in 00_testing capex check)
# - capex: negative (cash outflow), e.g. AAPL 2020: capex = -7,309,000,000
# - FCF reconstruction: ncfo + capex (adding negative = subtracting |capex|)
# - Verified: ncfo + capex matches Sharadar's pre-computed fcf field exactly
# DO NOT change to ncfo - capex

from __future__ import annotations

import logging
from typing import Any, Union

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def _fiscal_year_from_period(period: Any) -> int:
    """Extract fiscal year from reportperiod (Timestamp, datetime64, or date-like)."""
    if hasattr(period, "year"):
        return int(period.year)
    return int(pd.Timestamp(period).year)


def rebuild_annual_from_quarters(quarters: dict[Any, dict]) -> list[dict]:
    """
    Group quarter dicts by fiscal year, aggregate, filter >= 3 quarters per year.
    Returns list of annual dicts sorted by fiscal_year (no DataFrame). PIT-correct:
    same logic as groupby().agg().query('quarters_in_year >= 3'), handles amendments
    by rebuilding from current quarter state.
    """
    by_year: dict[int, list[dict]] = {}
    for q in quarters.values():
        fy = _fiscal_year_from_period(q["reportperiod"])
        by_year.setdefault(fy, []).append(q)

    annual_list: list[dict] = []
    for fy in sorted(by_year.keys()):
        qs = by_year[fy]
        if len(qs) < 3:
            continue
        ncfo_annual = sum(_v(q.get("ncfo")) for q in qs)
        fcf_annual = sum(_v(q.get("fcf")) for q in qs)
        capex_annual = sum(_v(q.get("capex")) for q in qs)
        roics = [q["roic"] for q in qs if q.get("roic") is not None and not (isinstance(q["roic"], float) and np.isnan(q["roic"]))]
        roic_avg = float(np.mean(roics)) if roics else None
        sharesbas_vals = [q["sharesbas"] for q in qs if q.get("sharesbas") is not None]
        sharesbas_annual = max(sharesbas_vals) if sharesbas_vals else None
        annual_list.append({
            "fiscal_year": fy,
            "ncfo_annual": ncfo_annual,
            "fcf_annual": fcf_annual,
            "capex_annual": capex_annual,
            "roic_avg": roic_avg,
            "sharesbas_annual": sharesbas_annual,
            "quarters_in_year": len(qs),
            "fcf_recon_annual": ncfo_annual + capex_annual,
        })
    return annual_list


def ary_records_to_annual_list(ary_records: dict[Any, dict]) -> list[dict]:
    """
    Convert ARY records (reportperiod -> row) to list of annual dicts for metric functions.
    ARY is already one row per fiscal year; reportperiod is the fiscal year-end date.
    PIT: caller passes the deduplicated map (latest datekey per reportperiod at vintage).
    """
    if not ary_records:
        return []
    out: list[dict] = []
    for period, r in ary_records.items():
        fy = _fiscal_year_from_period(period)
        ncfo = r.get("ncfo")
        capex = r.get("capex")
        roic = r.get("roic")
        roic_avg = roic if (roic is not None and not (isinstance(roic, float) and np.isnan(roic))) else None
        out.append({
            "fiscal_year": fy,
            "ncfo_annual": ncfo if ncfo is not None and not (isinstance(ncfo, float) and np.isnan(ncfo)) else None,
            "fcf_recon_annual": _v(ncfo) + _v(capex),
            "roic_avg": roic_avg,
            "sharesbas_annual": r.get("sharesbas"),
        })
    return sorted(out, key=lambda x: x["fiscal_year"])


def _v(x: Any) -> float:
    """Coerce to float for aggregation; None/NaN -> 0."""
    if x is None:
        return 0.0
    if isinstance(x, float) and np.isnan(x):
        return 0.0
    return float(x)


def _fast_linregress(y: np.ndarray) -> tuple[float, float]:
    """
    OLS slope and R² for y regressed on 0..n-1 (no intercept in formula; slope and R² match scipy.linregress).
    Returns (slope, r_squared). Avoids scipy per-call overhead when called many times.
    """
    n = len(y)
    if n < 2:
        return (np.nan, 0.0)
    x = np.arange(n, dtype=np.float64)
    x_mean = (n - 1) / 2.0
    y_mean = float(np.mean(y))
    dx = x - x_mean
    dy = y - y_mean
    ss_xx = float(dx @ dx)
    ss_xy = float(dx @ dy)
    ss_yy = float(dy @ dy)
    if ss_xx == 0:
        return (np.nan, 0.0)
    slope = ss_xy / ss_xx
    r_squared = (ss_xy * ss_xy) / (ss_xx * ss_yy) if ss_yy > 0 else 0.0
    return (float(slope), float(r_squared))

# Minimum observations for metrics
# MIN_YEARS_NCFO = 5: companies need 5 years of positive NCFO before R2 is computed;
# downstream should handle NULLs for newer companies (exclude from quality-filtered strategies, not necessarily from universe)
MIN_YEARS_NCFO = 5
MIN_YEARS_FCF = 3
MIN_YEARS_ROIC_SLOPE = 2
MIN_QUARTERS_GM = 4
MIN_QUARTERS_NET_DEBT = 4

# Years of history to load before date_start so rolling 10y metrics have enough data
ARQ_LOOKBACK_YEARS = 10
ARY_LOOKBACK_YEARS = 10


def _as_float1d(x: Union[pd.Series, np.ndarray]) -> np.ndarray:
    """Convert Series or array to 1d float64; NaN preserved."""
    if isinstance(x, np.ndarray):
        return x.ravel().astype(np.float64)
    return np.asarray(x, dtype=np.float64).ravel()


def ncfo_r2_cagr(ncfo_annual: Union[pd.Series, np.ndarray]) -> tuple[float | None, float | None, float | None]:
    """
    OLS R² of log(ncfo) ~ time, CAGR, and fraction of years with positive NCFO.
    Returns (r_squared, cagr, pct_positive). Needs at least MIN_YEARS_NCFO positive values.
    Accepts Series or array (time-ordered).
    """
    arr = _as_float1d(ncfo_annual)
    arr = arr[~np.isnan(arr)]
    if len(arr) < MIN_YEARS_NCFO:
        return None, None, None
    pct_positive = float((arr > 0).sum() / len(arr))
    arr_pos = arr[arr > 0]
    if len(arr_pos) < MIN_YEARS_NCFO:
        return None, None, pct_positive
    y = np.log(arr_pos)
    slope, r_squared = _fast_linregress(y)
    cagr = float(np.exp(slope) - 1.0) if np.isfinite(slope) else None
    return float(r_squared), cagr, pct_positive


def fcf_cagr(fcf_annual: Union[pd.Series, np.ndarray]) -> float | None:
    """Compound annual growth rate of FCF over last 5 years (or available). Accepts Series or array (time-ordered)."""
    arr = _as_float1d(fcf_annual)
    arr = arr[~np.isnan(arr)]
    if len(arr) < MIN_YEARS_FCF:
        return None
    first_val = float(arr[0])
    last_val = float(arr[-1])
    n = len(arr) - 1
    if n <= 0 or first_val <= 0 or last_val <= 0:
        return None
    return float((last_val / first_val) ** (1.0 / n) - 1.0)


def r2_and_pct_positive(series: Union[pd.Series, np.ndarray], min_points: int = 3) -> tuple[float | None, float | None]:
    """R² of log(series) ~ time and fraction of periods positive. Returns (r2, pct_positive). Accepts Series or array."""
    arr = _as_float1d(series)
    arr = arr[~np.isnan(arr)]
    if len(arr) < min_points:
        return None, None
    pct_positive = float((arr > 0).sum() / len(arr))
    arr_pos = arr[arr > 0]
    if len(arr_pos) < min_points:
        return None, pct_positive
    y = np.log(arr_pos)
    _, r_squared = _fast_linregress(y)
    return float(r_squared), pct_positive


def slope_series(series: Union[pd.Series, np.ndarray], min_points: int = 2) -> float | None:
    """Linear regression slope of series (index order). Accepts Series or array."""
    arr = _as_float1d(series)
    arr = arr[~np.isnan(arr)]
    if len(arr) < min_points:
        return None
    slope, _ = _fast_linregress(arr)
    return float(slope) if np.isfinite(slope) else None


def dilution_rate(sharesbas_annual: Union[pd.Series, np.ndarray]) -> float | None:
    """Annualized share count growth rate over available years. Positive = dilution, negative = buybacks. Accepts Series or array."""
    arr = _as_float1d(sharesbas_annual)
    arr = arr[~np.isnan(arr)]
    arr = arr[arr > 0]
    if len(arr) < 2:
        return None
    first_val = float(arr[0])
    last_val = float(arr[-1])
    n_years = len(arr) - 1
    return float((last_val / first_val) ** (1.0 / n_years) - 1.0)


def compute_quality_metrics_for_ticker(
    annual_data: Union[pd.DataFrame, list[dict]],
    quarterly_data: Union[pd.DataFrame, list[dict], None] = None,
) -> dict[str, Any]:
    """
    Given annual aggregates (fiscal_year, ncfo_annual, fcf_recon_annual, roic_avg, etc.)
    and optional quarterly (for gross margin 8q, net debt 8q), compute all quality metrics.
    Accepts DataFrame or list[dict] (from rebuild_annual_from_quarters) to avoid DataFrame overhead in inner loop.
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

    if isinstance(annual_data, list):
        return _compute_quality_metrics_from_dicts(annual_data, quarterly_data, out)
    # DataFrame path (legacy / external callers)
    annual_df = annual_data
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

    if quarterly_data is not None and not quarterly_data.empty and "grossmargin" in quarterly_data.columns:
        gm_8q = quarterly_data["grossmargin"].tail(8)
        out["grossmargin_slope"] = slope_series(gm_8q, MIN_QUARTERS_GM)

    if quarterly_data is not None and not quarterly_data.empty and "net_debt" in quarterly_data.columns:
        nd_8q = quarterly_data["net_debt"].tail(8)
        out["net_debt_trend"] = slope_series(nd_8q, MIN_QUARTERS_NET_DEBT)

    if "sharesbas_annual" in annual_df.columns:
        out["dilution_rate"] = dilution_rate(annual_df["sharesbas_annual"])

    return out


def _compute_quality_metrics_from_dicts(
    annual_list: list[dict],
    quarterly_list: list[dict] | None,
    out: dict[str, Any],
) -> dict[str, Any]:
    """Compute quality metrics from list of annual dicts and optional list of quarter dicts (no DataFrame)."""
    if not annual_list:
        return out
    annual_sorted = sorted(annual_list, key=lambda r: r["fiscal_year"])

    def _arr(key: str, tail_n: int) -> np.ndarray:
        vals = [r.get(key) for r in annual_sorted[-tail_n:]]
        return np.array(
            [v if v is not None and not (isinstance(v, float) and np.isnan(v)) else np.nan for v in vals],
            dtype=np.float64,
        )

    if "ncfo_annual" in annual_sorted[0]:
        ncfo_10 = _arr("ncfo_annual", 10)
        r2, cagr, pct_pos = ncfo_r2_cagr(ncfo_10)
        out["ncfo_r2_10y"] = r2
        out["ncfo_cagr_10y"] = cagr
        out["ncfo_pct_positive"] = pct_pos
        out["ncfo_r2_adjusted"] = (r2 * pct_pos) if (r2 is not None and pct_pos is not None) else None

    if "fcf_recon_annual" in annual_sorted[0]:
        fcf_5 = _arr("fcf_recon_annual", 5)
        out["fcf_cagr_5y"] = fcf_cagr(fcf_5)
        fcf_10 = _arr("fcf_recon_annual", 10)
        fcf_r2, fcf_pct = r2_and_pct_positive(fcf_10, min_points=MIN_YEARS_FCF)
        out["fcf_r2_10y"] = fcf_r2
        out["fcf_pct_positive"] = fcf_pct
        out["fcf_r2_adjusted"] = (fcf_r2 * fcf_pct) if (fcf_r2 is not None and fcf_pct is not None) else None
        ncfo_adj = out.get("ncfo_r2_adjusted")
        fcf_adj = out["fcf_r2_adjusted"]
        out["fcf_ncfo_r2_delta"] = round(ncfo_adj - fcf_adj, 4) if (ncfo_adj is not None and fcf_adj is not None) else None

    if "roic_avg" in annual_sorted[0]:
        out["roic_level"] = annual_sorted[-1].get("roic_avg")
        roic_3 = _arr("roic_avg", 3)
        out["roic_slope_3y"] = slope_series(roic_3, MIN_YEARS_ROIC_SLOPE)

    if quarterly_list:
        q_sorted = sorted(quarterly_list, key=lambda q: pd.Timestamp(q["reportperiod"]))
        if "grossmargin" in q_sorted[0]:
            gm_8 = np.array([q.get("grossmargin") for q in q_sorted[-8:]], dtype=np.float64)
            out["grossmargin_slope"] = slope_series(gm_8, MIN_QUARTERS_GM)
        if "net_debt" in q_sorted[0]:
            nd_8 = np.array([q.get("net_debt") for q in q_sorted[-8:]], dtype=np.float64)
            out["net_debt_trend"] = slope_series(nd_8, MIN_QUARTERS_NET_DEBT)

    if "sharesbas_annual" in annual_sorted[0]:
        sh = _arr("sharesbas_annual", len(annual_sorted))
        out["dilution_rate"] = dilution_rate(sh)

    return out


def compute_quality_metrics_table(
    con: Any,
    date_start: str,
    date_end: str,
    universe_tickers: set[str] | None = None,
) -> pd.DataFrame:
    """
    Build quality_metrics table: one row per (ticker, datekey) for each filing date.
    PIT-correct: ARY gives annual series (latest datekey per reportperiod at each vintage);
    ARQ gives quarterly state for 8Q metrics. Tickers_to_update = filers on ARQ or ARY this vintage.
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
            AND CAST(datekey AS DATE) BETWEEN ('{date_start}'::DATE - INTERVAL '{ARQ_LOOKBACK_YEARS} years') AND '{date_end}'::DATE
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

    # ARY: annual series for NCFO/FCF/ROIC/dilution (PIT: latest datekey per reportperiod at each vintage)
    all_ary = pd.DataFrame()
    try:
        all_ary = con.execute(f"""
            SELECT ticker, reportperiod, datekey, ncfo, capex, roic, sharesbas
            FROM sf1
            WHERE dimension = 'ARY'
            AND CAST(datekey AS DATE) BETWEEN ('{date_start}'::DATE - INTERVAL '{ARY_LOOKBACK_YEARS} years') AND '{date_end}'::DATE
            {ticker_filter}
            ORDER BY ticker, datekey
        """).df()
    except Exception as e:
        log.warning("ARY pull failed; annual metrics will use ARQ aggregation fallback: %s", e)
    if not all_ary.empty:
        all_ary["reportperiod"] = pd.to_datetime(all_ary["reportperiod"])
        all_ary["datekey"] = pd.to_datetime(all_ary["datekey"])

    # Unified vintages and tickers that filed (ARQ or ARY) on each date
    vintages = sorted(set(all_arq["datekey"].unique()))
    vintage_to_tickers = all_arq.groupby("datekey")["ticker"].apply(set).to_dict()
    if not all_ary.empty:
        vintages = sorted(set(vintages) | set(all_ary["datekey"].unique()))
        for d, s in all_ary.groupby("datekey")["ticker"].apply(set).to_dict().items():
            vintage_to_tickers[d] = vintage_to_tickers.get(d, set()) | s

    # Per-ticker state: ARQ quarters (for 8Q metrics); ARY annual (for NCFO/FCF/ROIC/dilution)
    ticker_quarters: dict[str, dict[Any, dict]] = {}
    ticker_ary: dict[str, dict[Any, dict]] = {}
    rows = []

    try:
        from tqdm import tqdm
        vintage_iter = tqdm(vintages, desc="Quality metrics", unit="vintage")
    except ImportError:
        vintage_iter = vintages

    for vintage in vintage_iter:
        # ARQ: accumulate quarterly filings (amendments: later datekey wins)
        new_arq = all_arq[all_arq["datekey"] == vintage]
        for record in new_arq.to_dict("records"):
            ticker = record["ticker"]
            period = record["reportperiod"]
            if ticker not in ticker_quarters:
                ticker_quarters[ticker] = {}
            existing = ticker_quarters[ticker].get(period)
            if existing is None or record["datekey"] >= existing["datekey"]:
                ticker_quarters[ticker][period] = record

        # ARY: accumulate annual filings (amendments: later datekey wins per reportperiod)
        if not all_ary.empty:
            new_ary = all_ary[all_ary["datekey"] == vintage]
            for record in new_ary.to_dict("records"):
                ticker = record["ticker"]
                period = record["reportperiod"]
                if ticker not in ticker_ary:
                    ticker_ary[ticker] = {}
                existing = ticker_ary[ticker].get(period)
                if existing is None or record["datekey"] >= existing["datekey"]:
                    ticker_ary[ticker][period] = record

        tickers_to_update = vintage_to_tickers.get(vintage, set())
        if not tickers_to_update:
            continue

        for ticker in tickers_to_update:
            quarters = ticker_quarters.get(ticker, {})
            # Annual series: ARY if available, else ARQ aggregation (fallback)
            annual_list = ary_records_to_annual_list(ticker_ary.get(ticker, {}))
            if not annual_list:
                annual_list = rebuild_annual_from_quarters(quarters)
            if not annual_list:
                continue
            quarterly_list = sorted(quarters.values(), key=lambda q: pd.Timestamp(q["reportperiod"]))

            metrics = compute_quality_metrics_for_ticker(
                annual_list,
                quarterly_list,
            )
            # Emit only vintages inside the pipeline window; we still accumulate state for earlier vintages
            # datekey = filing date (10-Q or 10-K) that triggered this row; downstream joins on datekey <= date (PIT).
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
