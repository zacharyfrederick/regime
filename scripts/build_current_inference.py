#!/usr/bin/env python3
"""
Build current inference dataset for the walk_forward_validation model.
One row per non-delisted ticker; features match load_fold SQL SELECT (lines 10-24).
Uses Sharadar dimensions: MRY/MRQ (full restated time series) for quality; MRT (TTM restated) for valuation.
TICKERS has two rows per ticker (table SF1 and SEP); we filter to table='SF1' for listed/sector.
Output: same columns as master for inference; optional filter by training WHERE.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd

from config import (
    DATA_DIR,
    SPY_TICKER,
    FRED_429_MAX_RETRIES,
    FRED_429_RETRY_WAIT_SEC,
    FRED_DIR,
    FRED_REQUEST_DELAY_SEC,
    VIX_FRED_CODE,
    apply_duckdb_limits,
)
from pipeline.fundamental_quality import compute_quality_metrics_for_ticker

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# FRED series used by macro (same as pipeline/00_fetch_fred.py). API key: FRED_API_KEY in env or .env (https://fred.stlouisfed.org/docs/api/api_key.html).
FRED_SERIES = {
    "yield_curve": "T10Y2Y",
    "hy_spread": "BAMLH0A0HYM2",
    "ig_spread": "BAMLC0A4CBBB",
    "vix": VIX_FRED_CODE,
    "nfci": "NFCI",
    "fed_funds": "FEDFUNDS",
    "cpi": "CPIAUCSL",
    "treasury_10y": "DGS10",
}
FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


def _fred_get(url: str, params: dict) -> "requests.Response":
    """Rate-limited FRED API request with 429 retry. Requires requests."""
    import requests
    last_exc = None
    for attempt in range(FRED_429_MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = FRED_429_RETRY_WAIT_SEC * (2**attempt)
                log.warning("FRED 429 (rate limit); waiting %s s before retry %s/%s", wait, attempt + 1, FRED_429_MAX_RETRIES)
                time.sleep(wait)
                last_exc = None
                continue
            r.raise_for_status()
            time.sleep(FRED_REQUEST_DELAY_SEC)
            return r
        except requests.RequestException as e:
            last_exc = e
            if attempt < FRED_429_MAX_RETRIES:
                time.sleep(FRED_429_RETRY_WAIT_SEC)
    if last_exc:
        raise last_exc
    raise RuntimeError("FRED request failed after retries")


def fetch_fred_live(api_key: str, as_of: pd.Timestamp) -> dict[str, float | None]:
    """Pull FRED series live from API; return latest value per stem and derived vix_change_20d, real_rate. Requires requests."""
    try:
        import requests  # noqa: F401
    except ImportError:
        raise ImportError("Live FRED fetch requires the requests package. Install with: pip install requests") from None
    start = (as_of - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
    end = as_of.strftime("%Y-%m-%d")
    out: dict[str, float | None] = {}
    series_dfs: dict[str, pd.DataFrame] = {}
    for name, series_id in FRED_SERIES.items():
        try:
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "observation_start": start,
                "observation_end": end,
                "sort_order": "asc",
            }
            r = _fred_get(FRED_OBSERVATIONS_URL, params)
            data = r.json()
            if "error_message" in data:
                log.warning("FRED API error for %s: %s", series_id, data["error_message"])
                out[name] = None
                continue
            obs = data.get("observations", [])
            if not obs:
                out[name] = None
                continue
            df = pd.DataFrame(obs)[["date", "value"]]
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"]).drop_duplicates(subset=["date"], keep="last").sort_values("date")
            series_dfs[name] = df
            out[name] = float(df["value"].iloc[-1]) if len(df) else None
        except Exception as e:
            log.warning("FRED fetch failed for %s: %s", series_id, e)
            out[name] = None
    # vix_change_20d: latest vix minus vix 20 observations back (FRED daily)
    if "vix" in series_dfs and series_dfs["vix"] is not None and len(series_dfs["vix"]) >= 21:
        vdf = series_dfs["vix"].tail(21)
        out["vix_change_20d"] = float(vdf["value"].iloc[-1] - vdf["value"].iloc[0])
    else:
        out["vix_change_20d"] = None
    # real_rate = treasury_10y - cpi_yoy (percent)
    if out.get("treasury_10y") is not None and "cpi" in series_dfs and series_dfs["cpi"] is not None and len(series_dfs["cpi"]) >= 2:
        cdf = series_dfs["cpi"].tail(2)
        cpi_yoy = (float(cdf["value"].iloc[-1]) - float(cdf["value"].iloc[0])) / float(cdf["value"].iloc[0]) * 100
        out["real_rate"] = out["treasury_10y"] - cpi_yoy
    else:
        out["real_rate"] = out.get("treasury_10y")
    return out


# Calendar days: need enough to cover 252 trading days for ret_12m (~365 + buffer for holidays)
LOOKBACK_DAYS = 400
ARQ_ARY_LOOKBACK_YEARS = 10


def _parquet(name: str) -> Path | None:
    p = DATA_DIR / f"{name}.parquet"
    if p.exists():
        return p
    p = DATA_DIR / f"{name.lower()}.parquet"
    return p if p.exists() else None


def _path_sql(p: Path) -> str:
    return repr(str(p.resolve()))


def get_listed_tickers(con: duckdb.DuckDBPyConnection, tickers_path: Path | None) -> set[str]:
    """Tickers with isdelisted = 'N'. TICKERS has two rows per ticker (SF1 and SEP); filter to table = 'SF1' for one row per ticker."""
    if not tickers_path or not tickers_path.exists():
        return set()
    con.execute(f"""
        CREATE OR REPLACE VIEW tickers_sf1 AS
        SELECT * FROM read_parquet({_path_sql(tickers_path)})
        WHERE "table" = 'SF1' AND ticker IS NOT NULL AND TRIM(COALESCE(ticker, '')) <> ''
    """)
    try:
        df = con.execute("""
            SELECT ticker FROM tickers_sf1
            WHERE COALESCE(isdelisted, '') = 'N' OR LOWER(COALESCE(isdelisted, '')) = 'n'
        """).df()
    except Exception:
        df = con.execute("""
            SELECT ticker FROM tickers_sf1
            WHERE COALESCE("isdelisted", '') = 'N' OR LOWER(COALESCE("isdelisted", '')) = 'n'
        """).df()
    return set(df["ticker"].tolist())


def get_as_of_date(con: duckdb.DuckDBPyConnection, sep_path: Path) -> pd.Timestamp | None:
    """Latest trading date in SEP."""
    row = con.execute(f"""
        SELECT MAX(CAST(date AS DATE)) AS d
        FROM read_parquet({_path_sql(sep_path)})
    """).fetchone()
    if row and row[0] is not None:
        return pd.Timestamp(row[0])
    return None


def build_quality_current(
    con: duckdb.DuckDBPyConnection,
    tickers: set[str],
    sf1_path: Path,
) -> pd.DataFrame:
    """One row per ticker: quality metrics from MRY + MRQ (full restated time series) via compute_quality_metrics_for_ticker."""
    if not tickers:
        return pd.DataFrame()
    ticker_list = ",".join(repr(t) for t in sorted(tickers))
    # Detect grossmargin for MRQ
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({_path_sql(sf1_path)})").df()
    cols = desc["column_name"].tolist() if "column_name" in desc.columns else []
    has_gp = "gp" in cols
    has_gm = "grossmargin" in cols
    if has_gm and has_gp:
        grossmargin_expr = "COALESCE(CASE WHEN grossmargin BETWEEN 0 AND 2 THEN grossmargin ELSE NULL END, gp / NULLIF(revenue, 0)) AS grossmargin"
    elif has_gm:
        grossmargin_expr = "grossmargin"
    elif has_gp:
        grossmargin_expr = "gp / NULLIF(revenue, 0) AS grossmargin"
    else:
        grossmargin_expr = "CAST(NULL AS DOUBLE) AS grossmargin"

    # MRQ = full quarterly time series with restatements (everything currently known)
    mrq = con.execute(f"""
        SELECT ticker, reportperiod, datekey,
               ncfo, fcf, capex, roic, sharesbas,
               revenue, debt, cashnequsd, {grossmargin_expr}
        FROM read_parquet({_path_sql(sf1_path)})
        WHERE dimension = 'MRQ' AND ticker IN ({ticker_list})
        ORDER BY ticker, reportperiod
    """).df()
    if mrq.empty:
        return pd.DataFrame({"ticker": list(tickers)})

    mrq["reportperiod"] = pd.to_datetime(mrq["reportperiod"])
    mrq["net_debt"] = mrq["debt"] - mrq["cashnequsd"]

    # MRY = full annual time series with restatements (everything currently known)
    mry = con.execute(f"""
        SELECT ticker, reportperiod, datekey, ncfo, capex, roic, sharesbas
        FROM read_parquet({_path_sql(sf1_path)})
        WHERE dimension = 'MRY' AND ticker IN ({ticker_list})
        ORDER BY ticker, reportperiod
    """).df()

    rows = []
    for ticker in sorted(tickers):
        mry_t = mry[mry["ticker"] == ticker].sort_values("reportperiod").tail(11)  # up to 11 years for 10y CAGR
        mrq_t = mrq[mrq["ticker"] == ticker].sort_values("reportperiod").tail(8)   # last 8 quarters for 8Q metrics
        # Build annual_list from MRY (one row per year; already restated)
        annual_list = []
        if not mry_t.empty:
            for _, r in mry_t.iterrows():
                fy = pd.Timestamp(r["reportperiod"]).year
                ncfo = r.get("ncfo")
                capex = r.get("capex")
                annual_list.append({
                    "fiscal_year": fy,
                    "ncfo_annual": ncfo if ncfo is not None and not (isinstance(ncfo, float) and pd.isna(ncfo)) else None,
                    "fcf_recon_annual": (float(ncfo) if ncfo is not None and not (isinstance(ncfo, float) and pd.isna(ncfo)) else 0) + (float(capex) if capex is not None and not (isinstance(capex, float) and pd.isna(capex)) else 0),
                    "roic_avg": r.get("roic"),
                    "sharesbas_annual": r.get("sharesbas"),
                })
            annual_list = sorted(annual_list, key=lambda x: x["fiscal_year"])
        if not annual_list:
            rows.append({"ticker": ticker})
            continue
        # Quarterly list from MRQ (last 8 quarters) for grossmargin_slope, net_debt_trend
        quarterly_list = []
        if not mrq_t.empty:
            quarterly_list = [r.to_dict() for _, r in mrq_t.iterrows()]
        metrics = compute_quality_metrics_for_ticker(annual_list, quarterly_list if quarterly_list else None)
        row = {"ticker": ticker}
        for k in ["ncfo_r2_5y", "ncfo_cagr_5y", "ncfo_r2_10y", "ncfo_cagr_10y", "ncfo_pct_positive",
                  "grossmargin_slope", "roic_level", "fcf_r2_5y", "fcf_r2_10y", "fcf_cagr_5y", "fcf_cagr_10y"]:
            row[k] = metrics.get(k)
        rows.append(row)

    return pd.DataFrame(rows)


def build_art_valuation_current(
    con: duckdb.DuckDBPyConnection,
    tickers: set[str],
    sf1_path: Path,
    sep_path: Path,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """Latest MRT (TTM, restated) per ticker + latest SEP closeadj; valuation ratios; prior-year MRT for earnings_growth_yoy."""
    if not tickers:
        return pd.DataFrame()
    ticker_list = ",".join(repr(t) for t in sorted(tickers))
    as_str = as_of.strftime("%Y-%m-%d")
    # Latest MRT per ticker (Most Recent Trailing = TTM with restatements; one row per ticker or take latest)
    art = con.execute(f"""
        WITH mrt AS (
            SELECT *, CAST(datekey AS DATE) AS datekey_date
            FROM read_parquet({_path_sql(sf1_path)})
            WHERE dimension = 'MRT' AND ticker IN ({ticker_list})
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY datekey_date DESC) AS rn
            FROM mrt WHERE datekey_date <= CAST('{as_str}' AS DATE)
        )
        SELECT * FROM ranked WHERE rn = 1
    """).df()
    if art.empty:
        return pd.DataFrame({"ticker": list(tickers)})

    # Prior-year MRT for epsdil_prior (same dimension as current; restated TTM as of ~1y ago)
    mrt_prior = con.execute(f"""
        WITH mrt AS (
            SELECT ticker, CAST(datekey AS DATE) AS datekey_date, epsdil
            FROM read_parquet({_path_sql(sf1_path)})
            WHERE dimension = 'MRT' AND ticker IN ({ticker_list})
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY datekey_date DESC) AS rn
            FROM mrt WHERE datekey_date <= CAST('{as_str}' AS DATE) - INTERVAL '1 year'
        )
        SELECT ticker, epsdil AS epsdil_prior FROM ranked WHERE rn = 1
    """).df()

    # Latest SEP close per ticker (date <= as_of)
    sep = con.execute(f"""
        WITH s AS (
            SELECT ticker, CAST(date AS DATE) AS dt, closeadj
            FROM read_parquet({_path_sql(sep_path)})
            WHERE ticker IN ({ticker_list}) AND CAST(date AS DATE) <= CAST('{as_str}' AS DATE)
        ),
        latest AS (SELECT ticker, MAX(dt) AS latest_dt FROM s GROUP BY ticker)
        SELECT s.ticker, s.closeadj FROM s JOIN latest l ON l.ticker = s.ticker AND l.latest_dt = s.dt
    """).df()

    art = art.merge(sep, on="ticker", how="left")
    art = art.merge(mrt_prior, on="ticker", how="left")

    rev = art["revenueusd"].fillna(art["revenue"])
    mkt = art["closeadj"] * art["shareswa"]
    art["pcf_pit"] = mkt / art["ncfo"].replace(0, float("nan")) if "ncfo" in art.columns else None
    art["pe_pit"] = mkt / art["netinccmn"].replace(0, float("nan")) if "netinccmn" in art.columns else None
    art["pb_pit"] = mkt / art["equity"].replace(0, float("nan")) if "equity" in art.columns else None
    art["ps_pit"] = mkt / rev.replace(0, float("nan"))
    art["evebitda_pit"] = (mkt + art["debt"].fillna(0) - art["cashnequsd"].fillna(0)) / art["ebitda"].replace(0, float("nan")) if "ebitda" in art.columns else None
    art["roe"] = art["roe"].fillna(art["netinccmn"] / art["equity"].replace(0, float("nan"))) if "netinccmn" in art.columns else art["roe"]
    art["current_ratio"] = art["currentratio"]
    art["dividend_yield"] = art["divyield"].fillna(art["dps"] / art["closeadj"].replace(0, float("nan")))
    art["pretax_margin"] = art["ebt"] / rev.replace(0, float("nan")) if "ebt" in art.columns else None
    art["debt_to_equity"] = art["debt"] / art["equity"].replace(0, float("nan"))
    art["liabilities_to_assets"] = art["liabilities"] / art["assets"].replace(0, float("nan"))
    art["payout_ratio"] = art["dps"] / art["epsdil"].replace(0, float("nan"))
    art["earnings_growth_yoy"] = (art["epsdil"] - art["epsdil_prior"]) / art["epsdil_prior"].replace(0, float("nan"))
    art["capex_intensity"] = art["capex"].abs() / rev.replace(0, float("nan"))
    art["accrual_ratio"] = (art["netinccmn"] - art["ncfo"]) / art["assets"].replace(0, float("nan")) if "netinccmn" in art.columns else None

    out_cols = ["ticker", "pcf_pit", "roe", "current_ratio", "pe_pit", "pb_pit", "ps_pit", "evebitda_pit",
                "dividend_yield", "pretax_margin", "debt_to_equity", "liabilities_to_assets", "payout_ratio",
                "earnings_growth_yoy", "capex_intensity", "accrual_ratio"]
    return art[[c for c in out_cols if c in art.columns]]


def build_price_current(
    con: duckdb.DuckDBPyConnection,
    tickers: set[str],
    sep_path: Path,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """One row per ticker: price features at latest trading date (03 logic, 252d lookback)."""
    if not tickers:
        return pd.DataFrame()
    ticker_list = ",".join(repr(t) for t in sorted(tickers))
    as_str = as_of.strftime("%Y-%m-%d")
    # Build full price features for window ending at as_of, then keep only latest date per ticker
    con.execute(f"""
        CREATE OR REPLACE VIEW sep_cur AS
        SELECT * FROM read_parquet({_path_sql(sep_path)})
        WHERE ticker IN ({ticker_list})
          AND CAST(date AS DATE) BETWEEN (CAST('{as_str}' AS DATE) - INTERVAL '{LOOKBACK_DAYS} days') AND CAST('{as_str}' AS DATE)
    """)
    con.execute("""
        CREATE OR REPLACE VIEW price_latest AS
        WITH daily AS (
            SELECT ticker, date, closeadj, volume, high, low,
                   closeadj / LAG(closeadj, 1) OVER w - 1 AS daily_ret,
                   closeadj / LAG(closeadj, 21) OVER w - 1 AS ret_1m,
                   closeadj / LAG(closeadj, 63) OVER w - 1 AS ret_3m,
                   closeadj / LAG(closeadj, 126) OVER w - 1 AS ret_6m,
                   closeadj / LAG(closeadj, 252) OVER w - 1 AS ret_12m,
                   high - low AS hl_range,
                   ABS(high - LAG(closeadj, 1) OVER w) AS hc_range,
                   ABS(low - LAG(closeadj, 1) OVER w) AS lc_range
            FROM sep_cur
            WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ),
        base AS (
            SELECT *,
                   STDDEV(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_20d,
                   STDDEV(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_60d,
                   AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_20d_avg,
                   AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS vol_60d_avg,
                   MIN(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS low_52w,
                   MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52w,
                   AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
                   AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                   AVG(GREATEST(hl_range, hc_range, lc_range)) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) / NULLIF(closeadj, 0) AS atr_14d
            FROM daily
        ),
        derived AS (
            SELECT ticker, date,
                   ret_1m, ret_3m, ret_6m, ret_12m,
                   vol_20d, vol_60d,
                   vol_20d / NULLIF(vol_60d, 0) AS vol_ratio,
                   vol_20d_avg / NULLIF(vol_60d_avg, 0) AS volume_ratio_1m,
                   (closeadj - low_52w) / NULLIF(high_52w - low_52w, 0) AS pct_52w_range,
                   (closeadj - ma50) / NULLIF(ma50, 0) AS ma50_cross,
                   (closeadj - ma200) / NULLIF(ma200, 0) AS ma200_cross,
                   atr_14d AS atr_14d_normalized,
                   SKEWNESS(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS momentum_skew_60d
            FROM base
        ),
        last_date AS (SELECT ticker, MAX(date) AS md FROM derived GROUP BY ticker)
        SELECT d.* FROM derived d JOIN last_date l ON l.ticker = d.ticker AND l.md = d.date
    """)
    return con.execute("SELECT * FROM price_latest").df()


def build_macro_current(
    con: duckdb.DuckDBPyConnection,
    sep_path: Path,
    as_of: pd.Timestamp,
    fred_live: dict[str, float | None],
) -> dict[str, float | None]:
    """Latest value per macro series from live FRED fetch. SPY from SFP (fund/ETF prices)."""
    as_str = as_of.strftime("%Y-%m-%d")
    out: dict[str, float | None] = {}
    stems = ["yield_curve", "hy_spread", "vix", "nfci", "fed_funds", "cpi", "treasury_10y"]
    for stem in stems:
        out[stem] = fred_live.get(stem)
    out["vix_change_20d"] = fred_live.get("vix_change_20d")
    out["real_rate"] = fred_live.get("real_rate")

    # SPY from SFP (same as pipeline/04_macro_features)
    sfp_path = DATA_DIR / "SFP.parquet"
    if not sfp_path.exists():
        sfp_path = DATA_DIR / "sfp.parquet"
    spy = con.execute(f"""
        WITH spy AS (
            SELECT date, closeadj,
                   closeadj / LAG(closeadj, 252) OVER (ORDER BY date) - 1 AS spy_ret_12m,
                   AVG(closeadj) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200
            FROM read_parquet({_path_sql(sfp_path)}) WHERE ticker = '{SPY_TICKER}'
        )
        SELECT * FROM spy WHERE CAST(date AS DATE) <= CAST('{as_str}' AS DATE) ORDER BY date DESC LIMIT 1
    """).df() if sfp_path.exists() else pd.DataFrame()
    if not spy.empty:
        out["spy_regime_ma"] = 1.0 if spy["closeadj"].iloc[0] > spy["ma200"].iloc[0] else 0.0
        out["spy_ret_12m"] = float(spy["spy_ret_12m"].iloc[0]) if pd.notna(spy["spy_ret_12m"].iloc[0]) else None
    else:
        out["spy_regime_ma"] = None
        out["spy_ret_12m"] = None

    return out


def build_sector_relative_current(
    combined: pd.DataFrame,
) -> pd.DataFrame:
    """Sector medians by famaindustry (>=5 tickers), then ratios and ret_3m_rank_sector."""
    if combined.empty or "famaindustry" not in combined.columns:
        return pd.DataFrame()
    need = ["pe_pit", "pb_pit", "ps_pit", "pcf_pit", "evebitda_pit", "roic_level", "liabilities_to_assets", "ret_3m", "vol_20d"]
    for c in need:
        if c not in combined.columns:
            combined[c] = None
    medians = combined.groupby("famaindustry", dropna=False).agg(
        sector_median_pe=("pe_pit", "median"),
        sector_median_pb=("pb_pit", "median"),
        sector_median_ps=("ps_pit", "median"),
        sector_median_pcf=("pcf_pit", "median"),
        sector_median_evebitda=("evebitda_pit", "median"),
        sector_median_roic=("roic_level", "median"),
        sector_median_liabilities_to_assets=("liabilities_to_assets", "median"),
        sector_median_ret_3m=("ret_3m", "median"),
        sector_median_vol=("vol_20d", "median"),
        _count=("ticker", "count"),
    ).reset_index()
    medians = medians[medians["_count"] >= 5].drop(columns=["_count"])
    merged = combined.merge(medians, on="famaindustry", how="left")
    merged["pe_vs_sector"] = merged["pe_pit"] / merged["sector_median_pe"].replace(0, float("nan"))
    merged["pb_vs_sector"] = merged["pb_pit"] / merged["sector_median_pb"].replace(0, float("nan"))
    merged["ps_vs_sector"] = merged["ps_pit"] / merged["sector_median_ps"].replace(0, float("nan"))
    merged["pcf_vs_sector"] = merged["pcf_pit"] / merged["sector_median_pcf"].replace(0, float("nan"))
    merged["evebitda_vs_sector"] = merged["evebitda_pit"] / merged["sector_median_evebitda"].replace(0, float("nan"))
    merged["roic_vs_sector"] = merged["roic_level"] - merged["sector_median_roic"]
    merged["ret_3m_vs_sector"] = merged["ret_3m"] - merged["sector_median_ret_3m"]
    merged["vol_vs_sector"] = merged["vol_20d"] - merged["sector_median_vol"]
    merged["ret_3m_rank_sector"] = merged.groupby("famaindustry")["ret_3m"].rank(pct=True, method="average")
    return merged[["ticker", "pe_vs_sector", "pb_vs_sector", "ps_vs_sector", "pcf_vs_sector",
                   "evebitda_vs_sector", "roic_vs_sector", "ret_3m_vs_sector", "vol_vs_sector", "ret_3m_rank_sector"]]


def build_current_inference(
    as_of_date: pd.Timestamp | str | None = None,
    data_dir: Path | None = None,
    fred_dir: Path | None = None,
    apply_training_filter: bool = False,
    out_path: Path | None = None,
) -> pd.DataFrame:
    """
    Build current inference dataset: one row per listed ticker, columns matching load_fold SELECT.
    If apply_training_filter: keep only rows satisfying notebook WHERE (ncfo_r2_5y > 0.5, etc.).
    Macro data is always pulled live from FRED API (requires FRED_API_KEY in env or .env).
    """
    data_dir = data_dir or DATA_DIR
    fred_dir = fred_dir or FRED_DIR
    tickers_path = _parquet("TICKERS")
    sf1_path = _parquet("SF1")
    sep_path = _parquet("SEP")
    if not sep_path or not sf1_path:
        log.warning("SEP or SF1 not found")
        return pd.DataFrame()

    con = duckdb.connect()
    apply_duckdb_limits(con)

    listed = get_listed_tickers(con, tickers_path)
    if not listed:
        log.warning("No listed tickers from TICKERS")
        con.close()
        return pd.DataFrame()

    as_of = pd.Timestamp(as_of_date) if as_of_date else get_as_of_date(con, sep_path)
    if as_of is None:
        as_of = pd.Timestamp.now().normalize()
    log.info("As-of date: %s; listed tickers: %d", as_of, len(listed))

    # Tickers that have price on or before as_of
    sep_tickers_df = con.execute(f"""
        SELECT DISTINCT ticker FROM read_parquet({_path_sql(sep_path)})
        WHERE CAST(date AS DATE) <= CAST('{as_of.strftime("%Y-%m-%d")}' AS DATE)
    """).df()
    sep_tickers = set(sep_tickers_df["ticker"].tolist())
    tickers = listed & sep_tickers
    if not tickers:
        con.close()
        return pd.DataFrame()
    log.info("Tickers with price and listed: %d", len(tickers))

    # Meta: sector, famaindustry from TICKERS
    meta = con.execute(f"""
        SELECT DISTINCT ON (ticker) ticker, sector, famaindustry
        FROM read_parquet({_path_sql(tickers_path)})
        WHERE "table" = 'SF1' AND ticker IN ({','.join(repr(t) for t in sorted(tickers))})
        ORDER BY ticker, sector NULLS LAST
    """).df()

    log.info("Building quality metrics (MRY+MRQ)...")
    quality_df = build_quality_current(con, tickers, sf1_path)
    log.info("Building MRT valuation...")
    art_df = build_art_valuation_current(con, tickers, sf1_path, sep_path, as_of)
    log.info("Building price features...")
    price_df = build_price_current(con, tickers, sep_path, as_of)
    log.info("Building macro (live FRED)...")
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        log.warning("FRED_API_KEY not set; macro will be missing. Set in env or .env (https://fred.stlouisfed.org/docs/api/api_key.html)")
        fred_live = {}
    else:
        try:
            fred_live = fetch_fred_live(api_key, as_of)
            log.info("Fetched FRED macro (%d series)", len([k for k, v in fred_live.items() if v is not None]))
        except Exception as e:
            log.warning("FRED fetch failed: %s; macro will be missing", e)
            fred_live = {}
    macro = build_macro_current(con, sep_path, as_of, fred_live=fred_live)

    con.close()

    # Merge: base = one row per ticker
    base = pd.DataFrame({"ticker": sorted(tickers), "date": as_of})
    base = base.merge(meta, on="ticker", how="left")
    base["fwd_ret_5td"] = float("nan")
    base = base.merge(quality_df, on="ticker", how="left")
    base = base.merge(art_df, on="ticker", how="left", suffixes=("", "_art"))
    # If art had duplicate cols, drop _art
    base = base[[c for c in base.columns if not c.endswith("_art")]]
    base = base.merge(price_df, on="ticker", how="left")
    for k, v in macro.items():
        base[k] = v
    # Sector-relative needs combined fundamental + price
    combined = base.copy()
    sector_rel = build_sector_relative_current(combined)
    base = base.merge(sector_rel, on="ticker", how="left")

    # Column order matching load_fold SELECT
    select_cols = [
        "ticker", "date", "sector", "famaindustry", "fwd_ret_5td",
        "ret_1m", "ret_3m", "ret_6m", "ret_12m", "vol_20d", "vol_60d", "vol_ratio", "volume_ratio_1m", "pct_52w_range",
        "ma50_cross", "ma200_cross", "atr_14d_normalized", "momentum_skew_60d",
        "ncfo_r2_5y", "ncfo_cagr_5y", "ncfo_r2_10y", "ncfo_cagr_10y", "ncfo_pct_positive",
        "grossmargin_slope", "capex_intensity", "accrual_ratio", "pcf_pit", "roe", "current_ratio", "vix",
        "pe_pit", "pb_pit", "ps_pit", "evebitda_pit", "dividend_yield", "pretax_margin", "debt_to_equity",
        "liabilities_to_assets", "payout_ratio", "earnings_growth_yoy",
        "pe_vs_sector", "pb_vs_sector", "ps_vs_sector", "pcf_vs_sector", "evebitda_vs_sector", "roic_vs_sector",
        "ret_3m_vs_sector", "vol_vs_sector", "ret_3m_rank_sector",
        "yield_curve", "hy_spread", "vix_change_20d", "nfci", "real_rate", "spy_regime_ma", "spy_ret_12m",
    ]
    if apply_training_filter:
        # ncfo_r2_5y > 0.5 AND fcf_cagr_5y > 0 AND roe > 0.12 AND debt_to_equity < 1.5 AND grossmargin_slope > 0
        # AND ret_12m > 0 AND ret_1m > -0.15 AND ret_6m > ret_3m (apply on base; fcf_cagr_5y is in quality)
        f = base["ncfo_r2_5y"] > 0.5
        if "fcf_cagr_5y" in base.columns:
            f = f & (base["fcf_cagr_5y"] > 0)
        f = f & (base["roe"] > 0.12) & (base["debt_to_equity"] < 1.5) & (base["grossmargin_slope"] > 0)
        f = f & (base["ret_12m"] > 0) & (base["ret_1m"] > -0.15)
        f = f & (base["ret_6m"] > base["ret_3m"])
        base = base.loc[f].copy()
        log.info("After training filter: %d rows", len(base))

    for c in select_cols:
        if c not in base.columns:
            base[c] = None
    out = base[[c for c in select_cols if c in base.columns]]

    if out_path:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(out_path, index=False)
        log.info("Wrote %s", out_path)

    return out


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Build current inference dataset for walk_forward_validation model")
    p.add_argument("--as-of", type=str, default=None, help="As-of date YYYY-MM-DD (default: latest in SEP)")
    p.add_argument("--filter", action="store_true", help="Apply training WHERE filter")
    p.add_argument("--out", type=str, default=None, help="Output parquet path (default: outputs/current_inference.parquet)")
    args = p.parse_args()
    out_path = Path(args.out) if args.out else ROOT / "outputs" / "current_inference.parquet"
    df = build_current_inference(
        as_of_date=args.as_of or None,
        apply_training_filter=args.filter,
        out_path=out_path,
    )
    if df.empty:
        sys.exit(1)
    log.info("Done: %d rows", len(df))


if __name__ == "__main__":
    main()
