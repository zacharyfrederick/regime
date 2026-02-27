#!/usr/bin/env python3
"""
Download FRED series used by 04_macro_features and write parquet under data/macro/.
Requires FRED_API_KEY in the environment (free at https://fred.stlouisfed.org/docs/api/api_key.html).
Output: one parquet per series in config.FRED_DIR.
  - Non-PIT: columns (date, <series_name>).
  - PIT (when FRED_USE_PIT and stem in FRED_PIT_SERIES): columns (observation_date, vintage_date, value).
"""
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import requests

from config import (
    DATE_END,
    DATE_START,
    FRED_429_MAX_RETRIES,
    FRED_429_RETRY_WAIT_SEC,
    FRED_DIR,
    FRED_PIT_SERIES,
    FRED_REQUEST_DELAY_SEC,
    FRED_USE_PIT,
    VIX_FRED_CODE,
)

from dotenv import load_dotenv

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

# Load the .env file
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_VINTAGEDATES_URL = "https://api.stlouisfed.org/fred/series/vintagedates"

# Parquet filename stem -> FRED series_id (must match what 04_macro_features expects to join)
SERIES = {
    "yield_curve": "T10Y2Y",           # 10Y - 2Y treasury spread
    "hy_spread": "BAMLH0A0HYM2",        # BofA ML US High Yield Master II OAS
    "ig_spread": "BAMLC0A4CBBB",       # BofA ML BBB US Corporate Option-Adjusted Spread
    "vix": VIX_FRED_CODE,              # VIX (e.g. VIXCLS)
    "nfci": "NFCI",                    # Chicago Fed National Financial Conditions Index
    "fed_funds": "FEDFUNDS",           # Effective Federal Funds Rate
    "cpi": "CPIAUCSL",                 # CPI All Urban (for cpi_yoy in 04)
    "treasury_10y": "DGS10",           # 10-Year Treasury (for real_rate derivation)
}


def _fred_get(url: str, params: dict) -> requests.Response:
    """Rate-limited FRED API request with 429 retry. Sleeps after every response to stay at or below 2 req/s."""
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


def fetch_series(
    series_id: str,
    api_key: str,
    start: str,
    end: str,
    realtime_start: str | None = None,
    realtime_end: str | None = None,
    to_daily: bool = True,
) -> pd.DataFrame:
    """Fetch one FRED series as DataFrame with columns (date, value). One row per date (last observation wins if duplicates).
    If realtime_start/realtime_end are set, returns as-known-on-that-date (PIT). If to_daily is False, no reindex/ffill.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end,
        "sort_order": "asc",
    }
    if realtime_start is not None:
        params["realtime_start"] = realtime_start
    if realtime_end is not None:
        params["realtime_end"] = realtime_end
    r = _fred_get(FRED_OBSERVATIONS_URL, params)
    data = r.json()
    if "error_message" in data:
        raise ValueError(data["error_message"])
    obs = data.get("observations", [])
    if not obs:
        return pd.DataFrame(columns=["date", "value"])
    df = pd.DataFrame(obs)[["date", "value"]]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.drop_duplicates(subset=["date"], keep="last")
    if not to_daily:
        df["date"] = df["date"].dt.date
        return df
    # Reindex to business-day frequency and forward-fill so all series are daily
    date_range = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="B")
    df = df.set_index("date").reindex(date_range).ffill().reset_index()
    date_col = df.columns[0]
    if date_col != "date":
        df = df.rename(columns={date_col: "date"})
    df["date"] = df["date"].dt.date
    return df


def fetch_vintagedates(series_id: str, api_key: str, realtime_start: str, realtime_end: str) -> list[str]:
    """Return list of vintage dates (YYYY-MM-DD) in the realtime range. Used for PIT fetch."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
        "sort_order": "asc",
        "limit": 10000,
    }
    r = _fred_get(FRED_VINTAGEDATES_URL, params)
    data = r.json()
    if "error_message" in data:
        raise ValueError(data["error_message"])
    # Response has "vintage_dates" array of objects with "vintage_date" key, or direct array
    raw = data.get("vintage_dates", [])
    if not raw:
        return []
    if isinstance(raw[0], dict):
        dates = [x["vintage_date"] for x in raw if "vintage_date" in x]
    else:
        dates = list(raw)
    return [d for d in dates if DATE_START <= d <= DATE_END]


def main() -> None:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        log.error("FRED_API_KEY not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")
        sys.exit(1)

    FRED_DIR.mkdir(parents=True, exist_ok=True)
    pit_series = set(FRED_PIT_SERIES) if FRED_USE_PIT else set()

    for name, series_id in SERIES.items():
        log.info("Fetching %s (%s)", name, series_id)
        try:
            if name in pit_series:
                # PIT path: vintagedates then observations per vintage; write long-form (observation_date, vintage_date, value)
                vintage_dates = fetch_vintagedates(series_id, api_key, DATE_START, DATE_END)
                if not vintage_dates:
                    log.warning("No vintage dates in range for %s", series_id)
                    continue
                log.info("PIT %s: %d vintages", name, len(vintage_dates))
                chunks = []
                for vd in tqdm(vintage_dates, desc=f"PIT {name}", unit="vintage"):
                    df = fetch_series(
                        series_id, api_key, DATE_START, DATE_END,
                        realtime_start=vd, realtime_end=vd, to_daily=False,
                    )
                    if df.empty:
                        continue
                    df["vintage_date"] = pd.to_datetime(vd).date()
                    chunks.append(df)
                if not chunks:
                    log.warning("No observations for PIT %s", series_id)
                    continue
                out_df = pd.concat(chunks, ignore_index=True)
                out_df = out_df.rename(columns={"date": "observation_date"})
                out_df = out_df[["observation_date", "vintage_date", "value"]]
                out_df["observation_date"] = pd.to_datetime(out_df["observation_date"]).dt.date
                out_path = FRED_DIR / f"{name}.parquet"
                out_df.to_parquet(out_path, index=False)
                log.info("Wrote PIT %s (%d rows)", out_path, len(out_df))
            else:
                # Non-PIT: single observations call, write (date, stem)
                df = fetch_series(series_id, api_key, DATE_START, DATE_END, to_daily=True)
                if df.empty:
                    log.warning("No observations for %s", series_id)
                    continue
                df = df.rename(columns={"value": name})
                out_path = FRED_DIR / f"{name}.parquet"
                df.to_parquet(out_path, index=False)
                log.info("Wrote %s (%d rows)", out_path, len(df))
        except requests.RequestException as e:
            log.warning("Failed to fetch %s: %s", series_id, e)
            continue
        except ValueError as e:
            log.warning("FRED API error for %s: %s", series_id, e)
            continue
        except Exception as e:
            log.warning("Error writing %s: %s", name, e)

    log.info("FRED fetch complete. Outputs in %s", FRED_DIR)


if __name__ == "__main__":
    main()
