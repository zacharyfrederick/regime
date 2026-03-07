"""
Pipeline configuration: paths, date range, and key parameters.
Scripts and notebooks import from here. Change one file to run on a different
date range or data location.
"""
from pathlib import Path

# Project root (directory containing config.py)
ROOT_DIR = Path(__file__).resolve().parent

# Raw data: Sharadar parquet files live under DATA_DIR.
# Expected: TICKERS, ACTIONS, SF1, SF2, SF3, SF3A, SEP, DAILY, METRICS, SP500, etc.
DATA_DIR = ROOT_DIR / "data"

# FRED macro cache (parquet per series). Populate via download script or pipeline.
FRED_DIR = DATA_DIR / "macro"

# PIT FRED: use real-time vintages for revised series (see docs/pit_fred_plan.md).
FRED_USE_PIT = True  # Set True to fetch/store vintages for FRED_PIT_SERIES.
FRED_PIT_SERIES = ("cpi", "nfci", "fed_funds")  # Series that get vintage fetch; rest stay single-vintage.
# Rate limiting: FRED allows up to 2 req/s; 429 on exceed.
FRED_REQUEST_DELAY_SEC = 0.55  # Sleep after each request to stay at or below 2 req/s.
FRED_429_RETRY_WAIT_SEC = 5  # Wait before retry on HTTP 429.
FRED_429_MAX_RETRIES = 3  # Max retries on 429 before giving up.

# Pipeline outputs
OUTPUTS_DIR = ROOT_DIR / "outputs"
UNIVERSE_DIR = OUTPUTS_DIR / "universe"
FEATURES_DIR = OUTPUTS_DIR / "features"
MASTER_DIR = OUTPUTS_DIR / "master"
LABELS_DIR = OUTPUTS_DIR / "labels"

# Artifact paths
DAILY_UNIVERSE_PATH = UNIVERSE_DIR / "daily_universe.parquet"
FORWARD_LABELS_PATH = LABELS_DIR / "forward_labels.parquet"
FUNDAMENTAL_PIT_PATH = FEATURES_DIR / "fundamental_pit.parquet"
PRICE_FEATURES_PATH = FEATURES_DIR / "price_features.parquet"
MACRO_FEATURES_PATH = FEATURES_DIR / "macro_features.parquet"
SECTOR_RELATIVE_PATH = FEATURES_DIR / "sector_relative.parquet"
INSIDER_INSTITUTIONAL_PATH = FEATURES_DIR / "insider_institutional.parquet"
MASTER_FEATURES_PATH = MASTER_DIR / "master_features.parquet"

# Date range for pipeline (inclusive). Use a subset for development (e.g. 3 years).
DATE_START = "2000-01-01"
DATE_END = "2024-12-31"

# Universe / liquidity (used in universe construction or merge)
SEP_LOOKBACK_DAYS = 5  # require price in last N trading days for activity

# Benchmark / index tickers (verify in your SEP/SFP data)
SPY_TICKER = "SPY"
VIX_FRED_CODE = "VIXCLS"  # or use ETF proxy from SFP if preferred

# Debug: limit pipeline to a few tickers for fast runs (long-history names).
DEBUG = True
DEBUG_TICKERS = None  # When None and DEBUG is True, use DEBUG_TICKERS_DEFAULT.
DEBUG_TICKERS_DEFAULT = ("AAPL", "MSFT", "JPM", "XOM", "JNJ", "HTZ", "TWX", "MON", "TIF", "DNKN", "ETFC")

# DuckDB: optional memory cap and temp dir for spill (reduces peak RAM in heavy pipeline steps).


def apply_duckdb_limits(con) -> None:
    pass
