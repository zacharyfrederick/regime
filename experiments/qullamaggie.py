#!/usr/bin/env python3
"""
Qullamaggie-style setup: technical features from SEP + MFE/MAE labels from next-day open,
walk-forward Ridge/XGBoost, diagnostics (IC, decile lift) and portfolio metrics.

See module docstring at bottom of file for original feature/label spec.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

from config import DATA_DIR, DAILY_UNIVERSE_PATH, DATE_END, OUTPUTS_DIR, TICKERS_PATH
from walkforward import (
    DEFAULT_DAILY_FOLD_PERIODS,
    DEFAULT_WEEK_FOLD_PERIODS,
    encode_fold,
    generate_folds,
    get_rebal_dates,
    oos_drawdown_and_cagr,
    periods_per_year_for_freq,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_START_QM = "2023-01-01"
# Cap output dates to pipeline universe end (same as 01_universe / master range).
DATA_END_QM = DATE_END
# Longest trading-day lookback in features: 52-week window uses 251 prior rows + current = 252 bars.
FEATURE_MAX_LAG_TRADING = 252
# Calendar days before min(grid date) so LAG/rolling windows are warm by first signal date.
# ~252 trading days ≈ 360 calendar; 400 is a safe cushion (weekends/holidays).
LOOKBACK_CAL_DAYS = 400
# Calendar days after max(grid date) so next-open entry + HOLD_DAYS and terminal exits have SEP rows.
FORWARD_CAL_BUFFER_DAYS = 55
HOLD_DAYS = 21
ALPHA_SCORE = 0.5
TIGHT_CLOSE_ATR_MULT = 0.1
# ATR (dollars) = atr_14_pct * signal close; stop/target in price vs next-open entry
TP_SL_K_STOP = 1.0
TP_SL_K_TARGET = 2.0
IS_MONTHS = 84
OOS_MONTHS = 48
EMBARGO_MONTHS = 2
MIN_OOS_MONTHS = 12
PERIODS_PER_YEAR = 12.0  # monthly WF annualization; daily/week use periods_per_year_for_freq

# Close > max(close on prior N days excluding T); see docs/qullamaggie_flow.md
DEFAULT_BREAKOUT_COL = "breakout_close_20d"


# Daily WF with long IS/OOS; needs many distinct panel dates (see DEFAULT_DAILY_FOLD_PERIODS in walkforward).
DAILY_WF_COMPACT = {
    "is_periods": 80,
    "oos_periods": 60,
    "embargo_periods": 5,
    "min_oos_periods": 20,
}


def _wf_fold_periods(wf_freq: str, *, compact: bool = False) -> tuple[int, int, int, int]:
    """(is_periods, oos_periods, embargo_periods, min_oos_periods) in rebal-step units."""
    if wf_freq == "month":
        return IS_MONTHS, OOS_MONTHS, EMBARGO_MONTHS, MIN_OOS_MONTHS
    if wf_freq == "week":
        d = DEFAULT_WEEK_FOLD_PERIODS
        return d["is_periods"], d["oos_periods"], d["embargo_periods"], d["min_oos_periods"]
    if wf_freq == "day":
        d = DAILY_WF_COMPACT if compact else DEFAULT_DAILY_FOLD_PERIODS
        return d["is_periods"], d["oos_periods"], d["embargo_periods"], d["min_oos_periods"]
    raise ValueError(f"wf_freq must be month, week, or day, got {wf_freq!r}")

# Persist under outputs/momentum/ (shared outputs area; filename names this experiment).
MOMENTUM_OUTPUT_DIR = OUTPUTS_DIR / "momentum"
PANEL_PATH = MOMENTUM_OUTPUT_DIR / "qullamaggie_panel.parquet"
SCREEN_PATH = MOMENTUM_OUTPUT_DIR / "daily_screener_candidates.parquet"


@dataclass(frozen=True)
class ScreenerParams:
    """Historical multi-horizon momentum screener (1m / 3m / 6m trading-day lags)."""

    momentum_td_1m: int = 21
    momentum_td_3m: int = 63
    momentum_td_6m: int = 126
    adr_lookback_trading_days: int = 14
    min_dollar_volume_1d: float = 500_000.0
    min_adr_pct: float = 1.0
    top_gainer_fraction: float = 0.05

    def params_hash(self) -> str:
        payload = (
            f"{self.momentum_td_1m},{self.momentum_td_3m},{self.momentum_td_6m},"
            f"{self.adr_lookback_trading_days},{self.min_dollar_volume_1d},"
            f"{self.min_adr_pct},{self.top_gainer_fraction}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


DEFAULT_SCREENER_PARAMS = ScreenerParams()

RIDGE_ALPHA = 1.0
XGB_PARAMS = {
    "objective": "reg:squarederror",
    "max_depth": 4,
    "learning_rate": 0.05,
    "n_estimators": 100,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
}

# IS rows after target + feature validity; 100+ is comfortable, sparse panels need a lower floor.
MIN_IS_TRAIN_ROWS = 30
MIN_IS_TRAIN_ROWS_IDEAL = 100

TARGET_MFE = "mfe_atr"
TARGET_MAE = "mae_atr"
RET_COL = "fwd_ret_entry_open_H"  # horizon / terminal PnL (training row filter)
RET_COL_TP_SL_ATR = "fwd_ret_tp_sl_atr"  # first-hit stop or target in path, else label exit_price
# TP/SL path tags (daily bars; same-bar ambiguity: stop checked before target — see docs).
TP_SL_TP_HIT_FIRST = "tp_sl_tp_hit_first"
TP_SL_SL_HIT_FIRST = "tp_sl_sl_hit_first"
TP_SL_NEITHER_HIT = "tp_sl_neither_hit"  # horizon/terminal exit before both levels hit
TP_SL_BARS_TO_TP = "tp_sl_bars_to_tp"
TP_SL_BARS_TO_SL = "tp_sl_bars_to_sl"
TP_SL_OUTCOME_COLS = (
    TP_SL_TP_HIT_FIRST,
    TP_SL_SL_HIT_FIRST,
    TP_SL_NEITHER_HIT,
    TP_SL_BARS_TO_TP,
    TP_SL_BARS_TO_SL,
)
# Non-tradable diagnostic: portfolio sim uses score_pred as if it were a return (Option 1).
RET_COL_PRED_PROXY = "_eval_ret_pred_proxy"
RANK_SCORE_COL = "_rank_score"  # walk-forward: re-rank from mfe_pred/mae_pred without retraining

FEATURE_COLS = [
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "ret_21d",
    "ret_63d",
    "ret_126d",
    "adr_5",
    "adr_10",
    "adr_20",
    "atr_14_pct",
    "sma_10",
    "sma_20",
    "sma_50",
    "dist_sma_10",
    "dist_sma_20",
    "dist_sma_50",
    "dist_52w_high",
    "dist_52w_low",
    "slope_logret_21",
    "slope_logret_63",
    "slope_logret_126",
    "pullback_pct_5",
    "pullback_pct_10",
    "pullback_pct_20",
    "dist_to_high_5",
    "dist_to_high_10",
    "dist_to_high_20",
    "range_5",
    "range_10",
    "range_20",
    "range_5_over_range_20",
    "tight_close_frac_10",
    "vol_5_over_vol_20",
    "vol_log_slope_20",
    "days_since_high_5",
    "days_since_high_10",
    "days_since_high_20",
]

# Screener-derived columns (prefixed sc_) when --use-screener panel build
SCREENER_FEATURE_COLS = [
    "sc_dollar_volume_1d",
    "sc_adr_pct_trailing",
    "sc_ret_pct_1m",
    "sc_ret_pct_3m",
    "sc_ret_pct_6m",
    "sc_ret_rank_pct_1m",
    "sc_ret_rank_pct_3m",
    "sc_ret_rank_pct_6m",
    "screen_1m",
    "screen_3m",
    "screen_6m",
    "n_screen_horizons",
]


def _path_sql(p: Path) -> str:
    return repr(str(p.resolve()))


def _sep_path() -> Path:
    p = DATA_DIR / "SEP.parquet"
    return p if p.exists() else DATA_DIR / "sep.parquet"


def _actions_path() -> Path:
    p = DATA_DIR / "ACTIONS.parquet"
    return p if p.exists() else DATA_DIR / "actions.parquet"


def _tickers_path() -> Path:
    if TICKERS_PATH.exists():
        return TICKERS_PATH
    alt = DATA_DIR / "tickers.parquet"
    return alt if alt.exists() else TICKERS_PATH


def _register_tickers_base(con: duckdb.DuckDBPyConnection, uni_sql: str) -> None:
    """Eligible tickers: same Sharadar TICKERS slice as pipeline/01_universe (SF1 + US common + USD).

    Aligns with the screener snippet: ``table`` = SF1, strict exchange/category, no dot-tickers,
    not delisted (boolean false and/or Sharadar 'N'), ``currency`` default '' like 01_universe.
    """
    tp = _tickers_path()
    if tp.exists():
        tsql = _path_sql(tp)
        con.execute(
            f"""
            CREATE OR REPLACE VIEW tickers_base AS
            SELECT DISTINCT CAST(ticker AS VARCHAR) AS ticker
            FROM read_parquet({tsql})
            WHERE "table" = 'SF1'
              AND ticker IS NOT NULL AND TRIM(COALESCE(ticker, '')) <> ''
              AND exchange IN ('NYSE', 'NASDAQ', 'NYSEMKT')
              AND category IN (
                  'Domestic Common Stock Primary Class',
                  'Domestic Common Stock',
                  'Domestic Common Stock Secondary Class'
              )
              AND ticker NOT LIKE '%.%'
              AND UPPER(COALESCE(currency, '')) = 'USD'
              AND COALESCE(LOWER(CAST(isdelisted AS VARCHAR)), 'false') IN ('false', '0', 'n', '')
            """
        )
    else:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW tickers_base AS
            SELECT DISTINCT CAST(ticker AS VARCHAR) AS ticker
            FROM read_parquet({uni_sql})
            WHERE COALESCE(in_universe, TRUE) = TRUE
            """
        )


def build_screener_parquet(
    con: duckdb.DuckDBPyConnection,
    params: ScreenerParams = DEFAULT_SCREENER_PARAMS,
    write_parquet: bool = True,
    out_path: Path | None = None,
) -> Path:
    """Stage 1: daily historical screen; union of top gainers 1m/3m/6m; write parquet."""
    dest = out_path or SCREEN_PATH
    if not DAILY_UNIVERSE_PATH.exists():
        raise FileNotFoundError(f"Universe not found: {DAILY_UNIVERSE_PATH}")
    sep_path = _sep_path()
    if not sep_path.exists():
        raise FileNotFoundError(f"SEP not found: {sep_path}")

    uni_sql = _path_sql(DAILY_UNIVERSE_PATH)
    sep_sql = _path_sql(sep_path)
    _register_tickers_base(con, uni_sql)

    td1, td3, td6 = params.momentum_td_1m, params.momentum_td_3m, params.momentum_td_6m
    lb = max(2, params.adr_lookback_trading_days)
    min_dv = params.min_dollar_volume_1d
    min_adr = params.min_adr_pct
    frac = params.top_gainer_fraction
    phash = params.params_hash()
    lb_cal = LOOKBACK_CAL_DAYS
    fw_cal = FORWARD_CAL_BUFFER_DAYS

    con.execute(
        f"""
        CREATE OR REPLACE VIEW sep_for_screen AS
        SELECT
            CAST(ticker AS VARCHAR) AS ticker,
            CAST(date AS DATE) AS d,
            close,
            high,
            low,
            closeadj,
            volume
        FROM read_parquet({sep_sql})
        WHERE closeadj IS NOT NULL AND closeadj > 0
          AND ticker IN (SELECT ticker FROM tickers_base)
          AND CAST(date AS DATE) >= CAST('{DATA_START_QM}' AS DATE) - INTERVAL '{lb_cal} days'
          AND CAST(date AS DATE) <= CAST('{DATA_END_QM}' AS DATE) + INTERVAL '{fw_cal} days'
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_daily AS
        SELECT
            ticker,
            d AS date,
            closeadj * volume AS dollar_volume_1d,
            AVG((high - low) / NULLIF(closeadj, 0) * 100.0) OVER (
                PARTITION BY ticker
                ORDER BY d
                ROWS BETWEEN {lb - 1} PRECEDING AND CURRENT ROW
            ) AS adr_pct_trailing,
            (closeadj / NULLIF(LAG(closeadj, {td1}) OVER (PARTITION BY ticker ORDER BY d), 0) - 1.0)
                * 100.0 AS ret_pct_1m,
            (closeadj / NULLIF(LAG(closeadj, {td3}) OVER (PARTITION BY ticker ORDER BY d), 0) - 1.0)
                * 100.0 AS ret_pct_3m,
            (closeadj / NULLIF(LAG(closeadj, {td6}) OVER (PARTITION BY ticker ORDER BY d), 0) - 1.0)
                * 100.0 AS ret_pct_6m
        FROM sep_for_screen
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_in_range AS
        SELECT *
        FROM screen_daily
        WHERE date >= CAST('{DATA_START_QM}' AS DATE)
          AND date <= CAST('{DATA_END_QM}' AS DATE)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_gated AS
        SELECT *,
            CASE
                WHEN dollar_volume_1d > {min_dv}
                 AND adr_pct_trailing > {min_adr}
                 AND ret_pct_1m IS NOT NULL
                 AND ret_pct_3m IS NOT NULL
                 AND ret_pct_6m IS NOT NULL
                THEN 1 ELSE 0
            END AS pass_gate
        FROM screen_in_range
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW screen_n_gated AS
        SELECT date, COUNT(*) AS n_gated
        FROM screen_gated
        WHERE pass_gate = 1
        GROUP BY date
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_rank_1m AS
        SELECT ticker, date,
               ROW_NUMBER() OVER (PARTITION BY date ORDER BY ret_pct_1m DESC) AS rn
        FROM screen_gated
        WHERE pass_gate = 1
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_rank_3m AS
        SELECT ticker, date,
               ROW_NUMBER() OVER (PARTITION BY date ORDER BY ret_pct_3m DESC) AS rn
        FROM screen_gated
        WHERE pass_gate = 1
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_rank_6m AS
        SELECT ticker, date,
               ROW_NUMBER() OVER (PARTITION BY date ORDER BY ret_pct_6m DESC) AS rn
        FROM screen_gated
        WHERE pass_gate = 1
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_scored AS
        SELECT
            g.ticker,
            g.date,
            g.dollar_volume_1d,
            g.adr_pct_trailing,
            g.ret_pct_1m,
            g.ret_pct_3m,
            g.ret_pct_6m,
            g.pass_gate,
            n.n_gated,
            r1.rn AS rn_1m,
            r3.rn AS rn_3m,
            r6.rn AS rn_6m,
            CASE
                WHEN n.n_gated IS NULL OR n.n_gated < 1 THEN NULL
                WHEN n.n_gated = 1 THEN 1.0
                ELSE 1.0 - (r1.rn - 1.0) / (n.n_gated - 1.0)
            END AS ret_rank_pct_1m,
            CASE
                WHEN n.n_gated IS NULL OR n.n_gated < 1 THEN NULL
                WHEN n.n_gated = 1 THEN 1.0
                ELSE 1.0 - (r3.rn - 1.0) / (n.n_gated - 1.0)
            END AS ret_rank_pct_3m,
            CASE
                WHEN n.n_gated IS NULL OR n.n_gated < 1 THEN NULL
                WHEN n.n_gated = 1 THEN 1.0
                ELSE 1.0 - (r6.rn - 1.0) / (n.n_gated - 1.0)
            END AS ret_rank_pct_6m,
            CASE
                WHEN g.pass_gate = 1 AND r1.rn IS NOT NULL AND n.n_gated IS NOT NULL
                     AND r1.rn <= GREATEST(1, CEIL(n.n_gated * {frac}))
                THEN TRUE ELSE FALSE
            END AS screen_1m,
            CASE
                WHEN g.pass_gate = 1 AND r3.rn IS NOT NULL AND n.n_gated IS NOT NULL
                     AND r3.rn <= GREATEST(1, CEIL(n.n_gated * {frac}))
                THEN TRUE ELSE FALSE
            END AS screen_3m,
            CASE
                WHEN g.pass_gate = 1 AND r6.rn IS NOT NULL AND n.n_gated IS NOT NULL
                     AND r6.rn <= GREATEST(1, CEIL(n.n_gated * {frac}))
                THEN TRUE ELSE FALSE
            END AS screen_6m
        FROM screen_gated g
        LEFT JOIN screen_n_gated n ON n.date = g.date
        LEFT JOIN screen_rank_1m r1 ON r1.ticker = g.ticker AND r1.date = g.date
        LEFT JOIN screen_rank_3m r3 ON r3.ticker = g.ticker AND r3.date = g.date
        LEFT JOIN screen_rank_6m r6 ON r6.ticker = g.ticker AND r6.date = g.date
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW screen_candidates AS
        SELECT
            ticker,
            date,
            dollar_volume_1d,
            adr_pct_trailing,
            ret_pct_1m,
            ret_pct_3m,
            ret_pct_6m,
            ret_rank_pct_1m,
            ret_rank_pct_3m,
            ret_rank_pct_6m,
            screen_1m,
            screen_3m,
            screen_6m,
            (CAST(screen_1m AS INTEGER) + CAST(screen_3m AS INTEGER) + CAST(screen_6m AS INTEGER))
                AS n_horizons
        FROM screen_scored
        WHERE screen_1m OR screen_3m OR screen_6m
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW screen_out AS
        SELECT *, '{phash}' AS screener_params_hash
        FROM screen_candidates
        """
    )

    dest.parent.mkdir(parents=True, exist_ok=True)
    if write_parquet:
        con.execute(f"COPY (SELECT * FROM screen_out) TO {_path_sql(dest)} (FORMAT PARQUET)")
    return dest


def log_screener_candidate_stats(parquet_path: Path = SCREEN_PATH) -> None:
    if not parquet_path.exists():
        return
    df = pd.read_parquet(parquet_path, columns=["date"])
    c = df.groupby(df["date"].astype(str)).size()
    print(
        f"Screener candidates/day: mean={c.mean():.1f} p95={c.quantile(0.95):.1f} "
        f"max={c.max()} (n_dates={len(c)})"
    )


def register_terminal_event_views(con: duckdb.DuckDBPyConnection) -> None:
    """Same ACTIONS → terminal_events_resolved pattern as pipeline/07_labels.py."""
    con.execute(
        """
        CREATE OR REPLACE VIEW delist_dates AS
        SELECT ticker, CAST(date AS DATE) AS event_date
        FROM actions
        WHERE LOWER(TRIM(action)) = 'delisted'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW delist_reasons AS
        SELECT ticker, CAST(date AS DATE) AS event_date, LOWER(TRIM(action)) AS action
        FROM actions
        WHERE LOWER(TRIM(action)) IN (
            'acquisitionby','bankruptcyliquidation','regulatorydelisting',
            'voluntarydelisting','mergerfrom'
        )
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW resolved_delists_raw AS
        SELECT d.ticker, d.event_date, COALESCE(r.action, 'unknown') AS delist_type
        FROM delist_dates d
        LEFT JOIN delist_reasons r ON r.ticker = d.ticker AND r.event_date = d.event_date
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW renames_near_delist AS
        SELECT ticker, CAST(date AS DATE) AS rename_date
        FROM actions
        WHERE LOWER(TRIM(action)) = 'tickerchangefrom'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW terminal_events_resolved AS
        WITH base AS (
            SELECT r.ticker, r.event_date, r.delist_type
            FROM resolved_delists_raw r
            LEFT JOIN renames_near_delist tc ON tc.ticker = r.ticker
                AND ABS(DATEDIFF('day', tc.rename_date, r.event_date)) <= 5
            WHERE tc.rename_date IS NULL
        ),
        ranked AS (
            SELECT ticker, event_date, delist_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY ticker, event_date
                       ORDER BY CASE delist_type
                           WHEN 'acquisitionby' THEN 1
                           WHEN 'bankruptcyliquidation' THEN 2
                           WHEN 'voluntarydelisting' THEN 3
                           WHEN 'regulatorydelisting' THEN 4
                           WHEN 'mergerfrom' THEN 5
                           ELSE 6
                       END
                   ) AS rn
            FROM base
        )
        SELECT ticker, event_date, delist_type
        FROM ranked
        WHERE rn = 1
        """
    )


def add_days_since_high(df: pd.DataFrame, windows: tuple[int, ...] = (5, 10, 20)) -> pd.DataFrame:
    """Bars since last touch of the trailing N-day high (per ticker)."""
    df = df.sort_values(["ticker", "date"]).copy()
    if "closeadj" not in df.columns:
        return df
    for w in windows:
        col = f"days_since_high_{w}"
        blocks = []
        for _, g in df.groupby("ticker", sort=False):
            c = g["closeadj"].to_numpy(dtype=float)
            n = len(c)
            days = np.zeros(n, dtype=np.int32)
            for i in range(n):
                lo = max(0, i - w + 1)
                window = c[lo : i + 1]
                rm = np.nanmax(window)
                if not np.isfinite(rm):
                    days[i] = 0
                    continue
                rel = np.where(np.abs(window - rm) <= np.maximum(1e-9 * (abs(rm) + 1.0), 1e-12))[0]
                last_rel = int(rel[-1]) if len(rel) else 0
                days[i] = i - (lo + last_rel)
            blocks.append(days)
        df[col] = np.concatenate(blocks)
    return df


def _tp_sl_simulate_trade(
    g: pd.DataFrame,
    k_stop: float,
    k_target: float,
) -> dict[str, float | bool]:
    """Long-only path sim: ATR$ from **signal** bar; entry = next open.

    **Same-bar ambiguity (daily OHLC):** on each bar we test ``low <= stop`` *before*
    ``high >= target`` (conservative / stop-first). Open ordering within the bar is unknown;
    this matches a pessimistic intrabar assumption for longs.
    """
    g = g.sort_values("rn")
    entry = float(g["entry_open"].iloc[0])
    exit_p = float(g["exit_price"].iloc[0])
    atrp = g["atr_14_pct"].iloc[0]
    sig_c = g["sig_close"].iloc[0]
    nan_ret = {
        RET_COL_TP_SL_ATR: np.nan,
        TP_SL_TP_HIT_FIRST: np.nan,
        TP_SL_SL_HIT_FIRST: np.nan,
        TP_SL_NEITHER_HIT: np.nan,
        TP_SL_BARS_TO_TP: np.nan,
        TP_SL_BARS_TO_SL: np.nan,
    }
    if entry <= 0 or exit_p <= 0 or pd.isna(atrp) or pd.isna(sig_c) or float(sig_c) <= 0:
        return nan_ret
    atr_d = float(atrp) * float(sig_c)
    stop_p = entry - k_stop * atr_d
    tgt_p = entry + k_target * atr_d
    bars = 0
    for _, row in g.iterrows():
        bars += 1
        lo, hi = float(row["low"]), float(row["high"])
        if lo <= stop_p:
            return {
                RET_COL_TP_SL_ATR: stop_p / entry - 1.0,
                TP_SL_TP_HIT_FIRST: False,
                TP_SL_SL_HIT_FIRST: True,
                TP_SL_NEITHER_HIT: False,
                TP_SL_BARS_TO_TP: np.nan,
                TP_SL_BARS_TO_SL: float(bars),
            }
        if hi >= tgt_p:
            return {
                RET_COL_TP_SL_ATR: tgt_p / entry - 1.0,
                TP_SL_TP_HIT_FIRST: True,
                TP_SL_SL_HIT_FIRST: False,
                TP_SL_NEITHER_HIT: False,
                TP_SL_BARS_TO_TP: float(bars),
                TP_SL_BARS_TO_SL: np.nan,
            }
    return {
        RET_COL_TP_SL_ATR: exit_p / entry - 1.0,
        TP_SL_TP_HIT_FIRST: False,
        TP_SL_SL_HIT_FIRST: False,
        TP_SL_NEITHER_HIT: True,
        TP_SL_BARS_TO_TP: np.nan,
        TP_SL_BARS_TO_SL: np.nan,
    }


def _tp_sl_returns_from_duckdb(
    con: duckdb.DuckDBPyConnection,
    k_stop: float,
    k_target: float,
) -> pd.DataFrame:
    """One row per (ticker, date): simulated TP/SL return + path outcome columns."""
    hd = HOLD_DAYS
    path_df = con.execute(
        f"""
        SELECT
            e.ticker,
            CAST(e.date AS DATE) AS date,
            e.entry_open,
            lc.exit_price,
            sig.atr_14_pct,
            sig.closeadj AS sig_close,
            f.rn,
            f.high,
            f.low
        FROM entries e
        INNER JOIN labels_core lc ON lc.ticker = e.ticker AND lc.date = e.date
        INNER JOIN qm_features_full sig ON sig.ticker = e.ticker AND sig.date = e.date
        INNER JOIN sep_max_rn m ON m.ticker = e.ticker
        INNER JOIN sep_ranked f ON f.ticker = e.ticker
            AND f.rn >= e.entry_rn AND f.rn <= LEAST(e.entry_rn + {hd} - 1, m.max_rn)
        WHERE lc.exit_price IS NOT NULL AND lc.exit_price > 0
          AND e.entry_open IS NOT NULL AND e.entry_open > 0
        ORDER BY e.ticker, e.date, f.rn
        """
    ).df()
    cols = ["ticker", "date", RET_COL_TP_SL_ATR, *TP_SL_OUTCOME_COLS]
    if path_df.empty:
        return pd.DataFrame(columns=cols)
    rows = []
    for (ticker, date), g in path_df.groupby(["ticker", "date"], sort=False):
        row = {"ticker": ticker, "date": date}
        row.update(_tp_sl_simulate_trade(g, k_stop, k_target))
        rows.append(row)
    return pd.DataFrame(rows)


def build_panel(
    con: duckdb.DuckDBPyConnection,
    write_parquet: bool = True,
    use_screener: bool = False,
    screener_parquet_path: Path | None = None,
    tp_sl_k_stop: float = TP_SL_K_STOP,
    tp_sl_k_target: float = TP_SL_K_TARGET,
) -> Path:
    """Build full feature + label panel; optionally write PANEL_PATH.

    When use_screener is True, grid = INNER JOIN of persisted screener candidates with
    daily_universe (PIT). SEP is filtered to distinct tickers from that grid.
    Otherwise grid is full in_universe daily rows in the date range.
    """
    if not DAILY_UNIVERSE_PATH.exists():
        raise FileNotFoundError(f"Universe not found: {DAILY_UNIVERSE_PATH}")
    sep_path = _sep_path()
    if not sep_path.exists():
        raise FileNotFoundError(f"SEP not found: {sep_path}")

    uni = _path_sql(DAILY_UNIVERSE_PATH)
    sep_sql = _path_sql(sep_path)
    sc_path = screener_parquet_path or SCREEN_PATH

    if use_screener:
        if not sc_path.exists():
            raise FileNotFoundError(
                f"Screener parquet not found: {sc_path}. Run "
                "`python experiments/qullamaggie.py screen` first."
            )
        ssql = _path_sql(Path(sc_path).resolve())
        con.execute(
            f"""
            CREATE OR REPLACE VIEW grid AS
            SELECT
                u.ticker,
                CAST(u.date AS DATE) AS date,
                u.sector,
                u.famaindustry,
                u.marketcap_daily,
                s.dollar_volume_1d AS sc_dollar_volume_1d,
                s.adr_pct_trailing AS sc_adr_pct_trailing,
                s.ret_pct_1m AS sc_ret_pct_1m,
                s.ret_pct_3m AS sc_ret_pct_3m,
                s.ret_pct_6m AS sc_ret_pct_6m,
                s.ret_rank_pct_1m AS sc_ret_rank_pct_1m,
                s.ret_rank_pct_3m AS sc_ret_rank_pct_3m,
                s.ret_rank_pct_6m AS sc_ret_rank_pct_6m,
                s.screen_1m AS screen_1m,
                s.screen_3m AS screen_3m,
                s.screen_6m AS screen_6m,
                s.n_horizons AS n_screen_horizons
            FROM read_parquet({ssql}) AS s
            INNER JOIN read_parquet({uni}) AS u
                ON u.ticker = s.ticker AND CAST(u.date AS DATE) = s.date
            WHERE COALESCE(u.in_universe, TRUE) = TRUE
              AND CAST(u.date AS DATE) >= CAST('{DATA_START_QM}' AS DATE)
              AND CAST(u.date AS DATE) <= CAST('{DATA_END_QM}' AS DATE)
            """
        )
    else:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW grid AS
            SELECT ticker, CAST(date AS DATE) AS date, sector, famaindustry, marketcap_daily
            FROM read_parquet({uni})
            WHERE COALESCE(in_universe, TRUE) = TRUE
              AND CAST(date AS DATE) >= CAST('{DATA_START_QM}' AS DATE)
              AND CAST(date AS DATE) <= CAST('{DATA_END_QM}' AS DATE)
            """
        )

    lb = LOOKBACK_CAL_DAYS
    fw = FORWARD_CAL_BUFFER_DAYS
    con.execute(
        f"""
        CREATE OR REPLACE VIEW sep_filtered AS
        SELECT
            ticker,
            CAST(date AS DATE) AS date,
            COALESCE(open, closeadj) AS open,
            high,
            low,
            closeadj,
            volume
        FROM read_parquet({sep_sql})
        WHERE closeadj IS NOT NULL AND closeadj > 0
          AND ticker IN (SELECT DISTINCT ticker FROM grid)
          AND CAST(date AS DATE) >= CAST((SELECT MIN(date) - INTERVAL '{lb} days' FROM grid) AS DATE)
          AND CAST(date AS DATE) <= CAST((SELECT MAX(date) + INTERVAL '{fw} days' FROM grid) AS DATE)
        """
    )

    actions_p = _actions_path()
    if actions_p.exists():
        con.execute(
            f"""
            CREATE OR REPLACE VIEW actions AS
            SELECT a.*
            FROM read_parquet({_path_sql(actions_p)}) AS a
            WHERE a.ticker IN (SELECT DISTINCT ticker FROM grid)
            """
        )
        register_terminal_event_views(con)
    else:
        con.execute(
            """
            CREATE OR REPLACE VIEW actions AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS date,
                   CAST(NULL AS VARCHAR) AS action WHERE 1=0
            """
        )
        con.execute(
            """
            CREATE OR REPLACE VIEW terminal_events_resolved AS
            SELECT CAST(NULL AS VARCHAR) AS ticker, CAST(NULL AS DATE) AS event_date,
                   CAST(NULL AS VARCHAR) AS delist_type WHERE 1=0
            """
        )

    con.execute(
        """
        CREATE OR REPLACE TABLE sep_ranked AS
        SELECT ticker, date, open, high, low, closeadj, volume,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date) AS rn
        FROM sep_filtered
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW sep_max_rn AS
        SELECT ticker, MAX(rn) AS max_rn
        FROM sep_ranked
        GROUP BY ticker
        """
    )

    hd = HOLD_DAYS
    con.execute(
        f"""
        CREATE OR REPLACE VIEW qm_features_full AS
        WITH daily AS (
            SELECT
                ticker, date, open, high, low, closeadj, volume,
                LAG(closeadj, 1) OVER w AS closeadj_lag1,
                closeadj / LAG(closeadj, 1) OVER w - 1 AS daily_ret,
                high - low AS hl_range,
                ABS(high - LAG(closeadj, 1) OVER w) AS hc_range,
                ABS(low - LAG(closeadj, 1) OVER w) AS lc_range,
                LN(closeadj / NULLIF(LAG(closeadj, 1) OVER w, 0)) AS log_ret,
                closeadj / NULLIF(LAG(closeadj, 5) OVER w, 0) - 1 AS ret_5d,
                closeadj / NULLIF(LAG(closeadj, 10) OVER w, 0) - 1 AS ret_10d,
                closeadj / NULLIF(LAG(closeadj, 20) OVER w, 0) - 1 AS ret_20d,
                closeadj / NULLIF(LAG(closeadj, 21) OVER w, 0) - 1 AS ret_21d,
                closeadj / NULLIF(LAG(closeadj, 63) OVER w, 0) - 1 AS ret_63d,
                closeadj / NULLIF(LAG(closeadj, 126) OVER w, 0) - 1 AS ret_126d
            FROM sep_filtered
            WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ),
        win AS (
            SELECT *,
                AVG(hl_range) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS adr_5,
                AVG(hl_range) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS adr_10,
                AVG(hl_range) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS adr_20,
                AVG(GREATEST(hl_range, hc_range, lc_range)) OVER (
                    PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) / NULLIF(closeadj, 0) AS atr_14_pct,
                AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS sma_10,
                AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
                AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
                MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52w,
                MIN(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS low_52w,
                MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS roll_max_5,
                MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS roll_max_10,
                MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS roll_max_20,
                MIN(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS roll_min_5,
                MIN(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS roll_min_10,
                MIN(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS roll_min_20,
                AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS roll_mean_close_5,
                AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS roll_mean_close_10,
                AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS roll_mean_close_20,
                AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS vol_avg_5,
                AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_avg_20,
                AVG(GREATEST(hl_range, hc_range, lc_range)) OVER (
                    PARTITION BY ticker ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS tr_14_abs,
                LAG(closeadj, 21) OVER (PARTITION BY ticker ORDER BY date) AS close_lag_21,
                LAG(closeadj, 63) OVER (PARTITION BY ticker ORDER BY date) AS close_lag_63,
                LAG(closeadj, 126) OVER (PARTITION BY ticker ORDER BY date) AS close_lag_126,
                LAG(LN(NULLIF(volume, 0)), 20) OVER (PARTITION BY ticker ORDER BY date) AS ln_vol_lag20,
                MAX(closeadj) OVER (
                    PARTITION BY ticker ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ) AS prior_close_max_10d,
                MAX(closeadj) OVER (
                    PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS prior_close_max_20d
            FROM daily
        ),
        feat AS (
            SELECT *,
                (closeadj > prior_close_max_10d AND prior_close_max_10d IS NOT NULL) AS breakout_close_10d,
                (closeadj > prior_close_max_20d AND prior_close_max_20d IS NOT NULL) AS breakout_close_20d,
                closeadj / NULLIF(sma_10, 0) - 1 AS dist_sma_10,
                closeadj / NULLIF(sma_20, 0) - 1 AS dist_sma_20,
                closeadj / NULLIF(sma_50, 0) - 1 AS dist_sma_50,
                closeadj / NULLIF(high_52w, 0) - 1 AS dist_52w_high,
                closeadj / NULLIF(low_52w, 0) - 1 AS dist_52w_low,
                (LN(closeadj) - LN(NULLIF(close_lag_21, 0))) / 21.0 AS slope_logret_21,
                (LN(closeadj) - LN(NULLIF(close_lag_63, 0))) / 63.0 AS slope_logret_63,
                (LN(closeadj) - LN(NULLIF(close_lag_126, 0))) / 126.0 AS slope_logret_126,
                (roll_max_5 - closeadj) / NULLIF(roll_max_5, 0) AS pullback_pct_5,
                (roll_max_10 - closeadj) / NULLIF(roll_max_10, 0) AS pullback_pct_10,
                (roll_max_20 - closeadj) / NULLIF(roll_max_20, 0) AS pullback_pct_20,
                (roll_max_5 - closeadj) / NULLIF(closeadj, 0) AS dist_to_high_5,
                (roll_max_10 - closeadj) / NULLIF(closeadj, 0) AS dist_to_high_10,
                (roll_max_20 - closeadj) / NULLIF(closeadj, 0) AS dist_to_high_20,
                (roll_max_5 - roll_min_5) / NULLIF(roll_mean_close_5, 0) AS range_5,
                (roll_max_10 - roll_min_10) / NULLIF(roll_mean_close_10, 0) AS range_10,
                (roll_max_20 - roll_min_20) / NULLIF(roll_mean_close_20, 0) AS range_20,
                vol_avg_5 / NULLIF(vol_avg_20, 0) AS vol_5_over_vol_20,
                (LN(NULLIF(volume, 0)) - ln_vol_lag20) / 20.0 AS vol_log_slope_20
            FROM win
        ),
        tight AS (
            SELECT *,
                AVG(
                    CASE
                        WHEN closeadj_lag1 IS NOT NULL
                            AND ABS(closeadj - closeadj_lag1) < {TIGHT_CLOSE_ATR_MULT} * tr_14_abs
                        THEN 1.0 ELSE 0.0
                    END
                ) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS tight_close_frac_10
            FROM feat
        )
        SELECT
            ticker, date,
            ret_5d, ret_10d, ret_20d, ret_21d, ret_63d, ret_126d,
            adr_5, adr_10, adr_20, atr_14_pct,
            sma_10, sma_20, sma_50,
            dist_sma_10, dist_sma_20, dist_sma_50,
            dist_52w_high, dist_52w_low,
            slope_logret_21, slope_logret_63, slope_logret_126,
            pullback_pct_5, pullback_pct_10, pullback_pct_20,
            dist_to_high_5, dist_to_high_10, dist_to_high_20,
            range_5, range_10, range_20,
            range_5 / NULLIF(range_20, 0) AS range_5_over_range_20,
            tight_close_frac_10,
            vol_5_over_vol_20, vol_log_slope_20,
            breakout_close_10d,
            breakout_close_20d,
            closeadj
        FROM tight
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW entries AS
        SELECT
            g.ticker,
            g.date,
            g.sector,
            g.famaindustry,
            g.marketcap_daily,
            s0.rn AS rn_sig,
            se.open AS entry_open,
            se.rn AS entry_rn
        FROM grid g
        INNER JOIN sep_ranked s0 ON s0.ticker = g.ticker AND s0.date = g.date
        INNER JOIN sep_ranked se ON se.ticker = g.ticker AND se.rn = s0.rn + 1
        WHERE se.open IS NOT NULL AND se.open > 0
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW exc_path AS
        SELECT
            e.ticker,
            e.date,
            MAX(f.high / NULLIF(e.entry_open, 0) - 1.0) AS mfe_pct,
            MAX(1.0 - f.low / NULLIF(e.entry_open, 0)) AS mae_pct
        FROM entries e
        INNER JOIN sep_max_rn m ON m.ticker = e.ticker
        INNER JOIN sep_ranked f ON f.ticker = e.ticker
            AND f.rn BETWEEN e.entry_rn AND LEAST(e.entry_rn + {hd} - 1, m.max_rn)
        GROUP BY e.ticker, e.date
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW label_fwd AS
        SELECT
            e.ticker,
            e.date,
            e.entry_open,
            e.entry_rn,
            f.closeadj AS exit_sched,
            f.rn AS rn_exit_hit
        FROM entries e
        LEFT JOIN sep_ranked f ON f.ticker = e.ticker AND f.rn = e.entry_rn + {hd} - 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW terminal_exit AS
        SELECT
            e.ticker,
            e.date,
            s.closeadj AS term_closeadj,
            s.date AS term_date,
            s.rn AS term_rn
        FROM entries e
        INNER JOIN sep_max_rn m ON m.ticker = e.ticker
        INNER JOIN sep_ranked s ON s.ticker = e.ticker AND s.rn = m.max_rn
        INNER JOIN label_fwd lf ON lf.ticker = e.ticker AND lf.date = e.date
        WHERE lf.exit_sched IS NULL AND e.entry_rn < m.max_rn
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW last_day_exit AS
        SELECT e.ticker, e.date
        FROM entries e
        INNER JOIN sep_max_rn m ON m.ticker = e.ticker
        INNER JOIN label_fwd lf ON lf.ticker = e.ticker AND lf.date = e.date
        WHERE lf.exit_sched IS NULL AND e.entry_rn >= m.max_rn
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW labels_core AS
        SELECT
            e.ticker,
            e.date,
            e.entry_open,
            e.entry_rn,
            x.mfe_pct,
            x.mae_pct,
            CASE
                WHEN lf.exit_sched IS NOT NULL THEN lf.exit_sched
                WHEN t.term_closeadj IS NOT NULL THEN t.term_closeadj
                ELSE NULL
            END AS exit_price,
            CASE
                WHEN lf.exit_sched IS NOT NULL THEN {hd}
                WHEN t.term_rn IS NOT NULL THEN (t.term_rn - e.entry_rn + 1)::INTEGER
                ELSE NULL
            END AS fwd_holding_days,
            CASE
                WHEN lf.exit_sched IS NOT NULL THEN FALSE
                WHEN t.term_closeadj IS NOT NULL THEN
                    CASE WHEN ev.delist_type IS NULL THEN NULL ELSE ev.delist_type <> 'mergerfrom' END
                WHEN ld.ticker IS NOT NULL THEN NULL
                ELSE NULL
            END AS fwd_delisted,
            CASE
                WHEN lf.exit_sched IS NOT NULL THEN CAST(NULL AS VARCHAR)
                WHEN t.term_closeadj IS NOT NULL AND COALESCE(ev.delist_type, '') <> 'mergerfrom'
                    THEN ev.delist_type
                WHEN ld.ticker IS NOT NULL AND COALESCE(ev2.delist_type, '') <> 'mergerfrom'
                    THEN ev2.delist_type
                ELSE CAST(NULL AS VARCHAR)
            END AS fwd_delist_type
        FROM entries e
        INNER JOIN exc_path x ON x.ticker = e.ticker AND x.date = e.date
        INNER JOIN label_fwd lf ON lf.ticker = e.ticker AND lf.date = e.date
        LEFT JOIN terminal_exit t ON t.ticker = e.ticker AND t.date = e.date
        LEFT JOIN last_day_exit ld ON ld.ticker = e.ticker AND ld.date = e.date
        LEFT JOIN terminal_events_resolved ev ON ev.ticker = t.ticker AND ev.event_date = t.term_date
        LEFT JOIN terminal_events_resolved ev2 ON ev2.ticker = ld.ticker AND ev2.event_date = ld.date
        """
    )

    grid_screener_cols = ""
    if use_screener:
        grid_screener_cols = """
            g.sc_dollar_volume_1d,
            g.sc_adr_pct_trailing,
            g.sc_ret_pct_1m,
            g.sc_ret_pct_3m,
            g.sc_ret_pct_6m,
            g.sc_ret_rank_pct_1m,
            g.sc_ret_rank_pct_3m,
            g.sc_ret_rank_pct_6m,
            g.screen_1m,
            g.screen_3m,
            g.screen_6m,
            g.n_screen_horizons,
        """
    con.execute(
        """
        CREATE OR REPLACE VIEW qm_panel_pre AS
        SELECT
            g.ticker,
            g.date,
            g.sector,
            g.famaindustry,
            g.marketcap_daily,
        """
        + grid_screener_cols
        + """
            f.ret_5d, f.ret_10d, f.ret_20d, f.ret_21d, f.ret_63d, f.ret_126d,
            f.adr_5, f.adr_10, f.adr_20, f.atr_14_pct,
            f.sma_10, f.sma_20, f.sma_50,
            f.dist_sma_10, f.dist_sma_20, f.dist_sma_50,
            f.dist_52w_high, f.dist_52w_low,
            f.slope_logret_21, f.slope_logret_63, f.slope_logret_126,
            f.pullback_pct_5, f.pullback_pct_10, f.pullback_pct_20,
            f.dist_to_high_5, f.dist_to_high_10, f.dist_to_high_20,
            f.range_5, f.range_10, f.range_20, f.range_5_over_range_20,
            f.tight_close_frac_10, f.vol_5_over_vol_20, f.vol_log_slope_20,
            f.breakout_close_10d,
            f.breakout_close_20d,
            f.closeadj,
            l.entry_open,
            l.mfe_pct,
            l.mae_pct,
            l.exit_price,
            l.fwd_holding_days,
            l.fwd_delisted,
            l.fwd_delist_type,
            (l.exit_price / NULLIF(l.entry_open, 0) - 1.0) AS fwd_ret_entry_open_H,
            l.mfe_pct / NULLIF(f.atr_14_pct, 0) AS mfe_atr,
            l.mae_pct / NULLIF(f.atr_14_pct, 0) AS mae_atr,
            (l.mfe_pct - """
        + str(ALPHA_SCORE)
        + """ * l.mae_pct) AS score_realized,
            l.mfe_pct / NULLIF(l.mae_pct, 0) AS rr_realized
        FROM grid g
        INNER JOIN qm_features_full f ON f.ticker = g.ticker AND f.date = g.date
        INNER JOIN labels_core l ON l.ticker = g.ticker AND l.date = g.date
        WHERE l.exit_price IS NOT NULL AND l.exit_price > 0
        """
    )

    df = con.execute(
        """
        SELECT * FROM qm_panel_pre
        """
    ).df()
    tp_df = _tp_sl_returns_from_duckdb(con, tp_sl_k_stop, tp_sl_k_target)
    if not tp_df.empty:
        tp_df["date"] = pd.to_datetime(tp_df["date"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.merge(tp_df, on=["ticker", "date"], how="left")
    else:
        df[RET_COL_TP_SL_ATR] = np.nan
    df["date"] = pd.to_datetime(df["date"])
    df = add_days_since_high(df, (5, 10, 20))
    drop_cols = [c for c in ("closeadj",) if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    if write_parquet:
        df.to_parquet(PANEL_PATH, index=False)
    return PANEL_PATH


def load_fold_qm(
    con: duckdb.DuckDBPyConnection,
    is_start,
    is_end,
    oos_start,
    oos_end,
    panel_path: Path,
    rebal_table: str = "month_ends",
) -> pd.DataFrame:
    sql = f"""
        WITH rd AS (
            SELECT rebal_date FROM {rebal_table}
            WHERE (rebal_date >= ? AND rebal_date <= ?)
               OR (rebal_date >= ? AND rebal_date <= ?)
        )
        SELECT p.*,
               CASE WHEN p.date <= ? THEN 'is' ELSE 'oos' END AS fold
        FROM read_parquet(?) p
        INNER JOIN rd ON CAST(p.date AS DATE) = CAST(rd.rebal_date AS DATE)
        """
    return con.execute(
        sql,
        [is_start, is_end, oos_start, oos_end, is_end, str(panel_path.resolve())],
    ).df()


def regression_diagnostics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    m = np.isfinite(y_true) & np.isfinite(y_pred)
    if m.sum() < 3:
        return {"pearson_r": np.nan, "spearman_r": np.nan, "mse": np.nan}
    yt, yp = y_true[m], y_pred[m]
    pearson_r = float(np.corrcoef(yt, yp)[0, 1]) if len(yt) > 1 else np.nan
    spearman_r = float(stats.spearmanr(yt, yp).correlation)
    mse = float(np.mean((yt - yp) ** 2))
    return {"pearson_r": pearson_r, "spearman_r": spearman_r, "mse": mse}


def mean_cs_spearman(df: pd.DataFrame, pred_col: str, true_col: str) -> float:
    rs = []
    for _, g in df.groupby("date"):
        if len(g) < 5:
            continue
        a, b = g[true_col].values, g[pred_col].values
        m = np.isfinite(a) & np.isfinite(b)
        if m.sum() < 5:
            continue
        r = stats.spearmanr(a[m], b[m]).correlation
        if np.isfinite(r):
            rs.append(r)
    return float(np.mean(rs)) if rs else np.nan


def decile_lift_table(
    df: pd.DataFrame,
    score_col: str,
    mfe_col: str = "mfe_pct",
    mae_col: str = "mae_pct",
    ret_col: str = RET_COL,
) -> pd.DataFrame:
    """Cross-sectional deciles by score_col (qcut: low scores → decile 0, high → decile 9)."""
    rows = []
    for _, g in df.groupby("date"):
        g = g[np.isfinite(g[score_col])].copy()
        if len(g) < 10:
            continue
        try:
            g["decile"] = pd.qcut(g[score_col], q=10, labels=False, duplicates="drop")
        except ValueError:
            continue
        rows.append(g)
    if not rows:
        return pd.DataFrame()
    u = pd.concat(rows, ignore_index=True)
    return (
        u.groupby("decile")
        .agg(
            n=(score_col, "count"),
            avg_mfe=(mfe_col, "mean"),
            avg_mae=(mae_col, "mean"),
            avg_ret=(ret_col, "mean"),
        )
        .reset_index()
        .sort_values("decile")
    )


def _breakout_mask(df: pd.DataFrame, breakout_col: str | None) -> pd.Series:
    if not breakout_col:
        return pd.Series(True, index=df.index)
    if breakout_col not in df.columns:
        raise ValueError(f"breakout_col {breakout_col!r} not in dataframe (rebuild panel with --force-panel)")
    b = df[breakout_col]
    if b.dtype == bool or str(b.dtype) == "boolean":
        return b.fillna(False)
    return b.fillna(0).astype(float) > 0.5


def _apply_selection(
    work: pd.DataFrame,
    top_frac: float,
    *,
    selection_policy: str,
    breakout_col: str | None,
    eval_variant: str,
    score_threshold: float | None,
    top_k: int | None = None,
) -> pd.Series:
    """Boolean Series: selected rows for portfolio sim; ``work`` must include ``pred`` column."""
    if "pred" not in work.columns:
        raise ValueError("work must contain pred column")
    pred_col = "pred"
    n = len(work)
    if selection_policy == "baseline":
        if top_k is not None:
            sel = pd.Series(False, index=work.index)
            for _, g in work.groupby("date"):
                g2 = g[np.isfinite(g[pred_col])]
                if g2.empty:
                    continue
                kk = min(int(top_k), len(g2))
                win = g2.nlargest(kk, pred_col).index
                sel.loc[win] = True
            return sel
        nn = work.groupby("date")[pred_col].transform("count")
        top_n = np.maximum(1, np.ceil(nn * top_frac).astype(np.int64))
        rk = work.groupby("date")[pred_col].rank(ascending=False, method="first")
        return rk <= top_n

    bo = _breakout_mask(work, breakout_col)

    if selection_policy == "policy_a":
        if top_k is not None:
            raise ValueError("top_k is only supported with selection_policy=baseline")
        nn = work.groupby("date")[pred_col].transform("count")
        top_n = np.maximum(1, np.ceil(nn * top_frac).astype(np.int64))
        rk = work.groupby("date")[pred_col].rank(ascending=False, method="first")
        base = rk <= top_n
        if eval_variant == "top_frac":
            return base & bo
        if score_threshold is None:
            return pd.Series(False, index=work.index)
        return base & bo & (work[pred_col] >= score_threshold)

    if selection_policy == "policy_b":
        if top_k is not None:
            raise ValueError("top_k is only supported with selection_policy=baseline")
        sel = pd.Series(False, index=work.index)
        for _, g in work.groupby("date"):
            m = bo.loc[g.index]
            ge = g.loc[m]
            if ge.empty:
                continue
            if eval_variant == "top_frac":
                ne = len(ge)
                topn = max(1, int(np.ceil(ne * top_frac)))
                win = ge.nlargest(topn, pred_col).index
                sel.loc[win] = True
            else:
                if score_threshold is None:
                    continue
                win = ge.index[ge[pred_col] >= score_threshold]
                sel.loc[win] = True
        return sel

    raise ValueError(
        f"selection_policy must be baseline, policy_a, or policy_b, got {selection_policy!r}"
    )


def portfolio_period_returns_decile(
    df: pd.DataFrame,
    score_col: str,
    ret_col: str,
    top_frac: float = 0.1,
) -> pd.Series:
    out = {}
    for d, g in df.groupby("date"):
        if g.empty:
            continue
        n = max(1, int(np.ceil(len(g) * top_frac)))
        sub = g.nlargest(n, score_col)
        out[d] = sub[ret_col].mean()
    return pd.Series(out).sort_index()


def trade_level_stats(df: pd.DataFrame, ret_col: str) -> dict:
    r = df[ret_col].dropna()
    if r.empty:
        return {}
    wins = r[r > 0]
    losses = r[r <= 0]
    wr = (r > 0).mean()
    aw = wins.mean() if len(wins) else 0.0
    al = abs(losses.mean()) if len(losses) else 0.0
    el = 1 - wr
    exp_ = wr * aw - el * al
    return {
        "n_trades": int(len(r)),
        "win_rate": float(wr),
        "avg_trade_ret": float(r.mean()),
        "median_trade_ret": float(r.median()),
        "avg_win": float(aw),
        "avg_loss": float(al),
        "avg_win_over_avg_loss": float(aw / al) if al > 0 else np.nan,
        "expectancy": float(exp_),
    }


def _trades_top_frac(df: pd.DataFrame, score_col: str, top_frac: float) -> pd.DataFrame:
    parts = []
    for _, g in df.groupby("date"):
        if g.empty:
            continue
        n = max(1, int(np.ceil(len(g) * top_frac)))
        parts.append(g.nlargest(n, score_col))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def selection_quality_table(df: pd.DataFrame, score_col: str, ret_col: str) -> pd.DataFrame:
    rows = []
    for label, frac in [("all", 1.0), ("top_20pct", 0.2), ("top_10pct", 0.1)]:
        pr = portfolio_period_returns_decile(df, score_col, ret_col, top_frac=frac)
        tdf = df if frac >= 1.0 else _trades_top_frac(df, score_col, frac)
        tls = trade_level_stats(tdf, ret_col) if not tdf.empty else {}
        rows.append(
            {
                "bucket": label,
                "mean_period_ret": pr.mean(),
                "median_period_ret": pr.median(),
                "mean_trade_ret": tls.get("avg_trade_ret", np.nan),
                "median_trade_ret": tls.get("median_trade_ret", np.nan),
                "n_periods": len(pr),
            }
        )
    return pd.DataFrame(rows)


def turnover_proxy(selected_by_date: dict) -> float:
    dates = sorted(selected_by_date.keys())
    if len(dates) < 2:
        return np.nan
    churns = []
    for i in range(1, len(dates)):
        a, b = selected_by_date[dates[i - 1]], selected_by_date[dates[i]]
        u = len(a | b)
        if u == 0:
            continue
        churns.append(1 - len(a & b) / u)
    return float(np.mean(churns)) if churns else np.nan


def annualized_vol_period_returns(
    period_rets: pd.Series, periods_per_year: float = PERIODS_PER_YEAR
) -> float:
    if period_rets is None or len(period_rets) < 2 or period_rets.std() == 0:
        return np.nan
    return float(period_rets.std() * (periods_per_year**0.5))


def evaluate_portfolio_from_mask(
    df: pd.DataFrame,
    ret_col: str,
    selected: pd.Series,
    periods_per_year: float,
) -> dict:
    """Equal-weight mean ``ret_col`` per date over rows marked True in ``selected``."""
    selected = selected.reindex(df.index, fill_value=False).fillna(False).astype(bool)
    sel = df.loc[selected]
    if sel.empty:
        return {
            "sharpe": np.nan,
            "hit_rate": np.nan,
            "avg_period_ret": np.nan,
            "worst_period": np.nan,
            "n_periods": 0,
            "monthly_rets": pd.Series(dtype=float),
            "selected_by_date": {},
            "avg_universe_size": float(df.groupby("date").size().mean()) if len(df) else np.nan,
            "avg_selected_count": 0.0,
        }
    period_rets = sel.groupby("date")[ret_col].mean().rename("port_ret")
    selected_by_date = sel.groupby("date")["ticker"].apply(set).to_dict()
    if period_rets.empty or period_rets.std() == 0:
        sharpe = np.nan
    else:
        sharpe = float(period_rets.mean() / period_rets.std() * (periods_per_year**0.5))
    return {
        "sharpe": sharpe,
        "hit_rate": float((period_rets > 0).mean()) if len(period_rets) else np.nan,
        "avg_period_ret": float(period_rets.mean()) if len(period_rets) else np.nan,
        "worst_period": float(period_rets.min()) if len(period_rets) else np.nan,
        "n_periods": len(period_rets),
        "monthly_rets": period_rets,
        "selected_by_date": selected_by_date,
        "avg_universe_size": float(df.groupby("date").size().mean()) if len(df) else np.nan,
        "avg_selected_count": float(sel.groupby("date").size().mean()),
    }


def _select_random_k_per_date(
    df: pd.DataFrame,
    k: int,
    rng: np.random.Generator,
    ret_col: str,
) -> pd.Series:
    out = pd.Series(False, index=df.index)
    for _, g in df.groupby("date"):
        g2 = g[np.isfinite(g[ret_col])]
        if g2.empty:
            continue
        kk = min(int(k), len(g2))
        pick = rng.choice(g2.index.to_numpy(), size=kk, replace=False)
        out.loc[pick] = True
    return out


def _select_screener_top_k_per_date(df: pd.DataFrame, k: int, ret_col: str) -> pd.Series:
    rk_cols = ["sc_ret_rank_pct_1m", "sc_ret_rank_pct_3m", "sc_ret_rank_pct_6m"]
    if "n_screen_horizons" not in df.columns or any(c not in df.columns for c in rk_cols):
        raise ValueError(
            "Screener baseline needs n_screen_horizons and sc_ret_rank_pct_*; "
            "build panel with --use-screener."
        )
    out = pd.Series(False, index=df.index)
    for _, g in df.groupby("date"):
        g2 = g[np.isfinite(g[ret_col])].copy()
        if g2.empty:
            continue
        g2["_nh"] = g2["n_screen_horizons"].fillna(-1)
        g2["_rk"] = g2[rk_cols].max(axis=1).fillna(-1)
        g2 = g2.sort_values(["_nh", "_rk"], ascending=[False, False])
        kk = min(int(k), len(g2))
        out.loc[g2.index[:kk]] = True
    return out


def _select_oracle_top_k_per_date(df: pd.DataFrame, k: int, oracle_col: str) -> pd.Series:
    out = pd.Series(False, index=df.index)
    for _, g in df.groupby("date"):
        g2 = g[np.isfinite(g[oracle_col])]
        if g2.empty:
            continue
        kk = min(int(k), len(g2))
        out.loc[g2.nlargest(kk, oracle_col).index] = True
    return out


def rank_score_array(mfe_pred: np.ndarray, mae_pred: np.ndarray, mode: str) -> np.ndarray:
    """Re-rank OOS without retraining: variants of mfe − λ|mae| or mfe/max(|mae|,eps)."""
    mae_a = np.abs(np.asarray(mae_pred, dtype=float))
    mf = np.asarray(mfe_pred, dtype=float)
    if mode == "default":
        return mf - ALPHA_SCORE * mae_a
    if mode == "m05":
        return mf - 0.5 * mae_a
    if mode == "m10":
        return mf - 1.0 * mae_a
    if mode == "m15":
        return mf - 1.5 * mae_a
    if mode == "rr":
        return mf / np.maximum(mae_a, 1e-12)
    raise ValueError(f"unknown rank_score mode {mode!r}")


def tp_sl_outcome_decile_table(df: pd.DataFrame, score_col: str, ret_col: str) -> pd.DataFrame:
    """Cross-sectional deciles of ``score_col``; mean path tags and mean ``ret_col`` per decile."""
    miss = [c for c in TP_SL_OUTCOME_COLS if c not in df.columns]
    if miss:
        return pd.DataFrame()
    rows_cs = []
    for _, g in df.groupby("date"):
        g = g[np.isfinite(g[score_col])].copy()
        if len(g) < 10:
            continue
        try:
            g["decile"] = pd.qcut(g[score_col], q=10, labels=False, duplicates="drop")
        except ValueError:
            continue
        rows_cs.append(g)
    if not rows_cs:
        return pd.DataFrame()
    u = pd.concat(rows_cs, ignore_index=True)
    return (
        u.groupby("decile")
        .agg(
            n=(score_col, "count"),
            mean_tp_sl_ret=(ret_col, "mean"),
            tp_first_rate=(TP_SL_TP_HIT_FIRST, "mean"),
            sl_first_rate=(TP_SL_SL_HIT_FIRST, "mean"),
            neither_rate=(TP_SL_NEITHER_HIT, "mean"),
            mean_bars_to_tp=(TP_SL_BARS_TO_TP, "mean"),
            mean_bars_to_sl=(TP_SL_BARS_TO_SL, "mean"),
        )
        .reset_index()
        .sort_values("decile")
    )


def evaluate_fold_top_frac(
    df: pd.DataFrame,
    preds,
    ret_col: str,
    top_frac: float = 0.1,
    periods_per_year: float = PERIODS_PER_YEAR,
    *,
    selection_policy: str = "baseline",
    breakout_col: str | None = None,
    eval_variant: str = "top_frac",
    score_threshold: float | None = None,
    top_k: int | None = None,
) -> dict:
    """Top-fraction portfolio sim; optional breakout Policy A/B (see docs/qullamaggie_flow.md)."""
    d = df.copy()
    preds = np.asarray(preds).ravel()
    assert len(preds) == len(d), f"preds length {len(preds)} != df length {len(d)}"
    d["pred"] = preds
    if selection_policy in ("policy_a", "policy_b") and not breakout_col:
        raise ValueError("breakout_col is required for policy_a and policy_b")
    d["selected"] = _apply_selection(
        d,
        top_frac,
        selection_policy=selection_policy,
        breakout_col=breakout_col,
        eval_variant=eval_variant,
        score_threshold=score_threshold,
        top_k=top_k,
    )
    return evaluate_portfolio_from_mask(d, ret_col, d["selected"], periods_per_year)



def _print_tp_sl_baseline_matrix(
    stitched: pd.DataFrame,
    *,
    ret_col: str,
    top_k: int,
    periods_per_year: float,
    model_preds: np.ndarray,
    random_seed: int,
    rank_score_label: str,
) -> None:
    """A random / B screener / C model / D oracle; same K names per date, ``ret_col`` PnL."""
    df = stitched
    rng = np.random.default_rng(random_seed)
    print(
        f"\n=== TP/SL baseline matrix (stitched OOS, top_{top_k}/date, ret={ret_col}, "
        f"rank_score={rank_score_label!r}) ===\n"
        "Same-bar rule: stop before target (see docs). Oracle = max realized tp_sl ret per date."
    )
    sel_a = _select_random_k_per_date(df, top_k, rng, ret_col)
    ev_a = evaluate_portfolio_from_mask(df, ret_col, sel_a, periods_per_year)
    dd_a, cagr_a = oos_drawdown_and_cagr(ev_a["monthly_rets"], periods_per_year=periods_per_year)

    ev_b = None
    cagr_b = dd_b = np.nan
    try:
        sel_b = _select_screener_top_k_per_date(df, top_k, ret_col)
        ev_b = evaluate_portfolio_from_mask(df, ret_col, sel_b, periods_per_year)
        dd_b, cagr_b = oos_drawdown_and_cagr(ev_b["monthly_rets"], periods_per_year=periods_per_year)
    except ValueError as e:
        print(f"B_screener: skip ({e})")

    w = df.copy()
    w["_rank_m"] = np.asarray(model_preds, dtype=float).ravel()
    sel_c = pd.Series(False, index=df.index)
    for _, g in w.groupby("date"):
        g2 = g[np.isfinite(g["_rank_m"])]
        if g2.empty:
            continue
        kk = min(int(top_k), len(g2))
        sel_c.loc[g2.nlargest(kk, "_rank_m").index] = True
    ev_c = evaluate_portfolio_from_mask(df, ret_col, sel_c, periods_per_year)
    dd_c, cagr_c = oos_drawdown_and_cagr(ev_c["monthly_rets"], periods_per_year=periods_per_year)

    sel_d = _select_oracle_top_k_per_date(df, top_k, ret_col)
    ev_d = evaluate_portfolio_from_mask(df, ret_col, sel_d, periods_per_year)
    dd_d, cagr_d = oos_drawdown_and_cagr(ev_d["monthly_rets"], periods_per_year=periods_per_year)

    row_list = [
        ("A_random", ev_a, cagr_a, dd_a),
        ("C_model_rank", ev_c, cagr_c, dd_c),
        ("D_oracle_tp_sl", ev_d, cagr_d, dd_d),
    ]
    if ev_b is not None:
        row_list.insert(1, ("B_screener", ev_b, cagr_b, dd_b))
    hdr = f"{'baseline':<16} {'Sharpe':>8} {'CAGR':>10} {'maxDD':>10} {'avg_mu_ret':>12} {'n_p':>6} {'avg_K':>8}"
    print(hdr)
    print("-" * len(hdr))
    for name, ev, cg, dd in row_list:
        sh = ev["sharpe"]
        shs = f"{sh:8.3f}" if isinstance(sh, (int, float)) and np.isfinite(sh) else "     nan"
        cgs = f"{cg:10.2%}" if isinstance(cg, (int, float)) and np.isfinite(cg) else "       nan"
        dds = f"{dd:10.2%}" if isinstance(dd, (int, float)) and np.isfinite(dd) else "       nan"
        ar = ev["avg_period_ret"]
        ars = f"{ar:12.6f}" if isinstance(ar, (int, float)) and np.isfinite(ar) else "         nan"
        ak = ev["avg_selected_count"]
        aks = f"{ak:8.2f}" if isinstance(ak, (int, float)) and np.isfinite(ak) else "     nan"
        print(f"{name:<16} {shs:>8} {cgs:>10} {dds:>10} {ars:>12} {int(ev['n_periods']):6d} {aks:>8}")


def _print_rank_score_sweep_stitched(
    stitched: pd.DataFrame,
    *,
    ret_col: str,
    top_frac: float,
    top_k: int | None,
    periods_per_year: float,
    selection_policy: str,
    breakout_col: str | None,
    eval_variant: str,
    score_threshold: float | None,
) -> None:
    """Stitched OOS: Sharpe vs rank formula (no retrain)."""
    modes = ("default", "m05", "m10", "m15", "rr")
    print("\n=== Rank-score sweep (stitched OOS; selection unchanged) ===")
    hdr = f"{'mode':<10} {'Sharpe':>8} {'avg_mu':>12} {'n_p':>6}"
    print(hdr)
    print("-" * len(hdr))
    for mode in modes:
        pr = rank_score_array(
            stitched["mfe_pred"].values,
            stitched["mae_pred"].values,
            mode,
        )
        ev = evaluate_fold_top_frac(
            stitched,
            pr,
            ret_col=ret_col,
            top_frac=top_frac,
            periods_per_year=periods_per_year,
            selection_policy=selection_policy,
            breakout_col=breakout_col,
            eval_variant=eval_variant,
            score_threshold=score_threshold,
            top_k=top_k,
        )
        sh = ev["sharpe"]
        shs = f"{sh:8.3f}" if np.isfinite(sh) else "     nan"
        am = ev["avg_period_ret"]
        ams = f"{am:12.6f}" if np.isfinite(am) else "         nan"
        print(f"{mode:<10} {shs:>8} {ams:>12} {int(ev['n_periods']):6d}")


def capacity_stats(
    selected_by_date: dict,
    periods_per_year: float = PERIODS_PER_YEAR,
) -> dict:
    """Trades/year, avg names per rebalance, turnover from selected ticker sets per date."""
    if not selected_by_date:
        return {}
    dates = sorted(selected_by_date.keys())
    n_periods = len(dates)
    counts = [len(selected_by_date[d]) for d in dates]
    total_trades = int(sum(counts))
    years = n_periods / periods_per_year if periods_per_year else np.nan
    return {
        "n_rebalance_periods": n_periods,
        "total_trades": total_trades,
        "trades_per_year": float(total_trades / years) if years and years > 0 else np.nan,
        "avg_names_selected": float(np.mean(counts)) if counts else np.nan,
        "turnover_mean_churn": turnover_proxy(selected_by_date),
    }


def dedupe_oos_panel(df: pd.DataFrame) -> pd.DataFrame:
    """If the same (date, ticker) appears in multiple walk-forward OOS sets, keep latest fold."""
    if df.empty or "fold_idx" not in df.columns:
        return df
    return (
        df.sort_values(["date", "ticker", "fold_idx"])
        .drop_duplicates(subset=["date", "ticker"], keep="last")
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )


def rr_diagnostics(df: pd.DataFrame, mfe_p: str, mae_p: str) -> dict:
    ra = df["mfe_pct"] / df["mae_pct"].replace(0, np.nan)
    rp = df[mfe_p] / df[mae_p].replace(0, np.nan).abs()
    m = np.isfinite(ra) & np.isfinite(rp)
    if m.sum() < 3:
        return {}
    return {
        "rr_pearson": float(np.corrcoef(ra[m], rp[m])[0, 1]),
        "rr_spearman": float(stats.spearmanr(ra[m], rp[m]).correlation),
    }


def pain_filter_stats(df: pd.DataFrame, score_col: str, mae_col: str = "mae_pct") -> dict:
    rows = []
    for _, g in df.groupby("date"):
        g = g[np.isfinite(g[score_col])].copy()
        if len(g) < 10:
            continue
        try:
            g["dec"] = pd.qcut(g[score_col], q=10, labels=False, duplicates="drop")
        except ValueError:
            continue
        top = g[g["dec"] == g["dec"].max()][mae_col].mean()
        bot = g[g["dec"] == g["dec"].min()][mae_col].mean()
        rows.append((top, bot))
    if not rows:
        return {}
    tops, bots = zip(*rows)
    return {"mean_mae_top_decile": float(np.nanmean(tops)), "mean_mae_bottom_decile": float(np.nanmean(bots))}


def fit_predict_fold(
    is_df: pd.DataFrame,
    oos_df: pd.DataFrame,
    model_kind: str,
) -> tuple[pd.DataFrame, dict, dict]:
    """Adds mfe_pred, mae_pred, score_pred on oos_df copy.

    Third return value may include ``is_train_score_pred`` (scores on IS train rows) for thresholds.
    """
    feats = [c for c in FEATURE_COLS if c in is_df.columns]
    feats.extend(c for c in SCREENER_FEATURE_COLS if c in is_df.columns)
    for c in ("sector_enc", "famaindustry_enc"):
        if c in is_df.columns:
            feats.append(c)
    X_is = is_df[feats].replace([np.inf, -np.inf], np.nan)
    X_oos = oos_df[feats].replace([np.inf, -np.inf], np.nan)
    y_mfe = is_df[TARGET_MFE].values
    y_mae = is_df[TARGET_MAE].values
    train_m = np.isfinite(y_mfe) & np.isfinite(y_mae) & X_is.notna().all(axis=1)
    train_n = int(train_m.sum())
    if train_n < MIN_IS_TRAIN_ROWS:
        o = oos_df.copy()
        o["mfe_pred"] = np.nan
        o["mae_pred"] = np.nan
        o["score_pred"] = np.nan
        return (
            o,
            {"error": "insufficient_train", "n_is_train": train_n, "min_required": MIN_IS_TRAIN_ROWS},
            {},
        )

    scaler = StandardScaler()
    X_tr = scaler.fit_transform(X_is.loc[train_m])
    X_oo = scaler.transform(X_oos)

    if model_kind == "ridge":
        m_mfe = Ridge(alpha=RIDGE_ALPHA)
        m_mae = Ridge(alpha=RIDGE_ALPHA)
    else:
        m_mfe = xgb.XGBRegressor(**XGB_PARAMS)
        m_mae = xgb.XGBRegressor(**XGB_PARAMS)

    m_mfe.fit(X_tr, y_mfe[train_m])
    m_mae.fit(X_tr, y_mae[train_m])
    pred_mfe_tr = m_mfe.predict(X_tr)
    pred_mae_tr = m_mae.predict(X_tr)
    is_train_score_pred = pred_mfe_tr - ALPHA_SCORE * np.abs(pred_mae_tr)

    pred_mfe = m_mfe.predict(X_oo)
    pred_mae = m_mae.predict(X_oo)

    o = oos_df.copy()
    o["mfe_pred"] = pred_mfe
    o["mae_pred"] = pred_mae
    o["score_pred"] = pred_mfe - ALPHA_SCORE * np.abs(pred_mae)
    o["rr_pred"] = pred_mfe / np.maximum(np.abs(pred_mae), 1e-12)

    diag = {
        "mfe": regression_diagnostics(oos_df[TARGET_MFE].values, pred_mfe),
        "mae": regression_diagnostics(oos_df[TARGET_MAE].values, pred_mae),
        "mfe_cs_spear": mean_cs_spearman(o, "mfe_pred", TARGET_MFE),
        "mae_cs_spear": mean_cs_spearman(o, "mae_pred", TARGET_MAE),
    }
    if train_n < MIN_IS_TRAIN_ROWS_IDEAL:
        diag["sparse_is_train"] = train_n
    return o, diag, {"is_train_score_pred": is_train_score_pred}


def _resolve_eval_score_threshold(
    eval_variant: str,
    eval_score_threshold: float | None,
    eval_score_quantile: float | None,
    fit_extra: dict,
) -> float | None:
    if eval_variant != "score_threshold":
        return None
    if eval_score_threshold is not None:
        return float(eval_score_threshold)
    if eval_score_quantile is not None:
        iss = fit_extra.get("is_train_score_pred")
        if iss is None or len(iss) == 0:
            return None
        return float(np.quantile(iss, eval_score_quantile))
    return None


def _parquet_column_names(path: Path) -> set[str]:
    """Column names without reading row groups (pyarrow schema)."""
    try:
        import pyarrow.parquet as pq

        return set(pq.read_schema(path).names)
    except Exception as e:
        raise RuntimeError(
            f"Could not read parquet schema for {path} (need pyarrow). {e}"
        ) from e


def _walkforward_eval_ret_col(eval_ret_mode: str) -> str:
    if eval_ret_mode == "horizon":
        return RET_COL
    if eval_ret_mode == "tp_sl":
        return RET_COL_TP_SL_ATR
    if eval_ret_mode == "pred_proxy":
        return RET_COL_PRED_PROXY
    raise ValueError(
        f"eval_ret_mode must be horizon, tp_sl, or pred_proxy, got {eval_ret_mode!r}"
    )


def run_walk_forward(
    panel_path: Path | None = None,
    *,
    wf_freq: str = "month",
    wf_step: int = 1,
    selection_policy: str = "baseline",
    breakout_col: str | None = DEFAULT_BREAKOUT_COL,
    eval_variant: str = "top_frac",
    eval_score_threshold: float | None = None,
    eval_score_quantile: float | None = None,
    top_frac: float = 0.1,
    eval_ret_mode: str = "horizon",
    top_k: int | None = None,
    rank_score_mode: str = "default",
    experiment_matrix: bool = False,
    experiment_top_k: int = 1,
    random_seed: int = 42,
    rank_score_sweep: bool = False,
    wf_compact: bool = False,
) -> None:
    panel_path = panel_path or PANEL_PATH
    if not panel_path.exists():
        raise FileNotFoundError(f"Panel missing: {panel_path}; run build first.")

    ppy = periods_per_year_for_freq(wf_freq)
    bc = None if selection_policy == "baseline" else breakout_col

    panel_cols = _parquet_column_names(panel_path)
    if bc:
        if bc not in panel_cols:
            raise SystemExit(
                f"Panel {panel_path} has no column {bc!r} (likely built before breakout flags were added).\n"
                f"Rebuild: python experiments/qullamaggie.py panel --force-panel"
            )
    if eval_ret_mode == "tp_sl" and RET_COL_TP_SL_ATR not in panel_cols:
        raise SystemExit(
            f"Panel {panel_path} has no column {RET_COL_TP_SL_ATR!r} (stop/target sim column).\n"
            f"Rebuild: python experiments/qullamaggie.py panel --force-panel"
        )
    tp_sl_diag_ok = all(c in panel_cols for c in TP_SL_OUTCOME_COLS)
    if eval_ret_mode == "tp_sl" and not tp_sl_diag_ok:
        miss = [c for c in TP_SL_OUTCOME_COLS if c not in panel_cols]
        print(
            f"Note: panel missing TP/SL path columns {miss}; "
            "`python experiments/qullamaggie.py panel --force-panel` for decile path diagnostics."
        )

    ret_col_eval = _walkforward_eval_ret_col(eval_ret_mode)

    con = duckdb.connect()
    rebal_dates = get_rebal_dates(
        con,
        str(panel_path.resolve()),
        DATA_START_QM,
        freq=wf_freq,
        step=max(1, int(wf_step)),
    )
    con.register("rebal_dates", pd.DataFrame({"rebal_date": rebal_dates}))

    compact_effective = bool(wf_compact) if wf_freq == "day" else False
    is_p, oos_p, emb_p, min_oos = _wf_fold_periods(wf_freq, compact=compact_effective)
    folds = generate_folds(rebal_dates, is_p, oos_p, emb_p, min_oos)
    if not folds and wf_freq == "day" and not compact_effective:
        need = is_p + emb_p + min_oos
        if len(rebal_dates) < need + 1:
            print(
                f"[qullamaggie] 0 folds: default daily WF needs roughly {need}+ distinct panel dates "
                f"(IS={is_p}, embargo={emb_p}, min_oos={min_oos}), have {len(rebal_dates)}. "
                "Using compact daily windows (see --wf-compact)."
            )
            is_p, oos_p, emb_p, min_oos = _wf_fold_periods("day", compact=True)
            compact_effective = True
            folds = generate_folds(rebal_dates, is_p, oos_p, emb_p, min_oos)
    if not folds:
        ip, op, ep, mo = _wf_fold_periods(wf_freq, compact=False)
        nd = ip + ep + mo
        ipc, opc, epc, moc = (
            _wf_fold_periods("day", compact=True) if wf_freq == "day" else (ip, op, ep, mo)
        )
        nc = ipc + epc + moc
        raise SystemExit(
            f"No walk-forward folds: only {len(rebal_dates)} rebal date(s) in panel (>= {DATA_START_QM}).\n"
            f"For wf_freq={wf_freq!r}, default needs roughly > {nd} steps; "
            f"{'compact day needs > ' + str(nc) + '. ' if wf_freq == 'day' else ''}"
            f"Use --wf-compact (day), --wf-freq month/week, or rebuild a panel with more date coverage."
        )

    print(
        f"Folds: {len(folds)} (wf_freq={wf_freq!r}, rebal_dates={len(rebal_dates)}, "
        f"wf_compact={compact_effective}, IS/oos/emb/min_oos={is_p}/{oos_p}/{emb_p}/{min_oos}, "
        f"periods_per_year={ppy}, selection_policy={selection_policy!r}, eval_variant={eval_variant!r}, "
        f"eval_ret_mode={eval_ret_mode!r}, rank_score_mode={rank_score_mode!r}, "
        f"top_k={top_k}, top_frac={top_frac})"
    )
    if bc:
        print(f"  breakout_col={bc!r} (rebuild panel if column missing)")

    all_oos_ridge: list[pd.DataFrame] = []
    all_oos_xgb: list[pd.DataFrame] = []
    fold_summaries = []

    for fi, (is_s, is_e, oos_s, oos_e) in enumerate(folds):
        df = load_fold_qm(con, is_s, is_e, oos_s, oos_e, panel_path, rebal_table="rebal_dates")
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        is_df = df[df["fold"] == "is"].copy()
        oos_df = df[df["fold"] == "oos"].copy()
        need = FEATURE_COLS + [TARGET_MFE, TARGET_MAE, RET_COL, "sector", "famaindustry"]
        need.extend(c for c in SCREENER_FEATURE_COLS if c in is_df.columns)
        is_df = is_df.dropna(subset=[TARGET_MFE, TARGET_MAE, RET_COL])
        oos_df = oos_df.dropna(subset=[TARGET_MFE, TARGET_MAE, RET_COL])
        encode_fold(is_df, oos_df, cat_cols=("sector", "famaindustry"))
        need_fit = [c for c in need if c in is_df.columns]

        for kind in ("ridge", "xgb"):
            oos_pred, diag, fit_extra = fit_predict_fold(
                is_df.dropna(subset=need_fit, how="any"),
                oos_df.dropna(subset=need_fit, how="any"),
                "ridge" if kind == "ridge" else "xgb",
            )
            if diag.get("error"):
                msg = diag["error"]
                if msg == "insufficient_train":
                    msg = (
                        f"{msg} (IS rows with valid targets+features="
                        f"{diag.get('n_is_train')}, need >={diag.get('min_required', MIN_IS_TRAIN_ROWS)})"
                    )
                print(f"\n--- Fold {fi+1} {kind.upper()} OOS --- skip ({msg})")
                continue

            oos_pred["fold_idx"] = fi
            if kind == "ridge":
                all_oos_ridge.append(oos_pred)
            else:
                all_oos_xgb.append(oos_pred)

            oos_eval = oos_pred.copy()
            rank_preds = rank_score_array(
                oos_pred["mfe_pred"].values,
                oos_pred["mae_pred"].values,
                rank_score_mode,
            )
            oos_eval[RANK_SCORE_COL] = rank_preds
            if eval_ret_mode == "pred_proxy":
                oos_eval[RET_COL_PRED_PROXY] = oos_eval["score_pred"]

            eff_thresh = _resolve_eval_score_threshold(
                eval_variant, eval_score_threshold, eval_score_quantile, fit_extra
            )
            if eval_variant == "score_threshold" and eff_thresh is None:
                print(
                    f"\n--- Fold {fi+1} {kind.upper()} OOS --- skip (score_threshold: set "
                    "--eval-score-threshold or --eval-score-quantile)"
                )
                continue

            dec_tab = decile_lift_table(oos_eval, RANK_SCORE_COL, ret_col=ret_col_eval)
            ev = evaluate_fold_top_frac(
                oos_eval,
                rank_preds,
                ret_col=ret_col_eval,
                top_frac=top_frac,
                periods_per_year=ppy,
                selection_policy=selection_policy,
                breakout_col=bc,
                eval_variant=eval_variant,
                score_threshold=eff_thresh,
                top_k=top_k,
            )
            pr = ev["monthly_rets"]
            max_dd, cagr = oos_drawdown_and_cagr(pr, periods_per_year=ppy)
            oos_vol = annualized_vol_period_returns(pr, periods_per_year=ppy)
            cap = capacity_stats(ev["selected_by_date"], ppy)
            oos_sel = oos_eval.copy()
            oos_sel["pred"] = rank_preds
            sel_mask = _apply_selection(
                oos_sel,
                top_frac,
                selection_policy=selection_policy,
                breakout_col=bc,
                eval_variant=eval_variant,
                score_threshold=eff_thresh,
                top_k=top_k,
            )
            trades_df = oos_eval.loc[sel_mask]
            tls = trade_level_stats(trades_df, ret_col_eval) if not trades_df.empty else {}
            sel_tbl = selection_quality_table(oos_eval, RANK_SCORE_COL, ret_col_eval)
            rr_d = rr_diagnostics(oos_pred, "mfe_pred", "mae_pred")
            pain = pain_filter_stats(oos_pred, "score_pred", "mae_pct")

            fold_summaries.append(
                {
                    "fold": fi,
                    "model": kind,
                    "oos_sharpe": ev["sharpe"],
                    "oos_cagr": cagr,
                    "oos_max_dd": max_dd,
                    "oos_vol": oos_vol,
                    "mfe_pearson": diag.get("mfe", {}).get("pearson_r"),
                    "mfe_rank_ic_mean_cs": diag.get("mfe_cs_spear"),
                    "mae_pearson": diag.get("mae", {}).get("pearson_r"),
                    "mae_rank_ic_mean_cs": diag.get("mae_cs_spear"),
                    "trades_per_year": cap.get("trades_per_year", np.nan),
                    "avg_names_selected": cap.get("avg_names_selected", np.nan),
                    "turnover_churn": cap.get("turnover_mean_churn", np.nan),
                    **{f"trade_{k}": v for k, v in tls.items()},
                }
            )
            print(f"\n--- Fold {fi+1} {kind.upper()} OOS ---")
            if diag.get("sparse_is_train") is not None:
                print(
                    f"Warning: sparse IS train (n={diag['sparse_is_train']}, "
                    f"ideal >={MIN_IS_TRAIN_ROWS_IDEAL}); metrics are noisy — widen panel history if possible."
                )
            print(
                f"Sharpe: {ev['sharpe']:.3f}  ann.vol: {oos_vol:.2%}  "
                f"CAGR: {cagr:.2%}  maxDD: {max_dd:.2%}"
            )
            print(f"MFE Pearson: {diag['mfe']['pearson_r']:.4f}  MAE Pearson: {diag['mae']['pearson_r']:.4f}")
            print(f"Mean CS Spearman MFE: {diag['mfe_cs_spear']:.4f}  MAE: {diag['mae_cs_spear']:.4f}")
            print(f"MSE (sanity): MFE {diag['mfe']['mse']:.6f}  MAE {diag['mae']['mse']:.6f}")
            if cap:
                print(
                    f"Capacity: trades/yr {cap.get('trades_per_year', float('nan')):.1f}  "
                    f"avg names {cap.get('avg_names_selected', float('nan')):.1f}  "
                    f"turnover(churn) {cap.get('turnover_mean_churn', float('nan')):.3f}"
                )
            if tls:
                print(
                    f"Trades: n={tls.get('n_trades', 0)}  median ret {tls.get('median_trade_ret', float('nan')):.4f}  "
                    f"avg ret {tls.get('avg_trade_ret', float('nan')):.4f}"
                )
            if not dec_tab.empty:
                print(f"Decile lift ({rank_score_mode} rank score):")
                print(dec_tab.to_string(index=False))
            print("Selection quality:")
            print(sel_tbl.to_string(index=False))
            if pain:
                print(f"Pain filter MAE top/bottom decile means: {pain}")
            if rr_d:
                print(f"RR diag: {rr_d}")

    if fold_summaries:
        fs = pd.DataFrame(fold_summaries)
        print("\n=== Fold summary ===")
        cols = [
            c
            for c in [
                "oos_sharpe",
                "oos_cagr",
                "oos_vol",
                "mfe_rank_ic_mean_cs",
                "trades_per_year",
                "turnover_churn",
            ]
            if c in fs.columns
        ]
        if cols:
            print(fs.groupby("model")[cols].mean())

    for name, parts in [("Ridge", all_oos_ridge), ("XGB", all_oos_xgb)]:
        if not parts:
            continue
        raw = pd.concat(parts, ignore_index=True)
        stitched = dedupe_oos_panel(raw)
        n_dup = len(raw) - len(stitched)
        stitched = stitched.sort_values("date")
        stitched_eval = stitched.copy()
        rank_st = rank_score_array(
            stitched["mfe_pred"].values,
            stitched["mae_pred"].values,
            rank_score_mode,
        )
        stitched_eval[RANK_SCORE_COL] = rank_st
        if eval_ret_mode == "pred_proxy":
            stitched_eval[RET_COL_PRED_PROXY] = stitched_eval["score_pred"]
        thresh_st = eval_score_threshold
        if (
            eval_variant == "score_threshold"
            and thresh_st is None
            and eval_score_quantile is not None
        ):
            sp = stitched_eval["score_pred"].dropna()
            thresh_st = float(np.quantile(sp, eval_score_quantile)) if len(sp) else None
        ev_st = evaluate_fold_top_frac(
            stitched_eval,
            rank_st,
            ret_col=ret_col_eval,
            top_frac=top_frac,
            periods_per_year=ppy,
            selection_policy=selection_policy,
            breakout_col=bc,
            eval_variant=eval_variant,
            score_threshold=thresh_st,
            top_k=top_k,
        )
        pr = ev_st["monthly_rets"]
        max_dd, cagr = oos_drawdown_and_cagr(pr, periods_per_year=ppy)
        vol = annualized_vol_period_returns(pr, periods_per_year=ppy)
        cap_st = capacity_stats(ev_st["selected_by_date"], ppy)
        print(f"\n=== Stitched OOS {name} (deduped (date,ticker) by last fold; dropped {n_dup} rows) ===")
        print(
            f"CAGR: {cagr:.2%}  ann.vol: {vol:.2%}  maxDD: {max_dd:.2%}  "
            f"Sharpe: {ev_st['sharpe']:.3f}  n_periods: {len(pr)}"
        )
        if cap_st:
            print(
                f"Capacity: trades/yr {cap_st.get('trades_per_year', float('nan')):.1f}  "
                f"avg names {cap_st.get('avg_names_selected', float('nan')):.1f}  "
                f"turnover {cap_st.get('turnover_mean_churn', float('nan')):.3f}"
            )
        if eval_ret_mode == "tp_sl" and tp_sl_diag_ok:
            dt = tp_sl_outcome_decile_table(stitched_eval, RANK_SCORE_COL, ret_col_eval)
            if not dt.empty:
                print(f"\nTP/SL path by {rank_score_mode} rank-score decile (stitched {name}):")
                print(dt.to_string(index=False))
        if rank_score_sweep:
            _print_rank_score_sweep_stitched(
                stitched_eval,
                ret_col=ret_col_eval,
                top_frac=top_frac,
                top_k=top_k,
                periods_per_year=ppy,
                selection_policy=selection_policy,
                breakout_col=bc,
                eval_variant=eval_variant,
                score_threshold=thresh_st,
            )
        if experiment_matrix:
            if eval_ret_mode != "tp_sl":
                print("--experiment-matrix requires --eval-ret tp_sl (skipped)")
            elif selection_policy != "baseline":
                print("--experiment-matrix skipped (use --selection-policy baseline)")
            else:
                _print_tp_sl_baseline_matrix(
                    stitched_eval,
                    ret_col=ret_col_eval,
                    top_k=int(experiment_top_k),
                    periods_per_year=ppy,
                    model_preds=rank_st,
                    random_seed=int(random_seed),
                    rank_score_label=rank_score_mode,
                )

    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Qullamaggie experiment: optional historical screener parquet, feature panel "
        f"(cached under {MOMENTUM_OUTPUT_DIR}/), walk-forward backtest."
    )
    parser.add_argument(
        "command",
        choices=["screen", "panel", "build", "train", "both"],
        nargs="?",
        default="both",
        help="screen=screener only; panel|build=panel only; train=WF only; both=panel+train "
        "(runs screen first when --use-screener).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate screener and panel when those steps run (same as both force flags).",
    )
    parser.add_argument(
        "--force-screen",
        action="store_true",
        dest="force_screen",
        help=f"Regenerate {SCREEN_PATH.name} even if present.",
    )
    parser.add_argument(
        "--force-panel",
        action="store_true",
        dest="force_panel",
        help=f"Regenerate {PANEL_PATH.name} even if present.",
    )
    parser.add_argument(
        "--use-screener",
        action="store_true",
        help="Panel grid = screener candidates INNER JOIN daily_universe; adds screener feature columns.",
    )
    parser.add_argument(
        "--screener-path",
        type=Path,
        default=None,
        help=f"Override screener parquet path (default: {SCREEN_PATH}).",
    )
    parser.add_argument(
        "--wf-freq",
        choices=["month", "week", "day"],
        default="day",
        help="Walk-forward rebal calendar: last date per month, per week, or every panel trading day.",
    )
    parser.add_argument(
        "--wf-step",
        type=int,
        default=1,
        help="Keep every N-th rebal date after sorting (reduces cost for --wf-freq day).",
    )
    parser.add_argument(
        "--wf-compact",
        action="store_true",
        help="For --wf-freq day: use shorter IS/OOS windows (see DAILY_WF_COMPACT) when the panel has "
        "few distinct dates; also auto-applied when default daily windows yield 0 folds.",
    )
    parser.add_argument(
        "--selection-policy",
        choices=["baseline", "policy_a", "policy_b"],
        default="baseline",
        help="baseline=rank by score only; policy_a=top frac then require breakout; "
        "policy_b=breakout first then top frac among breakouts (see docs/qullamaggie_flow.md).",
    )
    parser.add_argument(
        "--breakout-col",
        type=str,
        default=DEFAULT_BREAKOUT_COL,
        help="Panel column for close > prior N-day high (default breakout_close_20d).",
    )
    parser.add_argument(
        "--eval-variant",
        choices=["top_frac", "score_threshold"],
        default="top_frac",
        help="With trigger policies: rank by top_frac among eligibles, or filter by score threshold.",
    )
    parser.add_argument(
        "--eval-score-threshold",
        type=float,
        default=None,
        help="With --eval-variant score_threshold: minimum score_pred (fixed).",
    )
    parser.add_argument(
        "--eval-score-quantile",
        type=float,
        default=None,
        help="With --eval-variant score_threshold: threshold = this quantile of IS train score_pred per fold.",
    )
    parser.add_argument(
        "--top-frac",
        type=float,
        default=0.1,
        help="Top fraction for ranking (policy-dependent; see docs/qullamaggie_flow.md).",
    )
    parser.add_argument(
        "--eval-ret",
        choices=["horizon", "tp_sl", "pred_proxy"],
        default="horizon",
        help="Portfolio PnL column: horizon=H-day/terminal open-to-exit; tp_sl=first hit stop/target in path "
        f"(needs {RET_COL_TP_SL_ATR} in panel); pred_proxy=use score_pred as fake return (diagnostic only).",
    )
    parser.add_argument(
        "--tp-sl-k-stop",
        type=float,
        default=TP_SL_K_STOP,
        help="ATR multiples: stop = entry_open - k * ATR$ (signal bar). Panel build only.",
    )
    parser.add_argument(
        "--tp-sl-k-target",
        type=float,
        default=TP_SL_K_TARGET,
        help="ATR multiples: target = entry_open + k * ATR$ (signal bar). Panel build only.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="If set, select this many names per date by rank score (baseline policy only); "
        "overrides top-frac sizing for portfolio sim.",
    )
    parser.add_argument(
        "--rank-score",
        choices=["default", "m05", "m10", "m15", "rr"],
        default="default",
        help="Re-rank OOS from mfe_pred/mae_pred without retraining: default=mfe−ALPHA·|mae| (matches score_pred); "
        "m05/m10/m15=mfe−λ|mae|; rr=mfe/max(|mae|,eps).",
    )
    parser.add_argument(
        "--rank-score-sweep",
        action="store_true",
        help="After each stitched model block, print Sharpe for all rank-score modes (same selection rules).",
    )
    parser.add_argument(
        "--experiment-matrix",
        action="store_true",
        help="With --eval-ret tp_sl and baseline policy: print A random / B screener / C model / D oracle "
        "(top K/day, see --experiment-top-k).",
    )
    parser.add_argument(
        "--experiment-top-k",
        type=int,
        default=1,
        help="Names per date for --experiment-matrix baselines.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="RNG seed for random baseline in --experiment-matrix.",
    )
    args = parser.parse_args()

    sc_path = (args.screener_path or SCREEN_PATH).resolve()
    force_screen = args.force or args.force_screen
    force_panel = args.force or args.force_panel

    run_screen = args.command == "screen" or (args.command == "both" and args.use_screener)
    run_panel = args.command in ("panel", "build", "both")
    run_train = args.command in ("train", "both")

    if run_screen:
        need_screen = force_screen or not sc_path.exists()
        if need_screen:
            MOMENTUM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(":memory:")
            out = build_screener_parquet(con, write_parquet=True, out_path=sc_path)
            print(f"Wrote screener: {out}")
            log_screener_candidate_stats(out)
            con.close()
        else:
            print(
                f"Skipping screener build; using {sc_path} "
                "(change ScreenerParams or use --force-screen to rebuild)."
            )

    if run_panel:
        if args.use_screener and not sc_path.exists():
            MOMENTUM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            print(
                f"Screener not found at {sc_path}; building it now (needed for --use-screener). "
                "Tip: `screen` alone or `both --use-screener` also refreshes the screener."
            )
            con_sc = duckdb.connect(":memory:")
            out_sc = build_screener_parquet(con_sc, write_parquet=True, out_path=sc_path)
            print(f"Wrote screener: {out_sc}")
            log_screener_candidate_stats(out_sc)
            con_sc.close()
        need_panel = force_panel or not PANEL_PATH.exists()
        if need_panel:
            MOMENTUM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(":memory:")
            path = build_panel(
                con,
                write_parquet=True,
                use_screener=args.use_screener,
                screener_parquet_path=sc_path if args.use_screener else None,
                tp_sl_k_stop=float(args.tp_sl_k_stop),
                tp_sl_k_target=float(args.tp_sl_k_target),
            )
            print(f"Wrote panel: {path} ({pd.read_parquet(path).shape[0]:,} rows)")
            con.close()
        else:
            print(
                f"Skipping panel build; using {PANEL_PATH} "
                "(use --force-panel or --force to rebuild). "
                "Two-phase workflow: run `screen` once, then `panel --use-screener` to refresh features "
                "without rescanning the full screener."
            )

    if run_train:
        if not PANEL_PATH.exists():
            raise SystemExit(
                f"Panel not found at {PANEL_PATH}. Run `python experiments/qullamaggie.py panel` "
                "or `both` first."
            )
        run_walk_forward(
            PANEL_PATH,
            wf_freq=args.wf_freq,
            wf_step=max(1, args.wf_step),
            selection_policy=args.selection_policy,
            breakout_col=args.breakout_col,
            eval_variant=args.eval_variant,
            eval_score_threshold=args.eval_score_threshold,
            eval_score_quantile=args.eval_score_quantile,
            top_frac=args.top_frac,
            eval_ret_mode=args.eval_ret,
            top_k=args.top_k,
            rank_score_mode=args.rank_score,
            experiment_matrix=args.experiment_matrix,
            experiment_top_k=max(1, int(args.experiment_top_k)),
            random_seed=int(args.random_seed),
            rank_score_sweep=args.rank_score_sweep,
            wf_compact=args.wf_compact,
        )


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# Original spec (docstring)
# ---------------------------------------------------------------------------
"""
Features:
- ret 1m, 3m, 6m
- ADR, ATR %
- 10d SMA, 20d SMA, 50d SMA
- distance from 52 week high, 52 week low
- slope of log returns 1m, 3m, 6m,
- pullback depth pullback_pct = (rolling_max_N - close) / rolling_max_N
- days_since_high = days_since(rolling_max_N)
- Distance from 10d SMA, 20d SMA, 50d SMA
- Range contraction, range_N = (rolling_max_N - rolling_min_N) / rolling_mean_N, range_5d / range_20d
- tight closes abs(close - close.shift(1)) < threshold
- Volume contraction vol_5d / vol_20d
- slope of volume over 20 days
- distance to breakout dist_to_high = (rolling_max_N - close) / close

Labels: mfe_pct, mae_pct; score / rr / EV; normalize MFE/MAE by ATR for targets.
Next-day open entry; universe from daily_universe.parquet; SEP from data/sep.parquet.
Models: ridge + xgboost; walkforward folds.
"""
