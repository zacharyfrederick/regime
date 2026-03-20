#!/usr/bin/env python3
"""
Momentum experiment: grid from master (top 1500/month-end), momentum features from SEP, join.

- Grid: (ticker, date) from master_features.parquet, month-end only, top 1500 by marketcap_daily per date.
- Features: computed from SEP in script (ret_12_1, ret_6_1, ret_3_1, price_to_ma200, dist_high_60d,
  dist_52w_high, drawdown_20d, ret_1m, ret_6m, vol_20d, volume_ratio).
- Strategy: XGBoost predicts rank of next-month return (rank_ret_1m); we go long top decile by prediction.
  Universe: stocks down >=30% over past 6 months (ret_6m <= -0.30) to focus on names that may have bottomed.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import numpy as np
import pandas as pd
import xgboost as xgb

from config import DATA_DIR, MASTER_FEATURES_PATH

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MOMENTUM_TOP_N = 1500
MOMENTUM_LOOKBACK_DAYS = 273  # 252 + 21 for skip-month 12m
FEATURES_DIR = ROOT / "outputs" / "features"
MOMENTUM_FEATURES_PATH = FEATURES_DIR / "momentum_features.parquet"
# Exclude sectors from momentum universe (e.g. biotech)
EXCLUDE_SECTORS = ("Biotechnology",)

# Features for XGBoost (predict rank of next-month return; identify names that have bottomed)
XGB_FEATURES = [
    "ret_12_1", "ret_6_1", "ret_3_1", "price_to_ma200",
    "dist_high_60d", "dist_52w_high", "drawdown_20d",
    "ret_1m", "ret_6m", "vol_20d", "volume_ratio",
]
# Label = cross-sectional rank of forward return (percentile 0-1); we predict rank, then take top decile by prediction and evaluate with actual return
LABEL_COL = "rank_ret_1m"
RETURN_COL = "fwd_ret_21td"  # actual return used for portfolio period return
MIN_TRAIN_MONTHS = 36  # minimum history before we start predicting
XGB_PARAMS = {
    "objective": "reg:squarederror",
    "max_depth": 4,
    "learning_rate": 0.05,
    "n_estimators": 100,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
}


def _sep_path() -> Path:
    p = DATA_DIR / "SEP.parquet"
    return p if p.exists() else DATA_DIR / "sep.parquet"


def build_grid(con, master_path: str) -> None:
    """Build grid: month-end dates, top 1500 by marketcap_daily per date. Expose ticker, date, marketcap_daily, fwd_ret_21td."""
    con.execute(
        f"""
        CREATE OR REPLACE VIEW grid AS
        WITH month_end_dates AS (
            SELECT MAX(date) AS rebal_date
            FROM read_parquet({master_path})
            GROUP BY DATE_TRUNC('month', date)
        ),
        ranked AS (
            SELECT
                m.ticker,
                m.date,
                m.marketcap_daily,
                m.fwd_ret_21td,
                m.sector,
                ROW_NUMBER() OVER (PARTITION BY m.date ORDER BY m.marketcap_daily DESC) AS mktcap_rank
            FROM read_parquet({master_path}) m
            INNER JOIN month_end_dates d ON m.date = d.rebal_date
            WHERE m.marketcap_daily IS NOT NULL
              AND m.fwd_ret_21td IS NOT NULL
              AND (m.sector IS NULL OR m.sector NOT IN ({", ".join(repr(s) for s in EXCLUDE_SECTORS)}))
        )
        SELECT ticker, date, marketcap_daily, fwd_ret_21td, sector
        FROM ranked
        WHERE mktcap_rank <= {MOMENTUM_TOP_N}
        """
    )


def load_sep_and_filter(con, sep_path: Path) -> None:
    """Load SEP into DuckDB and create sep_filtered: grid tickers, date in [min(grid.date) - lookback, max(grid.date)]."""
    path_sql = repr(str(sep_path.resolve()))
    con.execute(f"CREATE OR REPLACE VIEW sep AS SELECT * FROM read_parquet({path_sql})")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW sep_filtered AS
        SELECT * FROM sep
        WHERE ticker IN (SELECT DISTINCT ticker FROM grid)
          AND CAST(date AS DATE) >= (SELECT MIN(date) - INTERVAL '{MOMENTUM_LOOKBACK_DAYS} days' FROM grid)
          AND CAST(date AS DATE) <= (SELECT MAX(date) FROM grid)
        """
    )


def build_momentum_features_phase1(con) -> None:
    """Phase 1: ret_12_1, ret_6_1, ret_3_1, price_to_ma200, dist_high_60d, dist_52w_high, drawdown_20d, ret_1m, ret_6m, vol_20d, volume_ratio."""
    con.execute(
        """
        CREATE OR REPLACE VIEW momentum_features_full AS
        WITH daily AS (
            SELECT ticker, date, closeadj, volume, high, low,
                   closeadj / LAG(closeadj, 1) OVER w - 1 AS daily_ret,
                   LAG(closeadj, 21) OVER w AS price_1m_ago,
                   LAG(closeadj, 63) OVER w AS price_3m_ago,
                   LAG(closeadj, 126) OVER w AS price_6m_ago,
                   LAG(closeadj, 252) OVER w AS price_12m_ago,
                   closeadj / LAG(closeadj, 21) OVER w - 1 AS ret_1m,
                   closeadj / LAG(closeadj, 126) OVER w - 1 AS ret_6m
            FROM sep_filtered
            WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ),
        base AS (
            SELECT *,
                   AVG(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                   MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high_20d,
                   MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS high_60d,
                   MAX(closeadj) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_252d,
                   STDDEV(daily_ret) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_20d,
                   AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volume_ma20
            FROM daily
        ),
        derived AS (
            SELECT ticker, date,
                   price_1m_ago / NULLIF(price_12m_ago, 0) - 1 AS ret_12_1,
                   price_1m_ago / NULLIF(price_6m_ago, 0) - 1 AS ret_6_1,
                   price_1m_ago / NULLIF(price_3m_ago, 0) - 1 AS ret_3_1,
                   closeadj / NULLIF(ma200, 0) - 1 AS price_to_ma200,
                   closeadj / NULLIF(high_60d, 0) - 1 AS dist_high_60d,
                   closeadj / NULLIF(high_252d, 0) - 1 AS dist_52w_high,
                   closeadj / NULLIF(high_20d, 0) - 1 AS drawdown_20d,
                   ret_1m,
                   ret_6m,
                   vol_20d,
                   volume / NULLIF(volume_ma20, 0) AS volume_ratio
            FROM base
        )
        SELECT * FROM derived
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW momentum_features AS
        SELECT g.ticker, g.date, g.marketcap_daily, g.fwd_ret_21td, g.sector,
               m.ret_12_1, m.ret_6_1, m.ret_3_1, m.price_to_ma200,
               m.dist_high_60d, m.dist_52w_high, m.drawdown_20d,
               m.ret_1m, m.ret_6m, m.vol_20d, m.volume_ratio
        FROM grid g
        LEFT JOIN momentum_features_full m ON g.ticker = m.ticker AND g.date = m.date
        """
    )


def get_panel_with_rank(con) -> pd.DataFrame:
    """Return full panel as DataFrame; add fwd_ret_1m (= fwd_ret_21td) and rank_ret_1m (cross-sectional percentile rank of fwd return)."""
    df = con.execute("SELECT * FROM momentum_features").df()
    df["fwd_ret_1m"] = df["fwd_ret_21td"]
    df["rank_ret_1m"] = df.groupby("date")["fwd_ret_21td"].rank(pct=True)
    return df


def run_backtest_xgb(panel: pd.DataFrame) -> dict:
    """XGBoost predicts rank of next-month return (rank_ret_1m); we go long top decile by prediction. Portfolio return uses actual fwd_ret_21td. Universe: stocks down >=30% over past 6 months (ret_6m <= -0.30)."""
    df = panel.copy()
    df[XGB_FEATURES] = df[XGB_FEATURES].replace([np.inf, -np.inf], np.nan)
    # Require only label (rank of fwd return) and ret_6m; XGBoost handles NaN in features (missing value)
    df = df.dropna(subset=[LABEL_COL, "ret_6m"])
    df = df[df["ret_6m"] <= -0.30]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    rebal_dates = df["date"].drop_duplicates().sort_values().tolist()

    period_rets_list = []
    for pred_date in rebal_dates:
        train_dates = [d for d in rebal_dates if d < pred_date]
        if len(train_dates) < MIN_TRAIN_MONTHS:
            continue
        train_df = df[df["date"].isin(train_dates)]
        pred_df = df[df["date"] == pred_date]
        if pred_df.empty:
            continue
        X_train = train_df[XGB_FEATURES]
        y_train = train_df[LABEL_COL]
        X_pred = pred_df[XGB_FEATURES]
        model = xgb.XGBRegressor(**XGB_PARAMS)
        model.fit(X_train, y_train)
        pred_df = pred_df.copy()
        pred_df["pred"] = model.predict(X_pred)
        rank = pred_df["pred"].rank(method="first")
        n = len(pred_df)
        pred_df["decile"] = ((rank - 1) * 10 / n).astype(int).clip(0, 9) + 1
        top = pred_df[pred_df["decile"] == 10]
        if not top.empty:
            period_rets_list.append((pred_date, top[RETURN_COL].mean()))
        else:
            period_rets_list.append((pred_date, 0.0))

    if not period_rets_list:
        return {
            "cagr": np.nan,
            "sharpe": np.nan,
            "max_drawdown": np.nan,
            "period_rets": pd.Series(dtype=float),
            "n_months": 0,
        }
    period_rets = pd.Series({d: r for d, r in period_rets_list}).sort_index()
    all_dates = pd.to_datetime(df["date"].drop_duplicates().sort_values())
    period_rets = period_rets.reindex(all_dates).fillna(0).sort_index()
    cumulative = (1 + period_rets).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak
    n_periods = len(period_rets)
    years = n_periods / 12.0 if n_periods else 0
    cagr = (cumulative.iloc[-1] ** (1 / years) - 1) if years and cumulative.iloc[-1] > 0 else np.nan
    sharpe = (period_rets.mean() / period_rets.std() * np.sqrt(12)) if period_rets.std() > 0 else np.nan
    max_dd = drawdown.min() if len(drawdown) else np.nan
    return {
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "period_rets": period_rets,
        "n_months": n_periods,
    }


def main() -> None:
    assert MASTER_FEATURES_PATH.exists(), f"Master features not found: {MASTER_FEATURES_PATH}"
    sep_path = _sep_path()
    if not sep_path.exists():
        print(f"SEP not found at {sep_path}; cannot compute momentum features.")
        return

    con = duckdb.connect(":memory:")
    master_path = repr(str(MASTER_FEATURES_PATH.resolve()))

    # 1. Grid: month-end, top 1500
    build_grid(con, master_path)
    n_grid = con.execute("SELECT COUNT(*) FROM grid").fetchone()[0]
    print(f"Grid: {n_grid:,} rows (month-end, top {MOMENTUM_TOP_N} by marketcap per date)")

    if n_grid == 0:
        print("No grid rows. Exiting.")
        return

    # 2. SEP load and filter
    load_sep_and_filter(con, sep_path)
    n_sep = con.execute("SELECT COUNT(*) FROM sep_filtered").fetchone()[0]
    print(f"SEP filtered: {n_sep:,} rows")

    # 3. Phase 1 momentum features and join
    build_momentum_features_phase1(con)
    panel = get_panel_with_rank(con)
    print(f"Panel: {len(panel):,} rows, {panel['date'].nunique()} rebalance dates")

    # 4. Write parquet
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(MOMENTUM_FEATURES_PATH, index=False)
    print(f"Wrote {MOMENTUM_FEATURES_PATH}")

    # 5. Strategy: XGBoost predicts rank; top decile by prediction (universe: down 30%+ over 6m)
    stats = run_backtest_xgb(panel)
    print("\n--- Momentum XGBoost (top decile by predicted rank, universe: ret_6m <= -30%) ---")
    print(f"CAGR:        {stats['cagr']:.2%}")
    print(f"Sharpe:      {stats['sharpe']:.2f}")
    print(f"Max drawdown: {stats['max_drawdown']:.2%}")
    print(f"Months:      {stats['n_months']}")

    con.close()


if __name__ == "__main__":
    main()
