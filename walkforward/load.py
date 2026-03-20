"""
Data loading for a single fold. Expects conn to have a rebal-date table registered
(e.g. ``month_ends``, ``rebal_dates`` from ``get_rebal_dates`` with freq month/week/day).
"""
from __future__ import annotations


# Monthly: full column set, marketcap >= 1000, exclude Financial Services and Real Estate
MONTHLY_UNIVERSE_WHERE = (
    "AND (f.marketcap_daily IS NOT NULL AND f.marketcap_daily >= 2000)"
    " AND (f.sector IS NULL OR f.sector NOT IN ('Financial Services', 'Real Estate'))"
)
MONTHLY_SELECT_COLS = """
            SELECT f.ticker, f.date, f.sector, f.famaindustry,
                   f.fwd_ret_21td, f.fwd_ret_63td, f.fwd_ret_126td, f.fwd_ret_252td,
                   f.ret_1m, f.ret_3m, f.ret_6m, f.ret_12m,
                   f.vol_20d, f.vol_60d, f.vol_ratio, f.volume_ratio_1m, f.pct_52w_range,
                   f.ma50_cross, f.ma200_cross, f.atr_14d_normalized, f.momentum_skew_60d,
                   f.ncfo_r2_5y, f.ncfo_cagr_5y, f.ncfo_r2_10y, f.ncfo_cagr_10y,
                   f.fcf_r2_5y, f.fcf_r2_10y, f.fcf_cagr_5y, f.fcf_cagr_10y, f.fcf_pct_positive,
                   f.ncfo_pct_positive, f.ncfo_r2_adjusted_arcsinh, f.fcf_r2_adjusted_arcsinh,
                   f.grossmargin_slope, f.capex_intensity,
                   f.accrual_ratio, f.pcf_pit, f.pfcf_pit, f.roe, f.current_ratio, f.vix,
                   f.pe_pit, f.pb_pit, f.ps_pit, f.evebitda_pit,
                   f.dividend_yield, f.pretax_margin, f.debt_to_equity, f.liabilities_to_assets,
                   f.payout_ratio, f.earnings_growth_yoy,
                   f.pe_vs_sector, f.pb_vs_sector, f.ps_vs_sector, f.pcf_vs_sector,
                   f.evebitda_vs_sector, f.roic_vs_sector,
                   f.ret_3m_vs_sector, f.vol_vs_sector, f.ret_3m_rank_sector,
                   f.yield_curve, f.hy_spread, f.vix_change_20d, f.nfci, f.real_rate,
                   f.spy_regime_ma, f.spy_ret_12m,
                   f.marketcap_daily,
                   f.insider_buy_count_90d, f.insider_sell_count_90d, f.insider_net_shares_90d, f.insider_net_ratio_90d,
                   f.insider_officer_buy_90d,
                   f.inst_shrholders, f.inst_shrunits, f.inst_shrvalue, f.inst_put_call_ratio,
                   f.inst_shrholders_chg_qoq, f.inst_shrunits_chg_qoq
"""


def load_fold(
    conn,
    is_start,
    is_end,
    oos_start,
    oos_end,
    parquet_path: str,
    rebal_table_name: str = "month_ends",
    universe_where: str | None = None,
    quantile_path: str | None = None,
) -> "pd.DataFrame":
    """
    Load one fold: only IS and OOS dates (embargo excluded). Returns DataFrame with fold column.
    conn must have rebal dates registered, e.g. conn.register("month_ends", pd.DataFrame({"rebal_date": dates})).
    universe_where: optional extra AND clause for the universe (without leading AND), e.g. for weekly filters.
    quantile_path: optional path to pfcf_quantile_valuation.parquet; when provided and file exists,
        adds pfcf_3y_quantile and pfcf_5y_quantile via LEFT JOIN.
    """
    import pandas as pd

    if quantile_path is None:
        try:
            from config import PFCF_QUANTILE_VALUATION_PATH
            if PFCF_QUANTILE_VALUATION_PATH.exists():
                quantile_path = str(PFCF_QUANTILE_VALUATION_PATH.resolve())
        except Exception:
            pass

    where_clause = universe_where or MONTHLY_UNIVERSE_WHERE
    select_cols = MONTHLY_SELECT_COLS

    if quantile_path:
        sql = f"""
            WITH rebal_dates AS (
                SELECT rebal_date FROM {rebal_table_name}
                WHERE (rebal_date >= ? AND rebal_date <= ?)
                   OR (rebal_date >= ? AND rebal_date <= ?)
            ),
            universe AS (
                {select_cols}
                FROM read_parquet(?) f
                INNER JOIN rebal_dates r ON f.date = r.rebal_date
                {where_clause}
            ),
            with_quant AS (
                SELECT u.*, q.pfcf_3y_quantile, q.pfcf_5y_quantile
                FROM universe u
                LEFT JOIN read_parquet(?) q ON q.ticker = u.ticker AND q.date = u.date
            )
            SELECT *,
                   CASE WHEN date <= ? THEN 'is' ELSE 'oos' END AS fold
            FROM with_quant
        """
        return conn.execute(
            sql,
            [is_start, is_end, oos_start, oos_end, parquet_path, quantile_path, is_end],
        ).df()
    else:
        sql = f"""
            WITH rebal_dates AS (
                SELECT rebal_date FROM {rebal_table_name}
                WHERE (rebal_date >= ? AND rebal_date <= ?)
                   OR (rebal_date >= ? AND rebal_date <= ?)
            ),
            universe AS (
                {select_cols}
                FROM read_parquet(?) f
                INNER JOIN rebal_dates r ON f.date = r.rebal_date
                {where_clause}
            )
            SELECT *,
                   CASE WHEN date <= ? THEN 'is' ELSE 'oos' END AS fold
            FROM universe
        """
        return conn.execute(
            sql,
            [is_start, is_end, oos_start, oos_end, parquet_path, is_end],
        ).df()


def load_fold_monthly(
    conn,
    is_start,
    is_end,
    oos_start,
    oos_end,
    parquet_path: str,
) -> "pd.DataFrame":
    """Load one fold for monthly rebalance. Uses month_ends table and default universe."""
    return load_fold(
        conn, is_start, is_end, oos_start, oos_end, parquet_path,
        rebal_table_name="month_ends",
        universe_where=MONTHLY_UNIVERSE_WHERE,
    )
