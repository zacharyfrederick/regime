#!/usr/bin/env python3
"""
Merge universe + all feature parquets into master_features.parquet.
Filter to in_universe = True; one row per ticker per trading day.
Output: outputs/master/master_features.parquet
"""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
from tqdm import tqdm

from config import (
    apply_duckdb_limits,
    DAILY_UNIVERSE_PATH,
    FORWARD_LABELS_PATH,
    FUNDAMENTAL_PIT_PATH,
    INSIDER_INSTITUTIONAL_PATH,
    MACRO_FEATURES_PATH,
    MASTER_FEATURES_PATH,
    PRICE_FEATURES_PATH,
    SECTOR_RELATIVE_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    log.info("Starting 07_merge")
    MASTER_FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    apply_duckdb_limits(con)

    if not DAILY_UNIVERSE_PATH.exists():
        log.warning("Universe not found; run 01_universe.py first. Writing empty master.")
        import pyarrow as pa, pyarrow.parquet as pq
        schema = pa.schema([
            ("ticker", pa.string()), ("date", pa.date32()), ("sector", pa.string()), ("famaindustry", pa.string()),
            ("days_listed", pa.int32()), ("scalemarketcap", pa.float64()), ("marketcap_daily", pa.int64()), ("fwd_spinoff_60d", pa.bool_()),
            ("fwd_ret_5td", pa.float64()), ("fwd_holding_days_5td", pa.int64()), ("fwd_delisted_5td", pa.bool_()), ("fwd_delist_type_5td", pa.string()),
            ("fwd_ret_10td", pa.float64()), ("fwd_holding_days_10td", pa.int64()), ("fwd_delisted_10td", pa.bool_()), ("fwd_delist_type_10td", pa.string()),
            ("fwd_ret_21td", pa.float64()), ("fwd_holding_days_21td", pa.int64()), ("fwd_delisted_21td", pa.bool_()), ("fwd_delist_type_21td", pa.string()),
            ("fwd_ret_63td", pa.float64()), ("fwd_holding_days_63td", pa.int64()), ("fwd_delisted_63td", pa.bool_()), ("fwd_delist_type_63td", pa.string()),
            ("fwd_ret_126td", pa.float64()), ("fwd_holding_days_126td", pa.int64()), ("fwd_delisted_126td", pa.bool_()), ("fwd_delist_type_126td", pa.string()),
            ("fwd_ret_252td", pa.float64()), ("fwd_holding_days_252td", pa.int64()), ("fwd_delisted_252td", pa.bool_()), ("fwd_delist_type_252td", pa.string()),
            ("days_since_filing", pa.int32()), ("quarters_stale", pa.float64()),
            ("days_since_art", pa.int32()), ("days_since_arq", pa.int32()), ("days_since_filing_max", pa.int32()),
            ("ret_1m", pa.float64()), ("ret_3m", pa.float64()), ("ret_6m", pa.float64()), ("ret_12m", pa.float64()),
            ("vol_20d", pa.float64()), ("vol_60d", pa.float64()), ("vol_ratio", pa.float64()),
            ("volume_ratio_1m", pa.float64()), ("pct_52w_range", pa.float64()),
            ("ma50_cross", pa.float64()), ("ma200_cross", pa.float64()),
            ("atr_14d_normalized", pa.float64()), ("momentum_skew_60d", pa.float64()),
            ("ncfo_r2_10y", pa.float64()), ("ncfo_cagr_10y", pa.float64()), ("ncfo_pct_positive", pa.float64()),
            ("ncfo_r2_adjusted", pa.float64()), ("fcf_cagr_5y", pa.float64()), ("fcf_r2_10y", pa.float64()),
            ("fcf_pct_positive", pa.float64()), ("fcf_r2_adjusted", pa.float64()), ("fcf_ncfo_r2_delta", pa.float64()),
            ("roic_level", pa.float64()), ("roic_slope_3y", pa.float64()), ("grossmargin_slope", pa.float64()),
            ("fcf_conversion", pa.float64()), ("accrual_ratio", pa.float64()),
            ("sbc_pct_revenue", pa.float64()), ("capex_intensity", pa.float64()),
            ("net_debt_trend", pa.float64()), ("dilution_rate", pa.float64()),
            ("fcf_recon_ttm", pa.float64()), ("pcf_pit", pa.float64()), ("pfcf_pit", pa.float64()),
            ("ncfo_to_revenue", pa.float64()), ("fcf_to_revenue", pa.float64()),
            ("pe_pit", pa.float64()), ("pb_pit", pa.float64()), ("ps_pit", pa.float64()), ("evebitda_pit", pa.float64()),
            ("dividend_yield", pa.float64()), ("roe", pa.float64()), ("pretax_margin", pa.float64()),
            ("current_ratio", pa.float64()), ("debt_to_equity", pa.float64()), ("liabilities_to_assets", pa.float64()),
            ("payout_ratio", pa.float64()), ("earnings_growth_yoy", pa.float64()),
            ("pe_vs_sector", pa.float64()), ("pb_vs_sector", pa.float64()), ("ps_vs_sector", pa.float64()),
            ("pcf_vs_sector", pa.float64()), ("evebitda_vs_sector", pa.float64()), ("roic_vs_sector", pa.float64()),
            ("liabilities_to_assets_vs_sector", pa.float64()),
            ("ret_3m_vs_sector", pa.float64()), ("vol_vs_sector", pa.float64()), ("ret_3m_rank_sector", pa.float64()),
            ("yield_curve", pa.float64()), ("hy_spread", pa.float64()), ("vix", pa.float64()),
            ("vix_change_20d", pa.float64()), ("nfci", pa.float64()), ("real_rate", pa.float64()),
            ("spy_regime_ma", pa.float64()), ("spy_ret_12m", pa.float64()),
            ("insider_buy_count_90d", pa.float64()), ("insider_sell_count_90d", pa.float64()),
            ("insider_net_shares_90d", pa.float64()), ("insider_net_ratio_90d", pa.float64()),
            ("insider_officer_buy_90d", pa.float64()),
            ("inst_shrholders", pa.float64()), ("inst_shrunits", pa.float64()), ("inst_shrvalue", pa.float64()),
            ("inst_put_call_ratio", pa.float64()), ("inst_shrholders_chg_qoq", pa.float64()),
            ("inst_shrunits_chg_qoq", pa.float64()),
        ])
        tbl = pa.table({c: pa.array([], type=schema.field(c).type) for c in schema.names})
        pq.write_table(tbl, MASTER_FEATURES_PATH)
        log.info("Wrote empty %s", MASTER_FEATURES_PATH)
        return

    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    con.execute(f"""
        CREATE OR REPLACE VIEW universe AS
        SELECT * REPLACE (CAST(date AS DATE) AS date)
        FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})
    """)
    n_universe = con.execute("SELECT COUNT(*) FROM universe").fetchone()[0]
    n_tickers = con.execute("SELECT COUNT(DISTINCT ticker) FROM universe").fetchone()[0]
    log.info("Loaded universe: %d rows, %d tickers", n_universe, n_tickers)

    # Empty view schemas for missing parquets (upstream stages usually write empty-with-schema; this is defensive)
    _empty_fundamental_pit = """
        SELECT u.ticker, u.date,
               CAST(NULL AS INTEGER) AS days_since_filing, CAST(NULL AS DOUBLE) AS quarters_stale,
               CAST(NULL AS INTEGER) AS days_since_art, CAST(NULL AS INTEGER) AS days_since_arq,
               CAST(NULL AS INTEGER) AS days_since_filing_max,
               CAST(NULL AS DOUBLE) AS ncfo_r2_10y, CAST(NULL AS DOUBLE) AS ncfo_cagr_10y,
               CAST(NULL AS DOUBLE) AS ncfo_pct_positive, CAST(NULL AS DOUBLE) AS ncfo_r2_adjusted,
               CAST(NULL AS DOUBLE) AS fcf_cagr_5y, CAST(NULL AS DOUBLE) AS fcf_r2_10y,
               CAST(NULL AS DOUBLE) AS fcf_pct_positive, CAST(NULL AS DOUBLE) AS fcf_r2_adjusted,
               CAST(NULL AS DOUBLE) AS fcf_ncfo_r2_delta, CAST(NULL AS DOUBLE) AS roic_level,
               CAST(NULL AS DOUBLE) AS roic_slope_3y, CAST(NULL AS DOUBLE) AS grossmargin_slope,
               CAST(NULL AS DOUBLE) AS fcf_conversion, CAST(NULL AS DOUBLE) AS accrual_ratio,
               CAST(NULL AS DOUBLE) AS sbc_pct_revenue, CAST(NULL AS DOUBLE) AS capex_intensity,
               CAST(NULL AS DOUBLE) AS net_debt_trend, CAST(NULL AS DOUBLE) AS dilution_rate,
               CAST(NULL AS DOUBLE) AS fcf_recon_ttm, CAST(NULL AS DOUBLE) AS pcf_pit,
               CAST(NULL AS DOUBLE) AS pfcf_pit, CAST(NULL AS DOUBLE) AS ncfo_to_revenue,
               CAST(NULL AS DOUBLE) AS fcf_to_revenue, CAST(NULL AS DOUBLE) AS pe_pit,
               CAST(NULL AS DOUBLE) AS pb_pit, CAST(NULL AS DOUBLE) AS ps_pit,
               CAST(NULL AS DOUBLE) AS evebitda_pit,
               CAST(NULL AS DOUBLE) AS dividend_yield, CAST(NULL AS DOUBLE) AS roe, CAST(NULL AS DOUBLE) AS pretax_margin,
               CAST(NULL AS DOUBLE) AS current_ratio, CAST(NULL AS DOUBLE) AS debt_to_equity,
               CAST(NULL AS DOUBLE) AS liabilities_to_assets, CAST(NULL AS DOUBLE) AS payout_ratio,
               CAST(NULL AS DOUBLE) AS earnings_growth_yoy
        FROM universe u WHERE 1=0
    """
    _empty_price = """
        SELECT u.ticker, u.date,
               CAST(NULL AS DOUBLE) AS ret_1m, CAST(NULL AS DOUBLE) AS ret_3m,
               CAST(NULL AS DOUBLE) AS ret_6m, CAST(NULL AS DOUBLE) AS ret_12m,
               CAST(NULL AS DOUBLE) AS vol_20d, CAST(NULL AS DOUBLE) AS vol_60d,
               CAST(NULL AS DOUBLE) AS vol_ratio, CAST(NULL AS DOUBLE) AS volume_ratio_1m,
               CAST(NULL AS DOUBLE) AS pct_52w_range, CAST(NULL AS DOUBLE) AS ma50_cross,
               CAST(NULL AS DOUBLE) AS ma200_cross, CAST(NULL AS DOUBLE) AS atr_14d,
               CAST(NULL AS DOUBLE) AS momentum_skew_60d
        FROM universe u WHERE 1=0
    """
    _empty_sector = """
        SELECT u.ticker, u.date,
               CAST(NULL AS DOUBLE) AS pe_vs_sector, CAST(NULL AS DOUBLE) AS pb_vs_sector,
               CAST(NULL AS DOUBLE) AS ps_vs_sector, CAST(NULL AS DOUBLE) AS pcf_vs_sector,
               CAST(NULL AS DOUBLE) AS evebitda_vs_sector, CAST(NULL AS DOUBLE) AS roic_vs_sector,
               CAST(NULL AS DOUBLE) AS ret_3m_vs_sector, CAST(NULL AS DOUBLE) AS vol_vs_sector,
               CAST(NULL AS DOUBLE) AS ret_3m_rank_sector
        FROM universe u WHERE 1=0
    """
    _empty_macro = """
        SELECT u.date,
               CAST(NULL AS DOUBLE) AS yield_curve, CAST(NULL AS DOUBLE) AS hy_spread,
               CAST(NULL AS DOUBLE) AS vix, CAST(NULL AS DOUBLE) AS vix_change_20d,
               CAST(NULL AS DOUBLE) AS nfci, CAST(NULL AS DOUBLE) AS real_rate,
               CAST(NULL AS DOUBLE) AS spy_regime_ma, CAST(NULL AS DOUBLE) AS spy_ret_12m
        FROM universe u WHERE 1=0
    """
    _empty_insider = """
        SELECT u.ticker, u.date,
               CAST(NULL AS DOUBLE) AS insider_buy_count_90d, CAST(NULL AS DOUBLE) AS insider_sell_count_90d,
               CAST(NULL AS DOUBLE) AS insider_net_shares_90d, CAST(NULL AS DOUBLE) AS insider_net_ratio_90d,
               CAST(NULL AS DOUBLE) AS insider_officer_buy_90d, CAST(NULL AS DOUBLE) AS inst_shrholders,
               CAST(NULL AS DOUBLE) AS inst_shrunits, CAST(NULL AS DOUBLE) AS inst_shrvalue,
               CAST(NULL AS DOUBLE) AS inst_put_call_ratio, CAST(NULL AS DOUBLE) AS inst_shrholders_chg_qoq,
               CAST(NULL AS DOUBLE) AS inst_shrunits_chg_qoq
        FROM universe u WHERE 1=0
    """
    _empty_forward_labels = """
        SELECT u.ticker, u.date,
               CAST(NULL AS DOUBLE) AS fwd_ret_5td, CAST(NULL AS BIGINT) AS fwd_holding_days_5td,
               CAST(NULL AS BOOLEAN) AS fwd_delisted_5td, CAST(NULL AS VARCHAR) AS fwd_delist_type_5td,
               CAST(NULL AS DOUBLE) AS fwd_ret_10td, CAST(NULL AS BIGINT) AS fwd_holding_days_10td,
               CAST(NULL AS BOOLEAN) AS fwd_delisted_10td, CAST(NULL AS VARCHAR) AS fwd_delist_type_10td,
               CAST(NULL AS DOUBLE) AS fwd_ret_21td, CAST(NULL AS BIGINT) AS fwd_holding_days_21td,
               CAST(NULL AS BOOLEAN) AS fwd_delisted_21td, CAST(NULL AS VARCHAR) AS fwd_delist_type_21td,
               CAST(NULL AS DOUBLE) AS fwd_ret_63td, CAST(NULL AS BIGINT) AS fwd_holding_days_63td,
               CAST(NULL AS BOOLEAN) AS fwd_delisted_63td, CAST(NULL AS VARCHAR) AS fwd_delist_type_63td,
               CAST(NULL AS DOUBLE) AS fwd_ret_126td, CAST(NULL AS BIGINT) AS fwd_holding_days_126td,
               CAST(NULL AS BOOLEAN) AS fwd_delisted_126td, CAST(NULL AS VARCHAR) AS fwd_delist_type_126td,
               CAST(NULL AS DOUBLE) AS fwd_ret_252td, CAST(NULL AS BIGINT) AS fwd_holding_days_252td,
               CAST(NULL AS BOOLEAN) AS fwd_delisted_252td, CAST(NULL AS VARCHAR) AS fwd_delist_type_252td
        FROM universe u WHERE 1=0
    """
    _empty_views = {
        "fundamental_pit": _empty_fundamental_pit,
        "price_features": _empty_price,
        "sector_relative": _empty_sector,
        "macro_features": _empty_macro,
        "insider_institutional": _empty_insider,
        "forward_labels": _empty_forward_labels,
    }

    steps = ["universe", "feature_views", "build_master", "write_parquet", "validate"]
    pbar = tqdm(total=len(steps), desc="07_merge", unit="step", leave=True)
    pbar.set_postfix_str("universe")
    pbar.update(1)

    for path, name in [
        (FUNDAMENTAL_PIT_PATH, "fundamental_pit"),
        (PRICE_FEATURES_PATH, "price_features"),
        (MACRO_FEATURES_PATH, "macro_features"),
        (SECTOR_RELATIVE_PATH, "sector_relative"),
        (INSIDER_INSTITUTIONAL_PATH, "insider_institutional"),
        (FORWARD_LABELS_PATH, "forward_labels"),
    ]:
        if path.exists():
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({_path_sql(path)})")
            n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            log.info("  %s: %d rows", name, n)
        else:
            log.warning("  %s not found — features will be NULL in master", name)
            con.execute(f"CREATE OR REPLACE VIEW {name} AS {_empty_views[name]}")
    pbar.set_postfix_str("feature_views")
    pbar.update(1)

    # Join all on (ticker, date); filter in_universe; select master columns (use COALESCE for optional tables)
    con.execute(
        """
        CREATE OR REPLACE VIEW master_features AS
        SELECT
            u.ticker,
            u.date,
            u.sector,
            u.famaindustry,
            u.days_listed,
            u.scalemarketcap,
            u.marketcap_daily,
            u.fwd_spinoff_60d,
            l.fwd_ret_5td, l.fwd_holding_days_5td, l.fwd_delisted_5td, l.fwd_delist_type_5td,
            l.fwd_ret_10td, l.fwd_holding_days_10td, l.fwd_delisted_10td, l.fwd_delist_type_10td,
            l.fwd_ret_21td, l.fwd_holding_days_21td, l.fwd_delisted_21td, l.fwd_delist_type_21td,
            l.fwd_ret_63td, l.fwd_holding_days_63td, l.fwd_delisted_63td, l.fwd_delist_type_63td,
            l.fwd_ret_126td, l.fwd_holding_days_126td, l.fwd_delisted_126td, l.fwd_delist_type_126td,
            l.fwd_ret_252td, l.fwd_holding_days_252td, l.fwd_delisted_252td, l.fwd_delist_type_252td,
            f.days_since_filing,
            f.quarters_stale,
            f.days_since_art,
            f.days_since_arq,
            f.days_since_filing_max,
            p.ret_1m, p.ret_3m, p.ret_6m, p.ret_12m,
            p.vol_20d, p.vol_60d, p.vol_ratio,
            p.volume_ratio_1m, p.pct_52w_range,
            p.ma50_cross, p.ma200_cross,
            p.atr_14d AS atr_14d_normalized,
            p.momentum_skew_60d,
            f.ncfo_r2_10y, f.ncfo_cagr_10y, f.ncfo_pct_positive, f.ncfo_r2_adjusted,
            f.fcf_cagr_5y, f.fcf_r2_10y, f.fcf_pct_positive, f.fcf_r2_adjusted, f.fcf_ncfo_r2_delta,
            f.roic_level, f.roic_slope_3y, f.grossmargin_slope,
            f.fcf_conversion, f.accrual_ratio,
            f.sbc_pct_revenue, f.capex_intensity, f.net_debt_trend, f.dilution_rate,
            f.fcf_recon_ttm, f.pcf_pit, f.pfcf_pit, f.ncfo_to_revenue, f.fcf_to_revenue,
            f.pe_pit, f.pb_pit, f.ps_pit, f.evebitda_pit,
            f.dividend_yield, f.roe, f.pretax_margin, f.current_ratio, f.debt_to_equity,
            f.liabilities_to_assets, f.payout_ratio, f.earnings_growth_yoy,
            s.pe_vs_sector, s.pb_vs_sector, s.ps_vs_sector, s.pcf_vs_sector, s.evebitda_vs_sector, s.roic_vs_sector,
            s.ret_3m_vs_sector, s.vol_vs_sector, s.ret_3m_rank_sector,
            m.yield_curve, m.hy_spread, m.vix, m.vix_change_20d, m.nfci, m.real_rate,
            m.spy_regime_ma, m.spy_ret_12m,
            i.insider_buy_count_90d, i.insider_sell_count_90d, i.insider_net_shares_90d,
            i.insider_net_ratio_90d, i.insider_officer_buy_90d,
            i.inst_shrholders, i.inst_shrunits, i.inst_shrvalue, i.inst_put_call_ratio,
            i.inst_shrholders_chg_qoq, i.inst_shrunits_chg_qoq
        FROM universe u
        LEFT JOIN fundamental_pit f ON f.ticker = u.ticker AND f.date = u.date
        LEFT JOIN price_features p ON p.ticker = u.ticker AND p.date = u.date
        LEFT JOIN sector_relative s ON s.ticker = u.ticker AND s.date = u.date
        LEFT JOIN macro_features m ON m.date = u.date
        LEFT JOIN insider_institutional i ON i.ticker = u.ticker AND i.date = u.date
        LEFT JOIN forward_labels l ON l.ticker = u.ticker AND l.date = u.date
        WHERE CAST(u.in_universe AS BOOLEAN) = TRUE
        ORDER BY u.date, u.ticker
        """
    )
    pbar.set_postfix_str("build_master")
    pbar.update(1)

    log.info("Writing master_features.parquet...")
    master_out = repr(str(MASTER_FEATURES_PATH.resolve()))
    con.execute(f"COPY (SELECT * FROM master_features) TO {master_out} (FORMAT PARQUET)")
    pbar.set_postfix_str("write_parquet")
    pbar.update(1)
    log.info("Wrote %s", MASTER_FEATURES_PATH)

    # Post-write validation
    pbar.set_postfix_str("validate")
    pbar.update(1)
    pbar.close()

    result = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT ticker) AS tickers,
            COUNT(DISTINCT date) AS dates,
            SUM(CASE WHEN ret_1m IS NULL THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS ret_null_pct,
            SUM(CASE WHEN pcf_pit IS NULL THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS pcf_null_pct,
            SUM(CASE WHEN yield_curve IS NULL THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS macro_null_pct,
            MIN(date) AS date_min,
            MAX(date) AS date_max
        FROM master_features
    """).df()
    log.info("\n%s", result.to_string())

    total = int(result["total_rows"].iloc[0])
    tickers = int(result["tickers"].iloc[0])
    macro_null = float(result["macro_null_pct"].iloc[0])
    assert total > 0, "Master features is empty"
    assert tickers > 0, "No tickers — universe join failed"
    if tickers > 50:
        assert macro_null < 0.5, f"Macro null rate {macro_null:.1%} — macro join failed"
    else:
        log.info("Debug run (%d tickers) — skipping full-universe assertions", tickers)
    log.info("07_merge complete: %d rows, %d tickers, %d dates", total, tickers, int(result["dates"].iloc[0]))
    con.close()


if __name__ == "__main__":
    main()
