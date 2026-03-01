#!/usr/bin/env python3
"""
Insider and institutional features from SF2 (filingdate PIT) and SF3A (calendardate + 45d PIT).
SF2: open market purchases/sales only (transactioncode P/S; transactionshares not null and <> 0).
SF3A: institutional share holders, units, value, put/call ratio, QoQ changes.
PIT boundaries: SF2 uses filingdate directly (Form 4 due within 2 business days);
SF3A uses calendardate + 45 days (13F due 45 calendar days after quarter end — do not reduce to 0).
Output: outputs/features/insider_institutional.parquet
"""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from config import (
    apply_duckdb_limits,
    DATA_DIR,
    DATE_START,
    DATE_END,
    DAILY_UNIVERSE_PATH,
    DEBUG,
    DEBUG_TICKERS,
    DEBUG_TICKERS_DEFAULT,
    INSIDER_INSTITUTIONAL_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

OUTPUT_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("date", pa.date32()),
    ("insider_buy_count_90d", pa.float64()),
    ("insider_sell_count_90d", pa.float64()),
    ("insider_net_shares_90d", pa.float64()),
    ("insider_net_ratio_90d", pa.float64()),
    ("insider_officer_buy_90d", pa.float64()),
    ("inst_shrholders", pa.float64()),
    ("inst_shrunits", pa.float64()),
    ("inst_shrvalue", pa.float64()),
    ("inst_put_call_ratio", pa.float64()),
    ("inst_shrholders_chg_qoq", pa.float64()),
    ("inst_shrunits_chg_qoq", pa.float64()),
])


def _parquet(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"


def _write_empty() -> None:
    INSIDER_INSTITUTIONAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    tbl = pa.table(
        {c: pa.array([], type=OUTPUT_SCHEMA.field(c).type) for c in OUTPUT_SCHEMA.names}
    )
    pq.write_table(tbl, INSIDER_INSTITUTIONAL_PATH)
    log.info("Wrote empty %s", INSIDER_INSTITUTIONAL_PATH)


def main() -> None:
    INSIDER_INSTITUTIONAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    apply_duckdb_limits(con)

    def _path_sql(p: Path) -> str:
        return repr(str(p.resolve()))

    if not DAILY_UNIVERSE_PATH.exists():
        log.warning("Universe not found; writing empty output.")
        _write_empty()
        return

    # Grid: ticker, date from universe. In debug mode restrict to config debug tickers.
    grid_sql = f"""
        SELECT ticker, date
        FROM read_parquet({_path_sql(DAILY_UNIVERSE_PATH)})
    """
    if DEBUG and (DEBUG_TICKERS or DEBUG_TICKERS_DEFAULT):
        effective_tickers = DEBUG_TICKERS or DEBUG_TICKERS_DEFAULT
        tickers_list = ",".join(repr(t) for t in effective_tickers)
        grid_sql += f" WHERE ticker IN ({tickers_list})"
    con.execute(f"CREATE OR REPLACE VIEW grid AS {grid_sql}")

    sf2_path = _parquet("SF2")
    sf3a_path = _parquet("SF3A")
    has_sf2 = sf2_path.exists()
    has_sf3a = sf3a_path.exists()

    if not has_sf2 and not has_sf3a:
        log.warning("SF2 and SF3A not found; writing empty output.")
        _write_empty()
        return

    # ------------------------------------------------------------------ SF2
    # Open market only: P (purchase) / S (sale). No securityadcode filter — P/S is the reliable signal;
    # securityadcode is inconsistently populated for P (mostly NULL/DA in practice).
    # filingdate is PIT boundary — no offset (Form 4 due within 2 business days).
    # transactionshares: we normalize by transactioncode (P => +abs, S => -abs) so both
    # positive-only and signed conventions in the data are handled.
    # Alternative denominator for insider_net_ratio_90d: shares outstanding (SF1) for cross-ticker comparability.
    # ------------------------------------------------------------------
    if has_sf2:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW sf2_clean AS
            SELECT
                ticker,
                CAST(filingdate AS DATE) AS filingdate,
                transactioncode,
                securityadcode,
                isofficer,
                CASE
                    WHEN transactioncode = 'P' THEN ABS(transactionshares)
                    WHEN transactioncode = 'S' THEN -ABS(transactionshares)
                    ELSE NULL
                END AS net_shares,
                ABS(transactionshares) AS abs_shares,
                sharesownedbeforetransaction
            FROM read_parquet({_path_sql(sf2_path)})
            WHERE transactioncode IN ('P', 'S')
              AND transactionshares IS NOT NULL
              AND transactionshares <> 0
              -- 90-day lookback: grid dates from DATE_START need filings from DATE_START - 90d
              AND CAST(filingdate AS DATE) >= '{DATE_START}'::DATE - INTERVAL '90 days'
              AND CAST(filingdate AS DATE) <= '{DATE_END}'::DATE
              AND ticker IN (SELECT DISTINCT ticker FROM grid)
            """
        )
        con.execute(
            """
            CREATE OR REPLACE VIEW insider_features AS
            SELECT
                g.ticker,
                g.date,
                COUNT(CASE WHEN s.transactioncode = 'P' THEN 1 END)::DOUBLE AS insider_buy_count_90d,
                COUNT(CASE WHEN s.transactioncode = 'S' THEN 1 END)::DOUBLE AS insider_sell_count_90d,
                SUM(s.net_shares) AS insider_net_shares_90d,
                CASE
                    WHEN AVG(s.sharesownedbeforetransaction) > 0
                    THEN SUM(s.net_shares) / AVG(s.sharesownedbeforetransaction)
                    ELSE NULL
                END AS insider_net_ratio_90d,
                SUM(CASE
                    WHEN s.transactioncode = 'P'
                    AND UPPER(TRIM(CAST(COALESCE(s.isofficer, '') AS VARCHAR))) IN ('Y', '1', 'TRUE')
                    THEN s.abs_shares ELSE 0
                END)::DOUBLE AS insider_officer_buy_90d
            FROM grid g
            LEFT JOIN sf2_clean s
                ON s.ticker = g.ticker
                AND s.filingdate BETWEEN g.date - INTERVAL '90 days' AND g.date
            GROUP BY g.ticker, g.date
            """
        )
    else:
        log.warning("SF2 not found; insider features will be NULL.")
        con.execute(
            """
            CREATE OR REPLACE VIEW insider_features AS
            SELECT
                ticker, date,
                CAST(NULL AS DOUBLE) AS insider_buy_count_90d,
                CAST(NULL AS DOUBLE) AS insider_sell_count_90d,
                CAST(NULL AS DOUBLE) AS insider_net_shares_90d,
                CAST(NULL AS DOUBLE) AS insider_net_ratio_90d,
                CAST(NULL AS DOUBLE) AS insider_officer_buy_90d
            FROM grid
            """
        )

    # ------------------------------------------------------------------ SF3A
    # calendardate = quarter end. 13F due 45 calendar days after quarter end; fixed 45-day lag is conservative.
    # QoQ computed over calendardate order; ASOF by pit_date gives PIT-correct QoQ for that quarter.
    # ------------------------------------------------------------------
    if has_sf3a:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW sf3a_pit AS
            SELECT
                ticker,
                CAST(calendardate AS DATE) AS calendardate,
                CAST(calendardate AS DATE) + INTERVAL '45 days' AS pit_date,
                shrholders,
                shrunits,
                shrvalue,
                cllunits,
                putunits,
                totalvalue,
                shrholders - LAG(shrholders) OVER (
                    PARTITION BY ticker ORDER BY CAST(calendardate AS DATE)
                ) AS shrholders_chg_qoq,
                shrunits - LAG(shrunits) OVER (
                    PARTITION BY ticker ORDER BY CAST(calendardate AS DATE)
                ) AS shrunits_chg_qoq
            FROM read_parquet({_path_sql(sf3a_path)})
            -- 1-year lookback: LAG needs prior quarter for first quarter in range
            WHERE CAST(calendardate AS DATE) >= '{DATE_START}'::DATE - INTERVAL '1 year'
              AND CAST(calendardate AS DATE) <= '{DATE_END}'::DATE
              AND ticker IN (SELECT DISTINCT ticker FROM grid)
            """
        )
        con.execute(
            """
            CREATE OR REPLACE VIEW inst_features AS
            SELECT
                g.ticker,
                g.date,
                i.shrholders::DOUBLE AS inst_shrholders,
                i.shrunits::DOUBLE AS inst_shrunits,
                i.shrvalue::DOUBLE AS inst_shrvalue,
                -- Cap at 100: raw ratio explodes when cllunits is tiny; >100 is noise/data artifact
                (CASE
                    WHEN i.cllunits > 0 AND i.putunits / i.cllunits <= 100
                    THEN i.putunits / i.cllunits ELSE NULL
                END)::DOUBLE AS inst_put_call_ratio,
                i.shrholders_chg_qoq::DOUBLE AS inst_shrholders_chg_qoq,
                i.shrunits_chg_qoq::DOUBLE AS inst_shrunits_chg_qoq
            FROM grid g
            ASOF LEFT JOIN (
                SELECT * FROM sf3a_pit ORDER BY pit_date
            ) i ON i.ticker = g.ticker AND i.pit_date <= g.date
            """
        )
    else:
        log.warning("SF3A not found; institutional features will be NULL.")
        con.execute(
            """
            CREATE OR REPLACE VIEW inst_features AS
            SELECT
                ticker, date,
                CAST(NULL AS DOUBLE) AS inst_shrholders,
                CAST(NULL AS DOUBLE) AS inst_shrunits,
                CAST(NULL AS DOUBLE) AS inst_shrvalue,
                CAST(NULL AS DOUBLE) AS inst_put_call_ratio,
                CAST(NULL AS DOUBLE) AS inst_shrholders_chg_qoq,
                CAST(NULL AS DOUBLE) AS inst_shrunits_chg_qoq
            FROM grid
            """
        )

    con.execute(
        """
        CREATE OR REPLACE VIEW insider_institutional AS
        SELECT
            g.ticker,
            g.date,
            ins.insider_buy_count_90d,
            ins.insider_sell_count_90d,
            ins.insider_net_shares_90d,
            ins.insider_net_ratio_90d,
            ins.insider_officer_buy_90d,
            inst.inst_shrholders,
            inst.inst_shrunits,
            inst.inst_shrvalue,
            inst.inst_put_call_ratio,
            inst.inst_shrholders_chg_qoq,
            inst.inst_shrunits_chg_qoq
        FROM grid g
        LEFT JOIN insider_features ins ON ins.ticker = g.ticker AND ins.date = g.date
        LEFT JOIN inst_features inst ON inst.ticker = g.ticker AND inst.date = g.date
        """
    )

    path_out = _path_sql(INSIDER_INSTITUTIONAL_PATH)
    con.execute(f"COPY (SELECT * FROM insider_institutional) TO {path_out} (FORMAT PARQUET)")
    log.info("Wrote %s", INSIDER_INSTITUTIONAL_PATH)

    # Validation (when we have data)
    try:
        n = con.execute("SELECT COUNT(*) FROM insider_institutional").fetchone()[0]
        if n > 0:
            df = con.execute("SELECT * FROM insider_institutional").df()
            buy_nonzero = (df["insider_buy_count_90d"] > 0).mean()
            log.info("Pct rows with insider buy activity: %.1f%%", buy_nonzero * 100)
            aapl = df[df["ticker"] == "AAPL"].sort_values("date")
            if len(aapl) > 0:
                unit_changes = aapl["inst_shrunits"].diff().abs()
                change_dates = (unit_changes > 0).sum()
                years = len(aapl) / 252.0
                expected = years * 4
                log.info(
                    "AAPL inst_shrunits changed on %d dates (expect ~%.0f for %.1f years of data)",
                    change_dates, expected, years,
                )
            pcr = df["inst_put_call_ratio"].dropna()
            if len(pcr) > 0:
                log.info(
                    "inst_put_call_ratio: median=%.2f, max=%.1f (often NULL when cllunits=0)",
                    pcr.median(), pcr.max()
                )
    except Exception as e:
        log.debug("Validation skipped: %s", e)

    con.close()


if __name__ == "__main__":
    main()
