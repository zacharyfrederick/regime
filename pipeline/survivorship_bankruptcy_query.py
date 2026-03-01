#!/usr/bin/env python3
"""
Survivorship bias: bankruptcy coverage vs universe.

Runs the documented queries to quantify how many bankruptcies are missing from
the universe in the 90 days before the event, and which ones "dropped early"
(in universe but fell off >30 days before the bankruptcy event).

Consumes: data/ACTIONS.parquet, outputs/universe/daily_universe.parquet.
Run from repo root: python pipeline/survivorship_bankruptcy_query.py

Output: prints summary + detail table to stdout; optional --out writes markdown.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
import duckdb


def _path_sql(p: Path) -> str:
    return repr(str(p.resolve()))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run survivorship bankruptcy coverage queries")
    parser.add_argument("--out", type=Path, default=None, help="Write markdown report to this path")
    args = parser.parse_args()

    actions_path = config.DATA_DIR / "ACTIONS.parquet"
    universe_path = config.DAILY_UNIVERSE_PATH

    if not actions_path.exists():
        print("ERROR: ACTIONS.parquet not found at", actions_path, file=sys.stderr)
        sys.exit(1)
    if not universe_path.exists():
        print("ERROR: daily_universe.parquet not found at", universe_path, file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect()
    con.execute(f"CREATE VIEW actions AS SELECT * FROM read_parquet({_path_sql(actions_path)})")
    con.execute(f"CREATE VIEW universe AS SELECT * FROM read_parquet({_path_sql(universe_path)})")

    # Use LOWER(TRIM(action)) to match pipeline convention (01_universe, 07_labels)
    summary_sql = """
    WITH bankruptcies AS (
        SELECT ticker,
               CAST(date AS DATE) AS event_date
        FROM actions
        WHERE LOWER(TRIM(action)) = 'bankruptcyliquidation'
    ),
    universe_coverage AS (
        SELECT b.ticker,
               b.event_date,
               COUNT(u.date) AS universe_days_in_window,
               MIN(CAST(u.date AS DATE)) AS first_universe_date,
               MAX(CAST(u.date AS DATE)) AS last_universe_date
        FROM bankruptcies b
        LEFT JOIN universe u
            ON u.ticker = b.ticker
            AND CAST(u.date AS DATE) BETWEEN b.event_date - INTERVAL '90 days'
                                          AND b.event_date
        GROUP BY b.ticker, b.event_date
    )
    SELECT
        COUNT(*) AS total_bankruptcies,
        SUM(CASE WHEN universe_days_in_window > 0 THEN 1 ELSE 0 END) AS in_universe,
        SUM(CASE WHEN universe_days_in_window = 0 THEN 1 ELSE 0 END) AS missing_from_universe,
        SUM(CASE WHEN DATEDIFF('day', last_universe_date, event_date) <= 5 THEN 1 ELSE 0 END) AS coverage_to_event,
        SUM(CASE WHEN universe_days_in_window > 0
                 AND DATEDIFF('day', last_universe_date, event_date) > 30 THEN 1 ELSE 0 END) AS dropped_early
    FROM universe_coverage
    """
    detail_sql = """
    WITH bankruptcies AS (
        SELECT ticker,
               CAST(date AS DATE) AS event_date
        FROM actions
        WHERE LOWER(TRIM(action)) = 'bankruptcyliquidation'
    ),
    universe_coverage AS (
        SELECT b.ticker,
               b.event_date,
               COUNT(u.date) AS universe_days_in_window,
               MIN(CAST(u.date AS DATE)) AS first_universe_date,
               MAX(CAST(u.date AS DATE)) AS last_universe_date
        FROM bankruptcies b
        LEFT JOIN universe u
            ON u.ticker = b.ticker
            AND CAST(u.date AS DATE) BETWEEN b.event_date - INTERVAL '90 days'
                                          AND b.event_date
        GROUP BY b.ticker, b.event_date
    )
    SELECT uc.ticker,
           uc.event_date,
           uc.universe_days_in_window,
           uc.first_universe_date,
           uc.last_universe_date,
           DATEDIFF('day', uc.last_universe_date, uc.event_date) AS days_before_event_dropped
    FROM universe_coverage uc
    WHERE uc.universe_days_in_window > 0
      AND DATEDIFF('day', uc.last_universe_date, uc.event_date) > 30
    ORDER BY days_before_event_dropped DESC
    """

    lines: list[str] = []

    def out(s: str = "") -> None:
        print(s)
        lines.append(s + "\n")

    out("Survivorship bias: bankruptcy coverage vs universe")
    out("=" * 60)
    out("")

    # Summary (variable names match SQL column names)
    row = con.execute(summary_sql).fetchone()
    total_bankruptcies, in_universe, missing_from_universe, coverage_to_event, dropped_early = row
    out("Summary (90-day window before each bankruptcy event):")
    out(f"  total_bankruptcies      {total_bankruptcies}")
    out(f"  in_universe             {in_universe}  (at least one universe day in window)")
    out(f"  missing_from_universe   {missing_from_universe}  (never in universe in that window)")
    out(f"  coverage_to_event       {coverage_to_event}  (last universe date within 5 days of event)")
    out(f"  dropped_early           {dropped_early}  (in universe but dropped >30 days before event)")
    out("")

    # Detail: dropped_early
    detail_rows = con.execute(detail_sql).fetchall()
    cols = ["ticker", "event_date", "universe_days_in_window", "first_universe_date", "last_universe_date", "days_before_event_dropped"]
    out("Detail: tickers that dropped early (>30 days before bankruptcy event)")
    out("-" * 60)
    if not detail_rows:
        out("  (none)")
    else:
        # Header
        out("  " + "  ".join(f"{c:>24}" for c in cols))
        out("  " + "-" * (24 * len(cols) + 2 * (len(cols) - 1)))
        for r in detail_rows:
            out("  " + "  ".join(f"{str(v):>24}" for v in r))
    out("")

    # Interpretation note
    out("Interpretation:")
    if total_bankruptcies and missing_from_universe is not None:
        pct_missing = 100.0 * missing_from_universe / total_bankruptcies
        out(f"  - {missing_from_universe} of {total_bankruptcies} bankruptcies ({pct_missing:.1f}%) have no universe presence in the 90 days before the event.")
    out(f"  - {dropped_early} were in the universe but left the grid >30 days before the event (survivorship bias: backtest could have held them but grid lost them before terminal return).")
    out("  - If dropped_early is small (<50 for a top-1500 universe over full range), document as known limitation and proceed.")
    out("  - If large, consider keeping stale tickers on the grid with NULL fundamentals to capture terminal return labels.")

    if args.out:
        args.out = Path(args.out)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text("".join(lines), encoding="utf-8")
        print("Wrote", args.out, file=sys.stderr)


if __name__ == "__main__":
    main()
