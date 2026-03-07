#!/usr/bin/env python3
"""
One command to build current 0004 data and run selection.
Sets DATE_START = DATE_END = last real month-end (efficient single-date run),
runs the pipeline 01 through 08_merge, then runs current_selection_0004.
"""
import argparse
import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Set config date range before any pipeline module is imported
import config
from experiments.select_0004 import (
    get_last_real_month_end,
    get_last_trading_day_of_prev_month,
)

PIPELINE_STAGES = [
    "pipeline.01_universe",
    "pipeline.02_fundamentals",
    "pipeline.03_price_features",
    "pipeline.04_macro_features",
    "pipeline.05_sector_relative",
    "pipeline.06_insider_institutional",
    "pipeline.07_labels",
    "pipeline.08_merge",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run pipeline for last real month-end, then current 0004 selection."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override date (YYYY-MM-DD). Default: last day of previous calendar month.",
    )
    args = parser.parse_args()

    if args.date is not None:
        import pandas as pd
        target_date = pd.Timestamp(args.date)
        date_str = target_date.strftime("%Y-%m-%d")
    else:
        sep_path = config.DATA_DIR / "SEP.parquet"
        if not sep_path.exists():
            sep_path = config.DATA_DIR / "sep.parquet"
        if not sep_path.exists():
            print("SEP.parquet not found in data/. Cannot resolve last trading day.", file=sys.stderr)
            sys.exit(1)
        trading_day, last_calendar = get_last_trading_day_of_prev_month(sep_path)
        if trading_day is None:
            print(
                f"No trading data on or before {last_calendar.date()}. Ingest through that date or use --date YYYY-MM-DD.",
                file=sys.stderr,
            )
            sys.exit(1)
        if trading_day.to_period("M") != last_calendar.to_period("M"):
            print(
                f"No trading data for {last_calendar.date()} (last real month). Latest in SEP is {trading_day.date()}. Ingest through {last_calendar.date()} or use --date YYYY-MM-DD.",
                file=sys.stderr,
            )
            sys.exit(1)
        target_date = trading_day
        date_str = target_date.strftime("%Y-%m-%d")
        print(f"Using last trading day of previous month: {date_str}", flush=True)

    config.DATE_START = date_str
    config.DATE_END = date_str

    print(f"Building pipeline for {date_str}...", flush=True)

    for mod_name in PIPELINE_STAGES:
        mod = importlib.import_module(mod_name)
        mod.main()

    print(f"Pipeline done. Running current selection for {date_str}...", flush=True)
    from experiments import current_selection_0004
    current_selection_0004.main()


if __name__ == "__main__":
    main()
