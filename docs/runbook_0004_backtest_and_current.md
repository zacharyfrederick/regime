# Runbook: Experiment 0004 Backtest vs Current Selection

Same pipeline code and shared 0004 selector; only the date range and output differ.

## Backtest (historical)

1. Set the pipeline date range in `config.py`: `DATE_START`, `DATE_END` (e.g. `DATE_END = "2024-12-31"`).
2. Run the full pipeline (01_universe → 02_fundamentals → 03_price → 04_macro → 05_sector → 06_insider → 07_labels → 08_merge).
3. Run the experiment:
   ```bash
   python experiments/0004.py
   ```
   This loads `master_features.parquet`, applies the shared 0004 selector (month-end, quality filter, top 1500, quintiles), and runs the backtest and reporting.

## Current selection (live portfolio)

- **Default date**: Last *trading* day of the previous month (resolved from SEP so the date exists in the data; e.g. if the last calendar day is a Saturday, uses the prior Friday). If SEP is missing, falls back to last calendar day. There is no fallback to “latest date in master”; the script expects data for that exact date.
- **If there is no data for that date**: The script exits with code 1 and prints a clear message telling you to run the pipeline (e.g. `python scripts/run_current_0004.py`) and re-run. Use this as the signal to ingest/run the pipeline.
- Run the current selection script:
  ```bash
  python experiments/current_selection_0004.py
  ```
  It writes `experiments/runs/0004_current/current_assignments.parquet` and `current_metadata.json` unless `--no-save` is used.

  Options:
  - `--as-of YYYY-MM-DD`: use this exact date (no “on or before” fallback).
  - `--master PATH`: path to master parquet (default: `config.MASTER_FEATURES_PATH`).
  - `--out-dir PATH`: where to write outputs (default: `experiments/runs/0004_current`).
  - `--no-save`: only print to stdout; do not write files.

## One command to refresh and get current 0004

Run the runner script from the project root:

```bash
python scripts/run_current_0004.py
```

It (1) resolves the last *trading* day of the previous month from SEP (so the pipeline has data for that date), (2) sets `DATE_START` and `DATE_END` to that date (so only one date is computed — efficient), (3) runs the full pipeline (01_universe through 08_merge), (4) runs `current_selection_0004.py`. If there is no SEP data for the previous month, it exits with a clear message. Optional: `--date YYYY-MM-DD` overrides the date for testing.

## Efficiency

- **Backtest**: Use a full range in `config.py` (e.g. `DATE_START = "2000-01-01"`, `DATE_END = "2024-12-31"`).
- **Current only**: The runner sets `DATE_START = DATE_END` to the last trading day of the previous month (from SEP), so the pipeline builds a single-date grid and only that date is written to master. All lookbacks (e.g. 10y for fundamentals, 252d for price) are handled inside each stage.

## Single source of truth

- **Features**: The pipeline (same code for backtest and current) produces `master_features.parquet`. No separate “current” feature calculator.
- **Selection rules**: `experiments/select_0004.py` defines the 0004 filters and ranking. Both `0004.py` (backtest) and `current_selection_0004.py` use `select_0004_from_path()` (or `select_0004()`).
