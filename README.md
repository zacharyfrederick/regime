# Regime research

Point-in-time (PIT) feature pipeline from Sharadar data, producing a single **master feature table** for regime classification and modeling. Notebooks and models read only from the master table; they never touch raw Sharadar files.

## What this repo does

1. **Pipeline (scripts):** Builds parquet artifacts in order: universe → feature tables → master. Each script consumes prior outputs and raw data; nothing is recomputed downstream. Changing universe logic means re-running from script 01 and rebuilding.
2. **Research (notebooks):** Load `outputs/master/master_features.parquet` for validation, EDA, HMM baseline, feature reduction, and regime analysis. Fast iteration against a stable artifact.
3. **Data:** Raw Sharadar parquet and FRED cache live under **`data/`**. See [docs/data_sources.md](docs/data_sources.md).

## Run from scratch

```powershell
.\run_pipeline.ps1
```

Runs pipeline scripts 01 through 08 in order (01–07 build artifacts; 08 runs validation and writes the report). First run end-to-end may take 1.5–3 hours depending on hardware; subsequent single-stage runs ~5–40 minutes. Use a 3–5 year date subset in `config.py` for faster validation. Set `DEBUG = True` in `config.py` to limit the pipeline to a few tickers (default: AAPL, MSFT, JPM, XOM, JNJ) for quick runs.

## Layout

- **`data/`** — Raw data (Sharadar parquet, FRED macro). Data is located here.
- **`outputs/`** — Computed artifacts: `universe/`, `features/`, `master/`. Master table: `outputs/master/master_features.parquet`.
- **`pipeline/`** — Deterministic scripts: `01_universe.py` … `08_validation.py`. Shared helpers (e.g. `fundamental_quality.py`) live here.
- **`notebooks/`** — Validation and research (validate pipeline, feature EDA, HMM, feature reduction, regime analysis).
- **`config.py`** — Paths, date range, parameters. Change one file to switch environment.
- **`docs/`** — [data_sources.md](docs/data_sources.md), [artifact_schema.md](docs/artifact_schema.md), [pipeline_spec.md](docs/pipeline_spec.md), [validation_and_biases.md](docs/validation_and_biases.md), [validation_checks.md](docs/validation_checks.md), [validation_issues_remediation.md](docs/validation_issues_remediation.md), [run_errors_remediation.md](docs/run_errors_remediation.md), [validation_scripts.md](docs/validation_scripts.md). **`docs/future_features/`** holds non-authoritative design notes for possible future work (e.g. backtest SQL engine, semi-supervised HMM); not implemented in this repo.
- Optional: a `check.py` at repo root can run SF2/insider diagnostics after 06 (see validation docs); not included in this repo.

## Pipeline architecture

Execution is **procedural** (no orchestration framework). Run scripts in order: 00 optional fetch → 01–07 build artifacts → 08 validation. **Flow:** 01_universe → daily_universe.parquet; 02–06 → feature parquets; 07_merge → master_features.parquet; 08_validation → reads all, writes validation report. Each script reads from config paths and prior outputs and writes one parquet (or report). No recomputation downstream; changing universe or feature logic requires re-running from the changed step. Runner: [run_pipeline.ps1](run_pipeline.ps1) (00 commented out; 01–08; when `config.DEBUG` is True, runs `pipeline/validate_debug.py` after 08). See [docs/pipeline_spec.md](docs/pipeline_spec.md) for script contracts.

## Validation

Run `notebooks/01_validate_pipeline.ipynb` before modeling. It performs 13 checks:

| # | Check | Purpose |
|---|-------|---------|
| 1 | PIT integrity | No datekey > date; NULL rate; staleness |
| 2 | Survivorship bias | LEH/HTZ delist sequence; fwd_delisted_90d fraction |
| 3 | Duplicate rows | Zero (ticker, date) duplicates |
| 4 | Distribution sanity | Valuation ranges; ret_12m; vol; extreme vs splits |
| 5 | Sector relative | Median ~1.0 for ratios; mean ~0 for diffs |
| 6 | Macro temporal | Yield curve, VIX, SPY, CPI known events |
| 7 | Insider signal | Buy activity 5–15%; securityadcode warning |
| 8 | Cross-feature | pcf vs pe correlation; pe_pit vs DAILY.pe |
| 9 | Temporal consistency | vol_20d and pcf_pit jump checks |
| 10 | Null rate audit | Flag >80% null features |
| 11 | Null by year | Data sourcing gaps |
| 12 | Universe composition | Tickers per year growth |
| 13 | Restatement coverage | SF1 ARQ multi-filing diagnostic |

See [docs/validation_checks.md](docs/validation_checks.md) for what each check does, why it matters, and how to interpret results. The runner executes `08_validation.py`, which writes all output to `outputs/validation_report.md` (outputs/ is gitignored). After a debug run, run `python pipeline/validate_debug.py` for contract and sanity checks; see `outputs/validation_report_debug.md`.

## Research loop

1. Change a pipeline script (e.g. add a feature or fix universe logic).
2. Run `.\run_pipeline.ps1` (or from the step you changed).
3. Open the relevant notebook; re-run against the new artifact.
4. Run the full validation notebook (`01_validate_pipeline.ipynb`) before modeling. See [validation_checks.md](docs/validation_checks.md).

This keeps modeling iteration fast: you work in notebooks against a pre-computed master table and only re-run the pipeline when the feature set or universe changes.

## Data assumptions

The pipeline relies on documented assumptions about raw data (Sharadar SF1/SEP, universe, PIT, FRED, fundamentals, sector-relative, insider/06, price features). They are written so they can be validated in notebooks and post-generation checks. **Full list:** [docs/data_assumptions.md](docs/data_assumptions.md).

Macro checks are in `08_validation.py` (section 6). Insider/institutional: [validation_checks.md](docs/validation_checks.md), [validation_issues_remediation.md](docs/validation_issues_remediation.md). Optional spot-check scripts: [validation_scripts.md](docs/validation_scripts.md).

## Documentation

| Doc | Description |
|-----|-------------|
| [data_sources.md](docs/data_sources.md) | Raw inputs (Sharadar tables, FRED series, benchmark tickers) |
| [data_assumptions.md](docs/data_assumptions.md) | Data assumptions by pipeline stage (SF1, SEP, FRED, 02–06) |
| [pipeline_spec.md](docs/pipeline_spec.md) | Script order, contracts, inputs/outputs |
| [artifact_schema.md](docs/artifact_schema.md) | Parquet schemas (universe, features, master) |
| [validation_checks.md](docs/validation_checks.md) | The 13 validation checks (what, why, how to interpret) |
| [validation_issues_remediation.md](docs/validation_issues_remediation.md) | When checks fail: investigation steps and fix priority |
| [run_errors_remediation.md](docs/run_errors_remediation.md) | Run errors: critical vs bad assumptions, fixes applied |
| [validation_and_biases.md](docs/validation_and_biases.md) | PIT/survivorship checklist for code review |
| [validation_scripts.md](docs/validation_scripts.md) | Optional spot-check scripts (price, sector-relative) |
| [label_upgrade_roadmap.md](docs/label_upgrade_roadmap.md) | Calendar-anchored forward labels: weekly (isoyear, iso_week), monthly/quarterly/annual (year, month/quarter/year); T-1 feature_date; 4 horizons × 6 columns |
| [future_features/](docs/future_features/) | Design notes for possible future work (not implemented) |
