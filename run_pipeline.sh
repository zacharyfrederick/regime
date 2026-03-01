#!/usr/bin/env bash
# Run pipeline scripts 00 through 09 in order.
# Log start/end; re-run from scratch when universe or feature logic changes.
# Excludes: fundamental_quality.

set -e
cd "$(dirname "$0")"

run_step() {
    python "$1"
}

ts() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

is_debug=$(python -c "import config; print(1 if config.DEBUG else 0)")

echo "$(ts) Pipeline start"

if [ "$is_debug" = "0" ]; then
    run_step pipeline/00_fetch_fred.py
    echo "$(ts) 00_fetch_fred done"
fi

run_step pipeline/01_universe.py
echo "$(ts) 01_universe done"
run_step pipeline/02_fundamentals.py
echo "$(ts) 02_fundamentals done"
run_step pipeline/03_price_features.py
echo "$(ts) 03_price_features done"

if [ "$is_debug" = "0" ]; then
    run_step pipeline/04_macro_features.py
    echo "$(ts) 04_macro_features done"
fi

run_step pipeline/05_sector_relative.py
echo "$(ts) 05_sector_relative done"
run_step pipeline/06_insider_institutional.py
echo "$(ts) 06_insider_institutional done"
run_step pipeline/07_labels.py
echo "$(ts) 07_labels done"
run_step pipeline/08_merge.py
echo "$(ts) 08_merge done"
run_step pipeline/09_validation.py
echo "$(ts) 09_validation done"

if [ "$is_debug" = "1" ]; then
    run_step pipeline/validate_debug.py
    echo "$(ts) validate_debug done"
fi

echo "$(ts) Pipeline complete"
