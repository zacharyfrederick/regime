# Run pipeline scripts 00 through 08 in order.
# Log start/end; re-run from scratch when universe or feature logic changes.
# Excludes: fundamental_quality.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Run-Step {
    param([string]$Script)
    & python $Script
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$ts = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
$isDebug = python -c "import config; print(1 if config.DEBUG else 0)"

Write-Host "$ts Pipeline start"
if ($isDebug -eq "0") {
    Run-Step pipeline/00_fetch_fred.py
    Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 00_fetch_fred done"
}

Run-Step pipeline/01_universe.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 01_universe done"
Run-Step pipeline/02_fundamentals.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 02_fundamentals done"
Run-Step pipeline/03_price_features.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 03_price_features done"

if ($isDebug -eq "0") {
    Run-Step pipeline/04_macro_features.py
    Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 04_macro_features done"
}
Run-Step pipeline/05_sector_relative.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 05_sector_relative done"
Run-Step pipeline/06_insider_institutional.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 06_insider_institutional done"
Run-Step pipeline/07_labels.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 07_labels done"
Run-Step pipeline/08_merge.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 08_merge done"
Run-Step pipeline/09_validation.py
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') 09_validation done"
# When DEBUG is True, run debug validation and fail pipeline if it fails
if ($isDebug -eq "1") {
    Run-Step pipeline/validate_debug.py
    Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') validate_debug done"
}
Write-Host "$(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') Pipeline complete"
