# ============================================================================
# Run Update Invalid Records script (safest way)
# ============================================================================
# Prerequisites: PostgreSQL client (psql) in PATH
# Usage: .\scripts\run_update_invalid_records.ps1
# ============================================================================

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

# Load .env
$envPath = Join-Path $ProjectRoot ".env"
if (-not (Test-Path $envPath)) {
    Write-Error ".env not found at $envPath"
}
Get-Content $envPath | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
    }
}

$db = $env:POSTGRES_DB
$user = $env:POSTGRES_USER
$host = $env:POSTGRES_HOST
$port = $env:POSTGRES_PORT

if (-not $db -or -not $user) {
    Write-Error "POSTGRES_DB and POSTGRES_USER must be set in .env"
}

Write-Host "`n=== PRE-FLIGHT CHECK ===" -ForegroundColor Cyan
Write-Host "Checking for non-initial_load rows that would be affected...`n"

$preflightQuery = @"
SELECT created_by, COUNT(*) AS cnt
FROM silver.transactions
WHERE created_by IS DISTINCT FROM 'initial_load_script'
  AND (
    subcategory IN ('Books, audio, subscription','Public Transport','Others','Vehicle Insurance',
                   'Stationary, Tools','Stationery, tools','Stationery, office','Restaurant, fast-food','Furniture',
                   'Life & Entertainment','Missing','Transportation','Housing','Vehicle','Communication, PC','Income',
                   'Unknown Expense','UNKNOWNN_CATEGORY')
    AND (category IS NULL OR classification IS NULL)
  )
GROUP BY created_by;
"@

$preflightResult = $preflightQuery | psql -h $host -p $port -U $user -d $db -t -A 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Pre-flight check failed: $preflightResult"
}

if ($preflightResult -and $preflightResult.Trim()) {
    Write-Host "ABORT: Found rows NOT from initial_load_script that would be affected:" -ForegroundColor Red
    Write-Host $preflightResult
    Write-Host "`nThis script only updates initial_load data. Fix or remove incremental_load rows first.`n" -ForegroundColor Yellow
    exit 1
}

Write-Host "Pre-flight OK: No non-initial_load rows would be affected.`n" -ForegroundColor Green

Write-Host "=== RUNNING UPDATE SCRIPT ===" -ForegroundColor Cyan
$scriptPath = Join-Path $ProjectRoot "SQLs\Update_invalid_records_from_initial_upload.sql"
psql -h $host -p $port -U $user -d $db -f $scriptPath
if ($LASTEXITCODE -ne 0) {
    Write-Error "Update script failed. Run ROLLBACK if in a transaction."
}
Write-Host "`nDone." -ForegroundColor Green
