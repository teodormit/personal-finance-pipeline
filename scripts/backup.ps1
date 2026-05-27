# Backs up the finance_warehouse database from the Docker container,
# then uploads the dump to Google Drive via rclone (offsite copy).
#
# Run manually:   .\scripts\backup.ps1
# Weekly schedule: registered in Windows Task Scheduler (see RUNBOOK §Backup).
# One-time rclone setup required before Drive upload works — see RUNBOOK §Backup.

$ContainerName = "postgres_container"
$DbName        = "finance_warehouse"
$DbUser        = "teodor_admin"
$ProjectRoot   = Split-Path -Parent $PSScriptRoot
$BackupDir     = Join-Path $ProjectRoot "backups"
$Date          = Get-Date -Format "yyyy-MM-dd"
$DumpFile      = Join-Path $BackupDir "finance_warehouse_$Date.dump"
$RetainDays    = 45
$GdriveRemote  = "gdrive"
$GdriveFolder  = "Finance Warehouse Backups"

# Read POSTGRES_PASSWORD from .env (avoids hardcoding credentials)
$EnvFile = Join-Path $ProjectRoot ".env"
$PgPassword = (Get-Content $EnvFile | Where-Object { $_ -match "^POSTGRES_PASSWORD=" }) -replace "^POSTGRES_PASSWORD=", ""

if (-not $PgPassword) {
    Write-Host "ERROR: POSTGRES_PASSWORD not found in .env" -ForegroundColor Red
    exit 1
}

# Ensure backup directory exists
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null

# Dump inside container, then copy to host
Write-Host "Starting backup of $DbName..."
docker exec -e "PGPASSWORD=$PgPassword" $ContainerName `
    pg_dump -U $DbUser -d $DbName -F custom -f /tmp/backup.dump

if (-not $?) {
    Write-Host "ERROR: pg_dump failed." -ForegroundColor Red
    exit 1
}

docker cp "${ContainerName}:/tmp/backup.dump" $DumpFile

if ($?) {
    $SizeMB = [math]::Round((Get-Item $DumpFile).Length / 1MB, 2)
    Write-Host "Backup saved: $DumpFile ($SizeMB MB)"
} else {
    Write-Host "ERROR: Failed to copy backup file from container." -ForegroundColor Red
    exit 1
}

# Upload to Google Drive (offsite copy) — non-fatal; local backup is primary
Write-Host "Uploading to Google Drive ($GdriveRemote`:$GdriveFolder)..."
rclone copy $DumpFile "${GdriveRemote}:${GdriveFolder}/" --log-level INFO

if ($LASTEXITCODE -eq 0) {
    Write-Host "Google Drive upload complete."
} else {
    Write-Host "WARNING: Google Drive upload failed. Local backup is intact." -ForegroundColor Yellow
}

# Remove dumps older than $RetainDays days
$OldFiles = Get-ChildItem "$BackupDir\*.dump" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$RetainDays) }

if ($OldFiles.Count -gt 0) {
    $OldFiles | Remove-Item
    Write-Host "Removed $($OldFiles.Count) old backup(s) older than $RetainDays days."
}

Write-Host "Done."
