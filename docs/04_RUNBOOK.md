# Runbook

## Prerequisites
- PostgreSQL running (Docker compose stack up)
- `.env` configured (DB settings, API token)
- Python environment with dependencies installed
- Gold DDL scripts executed once:
  - `SQLs/create_gold_transaction_notability.sql`
  - `SQLs/create_gold_transaction_save_potential.sql`

## Standard Commands

### 1) Normal incremental (most common)
```bash
python scripts/run_pipeline.py --mode incremental
```
Behavior:
- API source by default
- Uses silver watermark (`MAX(transaction_date)+1` to now)
- Inserts only new silver hashes
- Refreshes both gold tables for new expense hashes

### 2) Incremental with explicit date range
```bash
python scripts/run_pipeline.py --mode incremental --from-date 2026-04-01 --to-date 2026-04-10
```

### 3) Incremental from file
```bash
python scripts/run_pipeline.py --mode incremental --source file --file data/raw/export.xlsx
```

### 4) Full rebuild from file
```bash
python scripts/run_pipeline.py --mode full --source file --file data/raw/full_export.xlsx
```

### 5) Full rebuild from API
```bash
python scripts/run_pipeline.py --mode full --source api --from-date 2024-01-01 --to-date 2026-04-01
```

### 6) Manual gold full refresh (repair/recompute)
```bash
python scripts/run_pipeline.py --refresh-gold notability
python scripts/run_pipeline.py --refresh-gold save-potential
python scripts/run_pipeline.py --refresh-gold both
```

## When to Use Manual Gold Refresh
- Category/classification remapping after historical rows already exist
- Score formula updates
- Backfills/corrections in silver that should reflect in historical gold rows

## Verification Checklist After Run
1. `metadata.pipeline_runs` has SUCCESS entry
2. `silver.transactions` row count increased as expected (incremental)
3. `gold.transaction_notability` updated rows are present
4. `gold.transaction_save_potential` updated rows are present
5. Sample labels/reasons look sensible in Tableau

## Common Pitfalls
- Missing `gold` schema or tables in preflight
- Missing API token for API source
- Classification not populated in silver can reduce save-potential quality
- Small history windows for rare/new subcategories produce sparse stats

---

## Backup & Offsite Copy

### What runs automatically
`scripts/backup.ps1` is registered in Windows Task Scheduler (weekly). Each run:
1. `pg_dump` inside the Postgres container → custom-format `.dump` file in `backups/`
2. Uploads the dump to Google Drive via rclone (`gdrive:Finance Backups/`)
3. Purges local dumps older than 45 days

Run manually at any time:
```powershell
.\scripts\backup.ps1
```

### One-time rclone setup (required before Drive upload works)

**1. Install rclone**
```powershell
winget install Rclone.Rclone
```

**2. Configure a Google Drive remote named `gdrive`**
```powershell
rclone config
```
At the prompts:
- `n` → new remote
- Name: `gdrive`
- Storage type: `drive` (Google Drive)
- `client_id` / `client_secret`: leave blank (uses rclone's built-in OAuth app — no GCP project needed)
- Scope: `1` (full access)
- `root_folder_id`: leave blank
- `service_account_file`: leave blank
- Auto-config: `y` → browser opens for Google account authorization

**3. Create the destination folder in Drive**
Create a folder called `Finance Backups` in your Google Drive root (or adjust `$GdriveFolder` in `backup.ps1`).

**4. Verify**
```powershell
rclone ls gdrive:"Finance Backups"/
```

### Verifying a backup
```powershell
# List local dumps
Get-ChildItem backups\*.dump | Select-Object Name, Length, LastWriteTime

# List Drive copies
rclone ls gdrive:"Finance Backups"/
```

### Restoring from a dump
```powershell
# Copy dump into container and restore
docker cp backups\finance_warehouse_YYYY-MM-DD.dump postgres_container:/tmp/restore.dump
docker exec -e "PGPASSWORD=<password>" postgres_container `
    pg_restore -U $POSTGRES_USER -d finance_warehouse --clean /tmp/restore.dump
```
