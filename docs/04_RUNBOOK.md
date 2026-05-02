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
