# Personal Finance Pipeline

A self-hosted, automated ETL pipeline that turns personal expense data from the Wallet (BudgetBakers) app into a structured, analytics-ready data warehouse in PostgreSQL -- with gold-layer intelligence and Tableau visualization.

---

## What This Project Does

1. **Extracts** expense and income data from BudgetBakers (REST API or CSV/XLSX export).
2. **Transforms** it through a 9-step cleaning pipeline (date parsing, currency conversion, deduplication via SHA-256 hash, derived fields).
3. **Loads** it into PostgreSQL following a **medallion architecture**: staging -> bronze -> silver -> gold.
4. **Scores** every expense transaction for **notability** (how surprising?) and **save potential** (could I have avoided this?).
5. **Visualized** in Tableau, connecting directly to silver and gold tables.

---

## Architecture

```
BudgetBakers API  ──┐
                    ├──> Extract ──> Transform ──> Load ──> PostgreSQL ──> Tableau
Wallet CSV/XLSX  ──┘
```

### Technology Stack

| Layer | Technology |
|-------|-----------|
| Data Warehouse | PostgreSQL 17 (Docker) |
| Extraction | BudgetBakers REST API (Premium) or file export |
| Transformation | Python 3.11+ with Pandas |
| Visualization | Tableau Public |
| Infrastructure | Docker Compose |

### Database Layers (Medallion Architecture)

| Schema | Purpose |
|--------|---------|
| `staging` | Temporary landing area, truncated each run |
| `bronze` | Raw data as received, append-only immutable archive |
| `silver` | Cleaned, deduplicated, and enriched -- main analytics layer |
| `gold` | Pre-computed transaction-level intelligence (notability, save potential) |
| `metadata` | Pipeline run logs and data quality issues |

### Key Features

- BudgetBakers API extraction (automated, no manual export needed)
- Incremental loading (only new transactions inserted via hash deduplication)
- Idempotent pipeline (safe to re-run without duplicates)
- Data quality validation (date parsing, null checks, row-count sanity)
- Gold-layer analytics computed automatically on each load
- Comprehensive pipeline logging to `metadata.pipeline_runs`

---

## Gold Layer: Transaction Intelligence

Two gold tables sit on top of silver and score every EXPENSE transaction at the `transaction_hash` level.

### Transaction Notability (`gold.transaction_notability`)

Answers: **"What's surprising this month?"**

Each transaction is scored by how unusual its amount is compared to the same subcategory over the prior 365 days (rolling z-score). Flags include:

- **New Category** -- first time spending in a subcategory
- **New Record** -- largest-ever transaction in a subcategory
- **Extreme / High Outlier** -- z-score >= 2 or 3 standard deviations above average
- **Insufficient History** -- not enough data to compute a meaningful z-score

### Transaction Save Potential (`gold.transaction_save_potential`)

Answers: **"Where could I have saved money?"**

Three signals combined into a weighted composite score:

| Signal | Weight | What it measures |
|--------|--------|-----------------|
| **Avoidability** (classification) | x3 (dominant) | WANT = 1.0, NEED = 0.4, MUST = 0.05 |
| **Frequency excess** | x2 | This month's transaction count vs. average monthly count in the subcategory (365-day lookback) |
| **Amount excess** | x1 | Positive z-score (same rolling window as notability), capped |

Labels: High Save Potential (>= 5), Medium (>= 3), Low (>= 1), Minimal (< 1).

Both gold tables refresh **automatically** after each pipeline load (incremental for new hashes, full on initial load). Manual full rebuilds are available via standalone scripts.

---

## Quick Start

### Prerequisites

- Docker & Docker Compose installed
- Python 3.11+
- BudgetBakers Wallet Premium + API token (from web.budgetbakers.com/settings/apiTokens) or extract .csv/.xlsx from the App
- Any data visualization tool. I have used Tableau Public Desktop or Tableau Desktop.

### Setup

1. Copy `.env.template` to `.env` and set `BUDGETBAKERS_API_TOKEN`
2. Start PostgreSQL: `docker compose up -d postgres`
3. Install deps: `pip install -r requirements.txt`
4. Run the gold DDL scripts once:
   ```bash
   psql -U teodor_admin -d finance_warehouse -f SQLs/create_gold_transaction_notability.sql
   psql -U teodor_admin -d finance_warehouse -f SQLs/create_gold_transaction_save_potential.sql
   ```

---

## Run Pipeline

All pipeline runs go through a single entry point: `scripts/run_pipeline.py`.

### Normal Incremental Run (most common)

Pull new transactions from the API starting from the day after the last silver record through today. This is the run you'd schedule daily or weekly.

```bash
python scripts/run_pipeline.py --mode incremental
```

This automatically:
- Extracts from the BudgetBakers API (default source)
- Transforms, deduplicates, and loads to staging -> bronze -> silver
- Refreshes gold notability + save potential for the new expense hashes

### Incremental with Explicit Date Range

Override the automatic watermark when you know exactly which days to pull:

```bash
python scripts/run_pipeline.py --mode incremental --from-date 2026-03-25 --to-date 2026-04-04
```

### Incremental from a File

Load a CSV or XLSX export instead of hitting the API. Only new rows (by hash) are appended to silver:

```bash
python scripts/run_pipeline.py --mode incremental --source file --file data/raw/export.xlsx
```

### Incremental with Account Filter

Filter to specific account presets during load:

```bash
python scripts/run_pipeline.py --mode incremental --account-filter bgn_final
python scripts/run_pipeline.py --mode incremental --account-filter eur       # default
```

### Full Load from File (Initial Setup or Rebuild)

Truncates silver and reloads everything from a file. Use for first-time setup or when you want to rebuild from a clean export:

```bash
python scripts/run_pipeline.py --mode full --source file --file data/raw/full_export.xlsx
```

### Full Load from API (Date Range)

Pull a date range from the API and do a full (truncate + reload) silver load:

```bash
python scripts/run_pipeline.py --mode full --source api --from-date 2024-01-01 --to-date 2026-04-01
```

### Manual Gold Refresh (Standalone)

After correcting silver data, remapping categories, or changing classification values, rebuild all gold scores from scratch:

```bash
python scripts/refresh_gold_notability.py --full
python scripts/refresh_gold_save_potential.py --full
```

These are **not needed** for normal pipeline runs -- gold refreshes automatically.

---

## Run Tests

```bash
python -m pytest tests/ -v
```

---

## Tableau

- Connect to `silver.transactions` for raw analytics
- Join `gold.transaction_notability` and `gold.transaction_save_potential` on `transaction_hash` for scoring columns
- Filter by `year_month`, color by `notability_label` or `save_potential_label`
- Refresh data extract after each pipeline run

---

## Project Layout

```
scripts/                    CLI entry points
  run_pipeline.py             unified pipeline runner
  refresh_gold_notability.py  standalone gold notability rebuild
  refresh_gold_save_potential.py  standalone gold save potential rebuild
src/
  extractors/               BudgetBakers API + file extraction
  transformers/             Pandas transformation logic
    expense_transformer.py    9-step cleaning pipeline
    notable_transactions_transformer.py   z-score + notability scoring
    save_potential_transformer.py         avoidability + frequency + amount scoring
  loaders/                  PostgreSQL loading (initial + incremental + gold)
  utils/                    DB connection helper, hash generator
SQLs/                       DDL scripts for gold tables
tests/                      Unit tests for transformers
data/raw/                   Source export files from Wallet app
```

---

## Project History

| Date | Milestone |
|------|-----------|
| Jun 2025 | Project initialized, local environment setup |
| Jul 2025 | PostgreSQL + MinIO Docker infrastructure |
| Nov 2025 | Phase 2: simplified pipeline, BudgetBakers API extraction |
| Mar 2026 | Initial + incremental loaders, silver layer complete |
| Apr 2026 | Gold layer: transaction notability (z-score) + save potential scoring |
