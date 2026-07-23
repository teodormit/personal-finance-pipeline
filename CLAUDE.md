# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Maintain a file called MEMORY.md in this project. After any significant decision, add an entry: What was decided / Why / What was rejected and why. Read MEMORY.md at the start of every session. Never contradict a logged decision without flagging it first.

When I say "session end", "wrapping up", or "let's stop here": write a session summary to MEMORY.md. Include: Worked on / Completed / In progress / Decisions made / Next session priorities.

Maintain a file called ERRORS.md. When an approach takes more than 2 attempts to work, log it: What didn't work / What worked instead / Note for next time. Check ERRORS.md before suggesting approaches to similar tasks.


## Project Context

This is a **long-term financial data warehouse** designed to run for ~30 years, not a script project. It is both a live personal finance system (real data, real decisions) and a portfolio showcase for someone transitioning from BI analyst to data engineer. Architecture decisions, code quality, and documented reasoning carry weight accordingly.

**Scope trajectory:** expenses and income are the foundation; investments and broader financial datasets will follow. `silver.transactions` already holds income rows, but the gold layer currently only models expenses. Avoid designing anything that assumes expenses are the only transaction type — income gold models are an explicit future gap. Keep new schema, transformer, or loader designs extensible to other `transaction_type` values.

**Infrastructure constraint:** open-source and self-hosted throughout. No managed cloud services as primary components.

See global CLAUDE.md for owner background and working preferences.

## Commands

```bash
# Run the pipeline (most common)
python scripts/run_pipeline.py --mode incremental
python scripts/run_pipeline.py --mode incremental --from-date 2026-04-01 --to-date 2026-04-10
python scripts/run_pipeline.py --mode incremental --source file --file data/raw/export.xlsx
python scripts/run_pipeline.py --mode full --source file --file data/raw/full_export.xlsx
python scripts/run_pipeline.py --mode full --source api --from-date 2024-01-01 --to-date 2026-04-01

# Manual gold full rebuild (after silver corrections or formula changes)
python scripts/run_pipeline.py --refresh-gold notability
python scripts/run_pipeline.py --refresh-gold save-potential
python scripts/run_pipeline.py --refresh-gold both

# Schema migrations
python scripts/migrate.py                 # apply pending migrations
python scripts/migrate.py --status        # list all migrations and status
python scripts/migrate.py --dry-run       # preview pending (no changes)
python scripts/migrate.py --baseline      # register existing files as applied (first-time only)

# Tests
python -m pytest tests/ -v
python -m pytest tests/test_notable_transactions_transformer.py -v
python -m pytest tests/test_save_potential_transformer.py -v
```

## Architecture

Data flows from BudgetBakers (REST API or CSV/XLSX export) through a medallion PostgreSQL warehouse running in Docker, visualized in Tableau.

```
Extract → Transform (Pandas) → staging → bronze → silver → gold → Tableau
```

**Database schemas:**
- `staging` — transient, truncated each run
- `bronze` — immutable raw archive, append-only
- `silver` — cleaned, deduped analytical base; `silver.transactions` is the primary analytics table
- `gold` — transaction-level intelligence: `gold.transaction_notability`, `gold.transaction_save_potential`
- `metadata` — pipeline run logs (`metadata.pipeline_runs`) and the silver change log (`metadata.transaction_audit`)

**Deduplication:** every transaction gets a SHA-256 `transaction_hash` (date + amount + category + description). Silver inserts only unknown hashes. Gold loaders upsert on `transaction_hash`.

**Gold refresh behavior:**
- Incremental runs: upsert gold only for newly inserted expense hashes.
- Full/initial loads: rebuild gold entirely.
- Both gold loaders widen their silver slice to `min_date - 365 days` through `max_date` to preserve historical context for scoring.

## Gold Scoring Models

### Notability (`gold.transaction_notability`)
*"Which transactions are unusual vs. my normal trend?"*

365-day rolling z-score per subcategory, deterministic baseline ordered by `(transaction_date, transaction_hash)`.

```
notability_score = max(z, 0) + 4*new_subcategory + 2*new_record
```

Labels: New Category, Insufficient History, Extreme Outlier (z≥3), High Outlier (z≥2), Above Average (z≥1), New Record, Normal.

### Save Potential (`gold.transaction_save_potential`)
*"Where could I have reasonably saved money?"*

```
save_potential_score = 3*avoidability + 2*freq_excess + 1*amt_excess
```

- `avoidability`: WANT=1.0, NEED=0.4, MUST=0.05 (from `silver.category_mapping`; missing defaults to NEED)
- `freq_excess = max(month_txn_count / hist_avg_monthly_count − 1, 0)` capped at 3.0
- `amt_excess = max(amount_z_score, 0)` capped at 5.0

Labels: High Save Potential (≥5), Medium (≥3), Low (≥1), Minimal (<1).

Any formula or weight change must be logged in `docs/05_DECISIONS_LOG.md` and `docs/06_CHANGELOG.md`.

## Code Conventions

**Data flow contract:** Extractors return a DataFrame → Transformers take and return a DataFrame → Loaders consume a DataFrame. Each loader follows: extract → transform → load_staging → load_bronze → load_silver.

**Database access:** always use the `DatabaseConnection` context manager:
```python
with self.db.connect() as conn:
    ...
```
Use `%s` placeholders for query parameters — never interpolate variables into SQL strings. Use `psycopg2.extras.execute_batch()` for bulk inserts.

**Imports:** `src/` modules use relative package names (`from utils.db_connector import get_db_connector`). Scripts add `src/` to `sys.path`.

**Environment:** load secrets with `python-dotenv`. Never hardcode credentials or tokens. DB connection details come exclusively from env vars via `src/utils/db_connector.py`.

**SQL init scripts** live in `scripts/sql/init/`, numbered for execution order (`01_`, `02_`, …), and run automatically when the Postgres container starts (mounted to `/docker-entrypoint-initdb.d`). Schema changes after initial setup go in `scripts/sql/migrations/` and are applied via `scripts/migrate.py`. Always schema-qualify identifiers (`silver.transactions`). Grant privileges to `$POSTGRES_USER` after creating tables. Staging scripts use `DROP IF EXISTS + CREATE`; bronze and silver are permanent.

**Style:** use `print()` for progress output (personal project, not a production service). Use `pathlib.Path` for file paths.

## Key Entry Points

| File | Purpose |
|------|---------|
| `scripts/run_pipeline.py` | Unified pipeline runner for all modes |
| `scripts/migrate.py` | DIY schema migration runner (apply / status / dry-run / baseline) |
| `src/loaders/incremental_load.py` | Append-only + dedupe load path |
| `src/loaders/initial_load.py` | Full truncate + reload path |
| `src/transformers/expense_transformer.py` | 9-step core cleaning/enrichment |
| `src/transformers/notable_transactions_transformer.py` | Notability scoring logic |
| `src/transformers/save_potential_transformer.py` | Save-potential scoring logic |
| `src/loaders/gold_notable_loader.py` | Gold upsert for notability |
| `src/loaders/gold_save_potential_loader.py` | Gold upsert for save potential |

## Strategy & Execution Plans

`docs/10_PRODUCT_STRATEGY.md` (2026-07) is the current strategy; `plan/` holds the execution playbooks — read `plan/00_OVERVIEW.md` first (executor contract, blind-spot register, status board) before proposing or starting work. When asked to run a playbook, follow it: goals and boundaries outrank steps, ground every claim in a tool result, stop-and-ask on destructive or scope-changing actions, and update the playbook's status/checkboxes plus MEMORY.md when done. `docs/08_STRATEGIC_ROADMAP.md` is the 2026-04 assessment — still valid background, but its Phase B.1/B.4 and the Approach C deferral are superseded by docs/10.

## Diagnostic Scripts

`scripts/inspect_incremental_load.py` — dry-run inspection without writing. **Use this before any incremental pipeline run to verify what would change.**
`scripts/inspect_api_output.py` — raw API data inspection.
`scripts/archive/` — retired one-off utilities kept for git history (BGN→EUR migration era).
