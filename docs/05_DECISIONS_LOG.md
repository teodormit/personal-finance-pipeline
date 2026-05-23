# Decisions Log

## 2026-04 � Gold notability model added
- Decision: Compute transaction-level notability in `gold.transaction_notability`.
- Why: Surface unusual spending beyond simple amount sorting.
- Key design:
  - 365-day rolling subcategory baseline
  - z-score + novelty flags
  - incremental upsert + optional full refresh script
- Tradeoff: Existing old rows are not automatically re-scored on incremental runs.

## 2026-04 � Gold save-potential model added
- Decision: Compute `gold.transaction_save_potential` with avoidability-dominant weighting.
- Why: Identify transactions with practical savings opportunity, not only statistical surprise.
- Formula: `3*avoidability + 2*freq_excess + 1*amt_excess`
- Tradeoff: Weight choices are heuristic and require periodic calibration.

## 2026-04 � Post-commit gold refresh strategy
- Decision: Refresh gold after silver commit and keep failures non-fatal.
- Why: Preserve silver consistency even if scoring layer has transient issues.
- Tradeoff: Gold can briefly lag silver until next successful run/manual refresh.

## 2026-04 � Incremental slice strategy for gold loaders
- Decision: For targeted hashes, fetch silver rows for affected subcategories between `min_date-365` and `max_date`.
- Why: Preserve sufficient historical context while keeping refresh scoped.
- Tradeoff: Loader logic is more complex than full-table recompute.

## 2026-05-17 - Audit trail via trigger; SCD Type 2 on silver rejected
- Decision: Close the silver immutability gap (roadmap §2.1) with `metadata.transaction_audit`
  alone - an immutable change log written by an `AFTER UPDATE/DELETE` trigger on
  `silver.transactions`. Do NOT add SCD Type 2 columns (`version`, `is_current`,
  `valid_from`, `valid_to`).
- Why:
  - The dataset is small and corrections are infrequent. SCD2 imposes a permanent cost -
    a mandatory `WHERE is_current = TRUE` on every silver query, and the loss of the
    `transaction_hash` unique constraint (multiple live versions per hash) - in exchange
    for point-in-time queries that would rarely be run.
  - The audit log answers the real questions ("what changed, when, from what to what")
    with no query-time tax. History is reconstructable by replaying the log.
  - If point-in-time querying is ever genuinely needed, dbt (Phase C) ships native
    `snapshot` SCD2 support - better to adopt it there than hand-roll and later remove it.
- Key design:
  - Trigger fires on UPDATE and DELETE only. INSERTs are not audited - `bronze.transactions_raw`
    plus `created_at`/`created_by` already lineage ingestion.
  - UPDATE logs only the columns that actually changed; a no-op UPDATE logs nothing.
    DELETE logs the entire removed row.
  - Pipeline loads set the `audit.suppress` session flag so routine ingestion writes
    (the category backfill on freshly inserted rows) stay out of the log.
  - Optional `audit.actor` / `audit.reason` session settings annotate manual corrections.
- Tradeoff: A manual `TRUNCATE` of silver bypasses row-level triggers and is not logged;
  backups (§2.9), not the audit trail, guard against catastrophic loss.

## 2026-05-22 - DIY migration runner over Alembic; SQL restructure

- Decision: `scripts/migrate.py` — custom runner, no Alembic. Tracks applied migrations in `metadata.schema_migrations` (filename + SHA-256). Each migration runs in a transaction with `audit.suppress = 'on'`. `--baseline` mode for DBs set up before the runner existed.
- Why: Alembic's value is autogeneration from SQLAlchemy models. This project uses raw psycopg2 — autogenerate doesn't apply. DIY does the same thing with zero new dependencies.
- Rejected: Alembic (revisit if dbt adopted in Phase C); keeping init_scripts as sole schema source (no version tracking, drift already visible in `06_alter_silver_schema.sql`).
- SQL restructure: `init_scripts/` moved to `scripts/sql/init/`; docker-compose mount updated. Groups all SQL under `scripts/sql/`.
- Public/private boundary: all `.sql` remains gitignored (Approach B). Approach C — anonymized seed data for a fully public-bootstrappable repo — is a planned future session.

## Revisit Triggers
- Material score drift observed in dashboard behavior
- Increased data volume impacting refresh performance
- Significant changes to category mapping policy
- Need for stricter consistency guarantees between silver and gold
