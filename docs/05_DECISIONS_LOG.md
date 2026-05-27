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

## 2026-05-25 - Pipeline dockerization (Phase A §3.3, §5 Phase A.5)

- Decision: Package the Python pipeline as a one-shot Docker image, joined to the existing Postgres service via `docker-compose.yml`. Single image hosts `run_pipeline.py`, `migrate.py`, and the inspect tools; subcommand dispatcher in `docker/pipeline-entrypoint.sh` for ergonomics, but the verbose form (`docker compose run --rm pipeline python scripts/...`) is the documented primary path.
- Why:
  - Reproducibility: a 30-year warehouse cannot depend on a specific Python install on a specific laptop.
  - Phase B prerequisite: Prefect/Dagster expects a containerized job to schedule.
  - Portfolio signal: `docker compose run pipeline ...` is the qualitative shift from "scripts you run" to "an app you operate."
- Key design choices (with reasoning):
  - **Base image `python:3.12-slim-bookworm`** — Debian release pinned explicitly so the unsuffixed tag's silent promotion to trixie cannot break us. All deps (`pandas`, `numpy`, `psycopg2-binary`, `prefect`, `pydantic`) ship manylinux glibc wheels — zero compilation. Alpine rejected: musl has no pandas/numpy wheels, forcing a 5–10 min source build per image.
  - **Single-stage build** — every dep is wheeled today; multi-stage would save ~10 MB. Defer until a future dep needs source compilation.
  - **Non-root user `pipeline` UID 1000** — matches typical Linux dev UID for clean bind-mount ownership; transparent on Windows Docker Desktop.
  - **`init: true` on the compose service, no `tini` in the image** — Docker's built-in init handles SIGTERM forwarding without bloating the image.
  - **`TZ=Europe/Sofia`** — real correctness fix, not cosmetic: `run_pipeline.py:161` uses naive `datetime.now()`; without TZ alignment, the container would compute "today" in UTC, diverging from the host path near midnight.
  - **Schema bootstrap stays dual**: init scripts run on first Postgres container init, migrations apply on top. Migrations-only consolidation considered and deferred.
  - **Host-based dev path preserved** — `.env` keeps `POSTGRES_HOST=localhost`; compose overrides to `postgres` for the in-container run. Same code, two valid execution paths.
  - **Pipeline is one-shot** — `docker compose run --rm pipeline ...`; no internal scheduler. Cron / Prefect plug in later (Phase B).
- Rejected:
  - Alpine base — pandas/numpy wheel incompatibility.
  - `tini` in image — replaced by `init: true` in compose; one less thing in the image.
  - Auto-migrate on every pipeline run — too risky for a 30-year warehouse; migrations stay explicit.
  - Internal scheduler in the container — Phase B's orchestration is the right vehicle.
  - Backup sidecar in this scope — backups are Phase A.4, kept separate.
  - MinIO in compose — removed entirely; unused by the pipeline today, re-add when Phase D needs object storage. The `personal-finance-pipeline_minio_data` volume persists until manually pruned.
- Tradeoffs:
  - Image is ~1.1 GB due to `jupyter`/`ipykernel`/`prefect` bulk. Acceptable for now; follow-up will split `requirements.txt` into runtime vs dev to drop it to ~600 MB.
  - Postgres healthcheck bug fixed in passing (`-d personal_finance` → `-d ${POSTGRES_DB}`) — required for `depends_on: service_healthy` to actually trigger.
- Verified end-to-end:
  - Image build clean (1.13 GB), Postgres healthy, in-container `migrate --status` connects via service name, 119/119 tests pass inside the container, `inspect_incremental_load.py` reads 5,894 live silver rows and pulls 105 from the BudgetBakers API with no writes, container time = `EEST` (Sofia), runs as UID 1000.
- Baseline applied: `metadata.schema_migrations` registered 001 and 002 as applied (verified that `silver.transactions.income_type`, `trg_audit_transaction`, and `metadata.transaction_audit` already existed in the live DB before recording).

## 2026-05-22 - DIY migration runner over Alembic; SQL restructure

- Decision: `scripts/migrate.py` — custom runner, no Alembic. Tracks applied migrations in `metadata.schema_migrations` (filename + SHA-256). Each migration runs in a transaction with `audit.suppress = 'on'`. `--baseline` mode for DBs set up before the runner existed.
- Why: Alembic's value is autogeneration from SQLAlchemy models. This project uses raw psycopg2 — autogenerate doesn't apply. DIY does the same thing with zero new dependencies.
- Rejected: Alembic (revisit if dbt adopted in Phase C); keeping init_scripts as sole schema source (no version tracking, drift already visible in `06_alter_silver_schema.sql`).
- SQL restructure: `init_scripts/` moved to `scripts/sql/init/`; docker-compose mount updated. Groups all SQL under `scripts/sql/`.
- Public/private boundary: all `.sql` remains gitignored (Approach B). Approach C — anonymized seed data for a fully public-bootstrappable repo — is a planned future session.

## 2026-05-27 - rclone for Google Drive offsite backup (Phase A §2.9)

- Decision: Add Google Drive offsite copy to `scripts/backup.ps1` via rclone. Remote named `gdrive`, destination folder `Finance Backups/`. Upload is non-fatal — a rclone failure prints a WARNING and exits 0 so the local backup is never blocked by a Drive outage.
- Why: Closes the single remaining open item in Phase A. rclone was chosen over the Python Drive API (google-api-python-client) because it requires no GCP project, no OAuth credential management, and no new Python dependencies — the Drive remote is configured once with `rclone config` using rclone's built-in OAuth app. The entire integration is one `rclone copy` line in the existing PowerShell script.
- Rejected:
  - `google-api-python-client` with service account — requires GCP project creation, API enablement, and a JSON key file to manage.
  - `google-api-python-client` with personal OAuth — same GCP overhead plus refresh-token expiry risk after 6 months of inactivity.
- Tradeoff: rclone is an external binary dependency. If rclone is not installed or the `gdrive` remote is not configured, the upload step fails silently with a WARNING (local backup remains intact). Setup instructions in `docs/04_RUNBOOK.md §Backup`.

## Revisit Triggers
- Material score drift observed in dashboard behavior
- Increased data volume impacting refresh performance
- Significant changes to category mapping policy
- Need for stricter consistency guarantees between silver and gold
