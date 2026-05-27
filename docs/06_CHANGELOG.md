# Changelog

## 2026-05-27

### Added
- `scripts/backup.ps1` — rclone Google Drive upload block after each successful `pg_dump`. Remote `gdrive:Finance Backups/` receives a copy of every weekly dump. Upload is non-fatal: rclone failure prints a WARNING and does not block the local backup.
- `docs/04_RUNBOOK.md` — "Backup & Offsite Copy" section covering the automated schedule, one-time rclone setup steps, verification commands, and restore procedure.

### Notes
- Closes Phase A item 4 (§2.9 offsite backup). Phase A is now complete.
- rclone setup is a one-time interactive step on the host (`rclone config`). No GCP project or credential files required — rclone's built-in OAuth app handles Drive authorization.
- `sqlfluff` CI linting (Phase A item 3 partial) was explicitly dropped from Phase A scope — no concrete SQL linting rules defined yet.

## 2026-05-25

### Added
- `Dockerfile` — single-stage runtime image on `python:3.12-slim-bookworm`, non-root `pipeline` user (UID 1000), `TZ=Europe/Sofia`, layered for build cache efficiency.
- `.dockerignore` — keeps secrets, virtualenvs, raw data, and backups out of the build context; `tests/` deliberately included so in-container `pytest` works.
- `docker/pipeline-entrypoint.sh` — subcommand dispatcher (`pipeline` / `migrate` / `inspect` / `inspect-api` / `shell` / `bash` / `sh` / `python` / `pytest`). The verbose form (`python scripts/...`) is the documented primary path; the dispatcher is a convenience layer.
- `.env.example` — committable template for first-time setup on a new machine.
- `.gitattributes` — locks `*.sh`, `Dockerfile`, `docker-compose.yml` to LF endings so Windows checkouts don't break the container shebang.
- `pipeline` service in `docker-compose.yml` — one-shot ETL runner, builds from `./Dockerfile`, joins `data_platform_network`, depends on Postgres healthy. Bind-mounts `./data`, `./logs`, `./backups`. Overrides `POSTGRES_HOST=postgres` while leaving `.env` set to `localhost` (preserves the host-Python dev path).
- README "Run with Docker (recommended)" section documenting first-time setup, daily incremental, common subcommands, and the host-dev fallback.

### Changed
- Postgres healthcheck fixed: was `-d personal_finance` (DB doesn't exist), now `-d ${POSTGRES_DB}`. Without this fix, `depends_on: service_healthy` for the pipeline service would never trigger.
- MinIO service and its `minio_data` volume removed from `docker-compose.yml` (unused by the pipeline; the on-disk Docker volume persists until manually pruned).
- `.env.template` reference in README replaced with `.env.example`.

### Notes
- `metadata.schema_migrations` baselined: 001 and 002 registered as applied (the live DB already had `income_type`, the audit trigger, and `metadata.transaction_audit` — verified before recording).
- Image lands at ~1.13 GB. Follow-up: split `requirements.txt` into runtime vs dev (move `jupyter`, `ipykernel`, `pytest`, `pytest-cov` to `requirements-dev.txt`) to drop the image to ~600 MB.
- Orphan `personal_finance_minio` container still running from before MinIO was removed; can be stopped with `docker stop personal_finance_minio && docker rm personal_finance_minio` when convenient.

## 2026-05-22

### Added
- `scripts/migrate.py` — DIY schema migration runner. Tracks applied migrations in `metadata.schema_migrations` with SHA-256 checksums. Supports `--status`, `--dry-run`, `--baseline`. On first use run `--baseline` to register the two existing migrations without re-running them.

### Changed
- `init_scripts/` relocated to `scripts/sql/init/`. Docker Compose mount updated from `./init_scripts` to `./scripts/sql/init`.

### Notes
- Anonymized seed data for a public-bootstrappable repo (Approach C) is a planned future session.

## 2026-05-17

### Added
- `metadata.transaction_audit` change log, populated by the `trg_audit_transaction`
  trigger (`AFTER UPDATE/DELETE` on `silver.transactions`). Records before/after
  values for every out-of-band correction, including manual SQL edits.
- Migration `scripts/sql/migrations/002_add_transaction_audit.sql`.
- `tests/test_transaction_audit.py` — integration tests for the audit trigger.

### Changed
- Pipeline loads now set the `audit.suppress` session flag so routine ingestion
  writes are kept out of the audit log.

### Notes
- SCD Type 2 on silver was evaluated and deliberately rejected — see
  `docs/05_DECISIONS_LOG.md` (2026-05-17). The audit log closes the §2.1
  immutability gap without versioned-row query complexity.
- Run migration 002 once against the existing database to activate the feature.

## 2026-05-01

### Changed
- Consolidated standalone gold refresh scripts into `scripts/run_pipeline.py` via the new `--refresh-gold {notability,save-potential,both}` flag. `--window-days` carried over (default 365).
- Removed `scripts/refresh_gold_notability.py` and `scripts/refresh_gold_save_potential.py`.

### Removed
- `scripts/compare_datasets.py`, `scripts/deep_analysis.py`, `scripts/inspect_wallet_export.py` moved to `scripts/archive/` (BGN→EUR migration-era utilities, kept for git history only).

## 2026-04-30

### Added
- Gold scoring table and refresh flow for transaction notability.
- Gold scoring table and refresh flow for transaction save potential.
- Standalone full-refresh scripts:
  - `scripts/refresh_gold_notability.py`
  - `scripts/refresh_gold_save_potential.py`
- Unit test coverage for both scoring transformers.
- Project knowledge-base docs under `docs/`.

### Changed
- `scripts/run_pipeline.py` preflight includes `gold` schema requirement.
- `initial_load.py` now triggers full gold refreshes after silver load.
- `incremental_load.py` now triggers incremental gold refreshes after silver commit.

### Notes
- Incremental runs upsert gold only for newly inserted expense hashes.
- Full refresh scripts should be used after major remaps/corrections.
