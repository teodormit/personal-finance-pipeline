# Project Memory

Significant decisions — what was decided, why, what was rejected.

---

## 2026-05-25 — Pipeline dockerization (Phase A.5)

- **Decided:** Pipeline packaged as a one-shot Docker image (`python:3.12-slim-bookworm`, non-root UID 1000, `TZ=Europe/Sofia`). Invoked via `docker compose run --rm pipeline python scripts/...`. Postgres remains a separate long-lived service. Subcommand dispatcher exists but verbose `python scripts/...` is the documented primary path.
- **Why:** 30-year reproducibility, Phase B (Prefect) prerequisite, and the qualitative shift from "scripts you run" to "an app you operate." Image is the deployable unit; volume is the data; dumps are the durable artifacts.
- **Key design choices:** Debian release pinned explicitly (`-bookworm`) — the unsuffixed tag silently promotes to trixie. Single-stage build (all deps wheeled). `init: true` in compose instead of `tini` in the image. Host-Python dev path preserved via `POSTGRES_HOST=localhost` in `.env` + override to `postgres` in compose environment.
- **Rejected:** Alpine (no pandas/numpy musl wheels); multi-stage build (no compilation needed today); auto-migrate-on-run (too risky for a 30-year warehouse); internal scheduler (Phase B's job); backup sidecar in scope (Phase A.4); MinIO in compose (removed; re-add when Phase D needs it).
- **Side fixes:** Postgres healthcheck (`-d personal_finance` → `-d ${POSTGRES_DB}`); `.env.template` reference in README → `.env.example`; `.gitattributes` added to lock shell scripts to LF.
- **Baseline applied** to `metadata.schema_migrations`: 001 and 002 marked applied after verifying their effects (income_type column, audit trigger, transaction_audit table) already existed in the live DB.

---

## Session summary — 2026-05-25

**Worked on:** Phase A.5 — pipeline dockerization (the final big foundation-hardening item).

**Completed:**
- New: `Dockerfile`, `.dockerignore`, `docker/pipeline-entrypoint.sh`, `.env.example`, `.gitattributes`.
- Modified: `docker-compose.yml` (added `pipeline` service, fixed healthcheck, removed MinIO); `README.md` ("Run with Docker (recommended)" section).
- Live DB: `metadata.schema_migrations` baselined (2 applied, 0 pending).
- Docs updated: `docs/05_DECISIONS_LOG.md`, `docs/06_CHANGELOG.md`, `docs/08_STRATEGIC_ROADMAP.md`, this file.

**Verified end-to-end:** Image builds (1.13 GB), Postgres healthy, in-container migrate/inspect/run_pipeline all connect via service name `postgres`, **119/119 tests pass inside the container**, BudgetBakers API reachable from container (read-only inspect ran 5,894 silver rows + 105 incoming), container time is Sofia (EEST), runs as `pipeline` UID 1000.

**Decisions made:** All seven dockerization forks resolved in-session (recommended option each time): explicit-only migrations, host-dev path preserved, one-shot containers, backups deferred to A.4, dual schema bootstrap, MinIO removed, dispatcher exists but verbose form is primary. After re-analysis: pinned to `-bookworm`, dropped `tini` in favor of `init: true`, added `TZ=Europe/Sofia`, included `tests/` in image, accepted ~1 GB image with a follow-up to split runtime/dev deps.

**In progress / not done this session:**
- Phase A.3 (CI: pytest + sqlfluff on push).
- Phase A.4 (backup script: nightly pg_dump + offsite rclone).
- Orphan MinIO container (`personal_finance_minio`) still running — can be stopped at convenience.
- Image-size follow-up: split `requirements.txt` into runtime vs dev to drop ~500 MB.
- Nothing has been committed to git yet.

**Next session priorities:**
1. Commit the dockerization work as a single PR (review the diff first; `.env.example`, `.gitattributes`, the four new files, two modified files).
2. Decide whether to tackle Phase A.3 (CI) or A.4 (backups) next, or start Phase B (income gold + account balances) — A.3/A.4 are independent and don't block B.
3. Optional: prune the orphan MinIO container and consider the `requirements.txt` split.

---

## 2026-05-17 — Audit trail: trigger-based log, no SCD Type 2

- **Decided:** `metadata.transaction_audit` written by `AFTER UPDATE/DELETE` trigger on `silver.transactions`. Pipeline sets `audit.suppress` to stay out of the log.
- **Rejected:** SCD Type 2 — imposes `WHERE is_current = TRUE` on every silver query and breaks the `transaction_hash` unique constraint. If point-in-time querying is ever needed, dbt snapshots (Phase C) are the right vehicle.

---

## Session summary — 2026-05-22

**Worked on:** Phase A foundation hardening (final items).

**Completed:**
- `scripts/migrate.py` — DIY migration runner with `metadata.schema_migrations` tracking, SHA-256 checksums, `--baseline` / `--status` / `--dry-run` modes.
- `init_scripts/` relocated to `scripts/sql/init/`; `docker-compose.yml` mount updated.
- All docs updated: `05_DECISIONS_LOG.md`, `06_CHANGELOG.md`, `07_BACKLOG.md`, `CLAUDE.md` (commands + key entry points + SQL init path).
- `MEMORY.md` (this file) created.

**Decisions made:** DIY runner over Alembic (no SQLAlchemy models); SQL boundary stays gitignored (Approach B); anonymized seed data deferred (Approach C, future session).

**In progress / deferred:** Pipeline dockerization — three approaches explained; user needs to read further before deciding. Approach B (volume-mount, dev-friendly) is the recommendation. Backlog item added to `07_BACKLOG.md`.

**Next session priorities:**
1. Dockerize the pipeline (Phase A §3.3) — user to read up on Docker service networking and volume mounts first.
2. After dockerization: Phase A is fully complete → start Phase B (income gold models).

---

## 2026-05-22 — DIY migration runner, no Alembic

- **Decided:** `scripts/migrate.py` tracks applied migrations in `metadata.schema_migrations` (filename + SHA-256). Files live in `scripts/sql/migrations/`. Init scripts moved to `scripts/sql/init/`.
- **Rejected:** Alembic — its autogenerate feature requires SQLAlchemy models; this project uses raw psycopg2, so Alembic adds complexity with no benefit. Revisit if dbt is adopted in Phase C.

## 2026-05-22 — Public/private SQL boundary: status quo (Approach B)

- **Decided:** All `.sql` files remain gitignored. Migration runner is local-only. Public repo ships only `docs/postgres_init_blueprint.sql` as documentation.
- **Rejected (deferred):** Approach C — anonymized seed data for a fully public-bootstrappable repo. Highest portfolio value but requires a dedicated session. Planned for the future.
