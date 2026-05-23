# Project Memory

Significant decisions — what was decided, why, what was rejected.

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
