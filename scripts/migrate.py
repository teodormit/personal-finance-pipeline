"""
DIY Schema Migration Runner
===========================

Applies idempotent, content-checksummed SQL migrations to the warehouse.

Why DIY (not Alembic):
  No SQLAlchemy models in this project — Alembic's autogenerate feature would
  not apply, so it would reduce to "run these SQL files in order," which is
  exactly what this does in ~150 lines with zero new dependencies. See
  docs/05_DECISIONS_LOG.md.

Conventions:
  - Migration files live in scripts/sql/migrations/ named NNN_short_name.sql
    (three or more digits, zero-padded, lowercase snake_case suffix).
  - Files are applied in filename-sort order.
  - Migration files must NOT contain explicit BEGIN/COMMIT — the runner
    wraps each file in a single transaction.
  - Every applied file is recorded in metadata.schema_migrations together
    with a SHA-256 of its contents. Editing an already-applied file is a
    hard error: the runner refuses to proceed until the discrepancy is
    resolved (revert the edit, or write a new migration that supersedes it).
  - Each migration runs with audit.suppress = 'on' so schema backfills do
    not pollute the silver change-audit log.

Usage:
  python scripts/migrate.py                 # apply all pending migrations
  python scripts/migrate.py --status        # list every migration and status
  python scripts/migrate.py --dry-run       # show pending migrations, do nothing
  python scripts/migrate.py --baseline      # mark on-disk migrations as already
                                            # applied WITHOUT running them
                                            # (use ONCE per database that was
                                            #  set up before this runner existed)
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))

from utils.db_connector import get_db_connector  # noqa: E402

MIGRATIONS_DIR = _project_root / "scripts" / "sql" / "migrations"
FILENAME_PATTERN = re.compile(r"^(\d{3,})_[a-z0-9_]+\.sql$")

TRACKER_DDL = """
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.schema_migrations (
    filename     TEXT        PRIMARY KEY,
    checksum     TEXT        NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_by   TEXT        NOT NULL DEFAULT current_user
);
"""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _discover_migrations() -> list[Path]:
    if not MIGRATIONS_DIR.exists():
        print(f"Migrations directory does not exist: {MIGRATIONS_DIR}")
        return []
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    bad = [f.name for f in files if not FILENAME_PATTERN.match(f.name)]
    if bad:
        raise RuntimeError(
            "Migration filenames must match NNN_short_name.sql "
            f"(three+ digits, snake_case). Offending files: {bad}"
        )
    return files


def _ensure_tracker(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(TRACKER_DDL)
    conn.commit()


def _applied_state(conn) -> dict[str, str]:
    """Return {filename: stored_checksum} for already-applied migrations."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT filename, checksum FROM metadata.schema_migrations"
        )
        return dict(cur.fetchall())


def _verify_unchanged(files: list[Path], applied: dict[str, str]) -> None:
    """Refuse to proceed if any already-applied file was edited on disk."""
    mismatches = []
    for f in files:
        stored = applied.get(f.name)
        if stored is None:
            continue
        on_disk = _sha256(f)
        if on_disk != stored:
            mismatches.append((f.name, stored[:12], on_disk[:12]))
    if mismatches:
        lines = "\n".join(
            f"  {name}: stored={stored}... on_disk={current}..."
            for name, stored, current in mismatches
        )
        raise RuntimeError(
            "Refusing to proceed — already-applied migrations have been "
            f"edited on disk:\n{lines}\n"
            "Either revert the edits, or write a new migration that "
            "supersedes them."
        )


def _apply_one(conn, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    checksum = _sha256(path)
    with conn.cursor() as cur:
        cur.execute("SET LOCAL audit.suppress = 'on'")
        cur.execute(sql)
        cur.execute(
            "INSERT INTO metadata.schema_migrations (filename, checksum) "
            "VALUES (%s, %s)",
            (path.name, checksum),
        )
    conn.commit()
    print(f"  applied: {path.name}")


def _baseline(conn, files: list[Path], applied: dict[str, str]) -> int:
    pending = [f for f in files if f.name not in applied]
    if not pending:
        print("Nothing to baseline — all on-disk migrations are already "
              "registered.")
        return 0

    print(f"BASELINE MODE: registering {len(pending)} file(s) as already "
          "applied (without running them):")
    for f in pending:
        print(f"  {f.name}")
    print("This is correct only if these changes are already present in the "
          "target database.")

    rows = [(f.name, _sha256(f)) for f in pending]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO metadata.schema_migrations (filename, checksum) "
            "VALUES (%s, %s)",
            rows,
        )
    conn.commit()
    return len(pending)


def _print_status(files: list[Path], applied: dict[str, str]) -> None:
    print(f"Migrations in {MIGRATIONS_DIR}:")
    for f in files:
        flag = "applied" if f.name in applied else "PENDING"
        print(f"  [{flag}] {f.name}")
    pending = sum(1 for f in files if f.name not in applied)
    print(f"\n{len(applied)} applied, {pending} pending.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="DIY schema migration runner for the finance warehouse."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--status",
        action="store_true",
        help="List every migration on disk and whether it has been applied.",
    )
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending migrations without applying them.",
    )
    group.add_argument(
        "--baseline",
        action="store_true",
        help=(
            "Register on-disk migrations as already applied WITHOUT running "
            "them. Intended for one-time use on a database that was set up "
            "before this runner existed."
        ),
    )
    args = parser.parse_args()

    files = _discover_migrations()
    if not files:
        print("No migration files found.")
        return 0

    db = get_db_connector()
    with db.connect() as conn:
        _ensure_tracker(conn)
        applied = _applied_state(conn)

        if args.status:
            _print_status(files, applied)
            return 0

        if args.baseline:
            count = _baseline(conn, files, applied)
            print(f"Baseline complete: {count} migration(s) registered.")
            return 0

        _verify_unchanged(files, applied)

        pending = [f for f in files if f.name not in applied]
        if not pending:
            print("Database is up to date — no pending migrations.")
            return 0

        if args.dry_run:
            print(f"{len(pending)} pending migration(s):")
            for f in pending:
                print(f"  {f.name}")
            return 0

        print(f"Applying {len(pending)} pending migration(s)...")
        for f in pending:
            _apply_one(conn, f)
        print("Done.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
