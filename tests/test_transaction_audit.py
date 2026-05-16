"""
Integration tests for the metadata.transaction_audit change log.

These verify the AFTER UPDATE/DELETE trigger on silver.transactions
(trg_audit_transaction / metadata.fn_audit_transaction_change).

Strategy: every test runs inside a transaction that is rolled back at
fixture teardown. No commits ever happen, so the user's populated DB is
not modified — even on test failure.

Tests skip cleanly if Postgres isn't reachable, or if migration 002 has
not been applied (trigger absent), so the suite still runs everywhere.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Skip the whole module if Postgres / db_connector isn't usable.
psycopg2 = pytest.importorskip("psycopg2")

from utils.db_connector import DatabaseConnection  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def db():
    """Live DatabaseConnection. Skips the module if the DB is unreachable."""
    try:
        connection = DatabaseConnection()
        with connection.connect() as conn:
            conn.cursor().execute("SELECT 1")
    except Exception as e:
        pytest.skip(f"Postgres unavailable: {e}")
    return connection


@pytest.fixture
def conn(db):
    """One connection per test, rolled back at teardown.

    Skips the test if the audit trigger is missing (migration 002 not run),
    so a database that predates the audit feature doesn't fail the suite.
    """
    c = db.connect()
    cursor = c.cursor()
    cursor.execute(
        """
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_audit_transaction' AND NOT tgisinternal
        """
    )
    if cursor.fetchone() is None:
        c.rollback()
        c.close()
        pytest.skip(
            "trg_audit_transaction not found — run "
            "scripts/sql/migrations/002_add_transaction_audit.sql"
        )
    try:
        yield c
    finally:
        c.rollback()
        c.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _insert_silver_row(cursor, subcategory: str = "Food & Drinks") -> str:
    """Insert one minimal silver.transactions row; return its transaction_hash.

    Far-future date so it can never collide with real data. The caller's
    transaction is rolled back at teardown, so this row never persists.
    """
    test_hash = f"audit-test-{uuid.uuid4().hex[:20]}"
    cursor.execute(
        """
        INSERT INTO silver.transactions
            ("transaction_hash", "transaction_date", "transaction_type",
             "amount", "amount_abs", "currency", "subcategory",
             "year", "month", "quarter", "year_month",
             "day_of_week", "week_of_year", "is_weekend",
             "created_at", "created_by")
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            test_hash,
            datetime(2099, 1, 1),
            "EXPENSE",
            -12.50,
            12.50,
            "EUR",
            subcategory,
            2099, 1, 1, "2099-01", 4, 1, False,
            datetime.now(),
            "audit_test",
        ),
    )
    return test_hash


def _audit_rows(cursor, test_hash: str) -> list:
    """Return all audit rows for a transaction_hash, newest first."""
    cursor.execute(
        """
        SELECT change_type, changed_fields, old_values, new_values,
               changed_by, change_reason
        FROM metadata.transaction_audit
        WHERE transaction_hash = %s
        ORDER BY audit_id DESC
        """,
        (test_hash,),
    )
    return cursor.fetchall()


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------
def test_update_logs_changed_field(conn):
    """A real UPDATE logs one audit row with only the changed column."""
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor, subcategory="Groceries")

    cursor.execute(
        "UPDATE silver.transactions SET subcategory = %s WHERE transaction_hash = %s",
        ("Bar, cafe", test_hash),
    )

    rows = _audit_rows(cursor, test_hash)
    assert len(rows) == 1
    change_type, changed_fields, old_values, new_values, _, _ = rows[0]
    assert change_type == "UPDATE"
    assert changed_fields == ["subcategory"]
    assert old_values == {"subcategory": "Groceries"}
    assert new_values == {"subcategory": "Bar, cafe"}


def test_update_logs_multiple_changed_fields(conn):
    """An UPDATE touching several columns records each one."""
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor)

    cursor.execute(
        """
        UPDATE silver.transactions
        SET amount = %s, amount_abs = %s, payee = %s
        WHERE transaction_hash = %s
        """,
        (-99.00, 99.00, "Corrected Payee", test_hash),
    )

    rows = _audit_rows(cursor, test_hash)
    assert len(rows) == 1
    _, changed_fields, old_values, new_values, _, _ = rows[0]
    assert set(changed_fields) == {"amount", "amount_abs", "payee"}
    assert new_values["payee"] == "Corrected Payee"
    assert old_values["payee"] is None


def test_noop_update_logs_nothing(conn):
    """An UPDATE that changes no value writes no audit row."""
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor)

    cursor.execute(
        "UPDATE silver.transactions SET subcategory = subcategory "
        "WHERE transaction_hash = %s",
        (test_hash,),
    )

    assert _audit_rows(cursor, test_hash) == []


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------
def test_delete_logs_full_row(conn):
    """A DELETE logs the entire removed row in old_values; new_values is NULL."""
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor, subcategory="Taxi")

    cursor.execute(
        "DELETE FROM silver.transactions WHERE transaction_hash = %s",
        (test_hash,),
    )

    rows = _audit_rows(cursor, test_hash)
    assert len(rows) == 1
    change_type, changed_fields, old_values, new_values, _, _ = rows[0]
    assert change_type == "DELETE"
    assert changed_fields is None
    assert new_values is None
    assert old_values["transaction_hash"] == test_hash
    assert old_values["subcategory"] == "Taxi"


# ---------------------------------------------------------------------------
# Suppression — the pipeline's own writes
# ---------------------------------------------------------------------------
def test_suppressed_update_logs_nothing(conn):
    """With audit.suppress = 'on', an UPDATE writes no audit row.

    This is the flag the ETL pipeline sets so routine loads don't pollute
    the change log.
    """
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor)

    cursor.execute("SET LOCAL audit.suppress = 'on'")
    cursor.execute(
        "UPDATE silver.transactions SET payee = %s WHERE transaction_hash = %s",
        ("Should Not Be Logged", test_hash),
    )

    assert _audit_rows(cursor, test_hash) == []


# ---------------------------------------------------------------------------
# Actor / reason attribution
# ---------------------------------------------------------------------------
def test_actor_and_reason_session_settings(conn):
    """audit.actor / audit.reason settings annotate the audit row."""
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor)

    cursor.execute("SET LOCAL audit.actor = 'pytest'")
    cursor.execute("SET LOCAL audit.reason = 'reclassified for test'")
    cursor.execute(
        "UPDATE silver.transactions SET category = %s WHERE transaction_hash = %s",
        ("Food & Drinks", test_hash),
    )

    rows = _audit_rows(cursor, test_hash)
    assert len(rows) == 1
    _, _, _, _, changed_by, change_reason = rows[0]
    assert changed_by == "pytest"
    assert change_reason == "reclassified for test"


def test_changed_by_defaults_to_db_role(conn):
    """Without audit.actor set, changed_by falls back to the database role."""
    cursor = conn.cursor()
    test_hash = _insert_silver_row(cursor)

    cursor.execute(
        "UPDATE silver.transactions SET payee = %s WHERE transaction_hash = %s",
        ("role default test", test_hash),
    )

    rows = _audit_rows(cursor, test_hash)
    assert len(rows) == 1
    changed_by = rows[0][4]
    assert changed_by is not None and changed_by != ""
