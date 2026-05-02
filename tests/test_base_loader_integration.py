"""
Integration tests for src/loaders/base_loader.py against a real PostgreSQL.

Strategy: every test runs inside a transaction that is rolled back at
fixture teardown. No commits ever happen, so the user's populated DB is
not modified — even on test failure.

Tests skip cleanly if Postgres isn't reachable (so the suite still runs
in environments without the database).
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
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
        # Probe the connection so we fail fast rather than mid-test.
        with connection.connect() as conn:
            conn.cursor().execute("SELECT 1")
    except Exception as e:
        pytest.skip(f"Postgres unavailable: {e}")
    return connection


@pytest.fixture
def conn(db):
    """One connection per test, rolled back at teardown."""
    c = db.connect()
    try:
        yield c
    finally:
        c.rollback()
        c.close()


@pytest.fixture
def loader(db):
    """A BaseLoader instance with the live DatabaseConnection.

    The methods we test all accept a `conn` param and don't open their own,
    so the loader's `self.db` is irrelevant for transaction scope —
    the test-managed `conn` controls rollback.
    """
    from loaders.base_loader import BaseLoader

    bl = BaseLoader(source_file_name="integration_test.csv")
    bl.db = db  # ensure same DB (defensive — already true)
    return bl


def _sample_transformed_df(n: int = 2) -> pd.DataFrame:
    """Build a small DataFrame shaped like ExpenseTransformer's output.

    Real pipeline output has exactly one of {description, note} (depending
    on extraction path). We use `description` here, matching the API path —
    staging will rename it to `note`, bronze keeps it as `description`,
    silver keeps it as `description`.
    """
    base_date = datetime(2099, 1, 1)  # far-future date so it can't collide
    rows = []
    for i in range(n):
        h = f"integration-test-{uuid.uuid4().hex[:16]}-{i}"
        rows.append(
            {
                "transaction_hash": h,
                "date": base_date,
                "transaction_type": "EXPENSE",
                "type": "EXPENSE",
                "amount": -10.0 - i,
                "amount_abs": 10.0 + i,
                "currency": "EUR",
                "amount_eur": -10.0 - i,
                "amount_abs_eur": 10.0 + i,
                "eur_conversion_rate": 1.0,
                "amount_bgn": -19.56,
                "amount_abs_bgn": 19.56,
                "source_record_id": f"rec-{i}",
                "category_id": None,
                "description": f"integration test row {i}",
                "payee": "TestPayee",
                "subcategory": "Food",
                "account": "Cash in Euro",
                "payment_method": "card",
                "labels": "",
                "year": 2099,
                "month": 1,
                "quarter": 1,
                "year_month": "2099-01",
                "day_of_week": 4,
                "week_of_year": 1,
                "is_weekend": False,
                "classification": "NEED",
            }
        )
    return pd.DataFrame(rows)


def _count(conn, sql: str) -> int:
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# _bulk_insert
# ---------------------------------------------------------------------------
def test_bulk_insert_writes_rows_within_transaction(loader, conn):
    """Rows inserted via _bulk_insert are visible in the same transaction."""
    # Use staging.raw_transactions because TRUNCATE of it is what we already do
    # in production — and rollback undoes both the truncate and the insert.
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE staging.raw_transactions;")

    df = pd.DataFrame(
        [
            {
                "date": datetime(2099, 1, 1),
                "note": "row 1",
                "type": "EXPENSE",
                "payee": "p",
                "amount": -1.0,
                "labels": "",
                "account": "Cash in Euro",
                "category": "Food",
                "currency": "EUR",
                "payment": "card",
                "source_file": "test",
                "batch_id": str(uuid.uuid4()),
                "loaded_at": datetime.now(),
                "source_row_number": 1,
            }
        ]
    )
    n = loader._bulk_insert(df, "staging", "raw_transactions", conn)
    assert n == 1

    count = _count(conn, "SELECT COUNT(*) FROM staging.raw_transactions")
    assert count == 1


def test_bulk_insert_rollback_undoes_writes(db):
    """After rollback the rows are gone — proves the test isolation works."""
    # Stage 1: do an insert in connection A and roll back.
    conn_a = db.connect()
    cursor = conn_a.cursor()
    cursor.execute("SELECT COUNT(*) FROM staging.raw_transactions")
    initial_count = cursor.fetchone()[0]

    cursor.execute("TRUNCATE TABLE staging.raw_transactions;")
    cursor.execute(
        """
        INSERT INTO staging.raw_transactions
            ("date", "note", "type", "payee", "amount", "currency",
             "source_file", "batch_id", "loaded_at", "source_row_number")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            datetime(2099, 1, 1),
            "rollback test",
            "EXPENSE",
            "p",
            -1.0,
            "EUR",
            "test",
            str(uuid.uuid4()),
            datetime.now(),
            1,
        ),
    )
    conn_a.rollback()
    conn_a.close()

    # Stage 2: open connection B and confirm staging is back to its initial state.
    conn_b = db.connect()
    try:
        cursor_b = conn_b.cursor()
        cursor_b.execute("SELECT COUNT(*) FROM staging.raw_transactions")
        assert cursor_b.fetchone()[0] == initial_count
    finally:
        conn_b.close()


# ---------------------------------------------------------------------------
# _load_staging
# ---------------------------------------------------------------------------
def test_load_staging_truncates_then_inserts(loader, conn):
    df = _sample_transformed_df(n=3)
    loader._load_staging(df, conn)

    count = _count(conn, "SELECT COUNT(*) FROM staging.raw_transactions")
    assert count == 3
    assert loader.run_stats["rows_staged"] == 3

    # Verify a couple of column renames worked correctly.
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT note, category, payment, source_file
        FROM staging.raw_transactions
        ORDER BY source_row_number
        """
    )
    rows = cursor.fetchall()
    assert len(rows) == 3
    notes = [r[0] for r in rows]
    categories = [r[1] for r in rows]
    payments = [r[2] for r in rows]
    source_files = [r[3] for r in rows]

    assert all("integration test row" in n for n in notes)
    assert all(c == "Food" for c in categories)
    assert all(p == "card" for p in payments)
    assert all(s == "integration_test.csv" for s in source_files)


# ---------------------------------------------------------------------------
# _load_bronze
# ---------------------------------------------------------------------------
def test_load_bronze_appends_without_truncating(loader, conn):
    before = _count(conn, "SELECT COUNT(*) FROM bronze.transactions_raw")

    df = _sample_transformed_df(n=2)
    loader._load_bronze(df, conn)

    after = _count(conn, "SELECT COUNT(*) FROM bronze.transactions_raw")
    assert after == before + 2
    assert loader.run_stats["rows_loaded_bronze"] == 2

    # Verify that our inserted rows landed with the right shape.
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT transaction_date, description, account_name, payment_method,
               ingestion_batch_id, has_quality_issues
        FROM bronze.transactions_raw
        WHERE ingestion_batch_id = %s
        """,
        (str(loader.batch_id),),
    )
    rows = cursor.fetchall()
    assert len(rows) == 2
    for r in rows:
        assert r[2] == "Cash in Euro"  # account → account_name rename
        assert r[3] == "card"  # payment_method passthrough
        assert r[5] is False  # has_quality_issues default


# ---------------------------------------------------------------------------
# _update_category_mapping
# ---------------------------------------------------------------------------
def test_update_category_mapping_backfills_known_subcategory(loader, conn):
    """Insert a synthetic silver row with NULL category and a known subcategory.

    After running _update_category_mapping the row should have category and
    classification populated from silver.category_mapping. Then rollback
    discards the synthetic row so the user's data is untouched.
    """
    cursor = conn.cursor()

    # 1. Pick any subcategory that already has a mapping.
    cursor.execute(
        """
        SELECT subcategory, category, classification
        FROM silver.category_mapping
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if row is None:
        pytest.skip("silver.category_mapping is empty — nothing to backfill against")
    known_sub, expected_cat, expected_cls = row

    # 2. Insert a minimal silver row with the known subcategory but NULL category.
    test_hash = f"integration-cat-{uuid.uuid4().hex[:16]}"
    cursor.execute(
        """
        INSERT INTO silver.transactions
            ("transaction_hash", "transaction_date", "transaction_type",
             "amount", "amount_abs", "currency", "subcategory",
             "category", "classification",
             "year", "month", "quarter", "year_month",
             "day_of_week", "week_of_year", "is_weekend",
             "created_at", "created_by")
        VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL,
                %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            test_hash,
            datetime(2099, 1, 1),
            "EXPENSE",
            -1.0,
            1.0,
            "EUR",
            known_sub,
            2099, 1, 1, "2099-01", 4, 1, False,
            datetime.now(),
            "integration_test",
        ),
    )

    # 3. Run the helper (caller-managed transaction; no commit).
    loader._update_category_mapping(conn)

    # 4. Verify our row got backfilled.
    cursor.execute(
        "SELECT category, classification FROM silver.transactions "
        "WHERE transaction_hash = %s",
        (test_hash,),
    )
    cat, cls = cursor.fetchone()
    assert cat == expected_cat
    assert cls == expected_cls


def test_update_category_mapping_overrides_specific_income(loader, conn):
    """The second UPDATE statement re-classifies certain INCOME subcategories."""
    cursor = conn.cursor()
    test_hash = f"integration-inc-{uuid.uuid4().hex[:16]}"
    cursor.execute(
        """
        INSERT INTO silver.transactions
            ("transaction_hash", "transaction_date", "transaction_type",
             "amount", "amount_abs", "currency", "subcategory",
             "category", "classification",
             "year", "month", "quarter", "year_month",
             "day_of_week", "week_of_year", "is_weekend",
             "created_at", "created_by")
        VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL,
                %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            test_hash,
            datetime(2099, 1, 1),
            "INCOME",
            100.0,
            100.0,
            "EUR",
            "Child Support",
            2099, 1, 1, "2099-01", 4, 1, False,
            datetime.now(),
            "integration_test",
        ),
    )

    loader._update_category_mapping(conn)

    cursor.execute(
        "SELECT category, classification FROM silver.transactions "
        "WHERE transaction_hash = %s",
        (test_hash,),
    )
    cat, cls = cursor.fetchone()
    assert cat == "Income"
    assert cls == "WANT"
