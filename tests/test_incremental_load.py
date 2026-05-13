"""
Unit tests for IncrementalDataLoader._load_silver.

Strategy: pure mocks. No database required.

Verifies the deduplication contract — the core of incremental loading:
  - Existing hashes are read from silver
  - Only rows whose hash is NOT already in silver get inserted
  - Silver is never truncated (incremental != initial)
  - rows_skipped_duplicates and rows_loaded_silver are set correctly
  - _new_expense_hashes contains only EXPENSE rows (used to target gold refresh)
  - When all rows are duplicates, no insert is attempted (no crash)
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture
def loader():
    """IncrementalDataLoader with DB connector and transformer mocked out."""
    with patch("loaders.base_loader.get_db_connector") as mock_db, patch(
        "transformers.expense_transformer.ExpenseTransformer"
    ):
        mock_db.return_value = MagicMock()
        from loaders.incremental_load import IncrementalDataLoader

        return IncrementalDataLoader(source="file", file_path="/tmp/dummy.csv")


def _df(rows):
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Deduplication: only new hashes are inserted
# ---------------------------------------------------------------------------
def test_load_silver_skips_existing_hashes(loader):
    """Rows whose hash is already in silver are not inserted."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = [("h_existing",)]

    df = _df([
        {"transaction_hash": "h_existing", "transaction_type": "EXPENSE", "amount_abs_eur": 5.0},
        {"transaction_hash": "h_new",      "transaction_type": "EXPENSE", "amount_abs_eur": 10.0},
    ])

    with patch.object(loader, "_prepare_silver_df", side_effect=lambda d: d), \
         patch.object(loader, "_bulk_insert", return_value=1) as mock_insert:
        loader._load_silver(df, conn)

    # Exactly one row reached _bulk_insert — the new one.
    mock_insert.assert_called_once()
    inserted_df = mock_insert.call_args.args[0]
    assert len(inserted_df) == 1
    assert inserted_df["transaction_hash"].iloc[0] == "h_new"

    assert loader.run_stats["rows_skipped_duplicates"] == 1
    assert loader.run_stats["rows_loaded_silver"] == 1


# ---------------------------------------------------------------------------
# Silver is NEVER truncated by incremental load
# ---------------------------------------------------------------------------
def test_load_silver_never_truncates_or_deletes(loader):
    """Incremental load must only SELECT and INSERT — never TRUNCATE or DELETE."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = []  # silver empty

    df = _df([
        {"transaction_hash": "h1", "transaction_type": "EXPENSE", "amount_abs_eur": 5.0},
    ])

    with patch.object(loader, "_prepare_silver_df", side_effect=lambda d: d), \
         patch.object(loader, "_bulk_insert", return_value=1):
        loader._load_silver(df, conn)

    # Every SQL issued via the cursor must be a SELECT (no destructive verbs).
    for call in cursor.execute.call_args_list:
        sql = call.args[0].upper()
        assert "TRUNCATE" not in sql, f"TRUNCATE found in: {call.args[0]}"
        assert "DELETE"   not in sql, f"DELETE found in: {call.args[0]}"
        assert "DROP"     not in sql, f"DROP found in: {call.args[0]}"


# ---------------------------------------------------------------------------
# All-duplicates path: no insert, no crash
# ---------------------------------------------------------------------------
def test_load_silver_all_duplicates_skips_insert(loader):
    """When every incoming row is a duplicate, _bulk_insert is not called."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = [("h1",), ("h2",)]

    df = _df([
        {"transaction_hash": "h1", "transaction_type": "EXPENSE", "amount_abs_eur": 5.0},
        {"transaction_hash": "h2", "transaction_type": "EXPENSE", "amount_abs_eur": 10.0},
    ])

    with patch.object(loader, "_bulk_insert") as mock_insert:
        loader._load_silver(df, conn)

    mock_insert.assert_not_called()
    assert loader.run_stats["rows_skipped_duplicates"] == 2
    assert loader.run_stats["rows_loaded_silver"] == 0
    assert loader._new_expense_hashes == set()


# ---------------------------------------------------------------------------
# Gold refresh targeting: only EXPENSE hashes are tracked
# ---------------------------------------------------------------------------
def test_load_silver_tracks_only_expense_hashes_for_gold(loader):
    """_new_expense_hashes drives gold refresh and must exclude INCOME rows."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = []

    df = _df([
        {"transaction_hash": "h_exp1", "transaction_type": "EXPENSE", "amount_abs_eur": 10.0},
        {"transaction_hash": "h_inc",  "transaction_type": "INCOME",  "amount_abs_eur": 100.0},
        {"transaction_hash": "h_exp2", "transaction_type": "EXPENSE", "amount_abs_eur": 20.0},
    ])

    with patch.object(loader, "_prepare_silver_df", side_effect=lambda d: d), \
         patch.object(loader, "_bulk_insert", return_value=3):
        loader._load_silver(df, conn)

    # Only the two EXPENSE rows are tracked for gold refresh.
    assert loader._new_expense_hashes == {"h_exp1", "h_exp2"}
