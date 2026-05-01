"""
Unit tests for src/loaders/base_loader.py BaseLoader helpers.

Strategy: pure unit tests with mocks. No database required.
Verifies the SQL strings, value lists, and side effects of:
  - run_stats initialization
  - _log_pipeline_run
  - _display_summary
  - _bulk_insert (both with and without external conn)
  - _update_category_mapping
  - _refresh_gold_notability / _refresh_gold_save_potential (non-fatal wrappers)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture
def loader():
    """Build a BaseLoader with the DB connector and ExpenseTransformer mocked out."""
    with patch("loaders.base_loader.get_db_connector") as mock_db, patch(
        "transformers.expense_transformer.ExpenseTransformer"
    ) as mock_tx:
        mock_db.return_value = MagicMock()
        mock_tx.return_value = MagicMock()
        from loaders.base_loader import BaseLoader

        bl = BaseLoader(source_file_name="unit_test_source.xlsx")
    return bl


# ---------------------------------------------------------------------------
# run_stats init
# ---------------------------------------------------------------------------
def test_run_stats_has_expected_keys(loader):
    expected = {
        "run_id",
        "start_time",
        "source_file",
        "file_size_bytes",
        "rows_extracted",
        "rows_staged",
        "rows_loaded_bronze",
        "rows_loaded_silver",
        "rows_skipped_duplicates",
        "status",
    }
    assert set(loader.run_stats.keys()) == expected
    assert loader.run_stats["source_file"] == "unit_test_source.xlsx"
    assert loader.run_stats["status"] == "RUNNING"
    assert loader.run_stats["rows_extracted"] == 0
    assert isinstance(loader.run_stats["start_time"], datetime)


def test_batch_id_matches_run_id(loader):
    assert loader.run_stats["run_id"] == loader.batch_id


# ---------------------------------------------------------------------------
# _bulk_insert
# ---------------------------------------------------------------------------
def test_bulk_insert_with_external_conn_does_not_open_own(loader):
    """When a conn is provided, the loader must not open its own."""
    df = pd.DataFrame(
        [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
    )
    external_conn = MagicMock()
    external_cursor = MagicMock()
    external_conn.cursor.return_value = external_cursor

    with patch("loaders.base_loader.execute_batch") as mock_eb:
        n = loader._bulk_insert(df, "staging", "raw_transactions", conn=external_conn)

    assert n == 3
    # Caller's connection is used; loader does not open a new one.
    loader.db.connect.assert_not_called()
    # execute_batch is called exactly once with the cursor and a properly-built INSERT.
    assert mock_eb.call_count == 1
    args, kwargs = mock_eb.call_args
    assert args[0] is external_cursor
    assert args[1].startswith("INSERT INTO staging.raw_transactions")
    assert '"a"' in args[1] and '"b"' in args[1]
    assert args[2] == [[1, "x"], [2, "y"], [3, "z"]]
    assert kwargs.get("page_size") == 1000


def test_bulk_insert_without_conn_opens_own_connection(loader):
    """When no conn is supplied, the loader opens one via self.db.connect()."""
    df = pd.DataFrame([{"x": 1}])

    with patch("loaders.base_loader.execute_batch") as mock_eb:
        n = loader._bulk_insert(df, "schema", "table")

    assert n == 1
    loader.db.connect.assert_called_once()
    mock_eb.assert_called_once()


# ---------------------------------------------------------------------------
# _update_category_mapping
# ---------------------------------------------------------------------------
def test_update_category_mapping_runs_two_updates(loader):
    conn = MagicMock()
    cursor = MagicMock()
    cursor.rowcount = 5
    conn.cursor.return_value = cursor

    loader._update_category_mapping(conn)

    assert cursor.execute.call_count == 2
    sql_first = cursor.execute.call_args_list[0].args[0]
    sql_second = cursor.execute.call_args_list[1].args[0]
    assert "FROM silver.category_mapping cm" in sql_first
    assert "transaction_type = 'INCOME'" in sql_second
    assert "Child Support" in sql_second


def test_update_category_mapping_does_not_commit(loader):
    """The helper must not commit; that is the caller's responsibility."""
    conn = MagicMock()
    conn.cursor.return_value.rowcount = 0
    loader._update_category_mapping(conn)
    conn.commit.assert_not_called()


# ---------------------------------------------------------------------------
# _refresh_gold_notability / _refresh_gold_save_potential
# ---------------------------------------------------------------------------
def test_refresh_gold_notability_passes_args_through(loader):
    target_hashes = {"h1", "h2"}
    with patch(
        "loaders.gold_notable_loader.refresh_notability_for_hashes",
        return_value=2,
    ) as mock_refresh:
        loader._refresh_gold_notability(hashes=target_hashes, full=False)
    mock_refresh.assert_called_once_with(
        loader.db, hashes=target_hashes, full=False, window_days=365
    )


def test_refresh_gold_notability_swallows_exceptions(loader, capsys):
    """Gold refresh failure must not raise; should print a warning."""
    with patch(
        "loaders.gold_notable_loader.refresh_notability_for_hashes",
        side_effect=RuntimeError("boom"),
    ):
        loader._refresh_gold_notability(full=True)  # must not raise
    captured = capsys.readouterr()
    assert "Warning" in captured.out
    assert "boom" in captured.out


def test_refresh_gold_save_potential_full_mode(loader):
    with patch(
        "loaders.gold_save_potential_loader.refresh_save_potential_for_hashes",
        return_value=10,
    ) as mock_refresh:
        loader._refresh_gold_save_potential(full=True, window_days=180)
    mock_refresh.assert_called_once_with(
        loader.db, hashes=None, full=True, window_days=180
    )


def test_refresh_gold_save_potential_swallows_exceptions(loader, capsys):
    with patch(
        "loaders.gold_save_potential_loader.refresh_save_potential_for_hashes",
        side_effect=ValueError("nope"),
    ):
        loader._refresh_gold_save_potential(full=True)
    captured = capsys.readouterr()
    assert "Warning" in captured.out


# ---------------------------------------------------------------------------
# _log_pipeline_run
# ---------------------------------------------------------------------------
def test_log_pipeline_run_inserts_expected_row(loader):
    """Verify the INSERT SQL columns + value list match metadata.pipeline_runs schema."""
    loader.run_stats["status"] = "SUCCESS"
    loader.run_stats["rows_extracted"] = 100
    loader.run_stats["rows_staged"] = 100
    loader.run_stats["rows_loaded_bronze"] = 100
    loader.run_stats["rows_loaded_silver"] = 95
    loader.run_stats["rows_skipped_duplicates"] = 5
    loader.run_stats["file_size_bytes"] = 12345

    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    # self.db.connect() -> context manager -> conn
    loader.db.connect.return_value.__enter__.return_value = fake_conn

    loader._log_pipeline_run()

    fake_cursor.execute.assert_called_once()
    sql, values = fake_cursor.execute.call_args.args
    assert sql.startswith("INSERT INTO metadata.pipeline_runs")
    # Every key from the log_data dict must be quoted in the SQL column list.
    expected_cols = [
        "run_id",
        "run_timestamp",
        "source_file",
        "file_size_bytes",
        "status",
        "rows_extracted",
        "rows_staged",
        "rows_loaded_bronze",
        "rows_loaded_silver",
        "rows_skipped_duplicates",
        "rows_failed_validation",
        "start_time",
        "end_time",
        "duration_seconds",
        "error_message",
    ]
    for col in expected_cols:
        assert f'"{col}"' in sql
    assert len(values) == len(expected_cols)

    # Spot-check values match run_stats
    by_idx = dict(zip(expected_cols, values))
    assert by_idx["status"] == "SUCCESS"
    assert by_idx["rows_extracted"] == 100
    assert by_idx["rows_loaded_silver"] == 95
    assert by_idx["rows_skipped_duplicates"] == 5
    assert by_idx["file_size_bytes"] == 12345
    assert by_idx["rows_failed_validation"] == 0
    assert by_idx["run_id"] == str(loader.batch_id)
    assert isinstance(by_idx["duration_seconds"], float)
    assert by_idx["duration_seconds"] >= 0


def test_log_pipeline_run_includes_error_message_on_failure(loader):
    loader.run_stats["status"] = "FAILED"
    loader.run_stats["error_message"] = "something exploded"

    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    loader.db.connect.return_value.__enter__.return_value = fake_conn

    loader._log_pipeline_run()

    _, values = fake_cursor.execute.call_args.args
    # error_message is the last column in the dict ordering.
    assert "something exploded" in values


# ---------------------------------------------------------------------------
# _display_summary
# ---------------------------------------------------------------------------
def test_display_summary_default_format(loader, capsys):
    loader.run_stats["status"] = "SUCCESS"
    loader.run_stats["rows_extracted"] = 1234
    loader.run_stats["rows_loaded_bronze"] = 1234
    loader.run_stats["rows_loaded_silver"] = 1100
    loader.run_stats["rows_skipped_duplicates"] = 134
    # Make the duration deterministic-ish (>=0)
    loader.run_stats["start_time"] = datetime.now() - timedelta(seconds=2)

    loader._display_summary("INCREMENTAL LOAD COMPLETE")

    out = capsys.readouterr().out
    assert "INCREMENTAL LOAD COMPLETE" in out
    assert "Status: SUCCESS" in out
    assert "1,234" in out
    assert "Duplicates:" in out
    assert "134 skipped" in out


def test_display_summary_with_extra_lines(loader, capsys):
    loader._display_summary(
        "INITIAL LOAD COMPLETE",
        extra_lines=["\nNext Steps:", "  1. Verify data"],
    )
    out = capsys.readouterr().out
    assert "INITIAL LOAD COMPLETE" in out
    assert "Next Steps:" in out
    assert "1. Verify data" in out
