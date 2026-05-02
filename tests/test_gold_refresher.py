"""
Unit tests for src/loaders/gold_refresher.py GoldRefresher.

Strategy: pure mocks. No database required.

Verifies:
  - SQL string shape for _fetch_target_info, _fetch_silver_expenses,
    and _upsert_to_gold (covering both notable's and save-potential's params).
  - The refresh() template-method orchestration:
      * full=True path skips target-info lookup
      * incremental with empty hashes → 0
      * empty silver → 0
      * computed rows are filtered to target_hashes when set
      * JSON serialization fires for json_columns
  - Public functions refresh_notability_for_hashes /
    refresh_save_potential_for_hashes still work after refactor.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from loaders.gold_refresher import GoldRefresher, _coerce_to_date  # noqa: E402


# ---------------------------------------------------------------------------
# Test subclass
# ---------------------------------------------------------------------------
class _StubRefresher(GoldRefresher):
    gold_table = "gold.test_table"
    print_label = "test_metric"
    silver_extra_columns = ["classification", "year_month"]
    gold_columns = ["transaction_hash", "score", "label", "computed_at", "extra_stats"]
    json_columns = ["extra_stats"]

    def __init__(self, computed_df: pd.DataFrame = None):
        self._computed_df = computed_df

    def compute(self, silver_df, *, window_days):
        if self._computed_df is None:
            # Default: return one row per silver row with stub scores.
            return pd.DataFrame(
                {
                    "transaction_hash": silver_df["transaction_hash"],
                    "score": 1.0,
                    "label": "Stub",
                }
            )
        return self._computed_df


# ---------------------------------------------------------------------------
# _coerce_to_date
# ---------------------------------------------------------------------------
def test_coerce_to_date_passes_through_date():
    d = date(2025, 6, 15)
    assert _coerce_to_date(d) is d


def test_coerce_to_date_parses_iso_string():
    d = _coerce_to_date("2025-06-15")
    assert d == date(2025, 6, 15)


def test_coerce_to_date_handles_full_iso_with_time():
    d = _coerce_to_date("2025-06-15T12:34:56")
    assert d == date(2025, 6, 15)


# ---------------------------------------------------------------------------
# _fetch_target_info
# ---------------------------------------------------------------------------
def test_fetch_target_info_returns_subcats_and_date_bounds():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = [
        ("Food", date(2025, 1, 1)),
        ("Food", date(2025, 1, 5)),
        ("Transport", date(2025, 1, 3)),
    ]

    result = GoldRefresher._fetch_target_info(conn, {"h1", "h2", "h3"})

    assert result is not None
    subcats, min_d, max_d = result
    assert subcats == {"Food", "Transport"}
    assert min_d == date(2025, 1, 1)
    assert max_d == date(2025, 1, 5)

    # The SQL must filter on EXPENSE only.
    sql = cursor.execute.call_args.args[0]
    assert "transaction_type = 'EXPENSE'" in sql
    assert "transaction_hash IN (" in sql


def test_fetch_target_info_returns_none_when_no_rows():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = []

    result = GoldRefresher._fetch_target_info(conn, {"missing"})
    assert result is None


# ---------------------------------------------------------------------------
# _fetch_silver_expenses
# ---------------------------------------------------------------------------
def test_fetch_silver_expenses_full_mode_minimal_query():
    """No subcategories, no date range -> just the EXPENSE filter."""
    refresher = _StubRefresher()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.description = [("transaction_hash",), ("amount_abs_eur",)]
    cursor.fetchall.return_value = [("h1", 10.0)]

    df = refresher._fetch_silver_expenses(conn)

    assert len(df) == 1
    sql, params = cursor.execute.call_args.args
    assert "FROM silver.transactions" in sql
    assert "transaction_type = 'EXPENSE'" in sql
    # No subcategory IN clause, no date range.
    assert "subcategory IN" not in sql
    assert "transaction_date >=" not in sql
    assert params == []
    # Subclass extra columns appear in SELECT.
    assert "classification" in sql
    assert "year_month" in sql


def test_fetch_silver_expenses_incremental_filters_subcat_and_date():
    refresher = _StubRefresher()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.description = [("transaction_hash",)]
    cursor.fetchall.return_value = []

    refresher._fetch_silver_expenses(
        conn,
        subcategories={"Food", "Transport"},
        date_from=date(2025, 1, 10),
        date_to=date(2025, 1, 20),
        window_days=30,
    )

    sql, params = cursor.execute.call_args.args
    assert "subcategory IN (%s, %s)" in sql
    assert "transaction_date >= %s AND transaction_date <= %s" in sql
    # Params: 2 subcategories + fetch_from (10 Jan - 30d) + date_to.
    assert len(params) == 4
    # Subcategories are the first two params, then dates.
    assert set(params[:2]) == {"Food", "Transport"}
    assert params[2] == date(2024, 12, 11)  # 10 Jan - 30 days
    assert params[3] == date(2025, 1, 20)


# ---------------------------------------------------------------------------
# _upsert_to_gold
# ---------------------------------------------------------------------------
def test_upsert_to_gold_builds_expected_query_and_serializes_json():
    refresher = _StubRefresher()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    df = pd.DataFrame(
        [
            {
                "transaction_hash": "h1",
                "score": 2.5,
                "label": "High",
                "extra_stats": {"foo": 1, "bar": 2},
            },
            {
                "transaction_hash": "h2",
                "score": 0.0,
                "label": "Normal",
                # extra_stats missing → should become "{}"
            },
        ]
    )

    with patch("loaders.gold_refresher.execute_batch") as mock_eb:
        n = refresher._upsert_to_gold(conn, df)

    assert n == 2

    sql, values = mock_eb.call_args.args[1], mock_eb.call_args.args[2]
    # Table and ON CONFLICT verified.
    assert "INSERT INTO gold.test_table" in sql
    assert "ON CONFLICT (transaction_hash) DO UPDATE SET" in sql
    # Every gold_column appears exactly once in the column list.
    for c in refresher.gold_columns:
        assert f'"{c}"' in sql

    # Each row has len(gold_columns) values.
    assert all(len(v) == len(refresher.gold_columns) for v in values)

    # Locate extra_stats by column index in gold_columns.
    extra_idx = refresher.gold_columns.index("extra_stats")

    # Row 1's extra_stats was a dict → serialized to JSON.
    parsed = json.loads(values[0][extra_idx])
    assert parsed == {"foo": 1, "bar": 2}
    # Row 2's extra_stats was missing/NaN → defaulted to "{}" then None-coerced
    # by the where(notna) call. Either is acceptable.
    assert values[1][extra_idx] in ("{}", None)

    conn.commit.assert_called_once()


def test_upsert_to_gold_fills_missing_columns_with_none():
    refresher = _StubRefresher()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    df = pd.DataFrame([{"transaction_hash": "h1"}])  # missing every other gold col

    with patch("loaders.gold_refresher.execute_batch") as mock_eb:
        refresher._upsert_to_gold(conn, df)

    values = mock_eb.call_args.args[2]
    assert len(values) == 1
    row = values[0]
    assert row[refresher.gold_columns.index("transaction_hash")] == "h1"
    # All other columns should be None or "{}" (for json_columns).
    for c in refresher.gold_columns:
        if c == "transaction_hash":
            continue
        v = row[refresher.gold_columns.index(c)]
        if c in refresher.json_columns:
            assert v in ("{}", None)
        else:
            # computed_at is set to a real datetime; everything else is None.
            assert v is None or isinstance(v, (datetime,))


# ---------------------------------------------------------------------------
# refresh() orchestration
# ---------------------------------------------------------------------------
def _connect_returning(conn):
    """Build a db mock whose `connect()` context-manager yields `conn`."""
    db = MagicMock()
    db.connect.return_value.__enter__.return_value = conn
    return db


def test_refresh_full_skips_target_info_and_calls_compute(monkeypatch):
    refresher = _StubRefresher(
        computed_df=pd.DataFrame(
            {
                "transaction_hash": ["h1", "h2"],
                "score": [1.0, 2.0],
                "label": ["A", "B"],
            }
        )
    )
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.description = [("transaction_hash",), ("amount_abs_eur",)]
    cursor.fetchall.return_value = [("h1", 5.0), ("h2", 6.0)]

    db = _connect_returning(conn)
    fetch_target = MagicMock()
    monkeypatch.setattr(GoldRefresher, "_fetch_target_info", fetch_target)

    with patch("loaders.gold_refresher.execute_batch"):
        n = refresher.refresh(db, hashes=None, full=True, window_days=30)

    fetch_target.assert_not_called()
    assert n == 2


def test_refresh_incremental_with_empty_hashes_returns_zero():
    refresher = _StubRefresher()
    conn = MagicMock()
    db = _connect_returning(conn)

    n = refresher.refresh(db, hashes=set(), full=False)
    assert n == 0
    conn.cursor.assert_not_called()


def test_refresh_incremental_no_target_info_returns_zero(monkeypatch):
    refresher = _StubRefresher()
    conn = MagicMock()
    db = _connect_returning(conn)

    monkeypatch.setattr(
        GoldRefresher, "_fetch_target_info", staticmethod(lambda c, h: None)
    )

    n = refresher.refresh(db, hashes={"h1"}, full=False)
    assert n == 0


def test_refresh_incremental_filters_to_target_hashes():
    """Computed rows outside the target set must be dropped before upsert."""
    refresher = _StubRefresher(
        computed_df=pd.DataFrame(
            {
                "transaction_hash": ["h1", "h2", "h3"],  # h1 is target, h2/h3 are baseline
                "score": [1.0, 2.0, 3.0],
                "label": ["A", "B", "C"],
            }
        )
    )

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    # Sequence of cursor.execute calls:
    #   1) _fetch_target_info SELECT
    #   2) _fetch_silver_expenses SELECT
    # We only need fetchall() on the second call to be non-empty.
    cursor.description = [("transaction_hash",), ("amount_abs_eur",)]
    cursor.fetchall.side_effect = [
        [("Food", date(2025, 1, 5))],  # _fetch_target_info
        [("h1", 10.0), ("h2", 20.0), ("h3", 30.0)],  # _fetch_silver_expenses
    ]
    db = _connect_returning(conn)

    with patch("loaders.gold_refresher.execute_batch") as mock_eb:
        n = refresher.refresh(db, hashes={"h1"}, full=False, window_days=30)

    # Only h1 should be upserted.
    assert n == 1
    upserted = mock_eb.call_args.args[2]
    assert len(upserted) == 1
    hash_idx = refresher.gold_columns.index("transaction_hash")
    assert upserted[0][hash_idx] == "h1"


def test_refresh_empty_silver_returns_zero():
    refresher = _StubRefresher()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.description = [("transaction_hash",)]
    cursor.fetchall.return_value = []  # no silver rows

    db = _connect_returning(conn)

    n = refresher.refresh(db, hashes=None, full=True)
    assert n == 0


# ---------------------------------------------------------------------------
# Public function compatibility
# ---------------------------------------------------------------------------
def test_refresh_notability_for_hashes_delegates_to_class():
    from loaders.gold_notable_loader import (
        NotabilityRefresher,
        refresh_notability_for_hashes,
    )

    sentinel_db = object()
    with patch.object(NotabilityRefresher, "refresh", return_value=42) as mock_refresh:
        result = refresh_notability_for_hashes(
            sentinel_db, hashes={"h1"}, full=False, window_days=180
        )

    assert result == 42
    mock_refresh.assert_called_once_with(
        sentinel_db, hashes={"h1"}, full=False, window_days=180
    )


def test_refresh_save_potential_for_hashes_delegates_to_class():
    from loaders.gold_save_potential_loader import (
        SavePotentialRefresher,
        refresh_save_potential_for_hashes,
    )

    sentinel_db = object()
    with patch.object(
        SavePotentialRefresher, "refresh", return_value=7
    ) as mock_refresh:
        result = refresh_save_potential_for_hashes(
            sentinel_db, hashes=None, full=True, window_days=365
        )

    assert result == 7
    mock_refresh.assert_called_once_with(
        sentinel_db, hashes=None, full=True, window_days=365
    )


def test_subclass_attributes_match_expected():
    """Sanity-check: subclasses still declare the right table names + columns."""
    from loaders.gold_notable_loader import NotabilityRefresher
    from loaders.gold_save_potential_loader import SavePotentialRefresher

    assert NotabilityRefresher.gold_table == "gold.transaction_notability"
    assert "extra_stats" in NotabilityRefresher.json_columns
    assert "notability_score" in NotabilityRefresher.gold_columns
    assert "transaction_hash" in NotabilityRefresher.gold_columns

    assert SavePotentialRefresher.gold_table == "gold.transaction_save_potential"
    assert SavePotentialRefresher.json_columns == []
    assert "classification" in SavePotentialRefresher.silver_extra_columns
    assert "year_month" in SavePotentialRefresher.silver_extra_columns
    assert "save_potential_score" in SavePotentialRefresher.gold_columns
