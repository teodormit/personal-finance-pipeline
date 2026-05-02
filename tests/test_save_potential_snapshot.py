"""
Snapshot test for save_potential_transformer.

Locks the exact numerical output of compute_save_potential against a
realistic multi-month, multi-subcategory input. Captured BEFORE the
Step 6 vectorization of the frequency logic, so any drift will be
caught immediately.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transformers.save_potential_transformer import compute_save_potential


# ---------------------------------------------------------------------------
# Fixed input
# ---------------------------------------------------------------------------
def _input_df() -> pd.DataFrame:
    rows = []
    # 2024-09: 2 Groceries, 1 Restaurants
    rows += [
        {"transaction_hash": "s-a", "transaction_date": "2024-09-05", "transaction_type": "EXPENSE", "amount_abs_eur": 30.0, "subcategory": "Groceries", "classification": "NEED", "year_month": "2024-09"},
        {"transaction_hash": "s-b", "transaction_date": "2024-09-12", "transaction_type": "EXPENSE", "amount_abs_eur": 25.0, "subcategory": "Groceries", "classification": "NEED", "year_month": "2024-09"},
        {"transaction_hash": "s-c", "transaction_date": "2024-09-15", "transaction_type": "EXPENSE", "amount_abs_eur": 50.0, "subcategory": "Restaurants", "classification": "WANT", "year_month": "2024-09"},
    ]
    # 2024-10: 3 Groceries (frequency excess vs prior month avg of 2)
    rows += [
        {"transaction_hash": "s-d", "transaction_date": "2024-10-03", "transaction_type": "EXPENSE", "amount_abs_eur": 28.0, "subcategory": "Groceries", "classification": "NEED", "year_month": "2024-10"},
        {"transaction_hash": "s-e", "transaction_date": "2024-10-10", "transaction_type": "EXPENSE", "amount_abs_eur": 35.0, "subcategory": "Groceries", "classification": "NEED", "year_month": "2024-10"},
        {"transaction_hash": "s-f", "transaction_date": "2024-10-20", "transaction_type": "EXPENSE", "amount_abs_eur": 80.0, "subcategory": "Groceries", "classification": "NEED", "year_month": "2024-10"},
        {"transaction_hash": "s-g", "transaction_date": "2024-10-08", "transaction_type": "EXPENSE", "amount_abs_eur": 60.0, "subcategory": "Restaurants", "classification": "WANT", "year_month": "2024-10"},
        {"transaction_hash": "s-h", "transaction_date": "2024-10-22", "transaction_type": "EXPENSE", "amount_abs_eur": 200.0, "subcategory": "Restaurants", "classification": "WANT", "year_month": "2024-10"},
    ]
    # 2024-11: 1 Subscriptions (MUST), 1 Income (must be filtered out)
    rows += [
        {"transaction_hash": "s-i", "transaction_date": "2024-11-01", "transaction_type": "EXPENSE", "amount_abs_eur": 15.0, "subcategory": "Subscriptions", "classification": "MUST", "year_month": "2024-11"},
        {"transaction_hash": "s-j", "transaction_date": "2024-11-15", "transaction_type": "INCOME", "amount_abs_eur": 1000.0, "subcategory": "Salary", "classification": "WANT", "year_month": "2024-11"},
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Expected output (captured pre-Step 6)
#
# Hash order is alphabetical s-a..s-i (sorted before assertion).
# ---------------------------------------------------------------------------
EXPECTED = {
    "month_txn_count": {
        "s-a": 2, "s-b": 2, "s-c": 1, "s-d": 3, "s-e": 3, "s-f": 3,
        "s-g": 2, "s-h": 2, "s-i": 1,
    },
    "hist_avg_monthly_count": {
        "s-a": float("nan"), "s-b": float("nan"), "s-c": float("nan"),
        "s-d": 2.0, "s-e": 2.0, "s-f": 2.0,
        "s-g": 1.0, "s-h": 1.0,
        "s-i": float("nan"),
    },
    "freq_ratio": {
        "s-a": float("nan"), "s-b": float("nan"), "s-c": float("nan"),
        "s-d": 1.5, "s-e": 1.5, "s-f": 1.5,
        "s-g": 2.0, "s-h": 2.0,
        "s-i": float("nan"),
    },
    "freq_excess": {
        "s-a": 0.0, "s-b": 0.0, "s-c": 0.0,
        "s-d": 0.5, "s-e": 0.5, "s-f": 0.5,
        "s-g": 1.0, "s-h": 1.0,
        "s-i": 0.0,
    },
    "amt_excess": {
        "s-a": 0.0, "s-b": 0.0, "s-c": 0.0,
        "s-d": 0.2,
        "s-e": 3.568871,
        "s-f": 5.0,  # capped
        "s-g": 0.0,
        "s-h": 5.0,  # capped
        "s-i": 0.0,
    },
    "avoidability": {
        "s-a": 0.4, "s-b": 0.4, "s-c": 1.0,
        "s-d": 0.4, "s-e": 0.4, "s-f": 0.4,
        "s-g": 1.0, "s-h": 1.0,
        "s-i": 0.05,
    },
    "save_potential_score": {
        "s-a": 1.2, "s-b": 1.2, "s-c": 3.0,
        "s-d": 2.4, "s-e": 5.768871, "s-f": 7.2,
        "s-g": 5.0, "s-h": 10.0,
        "s-i": 0.15,
    },
    "save_potential_label": {
        "s-a": "Low Save Potential",
        "s-b": "Low Save Potential",
        "s-c": "Medium Save Potential",
        "s-d": "Low Save Potential",
        "s-e": "High Save Potential",
        "s-f": "High Save Potential",
        "s-g": "High Save Potential",
        "s-h": "High Save Potential",
        "s-i": "Minimal",
    },
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def computed():
    df = _input_df()
    out = compute_save_potential(df, window_days=365)
    return out.set_index("transaction_hash")


def test_income_row_excluded(computed):
    assert "s-j" not in computed.index
    assert len(computed) == 9


@pytest.mark.parametrize("col", list(EXPECTED.keys()))
def test_snapshot_column_matches_baseline(computed, col):
    expected_for_col = EXPECTED[col]
    for h, expected_value in expected_for_col.items():
        actual = computed.loc[h, col]
        if isinstance(expected_value, float):
            if pd.isna(expected_value):
                assert pd.isna(actual), (
                    f"{col}[{h}]: expected NaN, got {actual!r}"
                )
            else:
                assert actual == pytest.approx(expected_value, rel=1e-4), (
                    f"{col}[{h}]: expected {expected_value}, got {actual}"
                )
        else:
            assert actual == expected_value, (
                f"{col}[{h}]: expected {expected_value!r}, got {actual!r}"
            )
