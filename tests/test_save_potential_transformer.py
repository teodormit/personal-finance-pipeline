"""
Unit tests for save_potential_transformer.compute_save_potential.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transformers.save_potential_transformer import (
    AVOIDABILITY,
    WEIGHT_AVOIDABILITY,
    WEIGHT_AMT,
    WEIGHT_FREQ,
    compute_save_potential,
)


def _expense_row(h, date, sub, cls, amt, ym):
    return {
        "transaction_hash": h,
        "transaction_date": date,
        "transaction_type": "EXPENSE",
        "amount_abs_eur": amt,
        "subcategory": sub,
        "classification": cls,
        "year_month": ym,
    }


def test_want_need_must_ordering():
    """Same history shape: WANT score > NEED > MUST (avoidability * 3 dominates)."""
    rows = [
        _expense_row("a", "2025-01-10", "CatA", "WANT", 10.0, "2025-01"),
        _expense_row("b", "2025-01-11", "CatB", "NEED", 10.0, "2025-01"),
        _expense_row("c", "2025-01-12", "CatC", "MUST", 10.0, "2025-01"),
    ]
    df = pd.DataFrame(rows)
    out = compute_save_potential(df)
    s_want = out[out["transaction_hash"] == "a"]["save_potential_score"].iloc[0]
    s_need = out[out["transaction_hash"] == "b"]["save_potential_score"].iloc[0]
    s_must = out[out["transaction_hash"] == "c"]["save_potential_score"].iloc[0]
    assert s_want > s_need > s_must
    assert abs(s_want - AVOIDABILITY["WANT"] * WEIGHT_AVOIDABILITY) < 0.01
    assert abs(s_need - AVOIDABILITY["NEED"] * WEIGHT_AVOIDABILITY) < 0.01
    assert abs(s_must - AVOIDABILITY["MUST"] * WEIGHT_AVOIDABILITY) < 0.01


def test_missing_classification_defaults_need():
    """Null classification maps to NEED avoidability."""
    df = pd.DataFrame(
        [
            _expense_row("x", "2025-01-10", "S", None, 5.0, "2025-01"),
        ]
    )
    out = compute_save_potential(df)
    assert out["avoidability"].iloc[0] == AVOIDABILITY["NEED"]


def test_income_filtered_out():
    df = pd.DataFrame(
        [
            {
                "transaction_hash": "i1",
                "transaction_date": "2025-01-01",
                "transaction_type": "INCOME",
                "amount_abs_eur": 100.0,
                "subcategory": "Income",
                "classification": "WANT",
                "year_month": "2025-01",
            }
        ]
    )
    out = compute_save_potential(df)
    assert len(out) == 0


def test_frequency_excess_increases_score():
    """Higher month count vs prior monthly average increases score."""
    rows = [
        # December: 1 txn Groceries
        _expense_row("d1", "2024-12-05", "Groceries", "NEED", 10.0, "2024-12"),
        # January: 3 txns same subcategory -> month_txn_count=3, hist_avg from prior month = 1
        _expense_row("j1", "2025-01-05", "Groceries", "NEED", 10.0, "2025-01"),
        _expense_row("j2", "2025-01-06", "Groceries", "NEED", 11.0, "2025-01"),
        _expense_row("j3", "2025-01-07", "Groceries", "NEED", 12.0, "2025-01"),
    ]
    df = pd.DataFrame(rows)
    out = compute_save_potential(df, window_days=365)
    jan = out[out["transaction_hash"] == "j1"].iloc[0]
    assert jan["month_txn_count"] == 3
    assert jan["hist_avg_monthly_count"] == 1.0
    assert jan["freq_ratio"] == pytest.approx(3.0)
    assert jan["freq_excess"] == pytest.approx(2.0, rel=0.01)
    expected = (
        AVOIDABILITY["NEED"] * WEIGHT_AVOIDABILITY
        + jan["freq_excess"] * WEIGHT_FREQ
        + jan["amt_excess"] * WEIGHT_AMT
    )
    assert jan["save_potential_score"] == pytest.approx(expected, rel=0.01)


def test_amount_z_increases_score():
    """After building history, a large outlier gets higher amt_excess."""
    rows = []
    # Varying amounts so population std > 0
    amounts = [8.0, 9.0, 10.0, 11.0, 12.0]
    for i, amt in enumerate(amounts):
        rows.append(
            _expense_row(
                f"h{i}",
                f"2024-11-{i+1:02d}",
                "Gz",
                "NEED",
                amt,
                "2024-11",
            )
        )
    rows.append(
        _expense_row("big", "2024-12-15", "Gz", "NEED", 50.0, "2024-12")
    )
    df = pd.DataFrame(rows)
    out = compute_save_potential(df, window_days=365)
    big = out[out["transaction_hash"] == "big"].iloc[0]
    assert big["amount_z_score"] > 2.0
    assert big["amt_excess"] > 0
    assert big["save_potential_score"] > AVOIDABILITY["NEED"] * WEIGHT_AVOIDABILITY
