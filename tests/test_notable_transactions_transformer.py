"""
Unit tests for notable_transactions_transformer.compute_notability.
"""

import pandas as pd
import pytest
import numpy as np
from datetime import datetime, timedelta

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transformers.notable_transactions_transformer import compute_notability, OUTPUT_COLUMNS


def _make_df(rows):
    """Build DataFrame with standard columns for testing."""
    return pd.DataFrame(rows, columns=[
        "transaction_hash", "transaction_date", "transaction_type",
        "amount_abs_eur", "subcategory"
    ])


def test_income_filtered_out():
    """INCOME rows are excluded from output."""
    df = _make_df([
        ["h1", "2025-03-01", "INCOME", 100.0, "Salary"],
    ])
    out = compute_notability(df)
    assert len(out) == 0
    assert list(out.columns) == OUTPUT_COLUMNS


def test_empty_input():
    """Empty DataFrame returns empty output with correct columns."""
    df = _make_df([])
    out = compute_notability(df)
    assert len(out) == 0
    assert list(out.columns) == OUTPUT_COLUMNS


def test_new_subcategory():
    """First transaction in a subcategory has hist_n_txns=0, is_new_subcategory=True."""
    df = _make_df([
        ["h1", "2025-03-01", "EXPENSE", 50.0, "BrandNewCategory"],
    ])
    out = compute_notability(df)
    assert len(out) == 1
    assert out["hist_n_txns"].iloc[0] == 0
    assert out["is_new_subcategory"].iloc[0] == True
    assert out["is_new_subcategory_max"].iloc[0] == True
    assert out["notability_label"].iloc[0] == "New Category"
    assert pd.isna(out["amount_z_score"].iloc[0])


def test_window_excludes_outside_365_days():
    """Transactions outside 365-day window are excluded from baseline."""
    base = datetime(2025, 3, 15)
    df = _make_df([
        ["h_old", (base - timedelta(days=366)).strftime("%Y-%m-%d"), "EXPENSE", 10.0, "Groceries"],
        ["h_mid", (base - timedelta(days=180)).strftime("%Y-%m-%d"), "EXPENSE", 20.0, "Groceries"],
        ["h_new", base.strftime("%Y-%m-%d"), "EXPENSE", 50.0, "Groceries"],
    ])
    out = compute_notability(df, window_days=365)
    # h_new's baseline: only h_mid (h_old is 366 days back, outside window)
    row_new = out[out["transaction_hash"] == "h_new"].iloc[0]
    assert row_new["hist_n_txns"] == 1
    assert row_new["hist_avg_amount_eur"] == 20.0
    # std with 1 sample is NaN (need n>=2 for population std)
    assert pd.isna(row_new["hist_std_amount_eur"])
    assert pd.isna(row_new["amount_z_score"])
    assert row_new["notability_label"] == "Insufficient History"


def test_window_includes_inside_365_days():
    """Transactions within 365-day window are included in baseline."""
    base = datetime(2025, 3, 15)
    df = _make_df([
        ["h1", (base - timedelta(days=364)).strftime("%Y-%m-%d"), "EXPENSE", 10.0, "Food"],
        ["h2", (base - timedelta(days=1)).strftime("%Y-%m-%d"), "EXPENSE", 30.0, "Food"],
        ["h3", base.strftime("%Y-%m-%d"), "EXPENSE", 100.0, "Food"],
    ])
    out = compute_notability(df, window_days=365)
    row3 = out[out["transaction_hash"] == "h3"].iloc[0]
    assert row3["hist_n_txns"] == 2
    assert row3["hist_avg_amount_eur"] == 20.0  # (10+30)/2
    # std = sqrt(((10-20)^2 + (30-20)^2)/2) = sqrt(100) = 10
    assert abs(row3["hist_std_amount_eur"] - 10.0) < 0.01
    # z = (100-20)/10 = 8
    assert abs(row3["amount_z_score"] - 8.0) < 0.01
    assert row3["notability_label"] == "Extreme Outlier"


def test_self_exclusion_same_day():
    """A transaction does not include itself in its baseline; same-day peers do count."""
    df = _make_df([
        ["h_a", "2025-03-10", "EXPENSE", 40.0, "Groceries"],  # smaller hash
        ["h_b", "2025-03-10", "EXPENSE", 60.0, "Groceries"],  # larger hash
    ])
    out = compute_notability(df)
    # h_a: no prior rows in window (h_b has same date but hash_b > hash_a, so h_b is NOT "strictly before" h_a)
    # "Strictly before" = (date < date_i) OR (date == date_i AND hash < hash_i)
    # For h_a: h_b has date==date_a and hash_b > hash_a => h_b not prior. So h_a sees 0 prior.
    # For h_b: h_a has date==date_b and hash_a < hash_b => h_a IS prior. So h_b sees h_a.
    row_a = out[out["transaction_hash"] == "h_a"].iloc[0]
    row_b = out[out["transaction_hash"] == "h_b"].iloc[0]
    assert row_a["hist_n_txns"] == 0
    assert row_b["hist_n_txns"] == 1
    assert row_b["hist_avg_amount_eur"] == 40.0
    # h_b is 60 vs avg 40, std=0 (single sample) -> NaN z
    assert pd.isna(row_b["amount_z_score"])


def test_is_new_subcategory_max():
    """Largest-ever transaction in subcategory gets is_new_subcategory_max=True."""
    df = _make_df([
        ["h1", "2025-01-01", "EXPENSE", 25.0, "Electronics"],
        ["h2", "2025-02-01", "EXPENSE", 30.0, "Electronics"],
        ["h3", "2025-03-01", "EXPENSE", 100.0, "Electronics"],  # new record + extreme outlier
    ])
    out = compute_notability(df)
    row3 = out[out["transaction_hash"] == "h3"].iloc[0]
    assert row3["is_new_subcategory_max"] == True
    assert row3["is_new_subcategory"] == False
    assert row3["hist_max_amount_eur"] == 30.0
    # When both new record and high z-score, z-based label takes priority
    assert row3["notability_label"] == "Extreme Outlier"


def test_output_columns():
    """Output has all expected columns for gold.transaction_notability."""
    df = _make_df([
        ["h1", "2025-03-01", "EXPENSE", 50.0, "Test"],
    ])
    out = compute_notability(df)
    assert list(out.columns) == OUTPUT_COLUMNS


def test_expense_type_case_insensitive():
    """Both 'EXPENSE' and 'expense' type are processed."""
    df = _make_df([
        ["h1", "2025-03-01", "expense", 25.0, "Food"],
    ])
    out = compute_notability(df)
    assert len(out) == 1
    assert out["transaction_hash"].iloc[0] == "h1"

