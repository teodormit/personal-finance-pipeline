"""
Unit tests for api_field_mapper module.
"""

import pandas as pd
import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from extractors.api_field_mapper import (
    map_record_types,
    map_payment_types,
    normalize_amounts,
    rename_to_transformer_schema,
    map_raw_to_transformer_input,
)


def test_map_record_types():
    """Verify expense/income/transfer mapping."""
    series = pd.Series(["income", "expense", "transfer", "expenses", None, "INCOME"])
    result = map_record_types(series)
    expected = pd.Series(["Income", "Expenses", "Transfer", "Expenses", "Expenses", "Income"])
    pd.testing.assert_series_equal(result, expected)


def test_map_payment_types():
    """Verify all payment type mappings."""
    series = pd.Series([
        "transfer", "cash", "debit_card", "credit_card",
        "mobile_payment", "web_payment", "voucher", None, "unknown"
    ])
    result = map_payment_types(series)
    expected = pd.Series([
        "TRANSFER", "CASH", "DEBIT_CARD", "CREDIT_CARD",
        "MOBILE_PAYMENT", "WEB_PAYMENT", "VOUCHER", "", "UNKNOWN"
    ])
    pd.testing.assert_series_equal(result, expected)


def test_normalize_amounts():
    """Verify sign convention: expense positive becomes negative, income negative becomes positive."""
    df = pd.DataFrame({
        "recordType": ["expense", "income", "expense", "income", "transfer"],
        "amount_value": [10.0, -50.0, -5.0, 100.0, 20.0],
    })
    result = normalize_amounts(df)
    # expense +10 -> -10; income -50 -> 50; expense -5 stays -5; income +100 stays 100; transfer unchanged
    assert result["amount_value"].iloc[0] == -10.0
    assert result["amount_value"].iloc[1] == 50.0
    assert result["amount_value"].iloc[2] == -5.0
    assert result["amount_value"].iloc[3] == 100.0
    assert result["amount_value"].iloc[4] == 20.0


def test_rename_columns():
    """Verify column name mapping."""
    df = pd.DataFrame({
        "recordDate": ["2026-02-05"],
        "note": ["test"],
        "recordType": ["expense"],
        "category_name": ["Groceries"],
        "account_name": ["Bank"],
        "amount_value": [-10.0],
        "amount_currency": ["EUR"],
        "paymentType": ["transfer"],
    })
    result = rename_to_transformer_schema(df)
    assert "date" in result.columns
    assert "type" in result.columns
    assert "category" in result.columns
    assert "account" in result.columns
    assert "amount" in result.columns
    assert "currency" in result.columns
    assert "payment" in result.columns


def test_full_mapping_empty_df():
    """Empty DataFrame returns empty DataFrame."""
    raw_df = pd.DataFrame()
    result = map_raw_to_transformer_input(raw_df)
    assert result.empty
