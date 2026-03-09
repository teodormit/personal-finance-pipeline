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


def test_full_mapping():
    """End-to-end: sample raw API DataFrame through map_raw_to_transformer_input."""
    raw_df = pd.DataFrame({
        "id": ["uuid-1"],
        "recordDate": ["2026-02-05T00:00:00Z"],
        "recordDateString": ["2026-02-05"],
        "recordType": ["expense"],
        "paymentType": ["transfer"],
        "note": ["281BATM2604201HH AC1 Плащане /импринтер/ 25.56 EUR авт.код:740853-GREEK RESTAURANT NAMOOS/SOFIA/PAN:5169****1763/CT:08,Операция с карта"],
        "payee": ["4591TATB0"],
        "payer": [""],
        "amount_value": [25.56],
        "amount_currency": ["EUR"],
        "category_id": ["d697d6ac-edd1-46b1-87a0-3a65ed6003c6"],
        "category_name": ["Food & Drinks"],
        "accountId": ["77a90a1e-a336-489b-8c40-577f2e236dbb"],
        "account_name": ["UniCredit Bulbank"],
        "labels": [""],
        "baseAmount": {"value": -51.1, "currencyCode": "BGN"},
    })
    result = map_raw_to_transformer_input(raw_df)

    # ExpenseTransformer expects: actually all columns and doesn't have a strict schema
    expected_cols = [
        "source_record_id", "date", "date_time", "note", "type", "payee", "payer", "amount", "labels",
        "account", "category", "currency", "payment", "category_id", "account_id",]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"

    assert result["date"].iloc[0] == "2026-02-05"
    assert result["type"].iloc[0] == "Expenses"
    assert result["payment"].iloc[0] == "TRANSFER"
    assert result["amount"].iloc[0] == -25.50  # expense positive -> negative
    assert result["category"].iloc[0] == "Food & Drinks"
    assert result["account"].iloc[0] == "UniCredit Bulbank"


def test_full_mapping_empty_df():
    """Empty DataFrame returns empty DataFrame."""
    raw_df = pd.DataFrame()
    result = map_raw_to_transformer_input(raw_df)
    assert result.empty
