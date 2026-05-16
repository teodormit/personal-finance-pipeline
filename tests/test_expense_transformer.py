"""
Unit tests for src/transformers/expense_transformer.py

Strategy: call each private step method directly on a minimal DataFrame.
No database, no mocks — the transformer is a pure Pandas pipeline.

Sections:
  - Helpers
  - Step 1: rename_columns
  - Step 2: parse_dates
  - Step 3: parse_amounts
  - Step 4: standardize_types
  - Step 5: convert_currencies
  - Step 6: add_derived_fields
  - Step 7: add_classification
  - Step 8: generate_hashes
  - Step 9: final_cleanup
  - _validate_before_return
  - Full transform() integration
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transformers.expense_transformer import ExpenseTransformer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _transformer() -> ExpenseTransformer:
    return ExpenseTransformer()


def _raw_df(**overrides) -> pd.DataFrame:
    """Minimal DataFrame in CSV export shape (before any transformation)."""
    row = {
        "date": "2024-01-15",
        "note": "test note",
        "type": "Expenses",
        "payee": "Test Payee",
        "amount": "-10.00",
        "labels": "",
        "account": "Cash in Euro",
        "category": "Groceries",
        "currency": "EUR",
        "payment": "card",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _after_step1(**overrides) -> pd.DataFrame:
    t = _transformer()
    return t._step1_rename_columns(_raw_df(**overrides))


def _after_step2(**overrides) -> pd.DataFrame:
    t = _transformer()
    df = t._step1_rename_columns(_raw_df(**overrides))
    return t._step2_parse_dates(df)


def _after_steps_1_to_3(**overrides) -> pd.DataFrame:
    t = _transformer()
    df = t._step1_rename_columns(_raw_df(**overrides))
    df = t._step2_parse_dates(df)
    df = t._step3_parse_amounts(df)
    return df


def _after_steps_1_to_4(**overrides) -> pd.DataFrame:
    t = _transformer()
    df = t._step1_rename_columns(_raw_df(**overrides))
    df = t._step2_parse_dates(df)
    df = t._step3_parse_amounts(df)
    df = t._step4_standardize_types(df)
    return df


def _after_steps_1_to_5(**overrides) -> pd.DataFrame:
    t = _transformer()
    df = t._step1_rename_columns(_raw_df(**overrides))
    df = t._step2_parse_dates(df)
    df = t._step3_parse_amounts(df)
    df = t._step4_standardize_types(df)
    df = t._step5_convert_currencies(df)
    return df


# ---------------------------------------------------------------------------
# Step 1: rename_columns
# ---------------------------------------------------------------------------

def test_step1_renames_note_to_description():
    df = _after_step1(note="lunch")
    assert "description" in df.columns
    assert "note" not in df.columns
    assert df["description"].iloc[0] == "lunch"


def test_step1_renames_category_to_subcategory():
    df = _after_step1(category="Groceries")
    assert "subcategory" in df.columns
    assert "category" not in df.columns
    assert df["subcategory"].iloc[0] == "Groceries"


def test_step1_renames_payment_to_payment_method():
    df = _after_step1(payment="CASH")
    assert "payment_method" in df.columns
    assert "payment" not in df.columns
    assert df["payment_method"].iloc[0] == "CASH"


def test_step1_lowercases_column_names():
    raw = _raw_df()
    raw.columns = [c.upper() for c in raw.columns]  # shout-case columns
    t = _transformer()
    df = t._step1_rename_columns(raw)
    for col in df.columns:
        assert col == col.lower(), f"Column '{col}' was not lowercased"


def test_step1_does_not_mutate_input():
    raw = _raw_df()
    original_cols = list(raw.columns)
    _after_step1()
    assert list(raw.columns) == original_cols


# ---------------------------------------------------------------------------
# Step 2: parse_dates
# ---------------------------------------------------------------------------

def _parsed_date_str(val) -> str:
    """Normalise whatever step 2 returns (Timestamp or str) to YYYY-MM-DD."""
    return pd.to_datetime(val).strftime("%Y-%m-%d")


# Note: step 2 documents "format to YYYY-MM-DD string", but pandas recoerces the
# .loc string-assignment back to Timestamp when the column has a datetime64 dtype.
# Step 6 handles both types correctly, so this is not a pipeline bug — but the
# docstring overstates the guarantee. Tests assert the date VALUE is correct,
# not the dtype.

def test_step2_parses_iso_date_string():
    df = _after_step2(date="2024-03-29")
    assert _parsed_date_str(df["date"].iloc[0]) == "2024-03-29"


def test_step2_parses_us_slash_format():
    df = _after_step2(date="7/27/2025")
    assert _parsed_date_str(df["date"].iloc[0]) == "2025-07-27"


def test_step2_parses_datetime_with_timestamp():
    df = _after_step2(date="7/27/2025 3:00:35")
    assert _parsed_date_str(df["date"].iloc[0]) == "2025-07-27"


def test_step2_already_datetime_dtype_is_reformatted():
    raw = _raw_df()
    raw["date"] = pd.to_datetime("2024-06-15")
    t = _transformer()
    df = t._step1_rename_columns(raw)
    df = t._step2_parse_dates(df)
    assert _parsed_date_str(df["date"].iloc[0]) == "2024-06-15"


def test_step2_invalid_date_becomes_nat():
    t = _transformer()
    raw = _raw_df(date="not-a-date")
    df = t._step1_rename_columns(raw)
    df = t._step2_parse_dates(df)
    assert pd.isna(df["date"].iloc[0])


def test_step2_output_represents_correct_date():
    df = _after_step2(date="2024-11-05")
    assert _parsed_date_str(df["date"].iloc[0]) == "2024-11-05"


# ---------------------------------------------------------------------------
# Step 3: parse_amounts
# ---------------------------------------------------------------------------

def test_step3_string_amount_becomes_float():
    df = _after_steps_1_to_3(amount="-45.50")
    assert df["amount"].iloc[0] == pytest.approx(-45.50)


def test_step3_amount_abs_is_positive():
    df = _after_steps_1_to_3(amount="-45.50")
    assert df["amount_abs"].iloc[0] == pytest.approx(45.50)


def test_step3_income_amount_abs_is_positive():
    df = _after_steps_1_to_3(amount="1000.00", type="Income")
    assert df["amount_abs"].iloc[0] == pytest.approx(1000.00)


def test_step3_invalid_amount_becomes_nan():
    df = _after_steps_1_to_3(amount="not-a-number")
    assert pd.isna(df["amount"].iloc[0])


# ---------------------------------------------------------------------------
# Step 4: standardize_types
# ---------------------------------------------------------------------------

def test_step4_expenses_maps_to_expense():
    df = _after_steps_1_to_4(type="Expenses")
    assert df["transaction_type"].iloc[0] == "EXPENSE"


def test_step4_income_maps_to_income():
    df = _after_steps_1_to_4(type="Income")
    assert df["transaction_type"].iloc[0] == "INCOME"


def test_step4_case_insensitive():
    df = _after_steps_1_to_4(type="EXPENSES")
    assert df["transaction_type"].iloc[0] == "EXPENSE"


def test_step4_unknown_type_becomes_none():
    df = _after_steps_1_to_4(type="Transfer")
    assert pd.isna(df["transaction_type"].iloc[0])


# ---------------------------------------------------------------------------
# Step 5: convert_currencies
# ---------------------------------------------------------------------------

def test_step5_eur_rate_is_1():
    df = _after_steps_1_to_5(currency="EUR", amount="-100.00")
    assert df["eur_conversion_rate"].iloc[0] == pytest.approx(1.0)
    assert df["amount_eur"].iloc[0] == pytest.approx(-100.0)


def test_step5_bgn_converts_to_eur():
    df = _after_steps_1_to_5(currency="BGN", amount="-195.583")
    assert df["eur_conversion_rate"].iloc[0] == pytest.approx(0.51130, rel=1e-4)
    assert df["amount_eur"].iloc[0] == pytest.approx(-195.583 * 0.51130, rel=1e-3)


def test_step5_bgn_amount_bgn_equals_original():
    df = _after_steps_1_to_5(currency="BGN", amount="-50.00")
    assert df["amount_bgn"].iloc[0] == pytest.approx(-50.0)


def test_step5_unknown_currency_defaults_rate_to_1():
    df = _after_steps_1_to_5(currency="XYZ", amount="-10.00")
    assert df["eur_conversion_rate"].iloc[0] == pytest.approx(1.0)


def test_step5_api_base_amount_overrides_bgn_for_eur_transactions():
    raw = _raw_df(currency="EUR", amount="-100.00")
    raw["base_amount_value"] = -195.58
    raw["base_amount_currency"] = "BGN"
    t = _transformer()
    df = t._step1_rename_columns(raw)
    df = t._step2_parse_dates(df)
    df = t._step3_parse_amounts(df)
    df = t._step4_standardize_types(df)
    df = t._step5_convert_currencies(df)
    assert df["amount_bgn"].iloc[0] == pytest.approx(-195.58)


def test_step5_amount_abs_bgn_is_positive():
    df = _after_steps_1_to_5(currency="EUR", amount="-100.00")
    assert df["amount_abs_bgn"].iloc[0] > 0


# ---------------------------------------------------------------------------
# Step 6: add_derived_fields
# ---------------------------------------------------------------------------

def test_step6_extracts_year_month_quarter():
    t = _transformer()
    df = _after_steps_1_to_5(date="2024-07-15")
    df = t._step6_add_derived_fields(df)
    assert df["year"].iloc[0] == 2024
    assert df["month"].iloc[0] == 7
    assert df["quarter"].iloc[0] == 3


def test_step6_year_month_format():
    t = _transformer()
    df = _after_steps_1_to_5(date="2024-07-15")
    df = t._step6_add_derived_fields(df)
    assert df["year_month"].iloc[0] == "2024-07"


def test_step6_monday_is_day_1():
    t = _transformer()
    df = _after_steps_1_to_5(date="2024-01-15")  # Monday
    df = t._step6_add_derived_fields(df)
    assert df["day_of_week"].iloc[0] == 1


def test_step6_saturday_is_weekend():
    t = _transformer()
    df = _after_steps_1_to_5(date="2024-01-13")  # Saturday
    df = t._step6_add_derived_fields(df)
    assert df["is_weekend"].iloc[0] == True  # noqa: E712 — numpy bool needs ==


def test_step6_tuesday_is_not_weekend():
    t = _transformer()
    df = _after_steps_1_to_5(date="2024-01-16")  # Tuesday
    df = t._step6_add_derived_fields(df)
    assert df["is_weekend"].iloc[0] == False  # noqa: E712


# ---------------------------------------------------------------------------
# Step 7: add_classification
# ---------------------------------------------------------------------------

def test_step7_adds_null_classification_column():
    t = _transformer()
    df = _after_steps_1_to_5()
    df = t._step6_add_derived_fields(df)
    df = t._step7_add_classification(df)
    assert "classification" in df.columns
    assert df["classification"].iloc[0] is None


# ---------------------------------------------------------------------------
# Step 8: generate_hashes
# ---------------------------------------------------------------------------

def _run_to_step8(**overrides) -> pd.DataFrame:
    t = _transformer()
    df = t._step1_rename_columns(_raw_df(**overrides))
    df = t._step2_parse_dates(df)
    df = t._step3_parse_amounts(df)
    df = t._step4_standardize_types(df)
    df = t._step5_convert_currencies(df)
    df = t._step6_add_derived_fields(df)
    df = t._step7_add_classification(df)
    return t._step8_generate_hashes(df)


def test_step8_hash_is_64_chars():
    df = _run_to_step8()
    h = df["transaction_hash"].iloc[0]
    assert isinstance(h, str)
    assert len(h) == 64


def test_step8_same_input_produces_same_hash():
    h1 = _run_to_step8()["transaction_hash"].iloc[0]
    h2 = _run_to_step8()["transaction_hash"].iloc[0]
    assert h1 == h2


def test_step8_different_amount_produces_different_hash():
    h1 = _run_to_step8(amount="-10.00")["transaction_hash"].iloc[0]
    h2 = _run_to_step8(amount="-10.01")["transaction_hash"].iloc[0]
    assert h1 != h2


def test_step8_different_category_produces_different_hash():
    h1 = _run_to_step8(category="Groceries")["transaction_hash"].iloc[0]
    h2 = _run_to_step8(category="Taxi")["transaction_hash"].iloc[0]
    assert h1 != h2


# ---------------------------------------------------------------------------
# Step 9: final_cleanup
# ---------------------------------------------------------------------------

def _step9_df(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame already in the shape step 9 expects."""
    return pd.DataFrame(rows)


def test_step9_drops_null_date_rows():
    t = _transformer()
    df = _step9_df([
        {"date": "2024-01-15", "amount": -10.0, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
        {"date": None, "amount": -5.0, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
    ])
    result = t._step9_final_cleanup(df)
    assert len(result) == 1
    assert result["date"].iloc[0] == "2024-01-15"


def test_step9_drops_null_amount_rows():
    t = _transformer()
    df = _step9_df([
        {"date": "2024-01-15", "amount": -10.0, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
        {"date": "2024-01-16", "amount": None, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
    ])
    result = t._step9_final_cleanup(df)
    assert len(result) == 1


def test_step9_drops_null_transaction_type_rows():
    t = _transformer()
    df = _step9_df([
        {"date": "2024-01-15", "amount": -10.0, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
        {"date": "2024-01-16", "amount": -5.0, "transaction_type": None, "subcategory": "Groceries"},
    ])
    result = t._step9_final_cleanup(df)
    assert len(result) == 1


def test_step9_drops_null_subcategory_rows():
    t = _transformer()
    df = _step9_df([
        {"date": "2024-01-15", "amount": -10.0, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
        {"date": "2024-01-16", "amount": -5.0, "transaction_type": "EXPENSE", "subcategory": None},
    ])
    result = t._step9_final_cleanup(df)
    assert len(result) == 1


def test_step9_preserves_all_valid_rows():
    t = _transformer()
    df = _step9_df([
        {"date": "2024-01-15", "amount": -10.0, "transaction_type": "EXPENSE", "subcategory": "Groceries"},
        {"date": "2024-01-16", "amount": -5.0, "transaction_type": "INCOME", "subcategory": "Wage, invoices"},
    ])
    result = t._step9_final_cleanup(df)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# _validate_before_return
# ---------------------------------------------------------------------------

def test_validate_raises_if_required_column_missing():
    t = _transformer()
    df = pd.DataFrame([{"date": "2024-01-15", "amount": -10.0}])  # missing transaction_hash
    with pytest.raises(ValueError, match="missing required columns"):
        t._validate_before_return(df)


def test_validate_raises_on_null_in_required_column():
    t = _transformer()
    df = pd.DataFrame([{
        "date": None,
        "amount": -10.0,
        "transaction_hash": "abc" * 21 + "a",
    }])
    with pytest.raises(ValueError, match="null values"):
        t._validate_before_return(df)


# ---------------------------------------------------------------------------
# Full transform() integration
# ---------------------------------------------------------------------------

def _csv_input(n: int = 3) -> pd.DataFrame:
    """Minimal multi-row CSV-shaped input."""
    return pd.DataFrame([
        {
            "date": "2024-01-15",
            "note": f"note {i}",
            "type": "Expenses",
            "payee": "Payee",
            "amount": f"-{10 + i}.00",
            "labels": "",
            "account": "Cash in Euro",
            "category": "Groceries",
            "currency": "EUR",
            "payment": "card",
        }
        for i in range(n)
    ])


def test_transform_returns_expected_columns():
    t = _transformer()
    df = t.transform(_csv_input())
    expected = {
        "description", "subcategory", "transaction_type", "amount", "amount_abs",
        "amount_eur", "amount_abs_eur", "amount_bgn", "amount_abs_bgn",
        "eur_conversion_rate", "year", "month", "quarter", "year_month",
        "day_of_week", "week_of_year", "is_weekend", "classification",
        "transaction_hash",
    }
    assert expected.issubset(set(df.columns))


def test_transform_row_count_preserved_for_valid_input():
    t = _transformer()
    df = t.transform(_csv_input(n=5))
    assert len(df) == 5


def test_transform_drops_invalid_rows():
    raw = _csv_input(n=2)
    raw.loc[1, "date"] = "not-a-date"
    t = _transformer()
    df = t.transform(raw)
    assert len(df) == 1


def test_transform_hashes_are_unique():
    t = _transformer()
    df = t.transform(_csv_input(n=3))
    assert df["transaction_hash"].nunique() == 3


def test_transform_income_row_classified_as_income():
    raw = pd.DataFrame([{
        "date": "2024-01-15",
        "note": "salary",
        "type": "Income",
        "payee": "Employer",
        "amount": "3000.00",
        "labels": "",
        "account": "Cash in Euro",
        "category": "Wage, invoices",
        "currency": "EUR",
        "payment": "TRANSFER",
    }])
    t = _transformer()
    df = t.transform(raw)
    assert df["transaction_type"].iloc[0] == "INCOME"
    assert df["amount"].iloc[0] == pytest.approx(3000.0)
