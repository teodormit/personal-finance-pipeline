"""
API Field Mapper
================

Pure functions to map raw BudgetBakers API output to ExpenseTransformer input schema.
Testable in tests/test_api_field_mapper.py
"""

import pandas as pd


def map_record_types(series: pd.Series) -> pd.Series:
    """Map API recordType (income/expense/transfer) to app export type (Income/Expenses/Transfer)."""
    mapping = {
        "income": "Income",
        "expense": "Expenses",
        "expenses": "Expenses",
        "transfer": "Transfer",
    }
    return series.fillna("expense").astype(str).str.lower().map(
        lambda x: mapping.get(x, "Expenses")
    )


def map_payment_types(series: pd.Series) -> pd.Series:
    """Map API paymentType to app export format (uppercase, e.g. CASH, TRANSFER)."""
    mapping = {
        "cash": "CASH",
        "debit_card": "DEBIT_CARD",
        "credit_card": "CREDIT_CARD",
        "transfer": "TRANSFER",
        "voucher": "VOUCHER",
        "mobile_payment": "MOBILE_PAYMENT",
        "web_payment": "WEB_PAYMENT",
    }

    def _map(val):
        if pd.isna(val):
            return ""
        return mapping.get(str(val).lower(), str(val).upper())

    return series.map(_map)


def normalize_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """Apply sign convention: expenses negative, income positive (matching CSV export format)."""
    df = df.copy()
    record_type_col = "recordType" if "recordType" in df.columns else "type"
    amount_col = "amount_value" if "amount_value" in df.columns else "amount"

    if record_type_col not in df.columns or amount_col not in df.columns:
        return df

    record_type = df[record_type_col].fillna("expense").astype(str).str.lower()
    amount = df[amount_col].astype(float)

    # For expenses: positive amount should become negative
    # For income: negative amount should become positive
    mask_expense = record_type == "expense"
    mask_income = record_type == "income"
    df[amount_col] = amount
    df.loc[mask_expense & (amount > 0), amount_col] = -amount[mask_expense & (amount > 0)]
    df.loc[mask_income & (amount < 0), amount_col] = amount[mask_income & (amount < 0)].abs()

    return df


def rename_to_transformer_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Rename API columns to what ExpenseTransformer expects."""
    rename_map = {
        "id": "source_record_id",        
        "recordDate": "date",
        "recordDateTimestamp": "date_time",
        "note": "note",
        "recordType": "type",
        "category_name": "category",
        "category_id": "category_id",
        "account_name": "account",
        "accountId": "account_id",
        "amount_value": "amount",
        "amount_currency": "currency",
        "paymentType": "payment",
        "payee": "payee",
        "payer": "payer",
        "labels": "labels"
    }
    actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=actual_rename)


def map_raw_to_transformer_input(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw API DataFrame to ExpenseTransformer input schema.
    Single entry point: calls all mapper functions in sequence.
    """
    if raw_df.empty:
        return raw_df.copy()

    df = raw_df.copy()

    # Normalize amounts first (uses recordType, amount_value)
    df = normalize_amounts(df)

    # Map record types
    if "recordType" in df.columns:
        df["recordType"] = map_record_types(df["recordType"])

    # Map payment types
    if "paymentType" in df.columns:
        df["paymentType"] = map_payment_types(df["paymentType"])

    # Extract date part from recordDate (handles string "2026-02-05T00:00:00Z" or datetime)
    if "recordDate" in df.columns:
        df["recordDateTimestamp"] = df["recordDate"]
        if pd.api.types.is_datetime64_any_dtype(df["recordDate"]):
            df["recordDate"] = df["recordDate"].dt.strftime("%Y-%m-%d")
        else:
            df["recordDate"] = df["recordDate"].astype(str).str.split("T").str[0]

    # Coalesce payee | payer for transformer (income uses payer, expense uses payee)
    if "payee" in df.columns or "payer" in df.columns:
        payee = df.get("payee", pd.Series(dtype=object)).fillna("")
        payer = df.get("payer", pd.Series(dtype=object)).fillna("")
        df["payee"] = payee.where(payee != "", payer)

    # Rename to transformer schema
    df = rename_to_transformer_schema(df)

    # Select only columns ExpenseTransformer expects (drop extras like id, category_id, etc.)
    expected_cols = [
        "source_record_id", "date", "date_time", "note", "type", "payee", "payer", "amount", "labels",
        "account", "category", "currency", "payment", "category_id", "account_id",
    ]
    output_cols = [c for c in expected_cols if c in df.columns]
    return df[output_cols].copy()
