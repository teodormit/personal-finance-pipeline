"""
Unit tests for BudgetBakersExtractor._flatten_record.
"""

import pandas as pd
import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from extractors.budgetbakers_extractor import BudgetBakersExtractor


@pytest.fixture
def extractor():
    """Create extractor with mock token (won't make real API calls for _flatten_record)."""
    return BudgetBakersExtractor(api_token="test-token-no-real-calls")


def test_flatten_record_typical(extractor):
    """Sample API record dict, verify all fields extracted."""
    record = {
        "id": "285eb69c-a94c-4d56-ba5e-a82570de0701",
        "recordDate": "2026-02-05T00:00:00Z",
        "recordType": "income",
        "paymentType": "transfer",
        "note": "Refund",
        "payee": "",
        "payer": "Someone",
        "amount": {"value": 0.83, "currencyCode": "EUR"},
        "baseAmount": {"value": 1.65, "currencyCode": "BGN"},
        "category": {"id": "cat-1", "name": "Refunds"},
        "accountId": "77a90a1e-a336-489b-8c40-577f2e236dbb",
        "labels": [{"name": "tag1"}],
    }
    flat = extractor._flatten_record(record)
    assert flat["id"] == record["id"]
    assert flat["recordDate"] == "2026-02-05T00:00:00Z"
    assert flat["recordType"] == "income"
    assert flat["paymentType"] == "transfer"
    assert flat["note"] == "Refund"
    assert flat["amount_value"] == 0.83
    assert flat["amount_currency"] == "EUR"
    assert flat["base_amount_value"] == 1.65
    assert flat["base_amount_currency"] == "BGN"
    assert flat["category_id"] == "cat-1"
    assert flat["category_name"] == "Refunds"
    assert flat["accountId"] == "77a90a1e-a336-489b-8c40-577f2e236dbb"
    assert flat["labels"] == "tag1"


def test_flatten_record_missing_fields(extractor):
    """Verify graceful handling of nulls and missing fields."""
    record = {
        "id": "minimal",
        "recordDate": "2026-02-05",
    }
    flat = extractor._flatten_record(record)
    assert flat["id"] == "minimal"
    assert flat["recordDate"] == "2026-02-05"
    assert flat["recordType"] is None
    assert flat["amount_value"] is None
    assert flat["amount_currency"] is None
    assert flat["category_name"] is None
    assert flat["base_amount_value"] is None
    assert flat["base_amount_currency"] is None


def test_base_amount_parsing_dict(extractor):
    """Verify nested baseAmount dict is properly unpacked."""
    record = {
        "id": "x",
        "recordDate": "2026-02-05",
        "amount": {"value": -12.79, "currencyCode": "EUR"},
        "baseAmount": {"value": -12.79, "currencyCode": "BGN"},
    }
    flat = extractor._flatten_record(record)
    assert flat["base_amount_value"] == -12.79
    assert flat["base_amount_currency"] == "BGN"


def test_base_amount_parsing_string(extractor):
    """Verify baseAmount as string (e.g. from CSV) is parsed."""
    record = {
        "id": "x",
        "recordDate": "2026-02-05",
        "amount": {"value": -12.79, "currencyCode": "EUR"},
        "baseAmount": "{'value': -12.79, 'currencyCode': 'BGN'}",
    }
    flat = extractor._flatten_record(record)
    assert flat["base_amount_value"] == -12.79
    assert flat["base_amount_currency"] == "BGN"
