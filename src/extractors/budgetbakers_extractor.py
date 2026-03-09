"""
BudgetBakers Wallet API Extractor
=================================

Extracts transaction records from the BudgetBakers Wallet REST API.
Requires Wallet Premium and API token from web.budgetbakers.com/settings/apiTokens

API Reference: https://rest.budgetbakers.com/wallet/reference
"""

import ast
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from extractors.api_field_mapper import map_raw_to_transformer_input

load_dotenv()

# API base URL
API_BASE = "https://rest.budgetbakers.com/wallet"
MAX_RECORDS_PER_PAGE = 200
MAX_DAYS_PER_REQUEST = 365  # API allows up to 370
RETRY_AFTER_409_SECONDS = 60  # Wait before retry on 409 Conflict


class BudgetBakersExtractor:
    """
    Extracts transaction records from BudgetBakers Wallet API.
    Output DataFrame matches ExpenseTransformer input schema.
    """

    def __init__(self, api_token: Optional[str] = None):
        """
        Initialize extractor.

        Args:
            api_token: API token. If None, reads from BUDGETBAKERS_API_TOKEN env var.
        """
        self.api_token = api_token or os.getenv("BUDGETBAKERS_API_TOKEN")
        if not self.api_token:
            raise ValueError(
                "BudgetBakers API token required. Set BUDGETBAKERS_API_TOKEN in .env "
                "or pass api_token to constructor."
            )
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        })
        self._accounts_cache: Optional[dict] = None
        self._categories_cache: Optional[dict] = None

    def _get_accounts(self) -> dict:
        """Fetch accounts and cache. Maps accountId -> account name."""
        if self._accounts_cache is not None:
            return self._accounts_cache
        accounts = {}
        offset = 0
        while True:
            resp = self.session.get(
                f"{API_BASE}/v1/api/accounts",
                params={"limit": MAX_RECORDS_PER_PAGE, "offset": offset},
            )
            resp.raise_for_status()
            data = resp.json()
            for acc in data.get("accounts", []):
                accounts[acc["id"]] = acc.get("name", "Unknown")
            if data.get("nextOffset") is None:
                break
            offset = data["nextOffset"]
        self._accounts_cache = accounts
        return accounts

## Not sure if this will work
    def _get_categories(self) -> dict:
        """Fetch categories and cache. Maps category_id -> category info (name, parent, etc.)."""
        if self._categories_cache is not None:
            return self._categories_cache
        categories = {}
        offset = 0
        while True:
            resp = self.session.get(
                f"{API_BASE}/v1/api/categories",
                params={"limit": MAX_RECORDS_PER_PAGE, "offset": offset},
            )
            resp.raise_for_status()
            data = resp.json()
            for cat in data.get("categories", []):
                cat_id = cat.get("id")
                if cat_id:
                    categories[cat_id] = {
                        "name": cat.get("name", "Unknown"),
                        "parentId": cat.get("parentId"),
                        "parentName": None,  # Resolved below if needed
                    }
            if data.get("nextOffset") is None:
                break
            offset = data["nextOffset"]
        # Resolve parent names for hierarchy
        for cat_id, info in categories.items():
            parent_id = info.get("parentId")
            if parent_id and parent_id in categories:
                info["parentName"] = categories[parent_id].get("name")
        self._categories_cache = categories
        return categories

    def _fetch_records_page(
        self,
        date_from: str,
        date_to: str,
        offset: int = 0,
    ) -> dict:
        """Fetch one page of records. Handles 409 Conflict with retry."""
        url = f"{API_BASE}/v1/api/records"
        # API supports repeated recordDate for range: gte and lt
        params = [
            ("recordDate", f"gte.{date_from}"),
            ("recordDate", f"lt.{date_to}"),
            ("limit", MAX_RECORDS_PER_PAGE),
            ("offset", offset),
            ("sortBy", "+recordDate"),
        ]
        for attempt in range(5):
            resp = self.session.get(url, params=params)
            if resp.status_code == 409:
                # Initial sync in progress
                retry_mins = resp.json().get("retry_after_minutes", 5)
                wait = retry_mins * 60
                print(f"  API sync in progress. Retrying in {retry_mins} min...")
                time.sleep(min(wait, RETRY_AFTER_409_SECONDS))
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError("API returned 409 Conflict repeatedly. Try again later.")

    def _flatten_record(self, record: dict) -> dict:
        """Flatten a single API record's nested objects into a flat dict.
        No business logic -- just structural flattening for inspection.
        """
        amount_obj = record.get("amount") or {}
        category_obj = record.get("category") or {}
        labels_arr = record.get("labels") or []

        flat = {
            "id": record.get("id"),
            "recordDate": record.get("recordDate"),
            "recordType": record.get("recordType"),
            "paymentType": record.get("paymentType"),
            "note": record.get("note"),
            "payee": record.get("payee"),
            "payer": record.get("payer"),
            "amount_value": amount_obj.get("value"),
            "amount_currency": amount_obj.get("currencyCode"),
            "category_id": category_obj.get("id"),
            "category_name": category_obj.get("name"),
            "accountId": record.get("accountId"),
            "labels": ", ".join(lbl.get("name", "") for lbl in labels_arr) if labels_arr else "",
        }

        # Parse baseAmount: only handle dict, otherwise input data as is
        base_amount = record.get("baseAmount")
        if isinstance(base_amount, dict):
            flat["base_amount_value"] = base_amount.get("value")
            flat["base_amount_currency"] = base_amount.get("currencyCode")
        else:
            flat["base_amount_value"] = base_amount
            flat["base_amount_currency"] = None

        # Keep any other top-level keys we haven't explicitly handled
        known_keys = {
            "id", "recordDate", "recordType", "paymentType", "note",
            "payee", "payer", "amount", "category", "accountId", "labels",
            "baseAmount",
        }
        for key in record:
            if key not in known_keys:
                flat[key] = record[key]

        return flat

    def extract_raw(
        self,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Extract raw API records with only structural flattening.
        Resolves account names and category hierarchy from API lookups.
        """
        if date_to is None:
            date_to = datetime.now()
        if date_from is None:
            date_from = date_to - timedelta(days=30)

        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")

        print(f"\n[EXTRACT RAW] BudgetBakers API: {date_from_str} to {date_to_str}")

        accounts = self._get_accounts()
        print(f"  Loaded {len(accounts)} accounts")
        categories = self._get_categories()
        print(f"  Loaded {len(categories)} categories")

        all_rows = []
        offset = 0

        while True:
            data = self._fetch_records_page(date_from_str, date_to_str, offset)
            records = data.get("records", [])
            if not records:
                break
            for rec in records:
                all_rows.append(self._flatten_record(rec))
            print(f"  Fetched {len(all_rows)} records so far...")
            if data.get("nextOffset") is None:
                break
            offset = data["nextOffset"]
            time.sleep(0.2)

        df = pd.DataFrame(all_rows)

        # Resolve account names
        df["account_name"] = df["accountId"].map(
            lambda x: accounts.get(x, x) if pd.notna(x) else ""
        )

        # Add category parent for hierarchy exploration
        def _parent_name(cat_id):
            if pd.isna(cat_id):
                return None
            info = categories.get(cat_id)
            return info.get("parentName") if info else None

        df["category_parent_name"] = df["category_id"].map(_parent_name)

        print(f"  Total extracted: {len(df):,} raw records")
        return df

    def extract(
        self,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Extract records from API for the given date range.

        Args:
            date_from: Start date (inclusive). Default: 1 year ago.
            date_to: End date (exclusive). Default: today.

        Returns:
            DataFrame with columns: date, note, type, payee, amount, labels,
            account, category, currency, payment (matching ExpenseTransformer input).
        """
        if date_to is None:
            date_to = datetime.now()
        if date_from is None:
            date_from = date_to - timedelta(days=365)

        print(f"\n[EXTRACT] BudgetBakers API: {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")

        raw_df = self.extract_raw(date_from=date_from, date_to=date_to)
        if raw_df.empty:
            print("  No records to map")
            return raw_df

        # Map raw API DataFrame to ExpenseTransformer via the api_field_mapper module
        df = map_raw_to_transformer_input(raw_df)
        print(f"  Total extracted: {len(df):,} records (mapped for ExpenseTransformer)")
        return df
