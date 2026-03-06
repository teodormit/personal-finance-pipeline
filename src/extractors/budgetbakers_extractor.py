"""
BudgetBakers Wallet API Extractor
=================================

Extracts transaction records from the BudgetBakers Wallet REST API.
Requires Wallet Premium and API token from web.budgetbakers.com/settings/apiTokens

API Reference: https://rest.budgetbakers.com/wallet/reference
"""

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

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

    def _record_type_to_app_type(self, record_type: str) -> str:
        """Map API recordType (income/expense) to app export type (Income/Expenses)."""
        return "Income" if record_type == "income" else "Expenses"

    def _payment_type_to_app_format(self, payment_type: Optional[str]) -> str:
        """Map API paymentType to app export format (uppercase, e.g. CASH, TRANSFER)."""
        if not payment_type:
            return ""
        # API: cash, debit_card, credit_card, transfer, voucher, mobile_payment, web_payment
        mapping = {
            "cash": "CASH",
            "debit_card": "DEBIT_CARD",
            "credit_card": "CREDIT_CARD",
            "transfer": "TRANSFER",
            "voucher": "VOUCHER",
            "mobile_payment": "MOBILE_PAYMENT",
            "web_payment": "WEB_PAYMENT",
        }
        return mapping.get(payment_type.lower(), payment_type.upper())

    def _record_to_row(
        self,
        record: dict,
        accounts: dict,
    ) -> dict:
        """Convert single API Record to row dict matching ExpenseTransformer input."""
        amount_obj = record.get("amount") or {}
        amount_val = amount_obj.get("value", 0)
        currency = amount_obj.get("currencyCode", "EUR")
        record_type = record.get("recordType", "expense")
        # For expenses, amount is typically negative in app export
        if record_type == "expense" and amount_val > 0:
            amount_val = -amount_val
        elif record_type == "income" and amount_val < 0:
            amount_val = abs(amount_val)

        category_obj = record.get("category")
        category_name = category_obj.get("name", "") if category_obj else ""

        payee = record.get("payee") or record.get("payer") or ""

        labels_arr = record.get("labels") or []
        labels_str = ", ".join(l.get("name", "") for l in labels_arr) if labels_arr else ""

        account_id = record.get("accountId", "")
        account_name = accounts.get(account_id, account_id or "")

        record_date = record.get("recordDate", "")
        if isinstance(record_date, str) and "T" in record_date:
            record_date = record_date.split("T")[0]

        return {
            "date": record_date,
            "note": record.get("note") or "",
            "type": self._record_type_to_app_type(record_type),
            "payee": payee,
            "amount": amount_val,
            "labels": labels_str,
            "account": account_name,
            "category": category_name,
            "currency": currency,
            "payment": self._payment_type_to_app_format(record.get("paymentType")),
            "record_id": record.get("id"),  # For lineage, optional
        }

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

        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")

        print(f"\n[EXTRACT] BudgetBakers API: {date_from_str} to {date_to_str}")

        accounts = self._get_accounts()
        print(f"  Loaded {len(accounts)} accounts")

        all_records = []
        offset = 0

        while True:
            data = self._fetch_records_page(date_from_str, date_to_str, offset)
            records = data.get("records", [])
            if not records:
                break
            for rec in records:
                row = self._record_to_row(rec, accounts)
                all_records.append(row)
            print(f"  Fetched {len(all_records)} records so far...")
            if data.get("nextOffset") is None:
                break
            offset = data["nextOffset"]
            time.sleep(0.2)  # Gentle rate limiting

        df = pd.DataFrame(all_records)

        # Drop record_id from output (ExpenseTransformer doesn't need it)
        if "record_id" in df.columns:
            df = df.drop(columns=["record_id"])

        print(f"  Total extracted: {len(df):,} records")
        return df
