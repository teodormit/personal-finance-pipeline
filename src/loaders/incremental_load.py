"""
Incremental Data Load Script
Loads only new transactions without truncating silver.
Uses transaction_hash for deduplication.
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

_src_root = Path(__file__).resolve().parent.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from extractors.budgetbakers_extractor import BudgetBakersExtractor
from loaders.base_loader import BaseLoader

# Account filter presets: allowed accounts + optional per-account end dates (YYYY-MM-DD inclusive)
ACCOUNT_FILTER_PRESETS = {
    "eur": {
        "allowed_accounts": ["UniCredit Bulbank - 1522449108EUR", "Cash in Euro"],
        "account_end_dates": {},
    },
    "bgn_final": {
        "allowed_accounts": ["UniCredit Bulbank - 1522449108BGN", "Cash"],
        "account_end_dates": {
            "UniCredit Bulbank - 1522449108BGN": "2025-12-22",
            "Cash": "2025-12-31",
        },
    },
}


class IncrementalDataLoader(BaseLoader):
    """Append-only loader. Dedupes against silver.transactions by transaction_hash."""

    header_text = "PERSONAL FINANCE PIPELINE - INCREMENTAL LOAD"
    summary_text = "INCREMENTAL LOAD COMPLETE"
    created_by = "incremental_load_script"

    def __init__(
        self,
        source: str = "api",
        file_path: str = None,
        account_filter: Optional[str] = "eur",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ):
        """
        Args:
            source: 'api' for BudgetBakers API, 'file' for local file
            file_path: Path to source file (required when source='file')
            account_filter: 'eur' (default) or 'bgn_final'. 'all' = no filter.
            from_date: Override start date for API extraction (YYYY-MM-DD). Default: day after last silver.
            to_date: Override end date for API extraction (YYYY-MM-DD). Default: today.
        """
        self.source = source
        self.file_path = Path(file_path) if file_path else None
        self.account_filter = account_filter
        self.from_date = from_date
        self.to_date = to_date

        source_file_name = (
            "budgetbakers_api"
            if source == "api"
            else (self.file_path.name if self.file_path else "unknown")
        )
        super().__init__(source_file_name=source_file_name)

    # ------------------------------------------------------------------ #
    # Extract
    # ------------------------------------------------------------------ #
    def _extract(self) -> pd.DataFrame:
        if self.source == "api":
            return self._extract_from_api()
        if not self.file_path:
            raise FileNotFoundError("file_path is required when source='file'")
        return self._extract_from_file(self.file_path)

    def _extract_from_api(self) -> pd.DataFrame:
        """Pull from BudgetBakers API. Watermark = max(silver.transaction_date) + 1d."""
        last_date = self._get_last_silver_date()
        date_to = (
            datetime.strptime(self.to_date, "%Y-%m-%d")
            if self.to_date
            else datetime.now()
        )
        date_from = (
            datetime.strptime(self.from_date, "%Y-%m-%d")
            if self.from_date
            else (
                (last_date + timedelta(days=1))
                if last_date
                else (date_to - timedelta(days=365))
            )
        )
        if date_from >= date_to:
            print("\n[EXTRACT] No new date range - silver is up to date.")
            return pd.DataFrame()

        df = BudgetBakersExtractor().extract(date_from=date_from, date_to=date_to)
        self.run_stats["rows_extracted"] = len(df)
        return df

    def _get_last_silver_date(self) -> Optional[datetime]:
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(transaction_date) FROM silver.transactions")
            row = cursor.fetchone()
            val = row[0] if row else None
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            return datetime.combine(val, datetime.min.time())

    # ------------------------------------------------------------------ #
    # Post-transform: account filter
    # ------------------------------------------------------------------ #
    def _post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return apply_account_filter(df, self.account_filter)

    # ------------------------------------------------------------------ #
    # Silver load: dedupe by hash
    # ------------------------------------------------------------------ #
    def _load_silver(self, df: pd.DataFrame, conn) -> None:
        print("\n[LOAD SILVER] Inserting new records to silver.transactions...")

        cursor = conn.cursor()
        cursor.execute("SELECT transaction_hash FROM silver.transactions")
        existing_hashes = {row[0] for row in cursor.fetchall()}

        new_df = df[~df["transaction_hash"].isin(existing_hashes)].copy()
        skipped = len(df) - len(new_df)
        self.run_stats["rows_skipped_duplicates"] = skipped

        if len(new_df) == 0:
            print(f"  All {len(df)} records already exist (duplicates). Skipped.")
            self.run_stats["rows_loaded_silver"] = 0
            self._new_expense_hashes = set()
            return

        # Track new EXPENSE hashes for targeted gold refresh after commit.
        expense_mask = (
            new_df["transaction_type"].astype(str).str.upper() == "EXPENSE"
        )
        self._new_expense_hashes = set(
            new_df.loc[expense_mask, "transaction_hash"].astype(str)
        )

        silver_df = self._prepare_silver_df(new_df)
        rows = self._bulk_insert(silver_df, "silver", "transactions", conn)
        self.run_stats["rows_loaded_silver"] = rows
        print(f"  Inserted {rows:,} new rows (skipped {skipped:,} duplicates)")

    # ------------------------------------------------------------------ #
    # Gold refresh: target only the new expense hashes
    # ------------------------------------------------------------------ #
    # `_new_expense_hashes` is set inside `_load_silver`, so it may not exist
    # on early-return paths (no new data, no rows post-filter). The
    # `getattr(..., None) or set()` defaults to "no targets", which makes the
    # gold refreshers no-op. Empty target set != full refresh.
    def _refresh_gold_notability(self):
        super()._refresh_gold_notability(
            hashes=getattr(self, "_new_expense_hashes", None) or set()
        )

    def _refresh_gold_save_potential(self):
        super()._refresh_gold_save_potential(
            hashes=getattr(self, "_new_expense_hashes", None) or set()
        )


def apply_account_filter(
    df: pd.DataFrame, account_filter: Optional[str]
) -> pd.DataFrame:
    """Filter DataFrame by account preset. Shared by loader and inspect script."""
    if account_filter in (None, "all"):
        return df

    preset = ACCOUNT_FILTER_PRESETS.get(account_filter)
    if preset is None:
        print(
            f"\n[ACCOUNT FILTER] Unknown preset '{account_filter}', skipping filter."
        )
        return df

    allowed = set(preset["allowed_accounts"])
    end_dates = preset.get("account_end_dates", {})

    if "account" not in df.columns or "date" not in df.columns:
        print(
            "\n[ACCOUNT FILTER] Missing 'account' or 'date' column, skipping filter."
        )
        return df

    dates = pd.to_datetime(df["date"], errors="coerce")
    mask_allowed = df["account"].astype(str).str.strip().isin(allowed)

    mask_date_ok = pd.Series(True, index=df.index)
    for acc, end_str in end_dates.items():
        end_ts = pd.Timestamp(end_str)
        acc_mask = df["account"].astype(str).str.strip() == acc
        mask_date_ok = mask_date_ok & (~acc_mask | (dates <= end_ts))

    mask = mask_allowed & mask_date_ok
    filtered = df[mask].copy()
    dropped = len(df) - len(filtered)
    print(
        f"\n[ACCOUNT FILTER] Preset '{account_filter}': "
        f"kept {len(filtered):,} of {len(df):,} rows (dropped {dropped:,})"
    )
    return filtered


def main():
    parser = argparse.ArgumentParser(description="Incremental load of expense data")
    parser.add_argument(
        "--source",
        choices=["api", "file"],
        default="api",
        help="Extract from API or file",
    )
    parser.add_argument(
        "--file", help="Path to source file (required when --source=file)"
    )
    parser.add_argument(
        "--account-filter",
        choices=["eur", "bgn_final"],
        default="eur",
        help="Account filter preset (default: eur)",
    )
    parser.add_argument(
        "--from-date",
        help="Start date for API extraction (YYYY-MM-DD). Default: day after last silver",
    )
    parser.add_argument(
        "--to-date",
        help="End date for API extraction (YYYY-MM-DD). Default: today",
    )
    args = parser.parse_args()

    if args.source == "file" and not args.file:
        parser.error("--file is required when --source=file")

    loader = IncrementalDataLoader(
        source=args.source,
        file_path=args.file,
        account_filter=args.account_filter,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    success = loader.load()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
