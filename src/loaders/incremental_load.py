"""
Incremental Data Load Script
Loads only new transactions without truncating silver.
Uses transaction_hash for deduplication.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import uuid
import argparse
import os

_src_root = Path(__file__).resolve().parent.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

try:
    from utils.db_connector import get_db_connector
except Exception:
    from utils.db_connector import DatabaseConnection

    def get_db_connector():
        return DatabaseConnection()

from transformers.expense_transformer import ExpenseTransformer
from extractors.budgetbakers_extractor import BudgetBakersExtractor

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


class IncrementalDataLoader:
    """Handles incremental loading - appends new transactions without truncating silver."""

    def __init__(
        self,
        source: str = "api",
        file_path: str = None,
        account_filter: Optional[str] = "eur",
    ):
        """
        Initialize loader.

        Args:
            source: 'api' for BudgetBakers API, 'file' for local file
            file_path: Path to source file (required when source='file')
            account_filter: 'eur' (default) or 'bgn_final'. 'all' = no filter.
        """
        self.source = source
        self.file_path = Path(file_path) if file_path else None
        self.account_filter = account_filter
        self.db = get_db_connector()
        self.transformer = ExpenseTransformer()
        self.batch_id = uuid.uuid4()
        self.run_stats = {
            "run_id": self.batch_id,
            "start_time": datetime.now(),
            "source_file": "budgetbakers_api" if source == "api" else (self.file_path.name if self.file_path else "unknown"),
            "file_size_bytes": None,
            "rows_extracted": 0,
            "rows_staged": 0,
            "rows_loaded_bronze": 0,
            "rows_loaded_silver": 0,
            "rows_skipped_duplicates": 0,
            "status": "RUNNING",
        }

    def load(self) -> bool:
        """Execute the incremental load pipeline.

        Staging, bronze, and silver are loaded inside a single DB transaction.
        If any layer fails the entire batch is rolled back so the layers stay
        consistent.  The metadata log is written on a separate connection so it
        persists even after a rollback.
        """
        print("\n" + "=" * 70)
        print("PERSONAL FINANCE PIPELINE - INCREMENTAL LOAD")
        print("=" * 70)
        print(f"Batch ID: {self.batch_id}")
        print(f"Source: {self.source}")
        print(f"Started: {self.run_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        try:
            raw_df = self._extract()
            if raw_df is None or len(raw_df) == 0:
                print("\n  No new data to load.")
                self.run_stats["status"] = "SUCCESS"
                self._log_pipeline_run()
                return True

            transformed_df = self._transform(raw_df)
            if len(transformed_df) == 0:
                print("\n  No valid records after transformation.")
                self.run_stats["status"] = "SUCCESS"
                self._log_pipeline_run()
                return True

            transformed_df = self._apply_account_filter(transformed_df)
            if len(transformed_df) == 0:
                print("\n  No records after account filter.")
                self.run_stats["status"] = "SUCCESS"
                self._log_pipeline_run()
                return True

            # Single transaction for all three layers
            conn = self.db.connect()
            try:
                self._load_staging(transformed_df, conn)
                self._load_bronze(transformed_df, conn)
                self._load_silver(transformed_df, conn)
                conn.commit()
                print("\n  All layers committed in a single transaction.")
            except Exception:
                conn.rollback()
                print("\n  Transaction rolled back - no partial data written.")
                raise
            finally:
                conn.close()

            self.run_stats["status"] = "SUCCESS"
            self._log_pipeline_run()
            self._display_summary()
            return True

        except Exception as e:
            self.run_stats["status"] = "FAILED"
            self.run_stats["error_message"] = str(e)
            print(f"\nPipeline failed: {str(e)}")
            try:
                self._log_pipeline_run()
            except Exception:
                pass
            raise

    def _get_last_silver_date(self) -> Optional[datetime]:
        """Get the max transaction_date from silver.transactions."""
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

    def _extract(self) -> pd.DataFrame:
        """Extract data from API or file."""
        if self.source == "api":
            return self._extract_from_api()
        return self._extract_from_file()

    def _extract_from_api(self) -> pd.DataFrame:
        """Extract from BudgetBakers API for date range after last silver date."""
        last_date = self._get_last_silver_date()
        date_to = datetime.now()
        date_from = (last_date + timedelta(days=1)) if last_date else (date_to - timedelta(days=365))
        if date_from >= date_to:
            print("\n[EXTRACT] No new date range - silver is up to date.")
            return pd.DataFrame()

        extractor = BudgetBakersExtractor()
        df = extractor.extract(date_from=date_from, date_to=date_to)
        self.run_stats["rows_extracted"] = len(df)
        return df

    def _extract_from_file(self) -> pd.DataFrame:
        """Extract from local file."""
        if not self.file_path or not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        self.run_stats["file_size_bytes"] = self.file_path.stat().st_size
        ext = self.file_path.suffix.lower()
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(self.file_path)
        elif ext == ".csv":
            df = pd.read_csv(self.file_path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        self.run_stats["rows_extracted"] = len(df)
        print(f"\n[EXTRACT] Read {len(df):,} rows from {self.file_path.name}")
        return df

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform using ExpenseTransformer."""
        print("\n[TRANSFORM] Applying transformations...")
        result = self.transformer.transform(df)
        print(f"  Output: {len(result):,} rows")
        return result

    def _apply_account_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter by allowed accounts and optional per-account end dates. Returns filtered DataFrame."""
        return apply_account_filter(df, self.account_filter)


def apply_account_filter(df: pd.DataFrame, account_filter: Optional[str]) -> pd.DataFrame:
    """Filter DataFrame by account preset. Shared by loader and inspect script."""
    if account_filter in (None, "all"):
        return df

    preset = ACCOUNT_FILTER_PRESETS.get(account_filter)
    if preset is None:
        print(f"\n[ACCOUNT FILTER] Unknown preset '{account_filter}', skipping filter.")
        return df

    allowed = set(preset["allowed_accounts"])
    end_dates = preset.get("account_end_dates", {})

    if "account" not in df.columns or "date" not in df.columns:
        print("\n[ACCOUNT FILTER] Missing 'account' or 'date' column, skipping filter.")
        return df

    # Ensure date is comparable
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
    print(f"\n[ACCOUNT FILTER] Preset '{account_filter}': kept {len(filtered):,} of {len(df):,} rows (dropped {dropped:,})")
    return filtered

    def _load_staging(self, df: pd.DataFrame, conn):
        """Load to staging (truncate first)."""
        print("\n[LOAD STAGING] Loading to staging.raw_transactions...")
        cols = ["date", "description", "type", "payee", "amount", "labels", "account", "subcategory", "currency"]
        payment_col = "payment_method" if "payment_method" in df.columns else "payment_type"
        cols.append(payment_col)
        cols = [c for c in cols if c in df.columns]
        staging_df = df[cols].copy()
        staging_df = staging_df.rename(columns={payment_col: "payment", "description": "note", "subcategory": "category"})
        staging_df["source_file"] = self.run_stats["source_file"]
        staging_df["batch_id"] = str(self.batch_id)
        staging_df["loaded_at"] = datetime.now()
        staging_df["source_row_number"] = range(1, len(staging_df) + 1)

        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE staging.raw_transactions;")
        rows = self._bulk_insert(staging_df, "staging", "raw_transactions", conn)
        self.run_stats["rows_staged"] = rows
        print(f"  Loaded {rows:,} rows to staging")

    def _load_bronze(self, df: pd.DataFrame, conn):
        """Append to bronze (immutable)."""
        print("\n[LOAD BRONZE] Appending to bronze.transactions_raw...")
        bronze_df = df.copy()
        bronze_df = bronze_df.rename(columns={
            "date": "transaction_date",
            "note": "description",
            "payee": "payee",
            "amount": "amount",
            "labels": "labels",
            "account": "account_name",
            "subcategory": "subcategory",
            "currency": "currency",
        })
        payment_col = "payment_method" if "payment_method" in bronze_df.columns else "payment_type"
        bronze_df = bronze_df.rename(columns={payment_col: "payment_method"})
        bronze_df["source_file"] = self.run_stats["source_file"]
        bronze_df["source_row_number"] = range(1, len(bronze_df) + 1)
        bronze_df["ingestion_timestamp"] = datetime.now()
        bronze_df["ingestion_batch_id"] = str(self.batch_id)
        bronze_df["has_quality_issues"] = False

        bronze_columns = [
            "transaction_date", "description", "transaction_type", "payee",
            "amount", "labels", "account_name", "subcategory", "currency",
            "payment_method", "source_file", "source_row_number",
            "ingestion_timestamp", "ingestion_batch_id", "has_quality_issues",
        ]
        bronze_df = bronze_df[[c for c in bronze_columns if c in bronze_df.columns]]
        rows = self._bulk_insert(bronze_df, "bronze", "transactions_raw", conn)
        self.run_stats["rows_loaded_bronze"] = rows
        print(f"  Appended {rows:,} rows to bronze")

    def _load_silver(self, df: pd.DataFrame, conn):
        """Insert only new records (deduplicate by transaction_hash). Does NOT truncate."""
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
            self._update_category_mapping(conn)
            return

        silver_df = new_df.copy()
        rename_map = {
            "date": "transaction_date",
            "account": "account_name",
        }
        if "note" in silver_df.columns:
            rename_map["note"] = "description"
        if "payment_type" in silver_df.columns and "payment_method" not in silver_df.columns:
            rename_map["payment_type"] = "payment_method"
        silver_df = silver_df.rename(columns=rename_map)
        silver_df["created_at"] = datetime.now()
        silver_df["created_by"] = "incremental_load_script"
        silver_df["source_raw_id"] = None

        silver_columns = [
            "transaction_hash", "transaction_date", "transaction_type",
            "amount", "amount_abs", "currency",
            "amount_eur", "amount_abs_eur", "eur_conversion_rate",
            "amount_bgn", "amount_abs_bgn",
            "source_record_id", "category_id",
            "description", "payee", "subcategory",
            "account_name", "payment_method", "labels",
            "year", "month", "quarter", "year_month",
            "day_of_week", "week_of_year", "is_weekend",
            "source_raw_id", "created_at", "created_by",
            "classification",
        ]
        silver_df = silver_df[[c for c in silver_columns if c in silver_df.columns]]
        rows = self._bulk_insert(silver_df, "silver", "transactions", conn)
        self.run_stats["rows_loaded_silver"] = rows
        print(f"  Inserted {rows:,} new rows (skipped {skipped:,} duplicates)")

        self._update_category_mapping(conn)

    def _update_category_mapping(self, conn):
        """Update category and classification from category_mapping for new rows."""
        print("  Updating category hierarchy...")
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE silver.transactions t
            SET category = cm.category, classification = cm.classification
            FROM silver.category_mapping cm
            WHERE t.subcategory = cm.subcategory
              AND (t.category IS NULL OR t.classification IS NULL);
        """)
        updated = cursor.rowcount
        cursor.execute("""
            UPDATE silver.transactions
            SET category = 'Income', classification = 'WANT'
            WHERE transaction_type = 'INCOME'
              AND subcategory IN ('Child Support', 'Lottery, gambling')
              AND (category IS NULL OR category != 'Income');
        """)
        income_override = cursor.rowcount
        if updated > 0 or income_override > 0:
            print(f"  Updated {updated + income_override:,} transactions with category groups")

    def _bulk_insert(self, df: pd.DataFrame, schema: str, table: str, conn=None) -> int:
        """Bulk insert DataFrame to PostgreSQL.
        If conn is provided, use it (caller manages commit/close). Otherwise open a new connection.
        """
        from psycopg2.extras import execute_batch

        columns = df.columns.tolist()
        values = df.values.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join([f'"{c}"' for c in columns])
        query = f'INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})'

        if conn is not None:
            cursor = conn.cursor()
            execute_batch(cursor, query, values, page_size=1000)
            return len(values)
        with self.db.connect() as conn:
            cursor = conn.cursor()
            execute_batch(cursor, query, values, page_size=1000)
        return len(values)

    def _log_pipeline_run(self):
        """Log to metadata.pipeline_runs."""
        end_time = datetime.now()
        duration = (end_time - self.run_stats["start_time"]).total_seconds()
        log_data = {
            "run_id": str(self.run_stats["run_id"]),
            "run_timestamp": self.run_stats["start_time"],
            "source_file": self.run_stats["source_file"],
            "file_size_bytes": self.run_stats["file_size_bytes"],
            "status": self.run_stats["status"],
            "rows_extracted": self.run_stats["rows_extracted"],
            "rows_staged": self.run_stats["rows_staged"],
            "rows_loaded_bronze": self.run_stats["rows_loaded_bronze"],
            "rows_loaded_silver": self.run_stats["rows_loaded_silver"],
            "rows_skipped_duplicates": self.run_stats["rows_skipped_duplicates"],
            "rows_failed_validation": 0,
            "start_time": self.run_stats["start_time"],
            "end_time": end_time,
            "duration_seconds": duration,
            "error_message": self.run_stats.get("error_message"),
        }
        cols = ", ".join([f'"{k}"' for k in log_data.keys()])
        placeholders = ", ".join(["%s"] * len(log_data))
        query = f"INSERT INTO metadata.pipeline_runs ({cols}) VALUES ({placeholders})"
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, list(log_data.values()))
        print(f"\n[LOG] Pipeline run logged (ID: {self.run_stats['run_id']})")

    def _display_summary(self):
        """Print summary."""
        duration = (datetime.now() - self.run_stats["start_time"]).total_seconds()
        print("\n" + "=" * 70)
        print("INCREMENTAL LOAD COMPLETE")
        print("=" * 70)
        print(f"Status: {self.run_stats['status']}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"\nData Flow:")
        print(f"  Extracted:       {self.run_stats['rows_extracted']:,} rows")
        print(f"  → Bronze:        {self.run_stats['rows_loaded_bronze']:,} rows")
        print(f"  → Silver:        {self.run_stats['rows_loaded_silver']:,} new rows")
        print(f"  Duplicates:      {self.run_stats['rows_skipped_duplicates']:,} skipped")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Incremental load of expense data")
    parser.add_argument("--source", choices=["api", "file"], default="api", help="Extract from API or file")
    parser.add_argument("--file", help="Path to source file (required when --source=file)")
    parser.add_argument(
        "--account-filter",
        choices=["eur", "bgn_final"],
        default="eur",
        help="Account filter preset (default: eur)",
    )
    args = parser.parse_args()

    if args.source == "file" and not args.file:
        parser.error("--file is required when --source=file")

    loader = IncrementalDataLoader(
        source=args.source,
        file_path=args.file,
        account_filter=args.account_filter,
    )
    success = loader.load()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
