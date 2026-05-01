"""
Shared base class for the staging/bronze/silver loaders.

Holds everything that is identical between IncrementalDataLoader and
InitialDataLoader: extract-from-file, transform, staging/bronze writes,
pipeline-run logging, summary printing, bulk insert, category-mapping
refresh, and the gold refresh wrappers.

Both subclasses share the same `load()` orchestration: a single
PostgreSQL transaction wraps staging + bronze + silver, with rollback
on any failure. The two subclasses only differ in:
  - `_extract`: where the raw data comes from
  - `_load_silver`: truncate-and-reload vs. dedupe-and-append
  - `_post_transform`: optional account filter (incremental only)
  - which gold-refresh mode they pass through (`hashes=...` vs `full=True`)
"""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from psycopg2.extras import execute_batch

from utils.db_connector import get_db_connector


class BaseLoader:
    """Common plumbing for the medallion loaders.

    Subclasses override:
      - `_extract()` — produce a raw DataFrame
      - `_load_silver(df, conn)` — silver-load policy
      - `_post_transform(df)` — optional row filter after transform (defaults to identity)
      - `_refresh_gold_*` — supply `hashes=...` or `full=True`
    """

    # Subclasses override these two for human-readable output and silver lineage.
    header_text: str = "PERSONAL FINANCE PIPELINE - DATA LOAD"
    summary_text: str = "LOAD COMPLETE"
    created_by: str = "base_loader"

    def __init__(self, source_file_name: str):
        self.db = get_db_connector()
        # Transformer is imported lazily so importing this module does not
        # force a heavy import chain when only the helpers are needed.
        from transformers.expense_transformer import ExpenseTransformer

        self.transformer = ExpenseTransformer()
        self.batch_id = uuid.uuid4()
        self.run_stats = {
            "run_id": self.batch_id,
            "start_time": datetime.now(),
            "source_file": source_file_name,
            "file_size_bytes": None,
            "rows_extracted": 0,
            "rows_staged": 0,
            "rows_loaded_bronze": 0,
            "rows_loaded_silver": 0,
            "rows_skipped_duplicates": 0,
            "status": "RUNNING",
        }

    # ------------------------------------------------------------------ #
    # Top-level orchestration
    # ------------------------------------------------------------------ #
    def load(self) -> bool:
        """Run extract → transform → staging → bronze → silver → gold.

        Staging, bronze, and silver are written under a single connection
        and committed together. Any error rolls back all three layers so
        the medallion stays consistent. The pipeline-run log writes on a
        fresh connection so it survives a rollback.
        """
        self._print_header()
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

            transformed_df = self._post_transform(transformed_df)
            if len(transformed_df) == 0:
                print("\n  No records after post-transform filter.")
                self.run_stats["status"] = "SUCCESS"
                self._log_pipeline_run()
                return True

            conn = self.db.connect()
            try:
                self._load_staging(transformed_df, conn)
                self._load_bronze(transformed_df, conn)
                self._load_silver(transformed_df, conn)
                self._update_category_mapping(conn)
                conn.commit()
                print("\n  All layers committed in a single transaction.")
            except Exception:
                conn.rollback()
                print("\n  Transaction rolled back - no partial data written.")
                raise
            finally:
                conn.close()

            self._refresh_gold_notability()
            self._refresh_gold_save_potential()

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

    def _print_header(self) -> None:
        print("\n" + "=" * 70)
        print(self.header_text)
        print("=" * 70)
        print(f"Batch ID: {self.batch_id}")
        print(
            f"Started: {self.run_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print("=" * 70)

    # ------------------------------------------------------------------ #
    # Extract / transform — overridable
    # ------------------------------------------------------------------ #
    def _extract(self) -> pd.DataFrame:
        """Subclasses override to extract from API or file."""
        raise NotImplementedError("Subclasses must implement _extract")

    def _extract_from_file(self, file_path: Path) -> pd.DataFrame:
        """Read xlsx or csv into a DataFrame."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        self.run_stats["file_size_bytes"] = file_path.stat().st_size

        ext = file_path.suffix.lower()
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        elif ext == ".csv":
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        self.run_stats["rows_extracted"] = len(df)
        print(f"\n[EXTRACT] Read {len(df):,} rows from {file_path.name}")
        return df

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply ExpenseTransformer.transform and return the cleaned DataFrame."""
        print("\n[TRANSFORM] Applying transformations...")
        result = self.transformer.transform(df)
        # The transformer historically sometimes returned (df, stats); accept either.
        if isinstance(result, tuple):
            transformed_df = result[0]
        else:
            transformed_df = result
        print(f"  Output: {len(transformed_df):,} rows")
        return transformed_df

    def _post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hook for subclass-specific post-processing (e.g. account filter)."""
        return df

    # ------------------------------------------------------------------ #
    # Staging + Bronze (identical between subclasses)
    # ------------------------------------------------------------------ #
    def _load_staging(self, df: pd.DataFrame, conn) -> None:
        """Truncate staging.raw_transactions and load the current batch."""
        print("\n[LOAD STAGING] Loading to staging.raw_transactions...")
        payment_col = (
            "payment_method" if "payment_method" in df.columns else "payment_type"
        )
        cols = [
            "date", "description", "type", "payee", "amount", "labels",
            "account", "subcategory", "currency", payment_col,
        ]
        cols = [c for c in cols if c in df.columns]
        staging_df = df[cols].copy()
        staging_df = staging_df.rename(
            columns={
                payment_col: "payment",
                "description": "note",
                "subcategory": "category",
            }
        )
        staging_df["source_file"] = self.run_stats["source_file"]
        staging_df["batch_id"] = str(self.batch_id)
        staging_df["loaded_at"] = datetime.now()
        staging_df["source_row_number"] = range(1, len(staging_df) + 1)

        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE staging.raw_transactions;")
        rows = self._bulk_insert(staging_df, "staging", "raw_transactions", conn)
        self.run_stats["rows_staged"] = rows
        print(f"  Loaded {rows:,} rows to staging")

    def _load_bronze(self, df: pd.DataFrame, conn) -> None:
        """Append the batch to bronze.transactions_raw (immutable archive)."""
        print("\n[LOAD BRONZE] Appending to bronze.transactions_raw...")
        bronze_df = df.copy()
        rename_map = {
            "date": "transaction_date",
            "note": "description",
            "account": "account_name",
        }
        if "payment_type" in bronze_df.columns and "payment_method" not in bronze_df.columns:
            rename_map["payment_type"] = "payment_method"
        bronze_df = bronze_df.rename(columns=rename_map)

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

    # ------------------------------------------------------------------ #
    # Silver — overridable
    # ------------------------------------------------------------------ #
    def _load_silver(self, df: pd.DataFrame, conn) -> None:
        """Subclasses override with their silver-loading policy."""
        raise NotImplementedError("Subclasses must implement _load_silver")

    def _silver_columns(self) -> list[str]:
        """Whitelist of columns the silver insert is allowed to use."""
        return [
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

    def _prepare_silver_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply renames + metadata columns to produce a silver-ready DataFrame."""
        silver_df = df.copy()
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
        silver_df["created_by"] = self.created_by
        silver_df["source_raw_id"] = None

        return silver_df[
            [c for c in self._silver_columns() if c in silver_df.columns]
        ]

    # ------------------------------------------------------------------ #
    # Bulk insert
    # ------------------------------------------------------------------ #
    def _bulk_insert(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        conn=None,
    ) -> int:
        """Insert a DataFrame into ``schema.table`` via psycopg2 execute_batch.

        If ``conn`` is provided the caller owns the transaction (no commit
        happens here). Otherwise a fresh connection is opened, written to,
        and closed via the context manager.
        """
        columns = df.columns.tolist()
        values = df.values.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join([f'"{c}"' for c in columns])
        query = (
            f'INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})'
        )

        if conn is not None:
            cursor = conn.cursor()
            execute_batch(cursor, query, values, page_size=1000)
            return len(values)

        with self.db.connect() as own_conn:
            cursor = own_conn.cursor()
            execute_batch(cursor, query, values, page_size=1000)
        return len(values)

    # ------------------------------------------------------------------ #
    # Category & classification refresh on silver
    # ------------------------------------------------------------------ #
    def _update_category_mapping(self, conn) -> None:
        """Backfill silver.category and silver.classification from category_mapping.

        Always takes an externally-managed connection — the caller decides
        whether to commit or rollback as part of its main transaction.
        """
        print("  Updating category hierarchy...")
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE silver.transactions t
            SET category = cm.category, classification = cm.classification
            FROM silver.category_mapping cm
            WHERE t.subcategory = cm.subcategory
              AND (t.category IS NULL OR t.classification IS NULL);
            """
        )
        updated = cursor.rowcount
        cursor.execute(
            """
            UPDATE silver.transactions
            SET category = 'Income', classification = 'WANT'
            WHERE transaction_type = 'INCOME'
              AND subcategory IN ('Child Support', 'Lottery, gambling')
              AND (category IS NULL OR category != 'Income');
            """
        )
        income_override = cursor.rowcount
        if updated > 0 or income_override > 0:
            print(
                f"  Updated {updated + income_override:,} transactions with category groups"
            )

    # ------------------------------------------------------------------ #
    # Gold refresh wrappers (non-fatal)
    # ------------------------------------------------------------------ #
    def _refresh_gold_notability(
        self,
        *,
        hashes: Optional[set] = None,
        full: bool = False,
        window_days: int = 365,
    ) -> None:
        try:
            from loaders.gold_notable_loader import refresh_notability_for_hashes

            n = refresh_notability_for_hashes(
                self.db, hashes=hashes, full=full, window_days=window_days
            )
            if n > 0:
                print(f"  [GOLD] Updated {n:,} rows in transaction_notability")
        except Exception as e:
            print(f"  [GOLD] Warning: Could not refresh transaction_notability: {e}")

    def _refresh_gold_save_potential(
        self,
        *,
        hashes: Optional[set] = None,
        full: bool = False,
        window_days: int = 365,
    ) -> None:
        try:
            from loaders.gold_save_potential_loader import (
                refresh_save_potential_for_hashes,
            )

            n = refresh_save_potential_for_hashes(
                self.db, hashes=hashes, full=full, window_days=window_days
            )
            if n > 0:
                print(f"  [GOLD] Updated {n:,} rows in transaction_save_potential")
        except Exception as e:
            print(
                f"  [GOLD] Warning: Could not refresh transaction_save_potential: {e}"
            )

    # ------------------------------------------------------------------ #
    # Pipeline-run logging
    # ------------------------------------------------------------------ #
    def _log_pipeline_run(self) -> None:
        """Append one row to metadata.pipeline_runs.

        Always opens a fresh connection so the log persists even if the
        main pipeline transaction was rolled back.
        """
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

    # ------------------------------------------------------------------ #
    # Summary printing
    # ------------------------------------------------------------------ #
    def _display_summary(
        self,
        header_text: Optional[str] = None,
        *,
        extra_lines: Optional[list[str]] = None,
    ) -> None:
        duration = (datetime.now() - self.run_stats["start_time"]).total_seconds()
        print("\n" + "=" * 70)
        print(header_text or self.summary_text)
        print("=" * 70)
        print(f"Status: {self.run_stats['status']}")
        print(f"Duration: {duration:.2f} seconds")
        print("\nData Flow:")
        print(f"  Extracted:       {self.run_stats['rows_extracted']:,} rows")
        print(f"  -> Bronze:       {self.run_stats['rows_loaded_bronze']:,} rows")
        print(f"  -> Silver:       {self.run_stats['rows_loaded_silver']:,} new rows")
        print(
            f"  Duplicates:      {self.run_stats['rows_skipped_duplicates']:,} skipped"
        )
        if extra_lines:
            for line in extra_lines:
                print(line)
        print("=" * 70)
