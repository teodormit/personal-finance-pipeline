"""
Shared base class for the staging/bronze/silver loaders.

Holds the helpers that are identical (or near-identical) between
IncrementalDataLoader and InitialDataLoader: pipeline-run logging,
summary printing, bulk insert, category-mapping refresh, and the gold
refresh wrappers. The actual extract / silver-load strategy is left
to subclasses.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
from psycopg2.extras import execute_batch

from utils.db_connector import get_db_connector


class BaseLoader:
    """Common plumbing for the medallion loaders.

    Subclasses are responsible for:
      - extracting raw data (`_extract`)
      - the silver-load policy (`_load_silver`) — truncate-and-reload vs. dedupe-and-append
      - the overall `load()` orchestration (transaction scope, gold refresh policy)
    """

    # Subclasses override this so silver.created_by reflects the load type.
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
        header_text: str,
        *,
        extra_lines: Optional[list[str]] = None,
    ) -> None:
        duration = (datetime.now() - self.run_stats["start_time"]).total_seconds()
        print("\n" + "=" * 70)
        print(header_text)
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
