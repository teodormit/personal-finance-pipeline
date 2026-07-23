"""
Initial Data Load Script
Loads historical expense data into the data warehouse.

Effectively retired now that silver is populated. Kept for completeness
and as the canonical "rebuild silver from a single export" entry point.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

_src_root = Path(__file__).resolve().parent.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from loaders.base_loader import BaseLoader


class InitialDataLoader(BaseLoader):
    """Truncate-and-reload silver from a single export file.

    Inherits the staging/bronze writes, transaction handling, and gold
    refresh from BaseLoader. Only the silver-load policy is special:
    silver is truncated, duplicate-hash records are exported for manual
    review, then everything is inserted (no dedupe).
    """

    header_text = "PERSONAL FINANCE PIPELINE - INITIAL DATA LOAD"
    summary_text = "INITIAL LOAD COMPLETE"
    created_by = "initial_load_script"

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        super().__init__(source_file_name=self.file_path.name)

    # ------------------------------------------------------------------ #
    # Extract
    # ------------------------------------------------------------------ #
    def _extract(self) -> pd.DataFrame:
        return self._extract_from_file(self.file_path)

    # ------------------------------------------------------------------ #
    # Silver load: truncate + export duplicate CSV + insert all
    # ------------------------------------------------------------------ #
    def _load_silver(self, df: pd.DataFrame, conn) -> None:
        print("\n[LOAD SILVER] Loading to silver.transactions...")

        # Export any duplicate hashes BEFORE truncating, for manual review.
        self._export_duplicate_hashes(df)

        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE silver.transactions;")
        print("  Silver table truncated")

        silver_df = self._prepare_silver_df(df)
        rows = self._bulk_insert(silver_df, "silver", "transactions", conn)
        self.run_stats["rows_loaded_silver"] = rows
        print(f"  Loaded {rows:,} rows to silver")

    def _export_duplicate_hashes(self, df: pd.DataFrame, output_file: str = None):
        """Export any duplicate transaction_hash records to CSV for manual review."""
        if "transaction_hash" not in df.columns:
            print("  WARNING - transaction_hash column not found")
            return

        hash_counts = df["transaction_hash"].value_counts()
        duplicates = hash_counts[hash_counts > 1]
        if len(duplicates) == 0:
            print("  No duplicate hashes found - no export needed")
            return

        if output_file is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            out_dir = project_root / "data" / "inspection"
            out_dir.mkdir(parents=True, exist_ok=True)
            output_file = str(out_dir / "duplicate_transaction_hashes.csv")

        duplicate_records = (
            df[df["transaction_hash"].isin(duplicates.index.tolist())]
            .copy()
            .sort_values(["transaction_hash", "date"])
        )
        review_columns = [
            "transaction_hash", "date", "description", "amount",
            "payee", "subcategory", "currency", "account_name",
        ]
        duplicate_records = duplicate_records[
            [c for c in review_columns if c in duplicate_records.columns]
        ]
        duplicate_records.to_csv(output_file, index=False)

        print("\n  DUPLICATES EXPORTED:")
        print(f"  Total duplicate records: {len(duplicate_records)}")
        print(f"  Unique duplicate hashes: {len(duplicates)}")
        print(f"  Export location: {output_file}")
        return output_file

    # ------------------------------------------------------------------ #
    # Gold refresh: full rebuild after silver is reloaded
    # ------------------------------------------------------------------ #
    def _refresh_gold_notability(self):
        super()._refresh_gold_notability(full=True)

    def _refresh_gold_save_potential(self):
        super()._refresh_gold_save_potential(full=True)

    # ------------------------------------------------------------------ #
    # Summary: append "Next Steps" hint
    # ------------------------------------------------------------------ #
    def _display_summary(self):
        super()._display_summary(
            extra_lines=[
                "\nNext Steps:",
                "  1. Verify data: SELECT * FROM silver.transactions LIMIT 10;",
                "  2. Connect Tableau to silver.transactions and the gold tables",
                "     (gold.transaction_notability, gold.transaction_save_potential).",
                "  3. For future updates, use: python scripts/run_pipeline.py --mode incremental",
            ],
        )


def main():
    parser = argparse.ArgumentParser(
        description="Initial load of historical expense data"
    )
    parser.add_argument(
        "--file", required=True, help="Path to source file (Excel or CSV)"
    )
    args = parser.parse_args()

    loader = InitialDataLoader(args.file)
    success = loader.load()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
