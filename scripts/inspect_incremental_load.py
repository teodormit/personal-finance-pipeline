"""
Inspect Incremental Load Pipeline (Dry Run)
=============================================

Runs the full extract -> transform -> load-prep pipeline WITHOUT writing to
the database.  Prints summaries and DataFrames at every stage so you can
manually verify the data before committing a real run.

Stages inspected:
  1. Extract  – raw API / file output
  2. Transform – after ExpenseTransformer
  3. Staging prep – DataFrame that would go to staging.raw_transactions
  4. Bronze prep  – DataFrame that would go to bronze.transactions_raw
  5. Silver prep  – DataFrame that would go to silver.transactions
                   (with dedup report against existing silver hashes)

Usage:
    python scripts/inspect_incremental_load.py
    python scripts/inspect_incremental_load.py --days 7
    python scripts/inspect_incremental_load.py --source file --file data/raw/export.csv
    python scripts/inspect_incremental_load.py --from-date 2026-02-01 --to-date 2026-03-01
    python scripts/inspect_incremental_load.py --account-filter bgn_final   # one-time BGN load
    python scripts/inspect_incremental_load.py --save   # persist CSVs to data/inspection/
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))

import os
import uuid
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

if not os.getenv("BUDGETBAKERS_API_TOKEN"):
    token_file = _project_root / "secrets" / "BUDGETBAKERS_API_TOKEN.env"
    if token_file.exists():
        os.environ["BUDGETBAKERS_API_TOKEN"] = token_file.read_text(encoding="utf-8").strip()


PD_OPTS = ("display.max_columns", None, "display.width", 220, "display.max_colwidth", 60)
SEPARATOR = "=" * 70


def _print_df_summary(df: pd.DataFrame, label: str, show_head: int = 5):
    """Print shape, columns, dtypes overview, and first N rows."""
    print(f"\n{SEPARATOR}")
    print(f"  {label}")
    print(SEPARATOR)
    print(f"\nShape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"\nColumns & non-null counts:")
    for col in df.columns:
        non_null = df[col].notna().sum()
        dtype = df[col].dtype
        sample = ""
        if non_null > 0:
            sample = str(df[col].dropna().iloc[0])[:70]
        print(f"  {col:<30} {non_null:>5} non-null  ({dtype})  | {sample}")
    print(f"\n--- First {show_head} rows ---")
    with pd.option_context(*PD_OPTS):
        print(df.head(show_head).to_string())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Dry-run inspection of incremental load pipeline")
    parser.add_argument("--source", choices=["api", "file"], default="api")
    parser.add_argument("--file", help="Path to source file (required when --source=file)")
    parser.add_argument("--days", type=int, default=30, help="Days to look back (default: 30)")
    parser.add_argument("--from-date", type=str, help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--to-date", type=str, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--use-silver-watermark",
        action="store_true",
        help="Use day after max(silver.transaction_date) as start date (for incremental-style dry run)",
    )
    parser.add_argument(
        "--account-filter",
        choices=["eur", "bgn_final"],
        default="eur",
        help="Account filter preset (default: eur)",
    )
    parser.add_argument("--save", action="store_true", help="Save each stage to CSV in data/inspection/")
    args = parser.parse_args()

    batch_id = uuid.uuid4()
    output_dir = _project_root / "data" / "inspection"

    print(f"\n{SEPARATOR}")
    print("  INCREMENTAL LOAD PIPELINE - DRY RUN INSPECTION")
    print(SEPARATOR)
    print(f"Source: {args.source}")
    print(f"Account filter: {args.account_filter}")
    print(f"Batch ID (simulated): {batch_id}")

    # ================================================================
    # STAGE 1: Extract
    # ================================================================
    if args.source == "api":
        from extractors.budgetbakers_extractor import BudgetBakersExtractor

        date_to = datetime.strptime(args.to_date, "%Y-%m-%d") if args.to_date else datetime.now()

        last_silver = None
        try:
            from utils.db_connector import get_db_connector
            db = get_db_connector()
            with db.connect() as conn:
                cur = conn.cursor()
                cur.execute("SELECT MAX(transaction_date) FROM silver.transactions")
                row = cur.fetchone()
                last_silver = row[0] if row else None
        except Exception as e:
            print(f"\nCould not read silver watermark: {e}")

        if args.use_silver_watermark:
            if last_silver is None:
                print("\n--use-silver-watermark requires silver.transactions to have data. Table is empty.")
                sys.exit(1)
            date_from = datetime.combine(last_silver, datetime.min.time()) + timedelta(days=1)
            print(f"\nSilver high-watermark: {last_silver}  (using date_from={date_from.date()})")
        elif args.from_date:
            date_from = datetime.strptime(args.from_date, "%Y-%m-%d")
        else:
            date_from = date_to - timedelta(days=args.days)
            if last_silver:
                watermark_from = datetime.combine(last_silver, datetime.min.time()) + timedelta(days=1)
                print(f"\nSilver high-watermark: {last_silver}  (loader would use date_from={watermark_from.date()})")
            else:
                print("\nSilver table is empty - loader would default to 1 year lookback")

        print(f"Extraction window: {date_from.date()} to {date_to.date()}")
        extractor = BudgetBakersExtractor()
        raw_df = extractor.extract(date_from=date_from, date_to=date_to)
    else:
        if not args.file:
            parser.error("--file is required when --source=file")
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"File not found: {file_path}")
            sys.exit(1)
        ext = file_path.suffix.lower()
        if ext in [".xlsx", ".xls"]:
            raw_df = pd.read_excel(file_path)
        else:
            raw_df = pd.read_csv(file_path)
        print(f"Read {len(raw_df):,} rows from {file_path.name}")

    if raw_df.empty:
        print("\nNo records extracted. Nothing to inspect.")
        return

    _print_df_summary(raw_df, "STAGE 1: EXTRACTED (raw input to transformer)")

    # ================================================================
    # STAGE 2: Transform
    # ================================================================
    from transformers.expense_transformer import ExpenseTransformer
    from loaders.incremental_load import apply_account_filter

    transformer = ExpenseTransformer()
    transformed_df = transformer.transform(raw_df)

    _print_df_summary(transformed_df, "STAGE 2: TRANSFORMED (ExpenseTransformer output)")

    transformed_df = apply_account_filter(transformed_df, args.account_filter)
    if transformed_df.empty:
        print("\nNo records after account filter. Nothing to inspect.")
        return

    # Quick sanity checks on transformed data
    print(f"\n--- Sanity Checks ---")
    print(f"  transaction_type distribution: {transformed_df['transaction_type'].value_counts().to_dict()}")
    print(f"  currency distribution:         {transformed_df['currency'].value_counts().to_dict()}")
    nulls = transformed_df[["date", "amount", "transaction_type", "subcategory"]].isnull().sum()
    if nulls.any():
        print(f"  NULL in critical columns:      {nulls[nulls > 0].to_dict()}")
    else:
        print(f"  NULL in critical columns:      None")
    dupes = transformed_df["transaction_hash"].duplicated().sum()
    print(f"  Duplicate hashes in batch:     {dupes}")
    if "amount_bgn" in transformed_df.columns:
        print(f"  amount_bgn range:              {transformed_df['amount_bgn'].min():.2f} to {transformed_df['amount_bgn'].max():.2f}")

    # ================================================================
    # STAGE 3: Staging prep
    # ================================================================
    staging_df = transformed_df.copy()
    cols = ["date", "description", "type", "payee", "amount", "labels", "account", "subcategory", "currency"]
    payment_col = "payment_method" if "payment_method" in staging_df.columns else "payment_type"
    cols.append(payment_col)
    cols = [c for c in cols if c in staging_df.columns]
    staging_df = staging_df[cols].copy()
    staging_df = staging_df.rename(columns={payment_col: "payment", "description": "note", "subcategory": "category"})
    staging_df["source_file"] = "dry_run_inspection"
    staging_df["batch_id"] = str(batch_id)
    staging_df["loaded_at"] = datetime.now()
    staging_df["source_row_number"] = range(1, len(staging_df) + 1)

    _print_df_summary(staging_df, "STAGE 3: STAGING PREP (staging.raw_transactions)")

    # ================================================================
    # STAGE 4: Bronze prep
    # ================================================================
    bronze_df = transformed_df.copy()
    bronze_df = bronze_df.rename(columns={
        "date": "transaction_date", "note": "description", "account": "account_name",
    })
    if "payment_type" in bronze_df.columns and "payment_method" not in bronze_df.columns:
        bronze_df = bronze_df.rename(columns={"payment_type": "payment_method"})
    bronze_df["source_file"] = "dry_run_inspection"
    bronze_df["source_row_number"] = range(1, len(bronze_df) + 1)
    bronze_df["ingestion_timestamp"] = datetime.now()
    bronze_df["ingestion_batch_id"] = str(batch_id)
    bronze_df["has_quality_issues"] = False
    bronze_columns = [
        "transaction_date", "description", "transaction_type", "payee",
        "amount", "labels", "account_name", "subcategory", "currency",
        "payment_method", "source_file", "source_row_number",
        "ingestion_timestamp", "ingestion_batch_id", "has_quality_issues",
    ]
    bronze_df = bronze_df[[c for c in bronze_columns if c in bronze_df.columns]]

    _print_df_summary(bronze_df, "STAGE 4: BRONZE PREP (bronze.transactions_raw)")

    # ================================================================
    # STAGE 5: Silver prep + dedup report
    # ================================================================
    silver_df = transformed_df.copy()
    rename_map = {"date": "transaction_date", "account": "account_name"}
    if "note" in silver_df.columns:
        rename_map["note"] = "description"
    if "payment_type" in silver_df.columns and "payment_method" not in silver_df.columns:
        rename_map["payment_type"] = "payment_method"
    silver_df = silver_df.rename(columns=rename_map)
    silver_df["created_at"] = datetime.now()
    silver_df["created_by"] = "dry_run_inspection"
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

    _print_df_summary(silver_df, "STAGE 5: SILVER PREP (silver.transactions)")

    # Dedup check against live DB
    try:
        from utils.db_connector import get_db_connector
        db = get_db_connector()
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT transaction_hash FROM silver.transactions")
            existing = {r[0] for r in cur.fetchall()}
        new_mask = ~silver_df["transaction_hash"].isin(existing)
        new_count = new_mask.sum()
        dup_count = len(silver_df) - new_count

        print(f"\n--- Deduplication Report (vs live silver table) ---")
        print(f"  Existing silver rows:    {len(existing):,}")
        print(f"  Incoming batch rows:     {len(silver_df):,}")
        print(f"  NEW (would insert):      {new_count:,}")
        print(f"  DUPLICATES (would skip): {dup_count:,}")

        if dup_count > 0 and dup_count <= 10:
            dup_hashes = silver_df.loc[~new_mask, "transaction_hash"].tolist()
            print(f"\n  Duplicate hashes: {dup_hashes}")
    except Exception as e:
        print(f"\n  Could not check dedup against DB: {e}")

    # ================================================================
    # Save to CSVs
    # ================================================================
    if args.save:
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for name, df_out in [
            ("1_extracted", raw_df),
            ("2_transformed", transformed_df),
            ("3_staging_prep", staging_df),
            ("4_bronze_prep", bronze_df),
            ("5_silver_prep", silver_df),
        ]:
            path = output_dir / f"{name}_{ts}.csv"
            df_out.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"  Saved: {path}")

    print(f"\n{SEPARATOR}")
    print("  DRY RUN COMPLETE - no data was written to the database")
    print(SEPARATOR)


if __name__ == "__main__":
    main()
