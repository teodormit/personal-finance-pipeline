"""
Inspect BudgetBakers API Output
================================
Shows data at all three pipeline stages:
  1. Raw API output (extract_raw)
  2. After api_field_mapper
  3. After ExpenseTransformer

Prints summaries and saves each stage to CSV for manual review.

Usage:
    python scripts/inspect_api_output.py
    python scripts/inspect_api_output.py --days 7
    python scripts/inspect_api_output.py --from-date 2025-01-01 --to-date 2025-02-01
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Fix Windows console encoding for Cyrillic/special characters
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Fallback: if env var not set, read raw token from secrets file
if not os.getenv("BUDGETBAKERS_API_TOKEN"):
    token_file = Path(__file__).resolve().parent.parent / "secrets" / "BUDGETBAKERS_API_TOKEN.env"
    if token_file.exists():
        os.environ["BUDGETBAKERS_API_TOKEN"] = token_file.read_text(encoding="utf-8").strip()

from extractors.budgetbakers_extractor import BudgetBakersExtractor
from extractors.api_field_mapper import map_raw_to_transformer_input
from transformers.expense_transformer import ExpenseTransformer


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Inspect raw BudgetBakers API output")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back (default: 30)")
    parser.add_argument("--from-date", type=str, help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--to-date", type=str, help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    date_to = datetime.strptime(args.to_date, "%Y-%m-%d") if args.to_date else datetime.now()
    if args.from_date:
        date_from = datetime.strptime(args.from_date, "%Y-%m-%d")
    else:
        date_from = date_to - timedelta(days=args.days)

    extractor = BudgetBakersExtractor()
    df = extractor.extract_raw(date_from=date_from, date_to=date_to)

    if df.empty:
        print("\nNo records returned from API.")
        return

    print(f"\n{'='*70}")
    print(f"RAW API OUTPUT SUMMARY")
    print(f"{'='*70}")

    print(f"\nShape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"\nColumns returned by API:")
    for col in df.columns:
        non_null = df[col].notna().sum()
        sample = df[col].dropna().iloc[0] if non_null > 0 else "N/A"
        sample_str = str(sample)[:80]
        print(f"  {col:<20} {non_null:>5} non-null  | sample: {sample_str}")

    print(f"\n--- recordType distribution ---")
    if "recordType" in df.columns:
        print(df["recordType"].value_counts().to_string())

    print(f"\n--- paymentType distribution ---")
    if "paymentType" in df.columns:
        print(df["paymentType"].value_counts(dropna=False).to_string())

    print(f"\n--- amount_currency distribution ---")
    if "amount_currency" in df.columns:
        print(df["amount_currency"].value_counts().to_string())

    print(f"\n--- First 5 records (all columns) ---")
    with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 60):
        print(df.head().to_string())

    output_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}"
    raw_file = output_dir / f"api_raw_output_{date_str}.csv"
    df.to_csv(raw_file, index=False, encoding="utf-8-sig")
    print(f"\nSaved raw output to: {raw_file}")

    # Stage 2: After api_field_mapper
    mapped_df = map_raw_to_transformer_input(df)
    print(f"\n{'='*70}")
    print(f"STAGE 2: AFTER API FIELD MAPPER")
    print(f"{'='*70}")
    print(f"\nShape: {mapped_df.shape[0]} rows x {mapped_df.shape[1]} columns")
    print(f"\nColumns: {list(mapped_df.columns)}")
    print(f"\n--- First 5 records ---")
    with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 60):
        print(mapped_df.head().to_string())
    mapped_file = output_dir / f"api_mapped_output_{date_str}.csv"
    mapped_df.to_csv(mapped_file, index=False, encoding="utf-8-sig")
    print(f"\nSaved mapped output to: {mapped_file}")

    # Stage 3: After ExpenseTransformer
    transformer = ExpenseTransformer()
    transformed_df = transformer.transform(mapped_df)
    print(f"\n{'='*70}")
    print(f"STAGE 3: AFTER EXPENSE TRANSFORMER")
    print(f"{'='*70}")
    print(f"\nShape: {transformed_df.shape[0]} rows x {transformed_df.shape[1]} columns")
    print(f"\nColumns: {list(transformed_df.columns)}")
    print(f"\n--- First 5 records ---")
    with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 60):
        print(transformed_df.head().to_string())
    transformed_file = output_dir / f"api_transformed_output_{date_str}.csv"
    transformed_df.to_csv(transformed_file, index=False, encoding="utf-8-sig")
    print(f"\nSaved transformed output to: {transformed_file}")


if __name__ == "__main__":
    main()
