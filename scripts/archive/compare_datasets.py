"""
Wallet Data Comparison Tool
Compares two expense datasets to identify:
1. Missing transactions
2. Amount discrepancies
3. Date range coverage
4. Category differences
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime


# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def load_csv_safe(file_path, dataset_name):
    """Load Excel or CSV with multiple encoding attempts"""
    
    print(f"\nLoading {dataset_name}...")
    print(f"  File: {file_path}")
    
    file_path = Path(file_path)
    file_ext = file_path.suffix.lower()
    print(f"  Type: {file_ext}")
    
    # Handle Excel files
    if file_ext in ['.xlsx', '.xls']:
        try:
            # Try reading Excel file
            df = pd.read_excel(file_path, engine='openpyxl' if file_ext == '.xlsx' else 'xlrd')
            print(f"  [OK] Loaded Excel file successfully")
            print(f"  Rows: {len(df):,}")
            print(f"  Columns: {len(df.columns)}")
            return df
        except Exception as e:
            print(f"  [FAIL] Failed with primary engine: {str(e)[:100]}")
            # Try with default engine
            try:
                df = pd.read_excel(file_path)
                print(f"  [OK] Loaded with default engine")
                print(f"  Rows: {len(df):,}")
                print(f"  Columns: {len(df.columns)}")
                return df
            except Exception as e2:
                print(f"  [FAIL] Failed with default engine: {str(e2)[:100]}")
                return None
    
    # Handle CSV files
    elif file_ext == '.csv':
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"  [OK] Loaded with {encoding} encoding")
                print(f"  Rows: {len(df):,}")
                print(f"  Columns: {len(df.columns)}")
                return df
            except Exception as e:
                continue
    
    else:
        print(f"  [ERROR] Unsupported file type: {file_ext}")
        print(f"  Supported types: .xlsx, .xls, .csv")
        return None
    
    print(f"  [ERROR] Could not load file with any method!")
    return None


def standardize_dataframe(df, name):
    """Standardize column names and types for comparison"""
    
    print(f"\nStandardizing {name}...")
    
    # Create a copy
    df = df.copy()
    
    # Lowercase all column names for consistency
    df.columns = df.columns.str.lower().str.strip()
    
    print(f"  Columns found: {list(df.columns)}")
    
    # Identify key columns
    date_col = next((col for col in df.columns if 'date' in col), None)
    amount_col = next((col for col in df.columns if 'amount' in col), None)
    category_col = next((col for col in df.columns if 'category' in col and 'custom' not in col), None)
    
    if not date_col:
        print(f"  [WARNING] No date column found!")
        return None
    
    if not amount_col:
        print(f"  [WARNING] No amount column found!")
        return None
    
    print(f"  Date column: {date_col}")
    print(f"  Amount column: {amount_col}")
    print(f"  Category column: {category_col}")
    
    # Parse dates
    try:
        df['parsed_date'] = pd.to_datetime(df[date_col], errors='coerce')
        valid_dates = df['parsed_date'].notna().sum()
        print(f"  [OK] Parsed {valid_dates}/{len(df)} dates successfully")
        
        if valid_dates == 0:
            print(f"  [ERROR] Could not parse any dates!")
            print(f"  Sample values: {df[date_col].head().tolist()}")
            return None
            
    except Exception as e:
        print(f"  [ERROR] Date parsing failed: {str(e)}")
        return None
    
    # Ensure amount is numeric
    try:
        df['parsed_amount'] = pd.to_numeric(df[amount_col], errors='coerce')
        valid_amounts = df['parsed_amount'].notna().sum()
        print(f"  [OK] Parsed {valid_amounts}/{len(df)} amounts successfully")
    except Exception as e:
        print(f"  [ERROR] Amount parsing failed: {str(e)}")
        return None
    
    # Store category
    if category_col:
        df['parsed_category'] = df[category_col].astype(str)
    else:
        df['parsed_category'] = 'Unknown'
    
    return df


def compare_datasets(manual_df, app_df):
    """Comprehensive comparison of two datasets"""
    
    print("\n" + "=" * 80)
    print("DATASET COMPARISON ANALYSIS")
    print("=" * 80)
    
    # Date range comparison
    print(f"\n{'-' * 80}")
    print("DATE RANGE COMPARISON")
    print(f"{'-' * 80}")
    
    manual_min = manual_df['parsed_date'].min()
    manual_max = manual_df['parsed_date'].max()
    app_min = app_df['parsed_date'].min()
    app_max = app_df['parsed_date'].max()
    
    print(f"Manual File:")
    print(f"  Earliest: {manual_min.date() if pd.notna(manual_min) else 'N/A'}")
    print(f"  Latest:   {manual_max.date() if pd.notna(manual_max) else 'N/A'}")
    
    print(f"\nApp Export:")
    print(f"  Earliest: {app_min.date() if pd.notna(app_min) else 'N/A'}")
    print(f"  Latest:   {app_max.date() if pd.notna(app_max) else 'N/A'}")
    
    # Find overlap period
    overlap_start = max(manual_min, app_min)
    overlap_end = min(manual_max, app_max)
    
    if overlap_start <= overlap_end:
        print(f"\nOverlap Period:")
        print(f"  From: {overlap_start.date()}")
        print(f"  To:   {overlap_end.date()}")
        print(f"  Days: {(overlap_end - overlap_start).days}")
    else:
        print(f"\n[WARNING] No date overlap between datasets!")
    
    # Total amounts comparison
    print(f"\n{'-' * 80}")
    print("AMOUNT TOTALS COMPARISON")
    print(f"{'-' * 80}")
    
    manual_total = manual_df['parsed_amount'].sum()
    app_total = app_df['parsed_amount'].sum()
    difference = abs(manual_total - app_total)
    
    print(f"Manual File Total:  {manual_total:,.2f}")
    print(f"App Export Total:   {app_total:,.2f}")
    print(f"Difference:         {difference:,.2f}")
    print(f"Difference %:       {(difference / abs(manual_total) * 100) if manual_total != 0 else 0:.2f}%")
    
    # Expense vs Income breakdown
    print(f"\n{'-' * 80}")
    print("EXPENSE VS INCOME BREAKDOWN")
    print(f"{'-' * 80}")
    
    for name, df in [("Manual File", manual_df), ("App Export", app_df)]:
        expenses = df[df['parsed_amount'] < 0]['parsed_amount'].sum()
        income = df[df['parsed_amount'] > 0]['parsed_amount'].sum()
        
        print(f"\n{name}:")
        print(f"  Expenses: {expenses:,.2f} ({(df['parsed_amount'] < 0).sum()} transactions)")
        print(f"  Income:   {income:,.2f} ({(df['parsed_amount'] > 0).sum()} transactions)")
        print(f"  Net:      {expenses + income:,.2f}")
    
    # Transaction count comparison
    print(f"\n{'-' * 80}")
    print("TRANSACTION COUNT COMPARISON")
    print(f"{'-' * 80}")
    
    print(f"Manual File: {len(manual_df):,} transactions")
    print(f"App Export:  {len(app_df):,} transactions")
    print(f"Difference:  {abs(len(manual_df) - len(app_df)):,} transactions")
    
    # Monthly comparison (for overlap period)
    if overlap_start <= overlap_end:
        print(f"\n{'-' * 80}")
        print("MONTHLY COMPARISON (Overlap Period)")
        print(f"{'-' * 80}")
        
        # Filter to overlap period
        manual_overlap = manual_df[
            (manual_df['parsed_date'] >= overlap_start) & 
            (manual_df['parsed_date'] <= overlap_end)
        ].copy()
        
        app_overlap = app_df[
            (app_df['parsed_date'] >= overlap_start) & 
            (app_df['parsed_date'] <= overlap_end)
        ].copy()
        
        # Group by month
        manual_overlap['month'] = manual_overlap['parsed_date'].dt.to_period('M')
        app_overlap['month'] = app_overlap['parsed_date'].dt.to_period('M')
        
        manual_monthly = manual_overlap.groupby('month')['parsed_amount'].agg(['sum', 'count'])
        app_monthly = app_overlap.groupby('month')['parsed_amount'].agg(['sum', 'count'])
        
        comparison = pd.merge(
            manual_monthly, app_monthly, 
            left_index=True, right_index=True, 
            suffixes=('_manual', '_app'),
            how='outer'
        ).fillna(0)
        
        comparison['amount_diff'] = comparison['sum_manual'] - comparison['sum_app']
        comparison['count_diff'] = comparison['count_manual'] - comparison['count_app']
        
        print("\nMonth        | Manual Amount | App Amount    | Difference   | Trans. Diff")
        print("-" * 80)
        
        for month, row in comparison.iterrows():
            print(f"{month} | {row['sum_manual']:>13,.2f} | {row['sum_app']:>13,.2f} | "
                  f"{row['amount_diff']:>12,.2f} | {row['count_diff']:>11.0f}")
    
    # Category comparison
    print(f"\n{'-' * 80}")
    print("CATEGORY COMPARISON (Top 10)")
    print(f"{'-' * 80}")
    
    manual_categories = manual_df['parsed_category'].value_counts().head(10)
    app_categories = app_df['parsed_category'].value_counts().head(10)
    
    print("\nManual File:")
    for cat, count in manual_categories.items():
        print(f"  {cat}: {count}")
    
    print("\nApp Export:")
    for cat, count in app_categories.items():
        print(f"  {cat}: {count}")
    
    # Recommendation
    print(f"\n{'=' * 80}")
    print("RECOMMENDATION")
    print(f"{'=' * 80}")
    
    if difference / abs(manual_total) < 0.05 if manual_total != 0 else False:
        print("\n[OK] Datasets are highly similar (< 5% difference)")
        print("Recommendation: Use App Export as primary source")
        print("Action: Validate any discrepancies manually")
    elif difference / abs(manual_total) < 0.15 if manual_total != 0 else False:
        print("\n[WARNING] Moderate differences detected (5-15%)")
        print("Recommendation: Investigate monthly discrepancies above")
        print("Action: Compare specific transactions in problem months")
    else:
        print("\n[ALERT] Significant differences detected (> 15%)")
        print("Recommendation: Manual reconciliation required")
        print("Action: Review both datasets for missing/duplicate entries")
    
    print("\nNext Steps:")
    print("1. Review monthly comparison table above")
    print("2. Identify months with largest discrepancies")
    print("3. Decide which dataset to use as 'source of truth'")
    print("4. Merge or choose one dataset for pipeline initialization")


def main():
    """Main execution"""
    
    print("\n" + "=" * 80)
    print("WALLET DATA COMPARISON TOOL")
    print("=" * 80)
    
    # Get file paths
    print("\nEnter file paths (or drag-and-drop files):")
    
    if len(sys.argv) >= 3:
        manual_path = sys.argv[1]
        app_path = sys.argv[2]
    else:
        manual_path = input("Path to Manual/Historical file (until June 2024): ").strip().strip('"')
        app_path = input("Path to App Export file (all entries): ").strip().strip('"')
    
    manual_path = Path(manual_path)
    app_path = Path(app_path)
    
    # Validate files exist
    if not manual_path.exists():
        print(f"\n[ERROR] Manual file not found: {manual_path}")
        return
    
    if not app_path.exists():
        print(f"\n[ERROR] App export file not found: {app_path}")
        return
    
    # Load datasets
    manual_df = load_csv_safe(manual_path, "Manual/Historical File")
    app_df = load_csv_safe(app_path, "App Export")
    
    if manual_df is None or app_df is None:
        print("\n[ERROR] Failed to load one or both files!")
        return
    
    # Standardize
    manual_df = standardize_dataframe(manual_df, "Manual File")
    app_df = standardize_dataframe(app_df, "App Export")
    
    if manual_df is None or app_df is None:
        print("\n[ERROR] Failed to standardize one or both datasets!")
        return
    
    # Compare
    compare_datasets(manual_df, app_df)
    
    print("\n" + "=" * 80)
    print("Comparison complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
