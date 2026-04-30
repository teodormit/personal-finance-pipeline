"""
Deep Transaction Analysis
Identifies missing transactions between two datasets
Filters for expenses excluding transfers
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def load_file(file_path, dataset_name):
    """Load Excel or CSV file"""
    
    print(f"\nLoading {dataset_name}...")
    file_path = Path(file_path)
    file_ext = file_path.suffix.lower()
    
    try:
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        elif file_ext == '.csv':
            df = pd.read_csv(file_path, encoding='utf-8')
        else:
            print(f"[ERROR] Unsupported file type: {file_ext}")
            return None
        
        print(f"  [OK] Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"  [ERROR] Failed to load: {str(e)}")
        return None


def standardize_dataset(df, name):
    """Standardize column names and parse dates/amounts"""
    
    print(f"\nStandardizing {name}...")
    df = df.copy()
    
    # Lowercase column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Parse dates
    date_col = next((col for col in df.columns if 'date' in col), None)
    if date_col:
        df['date'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Parse amounts
    amount_col = next((col for col in df.columns if 'amount' in col and 'ref' not in col), None)
    if amount_col:
        df['amount'] = pd.to_numeric(df[amount_col], errors='coerce')
    
    # Ensure we have category, note, type columns
    for col_name in ['category', 'note', 'type']:
        if col_name not in df.columns:
            matching = [c for c in df.columns if col_name in c.lower()]
            if matching:
                df[col_name] = df[matching[0]]
            else:
                df[col_name] = None
    
    print(f"  Columns: {list(df.columns)[:10]}...")
    print(f"  Date parsed: {df['date'].notna().sum()} / {len(df)}")
    print(f"  Amount parsed: {df['amount'].notna().sum()} / {len(df)}")
    
    return df


def filter_expenses(df, name):
    """Filter for true expenses (excluding transfers)"""
    
    print(f"\nFiltering {name} for expenses...")
    
    initial_count = len(df)
    
    # Filter 1: Type = Expense (if type column exists)
    if 'type' in df.columns and df['type'].notna().any():
        # Check what values exist in type column
        type_values = df['type'].value_counts()
        print(f"  Type values found: {dict(type_values)}")
        
        # Filter for expenses (case-insensitive)
        df = df[df['type'].str.lower().str.contains('expense', na=False)].copy()
        print(f"  After type filter: {len(df):,} rows")
    else:
        # If no type column, use negative amounts as expenses
        df = df[df['amount'] < 0].copy()
        print(f"  After amount filter (negative): {len(df):,} rows")
    
    # Filter 2: Exclude transfers
    if 'category' in df.columns:
        category_values = df['category'].value_counts().head(20)
        print(f"  Top categories: {dict(category_values)}")
        
        # Exclude transfer categories
        transfer_keywords = ['transfer', 'withdraw', 'withdrawal']
        
        # Create mask with proper index alignment
        mask = pd.Series([True] * len(df), index=df.index)
        for keyword in transfer_keywords:
            if df['category'].notna().any():
                keyword_mask = df['category'].str.lower().str.contains(keyword, na=False)
                mask = mask & ~keyword_mask
        
        before_transfer_filter = len(df)
        df = df[mask].copy()
        print(f"  After transfer exclusion: {len(df):,} rows (removed {before_transfer_filter - len(df)})")
    
    print(f"  Total filtered: {initial_count:,} -> {len(df):,} rows")
    
    return df


def create_transaction_key(df):
    """Create unique key for matching transactions"""
    
    # Create a composite key: date + amount + category
    df['txn_key'] = (
        df['date'].dt.strftime('%Y-%m-%d').fillna('NO_DATE') + '_' +
        df['amount'].round(2).astype(str).fillna('NO_AMOUNT') + '_' +
        df['category'].fillna('NO_CATEGORY').str.strip()
    )
    
    return df


def find_missing_transactions(manual_df, app_df):
    """Identify transactions in manual file but not in app export"""
    
    print("\n" + "=" * 80)
    print("IDENTIFYING MISSING TRANSACTIONS")
    print("=" * 80)
    
    # Create transaction keys
    manual_df = create_transaction_key(manual_df)
    app_df = create_transaction_key(app_df)
    
    print(f"\nManual file unique keys: {manual_df['txn_key'].nunique()}")
    print(f"App export unique keys:  {app_df['txn_key'].nunique()}")
    
    # Find keys in manual but not in app
    manual_keys = set(manual_df['txn_key'])
    app_keys = set(app_df['txn_key'])
    
    missing_keys = manual_keys - app_keys
    extra_keys = app_keys - manual_keys
    
    print(f"\nTransactions in Manual but NOT in App: {len(missing_keys)}")
    print(f"Transactions in App but NOT in Manual: {len(extra_keys)}")
    
    # Get missing transactions
    missing_txns = manual_df[manual_df['txn_key'].isin(missing_keys)].copy()
    extra_txns = app_df[app_df['txn_key'].isin(extra_keys)].copy()
    
    return missing_txns, extra_txns


def analyze_missing_transactions(missing_df, extra_df):
    """Detailed analysis of missing transactions"""
    
    print("\n" + "=" * 80)
    print("MISSING TRANSACTIONS ANALYSIS")
    print("=" * 80)
    
    # Temporal distribution
    print("\n" + "-" * 80)
    print("TEMPORAL DISTRIBUTION OF MISSING TRANSACTIONS")
    print("-" * 80)
    
    missing_df['year_month'] = missing_df['date'].dt.to_period('M')
    monthly_missing = missing_df.groupby('year_month').agg({
        'amount': ['count', 'sum']
    }).round(2)
    
    print("\nMonth     | Count | Total Amount")
    print("-" * 40)
    for month, row in monthly_missing.iterrows():
        print(f"{month} | {row[('amount', 'count')]:>5.0f} | {row[('amount', 'sum')]:>12,.2f}")
    
    # Category distribution
    print("\n" + "-" * 80)
    print("CATEGORY DISTRIBUTION OF MISSING TRANSACTIONS")
    print("-" * 80)
    
    category_missing = missing_df.groupby('category').agg({
        'amount': ['count', 'sum']
    }).sort_values(('amount', 'count'), ascending=False).head(15)
    
    print("\nCategory                    | Count | Total Amount")
    print("-" * 60)
    for category, row in category_missing.iterrows():
        cat_str = str(category)[:25].ljust(25)
        print(f"{cat_str} | {row[('amount', 'count')]:>5.0f} | {row[('amount', 'sum')]:>12,.2f}")
    
    # Amount distribution
    print("\n" + "-" * 80)
    print("AMOUNT DISTRIBUTION OF MISSING TRANSACTIONS")
    print("-" * 80)
    
    print(f"\nTotal missing amount: {missing_df['amount'].sum():,.2f}")
    print(f"Average: {missing_df['amount'].mean():,.2f}")
    print(f"Median:  {missing_df['amount'].median():,.2f}")
    print(f"Min:     {missing_df['amount'].min():,.2f}")
    print(f"Max:     {missing_df['amount'].max():,.2f}")
    
    # Sample missing transactions
    print("\n" + "-" * 80)
    print("SAMPLE MISSING TRANSACTIONS (First 20)")
    print("-" * 80)
    
    display_cols = ['date', 'amount', 'category', 'note']
    available_cols = [col for col in display_cols if col in missing_df.columns]
    
    sample = missing_df[available_cols].head(20)
    
    for idx, row in sample.iterrows():
        print(f"\n{idx + 1}.")
        for col in available_cols:
            val = row[col]
            if pd.isna(val):
                val = "N/A"
            elif isinstance(val, pd.Timestamp):
                val = val.strftime('%Y-%m-%d')
            print(f"  {col}: {val}")
    
    # Extra transactions analysis
    if len(extra_df) > 0:
        print("\n" + "=" * 80)
        print("EXTRA TRANSACTIONS IN APP (Not in Manual)")
        print("=" * 80)
        
        print(f"\nTotal count: {len(extra_df)}")
        print(f"Total amount: {extra_df['amount'].sum():,.2f}")
        
        extra_df['year_month'] = extra_df['date'].dt.to_period('M')
        monthly_extra = extra_df.groupby('year_month').agg({
            'amount': ['count', 'sum']
        }).round(2).head(10)
        
        print("\nMonth     | Count | Total Amount")
        print("-" * 40)
        for month, row in monthly_extra.iterrows():
            print(f"{month} | {row[('amount', 'count')]:>5.0f} | {row[('amount', 'sum')]:>12,.2f}")


def export_missing_transactions(missing_df, output_path):
    """Export missing transactions to CSV for review"""
    
    output_path = Path(output_path)
    
    # Select relevant columns
    export_cols = ['date', 'amount', 'category', 'note', 'type', 'account', 'payment_type']
    available_cols = [col for col in export_cols if col in missing_df.columns]
    
    export_df = missing_df[available_cols].copy()
    export_df = export_df.sort_values('date')
    
    export_df.to_csv(output_path, index=False)
    print(f"\n[OK] Exported {len(export_df)} missing transactions to: {output_path}")


def main():
    """Main execution"""
    
    print("\n" + "=" * 80)
    print("DEEP TRANSACTION ANALYSIS")
    print("Find missing expense transactions between datasets")
    print("=" * 80)
    
    # Get file paths
    if len(sys.argv) >= 3:
        manual_path = sys.argv[1]
        app_path = sys.argv[2]
    else:
        print("\nEnter file paths:")
        manual_path = input("Manual/Historical file (source of truth): ").strip().strip('"')
        app_path = input("App Export file: ").strip().strip('"')
    
    # Load files
    manual_df = load_file(manual_path, "Manual File")
    app_df = load_file(app_path, "App Export")
    
    if manual_df is None or app_df is None:
        print("\n[ERROR] Failed to load files!")
        return
    
    # Standardize
    manual_df = standardize_dataset(manual_df, "Manual File")
    app_df = standardize_dataset(app_df, "App Export")
    
    # Filter for expenses only
    manual_expenses = filter_expenses(manual_df, "Manual File")
    app_expenses = filter_expenses(app_df, "App Export")
    
    # Find missing transactions
    missing_txns, extra_txns = find_missing_transactions(manual_expenses, app_expenses)
    
    # Analyze
    analyze_missing_transactions(missing_txns, extra_txns)
    
    # Export for manual review
    output_path = Path("data/analysis_missing_transactions.csv")
    export_missing_transactions(missing_txns, output_path)
    
    # Export extra transactions too
    if len(extra_txns) > 0:
        extra_output = Path("data/analysis_extra_transactions.csv")
        export_missing_transactions(extra_txns, extra_output)
        print(f"[OK] Exported {len(extra_txns)} extra transactions to: {extra_output}")
    
    # Summary recommendation
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    
    missing_amount = abs(missing_txns['amount'].sum())
    missing_count = len(missing_txns)
    
    print(f"\nTotal missing: {missing_count} transactions, {missing_amount:,.2f} in value")
    
    if missing_count > 0:
        print("\nNext steps:")
        print("1. Review 'data/analysis_missing_transactions.csv'")
        print("2. Identify patterns (specific months, categories, or amounts)")
        print("3. Check if these are:")
        print("   - Duplicate entries in manual file")
        print("   - Transactions deleted from app")
        print("   - Data import/export issues")
        print("4. Decide on reconciliation strategy:")
        print("   a) Trust manual file entirely (use as-is)")
        print("   b) Merge: Use app export + add missing transactions")
        print("   c) Clean manual file to remove duplicates")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()