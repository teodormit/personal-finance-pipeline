"""
Wallet Export Inspector
Analyzes the structure and data types of your Wallet app export
Supports: .xlsx, .xls, .csv files
Run this FIRST before designing the database schema
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import re


# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def read_file(file_path):
    """Read Excel or CSV file with appropriate method"""
    
    file_path = Path(file_path)
    file_ext = file_path.suffix.lower()
    
    print(f"\nFile: {file_path.name}")
    print(f"Extension: {file_ext}")
    
    # Excel files (.xlsx, .xls)
    if file_ext in ['.xlsx', '.xls']:
        print("\nAttempting to read as Excel file...")
        
        # Try openpyxl for .xlsx
        if file_ext == '.xlsx':
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                print("[OK] Successfully read .xlsx file with openpyxl")
                return df
            except Exception as e:
                print(f"[FAIL] openpyxl failed: {str(e)[:100]}")
        
        # Try xlrd for .xls
        if file_ext == '.xls':
            try:
                df = pd.read_excel(file_path, engine='xlrd')
                print("[OK] Successfully read .xls file with xlrd")
                return df
            except Exception as e:
                print(f"[FAIL] xlrd failed: {str(e)[:100]}")
        
        # Try default pandas engine
        try:
            df = pd.read_excel(file_path)
            print("[OK] Successfully read Excel file with default engine")
            return df
        except Exception as e:
            print(f"[FAIL] Default engine failed: {str(e)[:100]}")
            return None
    
    # CSV files
    elif file_ext == '.csv':
        print("\nAttempting to read as CSV file...")
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"[OK] Successfully read CSV with {encoding}")
                return df
            except Exception as e:
                print(f"[FAIL] {encoding} failed: {str(e)[:50]}")
                continue
        
        return None
    
    else:
        print(f"\n[ERROR] Unsupported file extension: {file_ext}")
        print("Supported: .xlsx, .xls, .csv")
        return None


def inspect_file(file_path):
    """Comprehensive file inspection"""
    
    print("=" * 80)
    print("WALLET APP EXPORT - DATA STRUCTURE ANALYSIS")
    print("=" * 80)
    
    # Read the file
    df = read_file(file_path)
    
    if df is None:
        print("\n[ERROR] Could not read file!")
        return
    
    # Basic info
    print(f"\n{'─' * 80}")
    print("BASIC INFORMATION")
    print(f"{'─' * 80}")
    print(f"Total Rows:    {len(df):,}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"Memory Usage:  {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # Column names
    print(f"\n{'─' * 80}")
    print("COLUMN NAMES (in order)")
    print(f"{'─' * 80}")
    for idx, col in enumerate(df.columns, 1):
        print(f"{idx:2d}. {col}")
    
    # Data types and null counts
    print(f"\n{'─' * 80}")
    print("DATA TYPES & NULL ANALYSIS")
    print(f"{'─' * 80}")
    print(f"{'Column':<30} {'Type':<15} {'Non-Null':<10} {'Null':<8} {'Null %':<8}")
    print(f"{'-' * 80}")
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null = df[col].count()
        null_count = df[col].isnull().sum()
        null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0
        
        print(f"{col:<30} {dtype:<15} {non_null:<10} {null_count:<8} {null_pct:>6.1f}%")
    
    # Sample data - first 3 rows
    print(f"\n{'─' * 80}")
    print("FIRST 3 ROWS (SAMPLE DATA)")
    print(f"{'─' * 80}")
    
    # Display in a more readable format
    for idx, row in df.head(3).iterrows():
        print(f"\nRow {idx + 1}:")
        for col in df.columns:
            value = row[col]
            # Truncate long strings
            if isinstance(value, str) and len(str(value)) > 50:
                value = str(value)[:47] + "..."
            print(f"  {col}: {repr(value)}")
    
    # Date column analysis
    print(f"\n{'=' * 80}")
    print("DATE COLUMN ANALYSIS")
    print(f"{'=' * 80}")
    
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    
    if not date_cols:
        print("[WARNING] No column with 'date' in the name found!")
        print(f"Available columns: {list(df.columns)}")
    else:
        for date_col in date_cols:
            analyze_date_column(df, date_col)
    
    # Amount column analysis
    print(f"\n{'─' * 80}")
    print("AMOUNT COLUMN ANALYSIS")
    print(f"{'─' * 80}")
    
    amount_cols = [col for col in df.columns if 'amount' in col.lower()]
    
    if amount_cols:
        for amount_col in amount_cols:
            print(f"\nColumn: {amount_col}")
            print(f"  Data type: {df[amount_col].dtype}")
            
            # Try to convert to numeric if not already
            if not pd.api.types.is_numeric_dtype(df[amount_col]):
                print("  [INFO] Not numeric, attempting conversion...")
                try:
                    numeric_col = pd.to_numeric(df[amount_col], errors='coerce')
                    print(f"  [OK] Converted {numeric_col.notna().sum()} values")
                    df[amount_col] = numeric_col
                except Exception as e:
                    print(f"  [FAIL] Conversion failed: {str(e)}")
            
            # Statistics
            if pd.api.types.is_numeric_dtype(df[amount_col]):
                print(f"  Min:    {df[amount_col].min()}")
                print(f"  Max:    {df[amount_col].max()}")
                print(f"  Mean:   {df[amount_col].mean():.2f}")
                print(f"  Median: {df[amount_col].median():.2f}")
                
                # Distribution
                negative = (df[amount_col] < 0).sum()
                positive = (df[amount_col] > 0).sum()
                zero = (df[amount_col] == 0).sum()
                
                print(f"\n  Distribution:")
                print(f"    Negative: {negative} ({negative/len(df)*100:.1f}%)")
                print(f"    Positive: {positive} ({positive/len(df)*100:.1f}%)")
                print(f"    Zero:     {zero} ({zero/len(df)*100:.1f}%)")
    
    # Categorical columns with unique values
    print(f"\n{'─' * 80}")
    print("CATEGORICAL COLUMNS (Top 10 values)")
    print(f"{'─' * 80}")
    
    categorical_cols = ['category', 'account', 'payment_type', 'currency']
    
    for col_name in categorical_cols:
        # Find column (case-insensitive)
        matching_cols = [c for c in df.columns if col_name.lower() in c.lower()]
        
        if matching_cols:
            col = matching_cols[0]
            unique_count = df[col].nunique()
            print(f"\n{col} ({unique_count} unique values):")
            
            if unique_count <= 20:
                value_counts = df[col].value_counts()
            else:
                value_counts = df[col].value_counts().head(10)
            
            for val, count in value_counts.items():
                print(f"  {val}: {count}")
    
    # Final recommendations
    print(f"\n{'=' * 80}")
    print("INSPECTION COMPLETE")
    print(f"{'=' * 80}")
    print("\nKey findings:")
    print(f"  - Total transactions: {len(df):,}")
    print(f"  - Columns identified: {len(df.columns)}")
    print(f"  - Date columns: {', '.join(date_cols) if date_cols else 'None found'}")
    print(f"  - Amount columns: {', '.join(amount_cols) if amount_cols else 'None found'}")
    print("\nNext step: Review the output above to design PostgreSQL schema")


def analyze_date_column(df, date_col):
    """Detailed date column analysis"""
    
    print(f"\nColumn: '{date_col}'")
    print(f"Data type: {df[date_col].dtype}")
    print(f"Non-null values: {df[date_col].count()} / {len(df)}")
    
    # Sample raw values
    print(f"\nSample raw values (first 5):")
    for idx, val in enumerate(df[date_col].dropna().head(5), 1):
        print(f"  {idx}. {repr(val)} (type: {type(val).__name__})")
    
    # Check if already datetime
    if pd.api.types.is_datetime64_any_dtype(df[date_col]):
        print("\n[OK] Column is already datetime type")
        print(f"Date range: {df[date_col].min()} to {df[date_col].max()}")
        return True
    
    # Try parsing
    print(f"\nAttempting to parse as dates...")
    
    try:
        parsed = pd.to_datetime(df[date_col], errors='coerce')
        successful = parsed.notna().sum()
        failed = parsed.isna().sum() - df[date_col].isna().sum()
        
        print(f"[OK] Successfully parsed: {successful} / {df[date_col].count()}")
        
        if failed > 0:
            print(f"[WARNING] Failed to parse: {failed}")
            print(f"\nFailed values (first 5):")
            failed_mask = parsed.isna() & df[date_col].notna()
            for val in df.loc[failed_mask, date_col].head(5):
                print(f"  {repr(val)}")
        
        if successful > 0:
            print(f"\nDate range:")
            print(f"  Earliest: {parsed.min()}")
            print(f"  Latest:   {parsed.max()}")
            
            # Format detection
            sample = str(df[date_col].dropna().iloc[0])
            detected_format = identify_date_format(sample)
            print(f"\nDetected format: {detected_format}")
        
        return successful == df[date_col].count()
    
    except Exception as e:
        print(f"[FAIL] Parsing failed: {str(e)}")
        return False


def identify_date_format(date_string):
    """Identify date format from string"""
    
    patterns = {
        r'^\d{4}-\d{2}-\d{2}$': 'ISO 8601 (YYYY-MM-DD)',
        r'^\d{2}/\d{2}/\d{4}$': 'US Format (MM/DD/YYYY)',
        r'^\d{2}\.\d{2}\.\d{4}$': 'European Format (DD.MM.YYYY)',
        r'^\d{1,2}/\d{1,2}/\d{4}$': 'Flexible Format (M/D/YYYY)',
        r'^\d{4}/\d{2}/\d{2}$': 'YYYY/MM/DD',
        r'^\d{2}-\d{2}-\d{4}$': 'DD-MM-YYYY',
        r'^\d+$': 'Unix Timestamp or Excel Serial',
    }
    
    for pattern, format_name in patterns.items():
        if re.match(pattern, date_string):
            return format_name
    
    return "Unknown / Custom Format"


def main():
    """Main execution"""
    
    # Get file path
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip().strip('"')
    else:
        print("\n[WALLET EXPORT INSPECTOR]")
        print("=" * 80)
        file_path = input("Enter path to Wallet export (.xlsx, .xls, or .csv): ").strip().strip('"')
    
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"\n[ERROR] File not found: {file_path}")
        print(f"Current directory: {Path.cwd()}")
        
        # Show files in data/raw/ if it exists
        raw_dir = Path.cwd() / 'data' / 'raw'
        if raw_dir.exists():
            files = list(raw_dir.glob('*.xls*')) + list(raw_dir.glob('*.csv'))
            if files:
                print(f"\nFiles found in data/raw/:")
                for f in files:
                    print(f"  - {f.name}")
        return
    
    inspect_file(file_path)


if __name__ == "__main__":
    main()