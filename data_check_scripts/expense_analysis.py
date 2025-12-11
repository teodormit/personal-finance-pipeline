import pandas as pd
import numpy as np
from datetime import datetime
import re
import os

def analyze_expense_data(file_path):
    """
    Analyze expense tracking data with focus on date column validation
    """
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return
    
    try:
        # Read the file - try different formats
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            # Try CSV first, then Excel
            try:
                df = pd.read_csv(file_path)
            except:
                df = pd.read_excel(file_path)
        
        print("=" * 60)
        print("EXPENSE TRACKING DATA ANALYSIS")
        print("=" * 60)
        
        # Basic info about the dataset
        print(f"\nDataset Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"\nColumn Names: {list(df.columns)}")
        
        # Display data types
        print("\n" + "=" * 40)
        print("DATA TYPES ANALYSIS")
        print("=" * 40)
        
        for col in df.columns:
            dtype = df[col].dtype
            non_null_count = df[col].count()
            null_count = df[col].isnull().sum()
            print(f"{col:20} | {str(dtype):15} | Non-null: {non_null_count:4} | Null: {null_count:4}")
        
        # Find date column (case-insensitive)
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        
        if not date_columns:
            print("\nWARNING:  WARNING: No column with 'date' in the name found!")
            print("Available columns:", list(df.columns))
            return
        
        print(f"\nFound date column(s): {date_columns}")
        
        # Analyze each date column
        for date_col in date_columns:
            print(f"\n" + "=" * 50)
            print(f"DATE COLUMN ANALYSIS: '{date_col}'")
            print("=" * 50)
            
            analyze_date_column(df, date_col)
        
        # Display first few rows
        print(f"\n" + "=" * 40)
        print("FIRST 5 ROWS OF DATA")
        print("=" * 40)
        print(df.head())
        
        # PostgreSQL compatibility check
        print(f"\n" + "=" * 50)
        print("POSTGRESQL INGESTION READINESS")
        print("=" * 50)
        postgres_compatibility_check(df, date_columns)
        
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        print("Please check the file format and path.")

def analyze_date_column(df, date_col):
    """
    Detailed analysis of a date column
    """
    
    # Basic stats
    total_rows = len(df)
    non_null_count = df[date_col].count()
    null_count = df[date_col].isnull().sum()
    
    print(f"Total rows: {total_rows}")
    print(f"Non-null values: {non_null_count}")
    print(f"Null values: {null_count}")
    print(f"Data type: {df[date_col].dtype}")
    
    if null_count > 0:
        print(f"WARNING: {null_count} rows have missing dates!")
    
    # Check if already datetime
    if pd.api.types.is_datetime64_any_dtype(df[date_col]):
        print("SUCCESS: Column is already datetime type")
        print(f"Date range: {df[date_col].min()} to {df[date_col].max()}")
        return True
    
    # Sample non-null values
    non_null_values = df[date_col].dropna()
    if len(non_null_values) == 0:
        print("ERROR: No non-null values to analyze")
        return False
    
    print(f"\nSample values:")
    for i, val in enumerate(non_null_values.head(10)):
        print(f"  {i+1}. {repr(val)}")
    
    # Try to parse dates
    print(f"\nDATE PARSING ANALYSIS:")
    print("-" * 30)
    
    successful_parses = 0
    failed_parses = 0
    date_formats_found = set()
    
    for idx, value in non_null_values.items():
        if pd.isna(value):
            continue
            
        # Convert to string if not already
        str_value = str(value).strip()
        
        # Try parsing with pandas
        try:
            parsed_date = pd.to_datetime(str_value, errors='raise')
            successful_parses += 1
            
            # Try to identify format
            format_pattern = identify_date_format(str_value)
            if format_pattern:
                date_formats_found.add(format_pattern)
                
        except:
            failed_parses += 1
            if failed_parses <= 5:  # Show first 5 failures
                print(f"  ERROR: Failed to parse: {repr(str_value)}")
    
    print(f"\nParsing Results:")
    print(f"  SUCCESS: Successfully parsed: {successful_parses}/{non_null_count}")
    print(f"  ERROR: Failed to parse: {failed_parses}/{non_null_count}")
    
    if date_formats_found:
        print(f"  DATE RANGE: Detected formats: {', '.join(date_formats_found)}")
    
    # Overall assessment
    success_rate = successful_parses / non_null_count if non_null_count > 0 else 0
    print(f"\n ASSESSMENT:")
    if success_rate == 1.0:
        print("  SUCCESS: ALL date values can be parsed correctly!")
    elif success_rate >= 0.95:
        print("   WARNING:  Most dates are valid, but some issues found")
    else:
        print("  ERROR: Significant date parsing issues detected")
    
    return success_rate == 1.0

def identify_date_format(date_string):
    """
    Try to identify the format of a date string
    """
    date_patterns = [
        (r'^\d{4}-\d{2}-\d{2}$', 'YYYY-MM-DD'),
        (r'^\d{2}/\d{2}/\d{4}$', 'MM/DD/YYYY'),
        (r'^\d{2}/\d{2}/\d{2}$', 'MM/DD/YY'),
        (r'^\d{1,2}/\d{1,2}/\d{4}$', 'M/D/YYYY'),
        (r'^\d{4}/\d{2}/\d{2}$', 'YYYY/MM/DD'),
        (r'^\d{2}-\d{2}-\d{4}$', 'MM-DD-YYYY'),
        (r'^\d{4}-\d{1,2}-\d{1,2}$', 'YYYY-M-D'),
    ]
    
    for pattern, format_name in date_patterns:
        if re.match(pattern, date_string):
            return format_name
    
    return "Unknown format"

def postgres_compatibility_check(df, date_columns):
    """
    Check PostgreSQL compatibility
    """
    
    print("PostgreSQL Date/Timestamp Compatibility:")
    print("-" * 40)
    
    for date_col in date_columns:
        print(f"\nColumn: {date_col}")
        
        # Check if conversion to datetime works
        try:
            converted = pd.to_datetime(df[date_col], errors='coerce')
            invalid_dates = converted.isnull().sum() - df[date_col].isnull().sum()
            
            if invalid_dates == 0:
                print("  SUCCESS: Ready for PostgreSQL ingestion")
                
                # Check date range (PostgreSQL supports 4713 BC to 294276 AD)
                if not converted.dropna().empty:
                    min_date = converted.min()
                    max_date = converted.max()
                    print(f"  DATE RANGE: Date range: {min_date.date()} to {max_date.date()}")
                    
                    if min_date.year < 1900 or max_date.year > 2100:
                        print(" WARNING:   Dates outside typical range - verify data quality")
                
                # Suggest PostgreSQL data type
                print("  RECOMMENDATION: Recommended PostgreSQL type: DATE or TIMESTAMP")
                
            else:
                print(f"  ERROR: {invalid_dates} dates would be invalid in PostgreSQL")
                print("  RECOMMENDATION: Clean data before ingestion")
                
        except Exception as e:
            print(f"  ERROR: Conversion failed: {str(e)}")

# Main execution
if __name__ == "__main__":
    # Change working directory to the script's directory
    import os
    # os.chdir("/path/to/your/expense/files")  # Replace with your actual path
    os.chdir("C:\\Code Repos\\personal-finance-pipeline\\data_check_scripts")
    # File path - update this to match your file location
    #file_path = "Expense Tracking -TMITOV.xlsx"  # or .xlsx
    file_path = input()  # or .xlsx
    print("Please enter the file path to analyze (e.g., 'Expense Tracking.csv' or 'Expense Tracking.xlsx'):")
    # Alternative file paths to try
    possible_paths = [
        "report_2025-08-04_233220.xlsx"
        "Expense_Final_December.xlsx",
        #"C:\\Code Repos\\personal-finance-pipeline\\data_check_scripts\\Expense Tracking -TMITOV.csv",
        #"C:\\Code Repos\\personal-finance-pipeline\\data_check_scripts\\Expense_Final_December.xlsx"
    ]
    
    file_found = False
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found file: {path}")
            analyze_expense_data(path)
            file_found = True
            break
    
    if not file_found:
        print("File not found. Please ensure 'Expense Tracking.csv' or 'Expense Tracking.xlsx' is in the current directory.")
        print("Current directory:", os.getcwd())
        print("Files in current directory:", [f for f in os.listdir('.') if f.endswith(('.csv', '.xlsx', '.xls'))])
