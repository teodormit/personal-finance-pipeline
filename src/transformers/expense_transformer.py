"""
Expense Data Transformer - LEAN VERSION
========================================

PURPOSE: Clean and prepare raw expense data for database loading

WHAT THIS DOES:
Transform raw data from Wallet app into clean, standardized format

INPUT: Raw CSV/Excel with columns like:
    date, note, type, amount, category, currency, etc.

OUTPUT: Clean DataFrame ready for PostgreSQL with:
    - Parsed dates
    - Validated amounts
    - EUR conversion
    - Transaction hashes
    - Derived fields (year, month, etc.)
"""

import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path so we can import our utilities
sys.path.append(str(Path(__file__).parent.parent))

from utils.hash_generator import generate_hashes_for_dataframe


# ============================================================================
# MAIN TRANSFORMER CLASS
# ============================================================================

class ExpenseTransformer:
    """
    Transforms raw expense data into clean, analytics-ready format
    
    USAGE:
        transformer = ExpenseTransformer()
        clean_df = transformer.transform(raw_df)

    Reusable for monthly updates
    """
    
    # ========================================================================
    # CONCEPT: EUR CONVERSION RATES (Class Variable)
    # ========================================================================
    # This is a "class variable" - shared by all instances
    # Think of it as a reference table anyone can use
    # Not specific to one transformer object
    
    EUR_RATES = {
        'BGN': 0.5112,  # 1 BGN = 0.5112 EUR
        'EUR': 1.0,     # 1 EUR = 1 EUR
        'USD': 1.17,    # 1 USD = 1.17 EUR
    }
    
    def __init__(self):
        """
        Initialize the transformer
        
        WHEN THIS RUNS:
            transformer = ExpenseTransformer()  # __init__ runs here
        
        WHAT IT DOES:
        - Sets up empty statistics dictionary
        - Prepares transformer for first use
        
        WHY: We want to track stats (how many rows, how many issues, etc.)
        """
        
        self.stats = {
            'rows_input': 0,
            'rows_output': 0,
            'issues_found': []
        }
        
        print("Expense transformer initialized")
    
    def transform(self, df):
        """
        Main transformation pipeline - this is the public interface
        
        PARAMETERS:
        - df: Raw DataFrame from Excel/CSV file
        
        RETURNS:
        - Cleaned, transformed DataFrame ready for database
        
        USAGE:
            transformer = ExpenseTransformer()
            clean_df = transformer.transform(raw_df)
        """
        
        print(f"\n{'='*60}")
        print("STARTING DATA TRANSFORMATION")
        print(f"{'='*60}\n")
        
        self.stats['rows_input'] = len(df)
        print(f"Input: {len(df):,} rows")
        
        # ====================================================================
        # TRANSFORMATION PIPELINE
        # ====================================================================
        # Each step modifies the DataFrame and returns it
        # This is called "method chaining" - output of one = input of next
        
        df = self._step1_rename_columns(df)
        df = self._step2_parse_dates(df)
        df = self._step3_parse_amounts(df)
        df = self._step4_standardize_types(df)
        df = self._step5_convert_to_eur(df)
        df = self._step6_add_derived_fields(df)
        df = self._step7_add_classification(df)  
        df = self._step8_generate_hashes(df)
        df = self._step9_final_cleanup(df)
        
        self.stats['rows_output'] = len(df)
        
        print(f"\n{'='*60}")
        print(f"TRANSFORMATION COMPLETE")
        print(f"Output: {len(df):,} rows")
        print(f"{'='*60}\n")
        
        return df
    
    # ========================================================================
    # PRIVATE METHODS (Internal transformation steps)
    # ========================================================================
    # Methods starting with _ are "private" - internal use only
    
    def _step1_rename_columns(self, df):
        """
        Rename columns to match database schema
        
        WHY: Source file has different column names than database
        
        EXAMPLE:
            'note' → 'description'
            'category' → 'subcategory' (remember: we changed this!)
            'account' → 'account_name'
        """
        print("[1/9] Renaming columns...")
        
        df = df.copy()  # Don't modify original DataFrame
        
        # Lowercase all column names
        df.columns = df.columns.str.lower().str.strip()
        
        # Rename to match our schema
        df = df.rename(columns={
            'note': 'description',
            'category': 'subcategory',  # Original becomes subcategory
            'payment': 'payment_method'
        })
        
        print(f"  Columns: {list(df.columns)[:5]}...")
        return df
    
    def _step2_parse_dates(self, df):
        """
        Convert date strings to proper datetime objects
        
        WHY:
        - Database needs DATE type, not strings
        - Enables date calculations
        - Validates dates are real (Feb 30th would fail)
        
        Handles multiple date formats robustly
         - ISO format: '2024-03-29'
         - With timestamps: '7/27/2025 3:00:35'
         - Various separators: '-', '/', '.'
         - Different orderings: YYYY-MM-DD, MM/DD/YYYY, DD.MM.YYYY

        """
        print("[2/9] Parsing dates...")
        
        df = df.copy()
        
        parsed_dates = []
        failed_rows = []
        
        date_formats = [
                '%Y-%m-%d',           # 2024-03-29
                '%Y-%d-%m',           # 2025-20-02
                '%m-%d-%Y',           # 03-29-2024
                '%d-%m-%Y',           # 29-03-2024
                '%Y-%m-%d',           # 2024-03-29
                '%m/%d/%Y',           # 7/27/2025
                '%d/%m/%Y',           # 27/7/2025
                '%Y/%m/%d',           # 2025/07/27
                '%m/%d/%Y %H:%M:%S',  # 7/27/2025 3:00:35
                '%d/%m/%Y %H:%M:%S',  # 27/7/2025 3:00:35
                '%Y-%m-%d %H:%M:%S',  # 2024-03-29 12:00:00
                '%Y-%M-%D %H:%M:%S',  # 2024-03-29 12:00:00
                '%Y-%d-%m %H:%M:%S',  # 2025-20-02 12:00:00
                '%m-%d-%Y %H:%M:%S',  # 03-29-2024 12:00:00
                '%d-%m-%Y %H:%M:%S',  # 29-03-2024 12:00:00
                '%m/%d/%Y %H:%M:%S',  # 7/27/2025 3:00:35
                '%d/%m/%Y %H:%M:%S',  # 27/7/2025 3:00:35
                '%Y/%m/%d %H:%M:%S',  # 2025/07/27 3:00:35
                '%m/%d/%Y %H:%M:%S',  # 7/27/2025 3:00:35
                '%d/%m/%Y %H:%M:%S',  # 27/7/2025 3:00:35
                '%d.%m.%Y',           # 29.03.2024 (European)
                '%d-%m-%Y',           # 29-03-2024
                '%Y.%d.%m',           # 2025.20.02
                '%Y %d %m',           # "2025 20 02"
            ]
        
        for idx, date_value in enumerate(df['date']):
            parsed_date = None
            
            if pd.isna(date_value):
                parsed_dates.append(pd.NaT)
                continue  
            
            # Attempt 1: flexible pandas parser        
            try:
                # Try parsing with pandas (handles many formats)
                parsed_date = pd.to_datetime(date_value, errors='raise')
            except (ValueError, TypeError):
                parsed_date = None
            
             # ATTEMPT 2: Try common formats explicitly        
            if parsed_date is None:
                for fmt in date_formats:
                    try:
                        parsed_date = pd.to_datetime(str(date_value), format=fmt, errors='raise')
                        break
                    except (ValueError, TypeError):
                        parsed_date = None
                        continue

        # If all attempts failed
            if parsed_date is None:
                parsed_dates.append(pd.NaT)
                failed_rows.append((idx, date_value))
            else:
                parsed_dates.append(parsed_date)
     
        # Assign parsed dates
        df['date'] = pd.Series(parsed_dates, index=df.index)
    
    # Report results
        valid = df['date'].notna().sum()
        invalid = df['date'].isna().sum()
    
        print(f"  Parsed {valid:,} dates")
    
        if invalid > 0:
            print(f"  Warning: {invalid} invalid dates")
            self.stats['issues_found'].append(f"Invalid dates: {invalid}")
            # Show first few failed dates for debugging
            if failed_rows:
                print(f" Failed date examples:")
                for idx, val in failed_rows[:3]:
                    print(f"    Row {idx}: {repr(val)}")
    
        if valid > 0:
            try:
                print(f"  Range: {df['date'].min().date()} to {df['date'].max().date()}")
            except Exception:
                pass

        return df


    def _step3_parse_amounts(self, df):
        """
        Convert amount strings to numbers
        
        WHY:
        - Database needs NUMERIC type
        - Enables math operations (sum, average, etc.)
        
        WHAT HAPPENS:
        '-45.50' (string) → -45.50 (number)
        """
        print("[3/9] Parsing amounts...")
        
        df = df.copy()
        
        # Convert to numeric (float)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Calculate absolute value (for easier aggregations)
        # abs(-45.50) = 45.50
        df['amount_abs'] = df['amount'].abs()
        
        valid = df['amount'].notna().sum()
        print(f"  Parsed {valid:,} amounts")
        print(f"  Range: ${df['amount'].min():.2f} to ${df['amount'].max():.2f}")
        
        return df
    
    def _step4_standardize_types(self, df):
        """
        Convert transaction types to standard format
        
        SOURCE: 'Expenses', 'Income'
        TARGET: 'EXPENSE', 'INCOME'
        
        WHY: Database constraint requires exact values
        """
        print("[4/9] Standardizing transaction types...")
        
        df = df.copy()
        
        # Map source values to database values
        type_map = {
            'expenses': 'EXPENSE',
            'income': 'INCOME'
        }
        
        df['transaction_type'] = df['type'].str.lower().map(type_map)
        
        # Show distribution
        counts = df['transaction_type'].value_counts()
        print(f"  Types:")
        for txn_type, count in counts.items():
            print(f"    {txn_type}: {count:,}")
        
        return df
    
    def _step5_convert_to_eur(self, df):
        """
        Convert all amounts to EUR for standardization
        
        WHY:
        - Easy to compare across currencies
        - Simplifies aggregations
        - Keep original currency too (for reference)
        
        FORMULA:
        amount_eur = amount_original × conversion_rate
        
        EXAMPLE:
        -100 BGN × 0.5112 = -51.12 EUR
        """
        print("[5/9] Converting to EUR...")
        
        df = df.copy()
        
        # Get rate for each currency
        df['eur_conversion_rate'] = df['currency'].map(self.EUR_RATES)
        
        # Handle unknown currencies (default to 1.0)
        unknown = df['eur_conversion_rate'].isna().sum()
        if unknown > 0:
            print(f" {unknown} unknown currencies, defaulting to 1.0")
            df['eur_conversion_rate'] = df['eur_conversion_rate'].fillna(1.0)
        
        # Calculate EUR amounts
        df['amount_eur'] = (df['amount'] * df['eur_conversion_rate']).round(2)
        df['amount_abs_eur'] = (df['amount_abs'] * df['eur_conversion_rate']).round(2)
        
        print(f"  Converted to EUR")
        print(f"  Total: €{df['amount_eur'].sum():,.2f}")
        
        return df
    
    def _step6_add_derived_fields(self, df):
        """
        Calculate additional fields from date
        
        WHY:
        - Makes queries easier (no need to calculate in SQL)
        - Speeds up dashboard filtering
        
        ADDS:
        - year: 2024
        - month: 12
        - quarter: Q4
        - year_month: '2024-12'
        - day_of_week: 1-7 (1=Monday)
        - is_weekend: True/False
        """
        print("[6/9] Adding derived fields...")
        
        df = df.copy()
        
        # Extract date components
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.quarter
        df['year_month'] = df['date'].dt.strftime('%Y-%m')
        df['day_of_week'] = df['date'].dt.dayofweek + 1  # 1=Mon, 7=Sun
        df['week_of_year'] = df['date'].dt.isocalendar().week
        df['is_weekend'] = df['day_of_week'].isin([6, 7])
        
        print(f"  Added 7 date-derived fields")
        
        return df
    
    def _step7_add_classification(self, df):
        """
        Add wants vs needs classification
        """
        print("[7/9] Adding wants/needs classification...")
        
        df = df.copy()
        
        # Add placeholder column (will be populated from database later)
        df['classification'] = None
        
        print(f"  Classification column added (will populate from category_mapping)")
        
        return df
        
    def _step8_generate_hashes(self, df):
        """
        Create unique fingerprints for each transaction
        
        WHY: Detect duplicates in future loads
        
        HOW: Uses hash_generator.py
        """
        print("[8/9] Generating transaction hashes...")
        
        df = df.copy()
        
        # Use our hash generator utility
        df['transaction_hash'] = generate_hashes_for_dataframe(df)
        
        # Check for duplicates within this file
        duplicates = df['transaction_hash'].duplicated().sum()
        if duplicates > 0:
            print(f" {duplicates} duplicate transactions in source file")
        
        print(f"  Generated {len(df):,} hashes")
        
        return df
    
    def _step9_final_cleanup(self, df):
        """
        Remove rows with missing critical data
        
        CRITICAL FIELDS:
        - date (can't have transaction without date)
        - amount (can't have transaction without amount)
        - transaction_type (must be EXPENSE or INCOME)
        - subcategory (need for grouping)
        
        WHY: Database has NOT NULL constraints on these fields
        """
        print("[9/9] Final cleanup...")
        
        df = df.copy()
        
        # Count before
        before = len(df)
        
        # Remove rows missing critical data
        df = df[
            df['date'].notna() & 
            df['amount'].notna() & 
            df['transaction_type'].notna() &
            df['subcategory'].notna()
        ]
        
        # Count after
        after = len(df)
        removed = before - after
        
        if removed > 0:
            print(f" Removed {removed} rows with missing critical data")
        
        print(f"  Final: {after:,} clean rows")
        
        return df


# ============================================================================
# TESTING SECTION
# ============================================================================

if __name__ == "__main__":
    """
    Test the transformer with sample data
    """
    
    print("\n" + "="*60)
    print("EXPENSE TRANSFORMER TEST")
    print("="*60)
    
    # Create sample data (like what Wallet app exports)
    test_data = pd.DataFrame({
        'date': ['2024-03-29', '2025-20-02', '7/27/2025 3:00:35', '7/29/2025 3:00:35', '2025.20.02'],
        'note': ['Пердета', '281BATM2505200DE AC1 ПОС 23.47 BGN авт.код:438038-MANALI EOOD/Sofia/PAN:5169****1763/CT:08,Операция с карта', '281BATM2520905K6 AC1 ПОС 59.07 BGN авт.код:962648-LIDL BALGARIYA EOOD/BURGAS/PAN:5169****1763/CT:08,Операция с карта', '963FTRO25210AH6L TF2 превод,Получен вътр.банков превод', 'Invalid Date Example'],
        'type': ['Expenses', 'Expenses', 'Expenses', 'Income', 'Expenses'],
        'payee': ['', '4591TATB0', '4591TATB0','ПЕПА ТОНЕВА НИКОЛОВА, BG81UNCR70001524149621', ''],
        'amount': ['-135.000000000', '-23.470000000', '-59.07', '75', '100.00'],
        'labels': ['', '5002', '', '', ''],
        'account': ['Cash', 'UniCredit Bulbank - 1522449108BGN', 'UniCredit Bulbank - 1522449108BGN', 'UniCredit Bulbank - 1522449108BGN', 'Cash'],
        'category': ['Collections', 'Food & Drinks', 'Groceries', 'Child Support','Fines'],
        'currency': ['BGN', 'BGN', 'BGN', 'BGN', 'BGN'],
        'payment': ['CASH', 'TRANSFER', 'TRANSFER', 'TRANSFER', 'CASH'],
    })
    
    print("\nINPUT DATA:")
    print(test_data[['date', 'amount', 'category']])
    
    # Create transformer and process data
    transformer = ExpenseTransformer()
    clean_data = transformer.transform(test_data)
    
    print("\nOUTPUT DATA (selected columns):")
    print(clean_data[['date', 'amount', 'amount_eur', 'transaction_type', 'subcategory']])
    
    print("\n" + "="*60)
    print("Transformer test complete!")
    print("="*60 + "\n")


# ============================================================================
# KEY TAKEAWAYS
# ============================================================================
"""
1. CLASSES WITH MULTIPLE METHODS:
   - Group related functions together
   - Share state across methods (self.stats)
   - Clean public interface (transform())
   - Private methods for internal steps (_step1, _step2, etc.)

2. INSTANCE VARIABLES (self.something):
   - Belong to specific object
   - Persist between method calls
   - Track state (like statistics)

3. METHOD CHAINING:
   - Each step returns modified DataFrame
   - Output of step 1 = input to step 2
   - Easy to follow pipeline

4. PRIVATE vs PUBLIC METHODS:
   - Public: Users call these (transform())
   - Private: Internal use only (_step1, _step2)
   - Convention: _ prefix means "don't call me directly"

5. DATA VALIDATION:
   - Check for problems (invalid dates, missing amounts)
   - Track issues in stats
   - Remove or fix bad data

6. WHY CLASS HERE?
   - Multiple related steps
   - Need to track statistics
   - Reusable for future loads
   - Organized and maintainable

NEXT STEPS:
- Run: python src/transformers/expense_transformer.py
- Watch transformation happen step by step
- Try modifying test data to see how it handles issues
"""