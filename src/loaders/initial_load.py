"""
Initial Data Load Script
Loads historical expense data into the data warehouse
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import uuid
import argparse

# Add src directory to path
#sys.path.append(str(Path(__file__).parent.parent / 'src'))

_src_root = Path(__file__).resolve().parent.parent  # -> ...\personal-finance-pipeline\src
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from utils.db_connector import get_db_connector
from transformers.expense_transformer import ExpenseTransformer


class InitialDataLoader:
    """Handles initial loading of historical data"""
    
    def __init__(self, file_path: str):
        """
        Initialize loader
        
        Args:
            file_path: Path to source Excel/CSV file
        """
        self.file_path = Path(file_path)
        self.db = get_db_connector()
        self.transformer = ExpenseTransformer()
        self.batch_id = uuid.uuid4()
        self.run_stats = {
            'run_id': self.batch_id,
            'start_time': datetime.now(),
            'source_file': self.file_path.name,
            'file_size_bytes': None,
            'rows_extracted': 0,
            'rows_staged': 0,
            'rows_loaded_bronze': 0,
            'rows_loaded_silver': 0,
            'rows_skipped_duplicates': 0,
            'status': 'RUNNING'
        }
    
    def load(self):
        """Execute the initial load pipeline"""
        
        print("\n" + "=" * 70)
        print("PERSONAL FINANCE PIPELINE - INITIAL DATA LOAD")
        print("=" * 70)
        print(f"Batch ID: {self.batch_id}")
        print(f"Source: {self.file_path}")
        print(f"Started: {self.run_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        try:
            # Step 1: Extract
            raw_df = self._extract()
            
            # Step 2: Transform 
            transformed_df = self._transform(raw_df)
            
            # Step 3: Load to Staging
            self._load_staging(transformed_df)
            
            # Step 4: Load to Bronze
            self._load_bronze(transformed_df)
            
            # Step 5: Load to Silver
            self._load_silver(transformed_df)
            
            # Step 6: Log pipeline run
            self.run_stats['status'] = 'SUCCESS'
            self._log_pipeline_run()
            
            # Step 7: Display summary
            self._display_summary()
            
            return True
            
        except Exception as e:
            self.run_stats['status'] = 'FAILED'
            self.run_stats['error_message'] = str(e)
            print(f"\n Pipeline failed: {str(e)}")
            
            # Try to log the failure
            try:
                self._log_pipeline_run()
            except:
                pass
            
            raise
    
    def _extract(self) -> pd.DataFrame:
        """Extract data from source file"""
        
        print(f"\n[EXTRACT] Reading source file...")
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        # Get file size
        self.run_stats['file_size_bytes'] = self.file_path.stat().st_size
        
        # Read file based on extension
        file_ext = self.file_path.suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(self.file_path)
        elif file_ext == '.csv':
            df = pd.read_csv(self.file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        self.run_stats['rows_extracted'] = len(df)
        
        print(f"  Extracted {len(df):,} rows from {file_ext} file")
        print(f"  Columns: {list(df.columns)}")
        
        return df
    
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data using ExpenseTransformer"""
        
        print(f"\n[TRANSFORM] Applying transformations...")
        
        result = self.transformer.transform(df)
        
        if isinstance(result, tuple):
            transformed_df, transform_stats = result
        else:
            transformed_df = result
            transform_stats = {
                'rows_input': len(df),
                'rows_output': len(transformed_df),
                'rows_with_issues': 0
            }
        
        print(f"  Transformation complete")
        print(f"  Input rows: {transform_stats['rows_input']:,}")
        print(f"  Output rows: {transform_stats['rows_output']:,}")
        
        if transform_stats['rows_with_issues'] > 0:
            print(f"  Rows with issues: {transform_stats['rows_with_issues']}")
        
        return transformed_df
    
    def _load_staging(self, df: pd.DataFrame):
        """Load data to staging.raw_transactions"""
        
        print(f"\n[LOAD STAGING] Loading to staging.raw_transactions...")
        
        # Truncate staging table first
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE staging.raw_transactions;")
            print("  Staging table truncated")

        # Prepare data for staging (minimal processing)
        # Use payment_method (ExpenseTransformer output) or payment_type (legacy CSV)
        payment_col = "payment_method" if "payment_method" in df.columns else "payment_type"
        staging_cols = ['date', 'description', 'type', 'payee', 'amount', 'labels', 'account', 'subcategory', 'currency', payment_col]
        staging_df = df[[c for c in staging_cols if c in df.columns]].copy()
        staging_df = staging_df.rename(columns={payment_col: 'payment', 'description': 'note', 'subcategory': 'category' })
        staging_df['source_file'] = self.file_path.name
        staging_df['batch_id'] = str(self.batch_id)
        staging_df['loaded_at'] = datetime.now()
        
        # Add row numbers
        staging_df['source_row_number'] = range(1, len(staging_df) + 1)
        
        # Load to staging
        rows_loaded = self._bulk_insert(staging_df, 'staging', 'raw_transactions')
        
        self.run_stats['rows_staged'] = rows_loaded
        print(f"  Loaded {rows_loaded:,} rows to staging")
    
    def _load_bronze(self, df: pd.DataFrame):
        """Load data to bronze.transactions_raw"""
        
        print(f"\n[LOAD BRONZE] Loading to bronze.transactions_raw...")

        # Prepare bronze data
        bronze_df = df.copy()
        
        # Rename columns to match bronze schema
        rename_map = {
            'date': 'transaction_date',
            'note': 'description',
            'payee': 'payee',
            'amount': 'amount',
            'labels': 'labels',
            'account': 'account_name',
            'subcategory': 'subcategory',
            'currency': 'currency',
        }
        if "payment_type" in bronze_df.columns and "payment_method" not in bronze_df.columns:
            rename_map["payment_type"] = "payment_method"
        bronze_df = bronze_df.rename(columns=rename_map)
        
        # Add metadata
        bronze_df['source_file'] = self.file_path.name
        bronze_df['source_row_number'] = range(1, len(bronze_df) + 1)
        bronze_df['ingestion_timestamp'] = datetime.now()
        bronze_df['ingestion_batch_id'] = str(self.batch_id)
        bronze_df['has_quality_issues'] = False
        
        # Select only columns that exist in bronze table
        bronze_columns = [
            'transaction_date', 'description', 'transaction_type', 'payee',
            'amount', 'labels', 'account_name', 'subcategory', 'currency',
            'payment_method', 'source_file', 'source_row_number',
            'ingestion_timestamp', 'ingestion_batch_id', 'has_quality_issues'
        ]
        
        bronze_df = bronze_df[bronze_columns]
        
        # Load to bronze
        rows_loaded = self._bulk_insert(bronze_df, 'bronze', 'transactions_raw')
        
        self.run_stats['rows_loaded_bronze'] = rows_loaded
        print(f"  Loaded {rows_loaded:,} rows to bronze (immutable archive)")

    def _export_duplicate_hashes(self, df: pd.DataFrame, output_file: str = None):
        """Export all records with duplicate transaction_hashes to CSV"""
        
        if output_file is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            out_dir = project_root / "data" / "inspection"
            out_dir.mkdir(parents=True, exist_ok=True)
            output_file = str(out_dir / "duplicate_transaction_hashes.csv")
        
        if 'transaction_hash' not in df.columns:
            print("  WARNING - transaction_hash column not found")
            return
        
        # Find duplicate hashes
        hash_counts = df['transaction_hash'].value_counts()
        duplicates = hash_counts[hash_counts > 1]
        
        if len(duplicates) == 0:
            print("  No duplicate hashes found - no export needed")
            return
        
        # Filter to only records with duplicate hashes
        duplicate_hashes = duplicates.index.tolist()
        duplicate_records = df[df['transaction_hash'].isin(duplicate_hashes)].copy()
        
        # Sort by hash and date for easier review
        duplicate_records = duplicate_records.sort_values(['transaction_hash', 'date'])
        
        # Select key columns for review
        review_columns = [
            'transaction_hash', 'date', 'description', 'amount', 
            'payee', 'subcategory', 'currency', 'account_name'
        ]
        
        # Only select columns that exist
        review_columns = [col for col in review_columns if col in duplicate_records.columns]
        duplicate_records = duplicate_records[review_columns]
        
        # Export to CSV
        duplicate_records.to_csv(output_file, index=False)
        
        print(f"\n  DUPLICATES EXPORTED:")
        print(f"  Total duplicate records: {len(duplicate_records)}")
        print(f"  Unique duplicate hashes: {len(duplicates)}")
        print(f"  Export location: {output_file}")
        print(f"\n  Open this file to manually review and fix the source data")
        
        return output_file

    
    def _load_silver(self, df: pd.DataFrame):
        """Load data to silver.transactions"""
        
        print(f"\n[LOAD SILVER] Loading to silver.transactions...")

        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE silver.transactions;")
            print("  Silver table truncated")
        
        # Prepare silver data
        silver_df = df.copy()
        
        # Export duplicate hashes BEFORE truncating
        self._export_duplicate_hashes(silver_df)

        # Rename columns to match silver schema
        rename_map = {
            'date': 'transaction_date',
            'transaction_type': 'transaction_type',
            'amount': 'amount',
            'amount_abs': 'amount_abs',
            'currency': 'currency',
            'amount_eur': 'amount_eur',
            'amount_abs_eur': 'amount_abs_eur',
            'eur_conversion_rate': 'eur_conversion_rate',
            'payee': 'payee',
            'subcategory': 'subcategory',
            'account': 'account_name',
            'labels': 'labels'
        }
        if 'note' in silver_df.columns:
            rename_map['note'] = 'description'
        if 'payment_type' in silver_df.columns and 'payment_method' not in silver_df.columns:
            rename_map['payment_type'] = 'payment_method'
        silver_df = silver_df.rename(columns=rename_map)
        
        # Add metadata
        silver_df['created_at'] = datetime.now()
        silver_df['created_by'] = 'initial_load_script'
        
        # Get raw_id from bronze (for lineage)
        # For initial load, we'll set this after insert
        silver_df['source_raw_id'] = None
        
        # Select columns for silver table
        silver_columns = [
            'transaction_hash', 'transaction_date', 'transaction_type',
            'amount', 'amount_abs', 'currency',
            'amount_eur', 'amount_abs_eur', 'eur_conversion_rate',
            'amount_bgn', 'amount_abs_bgn',
            'source_record_id', 'category_id',
            'description', 'payee', 'subcategory',
            'account_name', 'payment_method', 'labels',
            'year', 'month', 'quarter', 'year_month',
            'day_of_week', 'week_of_year', 'is_weekend',
            'source_raw_id', 'created_at', 'created_by',
            'classification'
        ]

        silver_df = silver_df[[c for c in silver_columns if c in silver_df.columns]]
        
        # On initial load, we load everything without deduplication
        # (deduplication will be handled in incremental loads)
        rows_loaded = self._bulk_insert(silver_df, 'silver', 'transactions')
        
        self.run_stats['rows_loaded_silver'] = rows_loaded
        print(f"  Loaded {rows_loaded:,} rows to silver")
        
        # Update category and classification fields from category_mapping
        print("  Updating category hierarchy...")
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE silver.transactions t
                SET category = cm.category, classification = cm.classification
                FROM silver.category_mapping cm
                WHERE t.subcategory = cm.subcategory
                  AND (t.category IS NULL OR t.classification IS NULL);
            """)
            updated = cursor.rowcount
            cursor.execute("""
                UPDATE silver.transactions
                SET category = 'Income', classification = 'WANT'
                WHERE transaction_type = 'INCOME'
                  AND subcategory IN ('Child Support', 'Lottery, gambling')
                  AND (category IS NULL OR category != 'Income');
            """)
            income_override = cursor.rowcount
            print(f"  Updated {updated + income_override:,} transactions with category groups")
    
    def _bulk_insert(self, df: pd.DataFrame, schema: str, table: str) -> int:
        """
        Bulk insert DataFrame to PostgreSQL table
        
        Args:
            df: DataFrame to insert
            schema: Schema name
            table: Table name
        
        Returns:
            Number of rows inserted
        """
        
        from psycopg2.extras import execute_batch
        
        # Convert DataFrame to list of tuples
        columns = df.columns.tolist()
        values = df.values.tolist()
        
        # Build INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'"{col}"' for col in columns])
        query = f'INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})'
        
        # Execute batch insert
        with self.db.connect() as conn:
            cursor = conn.cursor()
            execute_batch(cursor, query, values, page_size=1000)
            return len(values)
    
    def _log_pipeline_run(self):
        """Log pipeline execution to metadata.pipeline_runs"""
        
        print(f"\n[LOG] Recording pipeline run...")
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - self.run_stats['start_time']).total_seconds()
        
        # Prepare log data
        log_data = {
            'run_id': str(self.run_stats['run_id']),
            'run_timestamp': self.run_stats['start_time'],
            'source_file': self.run_stats['source_file'],
            'file_size_bytes': self.run_stats['file_size_bytes'],
            'status': self.run_stats['status'],
            'rows_extracted': self.run_stats['rows_extracted'],
            'rows_staged': self.run_stats['rows_staged'],
            'rows_loaded_bronze': self.run_stats['rows_loaded_bronze'],
            'rows_loaded_silver': self.run_stats['rows_loaded_silver'],
            'rows_skipped_duplicates': self.run_stats['rows_skipped_duplicates'],
            'rows_failed_validation': 0,
            'start_time': self.run_stats['start_time'],
            'end_time': end_time,
            'duration_seconds': duration,
            'error_message': self.run_stats.get('error_message')            
        }
        
        # Insert to metadata table
        columns = ', '.join([f'"{k}"' for k in log_data.keys()])
        placeholders = ', '.join(['%s'] * len(log_data))
        query = f"INSERT INTO metadata.pipeline_runs ({columns}) VALUES ({placeholders})"
        
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, list(log_data.values()))
        
        print(f"  Pipeline run logged (ID: {self.run_stats['run_id']})")
    
    def _display_summary(self):
        """Display final summary"""
        
        end_time = datetime.now()
        duration = (end_time - self.run_stats['start_time']).total_seconds()
        
        print("\n" + "=" * 70)
        print("INITIAL LOAD COMPLETE")
        print("=" * 70)
        print(f"Status: {self.run_stats['status']}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"\nData Flow:")
        print(f"  Source file:     {self.run_stats['rows_extracted']:,} rows")
        print(f"  -> Staging:       {self.run_stats['rows_staged']:,} rows")
        print(f"  -> Bronze:        {self.run_stats['rows_loaded_bronze']:,} rows")
        print(f"  -> Silver:        {self.run_stats['rows_loaded_silver']:,} rows")
        print(f"\nNext Steps:")
        print(f"  1. Verify data: psql -U teodor_admin -d finance_warehouse")
        print(f"     SELECT * FROM silver.v_tableau_transactions LIMIT 10;")
        print(f"  2. Connect Tableau to silver.v_tableau_transactions")
        print(f"  3. For future updates, use: python scripts/run_pipeline.py --mode incremental")
        print("=" * 70)


def main():
    """Main execution"""
    
    parser = argparse.ArgumentParser(
        description='Initial load of historical expense data'
    )
    parser.add_argument(
        '--file',
        required=True,
        help='Path to source file (Excel or CSV)'
    )
    
    args = parser.parse_args()
    
    # Create loader and execute
    loader = InitialDataLoader(args.file)
    success = loader.load()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
