
"""
Export duplicate transaction hashes to CSV for manual review
Run this once after identifying duplicates in the initial load
"""

import pandas as pd
import sys
from pathlib import Path

# Add src directory to path
_src_root = Path(__file__).resolve().parent.parent / 'src'
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from utils.db_connector import get_db_connector


def export_duplicate_hashes():
    """Query silver.transactions and export duplicate hashes to CSV"""
    
    db = get_db_connector()
    output_file = r'C:\Users\teodo\Downloads\duplicate_transaction_hashes.csv'
    
    print(f"[EXPORT] Querying duplicate transaction hashes...")
    
    # Query to find all records with duplicate hashes
    query = """
    SELECT 
        t.*,
        COUNT(*) OVER (PARTITION BY transaction_hash) as hash_count
    FROM silver.transactions t
    WHERE transaction_hash IN (
        SELECT transaction_hash 
        FROM silver.transactions 
        GROUP BY transaction_hash 
        HAVING COUNT(*) > 1
    )
    ORDER BY transaction_hash, transaction_date, description
    """
    
    with db.connect() as conn:
        df = pd.read_sql(query, conn)
    
    if len(df) == 0:
        print("  No duplicate hashes found in database")
        return
    
    # Export to CSV
    df.to_csv(output_file, index=False)
    
    # Print summary
    unique_hashes = df['transaction_hash'].nunique()
    print(f"\n[RESULTS]")
    print(f"  Total duplicate records: {len(df):,}")
    print(f"  Unique duplicate hashes: {unique_hashes}")
    print(f"  Export location: {output_file}")
    print(f"\n  Columns exported: {list(df.columns)}")
    print(f"\n  Open the CSV file to review and identify which records to remove")


if __name__ == "__main__":
    export_duplicate_hashes()