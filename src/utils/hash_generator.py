"""
Transaction Hash Generator -
==========================================

"""

import hashlib
import pandas as pd


def generate_transaction_hash(date, amount, category, description=""):
    """
    Create a unique hash (fingerprint) for a transaction
    
    PARAMETERS:
    - date: Transaction date (string or datetime)
    - amount: Transaction amount (number)
    - category: Transaction category (string)
    - description: Optional note/description (string)
    
    RETURNS:
    - 64-character hexadecimal string (the hash/fingerprint)
    
    EXAMPLE:
        hash1 = generate_transaction_hash(
            date='2024-12-01',
            amount=-45.50,
            category='Food & Drinks',
            description='Lunch at restaurant'
        )
        # hash1 = 'a1b2c3d4e5f6...' (64 characters)
        
        # Same transaction = same hash
        hash2 = generate_transaction_hash(
            date='2024-12-01',
            amount=-45.50,
            category='Food & Drinks',
            description='Lunch at restaurant'
        )
        # hash2 = 'a1b2c3d4e5f6...' (identical to hash1!)
    
    WHY THIS WORKS:
    - SHA-256 algorithm creates unique fingerprints
    - We combine date + amount + category + description
    - Even tiny changes create completely different hash
    """
    
    # ========================================================================
    # STEP 1: Normalize the inputs
    # ========================================================================
    
    if isinstance(date, pd.Timestamp):
        date_str = date.strftime('%Y-%m-%d')
    else:
        try:
            date_obj = pd.to_datetime(date)
            date_str = date_obj.strftime('%Y-%m-%d')
        except:
            date_str = str(date)  # Fallback: just use as-is
    
    amount_str = f"{float(amount):.2f}"
    
    category_str = str(category).strip().lower()
    
    description_str = str(description or "")[:100].strip().lower()
    
    # ========================================================================
    # STEP 2: Combine into a single string
    # ========================================================================
    
    combined_string = f"{date_str}|{amount_str}|{category_str}|{description_str}"
    
    # Example: '2024-12-01|-45.50|food & drinks|lunch at restaurant'
    
    # ========================================================================
    # STEP 3: Create the hash
    # ========================================================================
    # SHA-256 is a "hash function" - creates unique fingerprints
    # Think of it like a magic blender that makes a smoothie:
    # - Same ingredients → Same smoothie
    # - Different ingredients → Different smoothie
    # - Can't un-blend the smoothie (one-way function)
    
    # Create hash object
    hash_object = hashlib.sha256(combined_string.encode('utf-8'))
    
    # Get hexadecimal representation (readable format)
    hash_hex = hash_object.hexdigest()
    
    # Returns 64 characters like: 'a1b2c3d4e5f6...'
    return hash_hex


def generate_hashes_for_dataframe(df):
    """
    Generate hashes for all rows in a pandas DataFrame
    
    PARAMETERS:
    - df: pandas DataFrame with columns: date, amount, subcategory, description
    
    RETURNS:
    - pandas Series (column) with hash for each row
    
    USAGE:
        df['transaction_hash'] = generate_hashes_for_dataframe(df)
    
    WHY SEPARATE FUNCTION?
    - Could do this in one line: df.apply(generate_transaction_hash)
    - But this function adds validation and error handling
    - Makes code more readable
    """
    
    # Check if required columns exist
    required = ['date', 'amount', 'subcategory']
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        raise ValueError(f"Missing columns: {missing}. Need: {required}")
    
    # ========================================================================
    # CONCEPT: df.apply()
    # ========================================================================
    # df.apply() runs a function on each row of the DataFrame
    # Think of it like:
    # for each row in df:
    #     hash = generate_transaction_hash(row)
    
    # axis=1 means "apply to each row" (axis=0 would be each column)
    hashes = df.apply(
        lambda row: generate_transaction_hash(
            date=row['date'],
            amount=row['amount'],
            category=row['subcategory'],
            description=row.get('description', row.get('note', ''))
        ),
        axis=1
    )
    
    return hashes


# ============================================================================
# TESTING SECTION
# ============================================================================

if __name__ == "__main__":
    
    print("\n" + "="*60)
    print("TRANSACTION HASH GENERATOR TEST")
    print("="*60 + "\n")
    
    # ========================================================================
    # TEST 1: Single transaction hash
    # ========================================================================
    print("TEST 1: Creating a hash for a single transaction")
    print("-" * 60)
    
    hash1 = generate_transaction_hash(
        date='2024-12-01',
        amount=-45.50,
        category='Food & Drinks',
        description='Restaurant lunch'
    )
    
    print(f"Transaction: Dec 1st, $-45.50, Food & Drinks, Restaurant lunch")
    print(f"Hash: {hash1}")
    print(f"Length: {len(hash1)} characters")
    
    # ========================================================================
    # TEST 2: Hash consistency
    # ========================================================================
    print("\n" + "-"*60)
    print("TEST 2: Same transaction = Same hash?")
    print("-" * 60)
    
    hash2 = generate_transaction_hash(
        date='2024-12-01',
        amount=-45.50,
        category='Food & Drinks',
        description='Restaurant lunch'
    )
    
    print(f"Hash 1: {hash1[:20]}...")
    print(f"Hash 2: {hash2[:20]}...")
    print(f"Identical: {hash1 == hash2}")
    
    if hash1 == hash2:
        print("PASS: Same transaction produces same hash")
    else:
        print("FAIL: Hashes should be identical!")
    
    # ========================================================================
    # TEST 3: Hash uniqueness
    # ========================================================================
    print("\n" + "-"*60)
    print("TEST 3: Different transaction = Different hash?")
    print("-" * 60)
    
    hash3 = generate_transaction_hash(
        date='2024-12-01',
        amount=-45.51,  # Changed by 1 cent!
        category='Food & Drinks',
        description='Restaurant lunch'
    )
    
    print(f"Original: $-45.50")
    print(f"Changed:  $-45.51")
    print(f"Hash 1: {hash1[:20]}...")
    print(f"Hash 3: {hash3[:20]}...")
    print(f"Different: {hash1 != hash3}")
    
    if hash1 != hash3:
        print("PASS: Different amounts produce different hashes")
    else:
        print("FAIL: Hashes should be different!")
    
    # ========================================================================
    # TEST 4: DataFrame batch processing
    # ========================================================================
    print("\n" + "-"*60)
    print("TEST 4: Processing multiple transactions (DataFrame)")
    print("-" * 60)
    
    # Create sample DataFrame
    test_df = pd.DataFrame({
        'date': ['2024-12-01', '2024-12-02', '2024-12-01'],
        'amount': [-45.50, -30.00, -45.50],
        'subcategory': ['Food & Drinks', 'Groceries', 'Food & Drinks'],
        'description': ['Lunch', 'Shopping', 'Lunch']
    })
    
    # Generate hashes for all rows
    test_df['transaction_hash'] = generate_hashes_for_dataframe(test_df)
    
    print(f"Processed {len(test_df)} transactions")
    print("\nResults:")
    print(test_df[['date', 'amount', 'subcategory', 'transaction_hash']])
    
    # Check for duplicates
    duplicates = test_df['transaction_hash'].duplicated().sum()
    print(f"\nDuplicates found: {duplicates}")
    
    if duplicates > 0:
        print("Note: Rows 1 and 3 are identical, so they have same hash")
        print("This is how we detect duplicate transactions!")
    
    print("\n" + "="*60)
    print("All tests complete!")
    print("="*60 + "\n")


# ============================================================================
# KEY TAKEAWAYS
# ============================================================================
"""
1. FUNCTIONS vs CLASSES:
   - Use functions for simple, stateless operations
   - Use classes when you need to "remember" state
   - Hash generation doesn't need state → function is fine!

2. HASHING for DEDUPLICATION:
   - Creates unique fingerprints for data
   - Fast to compare (64 chars vs entire rows)
   - Same data always = same hash

3. NORMALIZATION:
   - Make data consistent before hashing
   - '45.5' and '45.50' should be treated as same
   - 'Food & Drinks' and 'food & drinks' should match

4. PANDAS DATAFRAME OPERATIONS:
   - df.apply() runs function on each row
   - Like a for loop, but faster and cleaner
   - lambda = inline function (shorthand)

5. TESTING IN __main__:
   - Code only runs when file executed directly
   - Doesn't run when imported
   - Perfect for testing!

NEXT STEPS:
- Run this file: python src/utils/hash_generator.py
- Watch the tests to understand how hashing works
- Try changing values in TEST 3 to see how hashes change!
"""