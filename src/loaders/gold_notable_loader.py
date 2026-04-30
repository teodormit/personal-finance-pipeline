"""
Gold Notability Loader
======================

Refreshes gold.transaction_notability from silver.transactions.
Supports full refresh (all EXPENSE rows) or incremental upsert (only specified hashes).

STALENESS: If silver.subcategory or amount is corrected for an existing row,
re-run with full=True or include that hash in the targeted refresh.
"""

import json
import sys
from pathlib import Path
from typing import Optional, Set

import pandas as pd

_src_root = Path(__file__).resolve().parent.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from transformers.notable_transactions_transformer import compute_notability


def refresh_notability_for_hashes(
    db,
    hashes: Optional[Set[str]] = None,
    *,
    full: bool = False,
    window_days: int = 365,
) -> int:
    """
    Refresh gold.transaction_notability for specified (or all) EXPENSE hashes.

    Parameters
    ----------
    db
        Database connection manager (e.g. from get_db_connector())
    hashes : set of str, optional
        Transaction hashes to refresh. Ignored when full=True.
    full : bool
        If True, refresh all EXPENSE rows in silver; otherwise only the given hashes.
    window_days : int
        Historical lookback window in days (default 365).

    Returns
    -------
    int
        Number of rows upserted into gold.transaction_notability.
    """
    print("\n[GOLD] Refreshing transaction_notability...")
    with db.connect() as conn:
        if full:
            target_hashes = None
            silver_df = _fetch_silver_expenses(conn)
        else:
            if not hashes:
                print("  [GOLD] No hashes to refresh, skipping.")
                return 0
            target_hashes = set(str(h) for h in hashes)
            # Fetch slice: need subcategories and date range of target rows
            target_info = _fetch_target_info(conn, target_hashes)
            if not target_info:
                print("  [GOLD] No target rows found in silver, skipping.")
                return 0
            subcats, min_date, max_date = target_info
            silver_df = _fetch_silver_expenses(
                conn,
                subcategories=subcats,
                date_from=min_date,
                date_to=max_date,
                window_days=window_days,
            )

        if len(silver_df) == 0:
            print("  [GOLD] No silver expense data to process.")
            return 0

        computed = compute_notability(
            silver_df,
            window_days=window_days,
            amount_col="amount_abs_eur",
            date_col="transaction_date",
            subcategory_col="subcategory",
            hash_col="transaction_hash",
            type_col="transaction_type",
        )

        if target_hashes is not None:
            computed = computed[computed["transaction_hash"].astype(str).isin(target_hashes)]
        if len(computed) == 0:
            print("  [GOLD] No rows to upsert after filtering.")
            return 0

        rows = _upsert_to_gold(conn, computed)
        return rows


def _fetch_target_info(conn, hashes: Set[str]):
    """Return (subcategories, min_date, max_date) for the given hashes in silver."""
    import datetime
    cursor = conn.cursor()
    placeholders = ", ".join(["%s"] * len(hashes))
    cursor.execute(
        f"""
        SELECT DISTINCT subcategory, transaction_date
        FROM silver.transactions
        WHERE transaction_hash IN ({placeholders})
          AND transaction_type = 'EXPENSE'
        """,
        list(hashes),
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    subcats = {r[0] for r in rows if r[0]}
    dates = [r[1] for r in rows if r[1]]
    if not dates:
        return None
    min_date = min(dates)
    max_date = max(dates)
    return (subcats, min_date, max_date)


def _fetch_silver_expenses(
    conn,
    *,
    subcategories: Optional[set] = None,
    date_from=None,
    date_to=None,
    window_days: int = 365,
) -> pd.DataFrame:
    """
    Fetch EXPENSE rows from silver.transactions.

    When subcategories/date range given: fetch slice for incremental.
    When limit_subcategories is None and no filters: fetch all (full refresh).
    """
    cursor = conn.cursor()
    conditions = ["transaction_type = 'EXPENSE'"]
    params = []

    if subcategories is not None and len(subcategories) > 0:
        placeholders = ", ".join(["%s"] * len(subcategories))
        conditions.append(f"subcategory IN ({placeholders})")
        params.extend(subcategories)

    if date_from is not None and date_to is not None:
        # Expand range for baseline: need history going back window_days from date_from
        # We'll fetch from date_from - window_days to date_to
        from datetime import datetime, timedelta
        if isinstance(date_from, str):
            d_from = datetime.strptime(str(date_from)[:10], "%Y-%m-%d").date()
        else:
            d_from = date_from
        if isinstance(date_to, str):
            d_to = datetime.strptime(str(date_to)[:10], "%Y-%m-%d").date()
        else:
            d_to = date_to
        fetch_from = d_from - timedelta(days=window_days)
        conditions.append("transaction_date >= %s AND transaction_date <= %s")
        params.extend([fetch_from, d_to])

    where = " AND ".join(conditions)
    query = f"""
        SELECT transaction_hash, transaction_date, transaction_type,
               amount_abs_eur, subcategory
        FROM silver.transactions
        WHERE {where}
        """
    cursor.execute(query, params)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _upsert_to_gold(conn, df: pd.DataFrame) -> int:
    """UPSERT DataFrame into gold.transaction_notability. Returns row count."""
    from psycopg2.extras import execute_batch
    from datetime import datetime

    df = df.copy()
    df["computed_at"] = datetime.now()
    # Ensure extra_stats is JSON-serializable
    if "extra_stats" in df.columns:
        df["extra_stats"] = df["extra_stats"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else "{}"
        )

    cols = [
        "transaction_hash", "transaction_date", "subcategory", "amount_abs_eur",
        "hist_window_days", "hist_n_txns", "hist_avg_amount_eur",
        "hist_std_amount_eur", "hist_max_amount_eur", "amount_z_score",
        "is_new_subcategory", "is_new_subcategory_max", "notability_score",
        "notability_label", "notability_reason", "computed_at", "extra_stats",
    ]
    df = df[[c for c in cols if c in df.columns]]
    # Fill missing cols
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    # Replace NaN with None for PostgreSQL
    df = df.where(pd.notna(df), None)

    update_set = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in cols if c != "transaction_hash"
    )
    placeholders = ", ".join(["%s"] * len(cols))
    cols_str = ", ".join([f'"{c}"' for c in cols])
    query = f"""
        INSERT INTO gold.transaction_notability ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (transaction_hash) DO UPDATE SET {update_set}
        """
    values = df.values.tolist()
    cursor = conn.cursor()
    execute_batch(cursor, query, values, page_size=1000)
    conn.commit()
    return len(values)


def main():
    """Standalone entry point for manual refresh (e.g. scripts/refresh_gold_notability.py)."""
    import argparse
    from utils.db_connector import get_db_connector

    parser = argparse.ArgumentParser(description="Refresh gold.transaction_notability")
    parser.add_argument("--full", action="store_true", help="Full refresh of all EXPENSE rows")
    parser.add_argument("--window-days", type=int, default=365, help="Lookback window in days")
    args = parser.parse_args()

    db = get_db_connector()
    n = refresh_notability_for_hashes(db, hashes=None, full=args.full, window_days=args.window_days)
    print(f"\n[GOLD] Refreshed {n:,} rows in gold.transaction_notability")
    return n
