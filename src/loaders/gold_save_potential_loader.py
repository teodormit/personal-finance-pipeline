"""
Gold Save Potential Loader
==========================

Refreshes gold.transaction_save_potential from silver.transactions.
Same refresh semantics as gold_notable_loader (full vs incremental hashes).
"""

import sys
from pathlib import Path
from typing import Optional, Set

import pandas as pd

_src_root = Path(__file__).resolve().parent.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from transformers.save_potential_transformer import compute_save_potential


def refresh_save_potential_for_hashes(
    db,
    hashes: Optional[Set[str]] = None,
    *,
    full: bool = False,
    window_days: int = 365,
) -> int:
    """UPSERT gold.transaction_save_potential for given EXPENSE hashes or all."""
    print("\n[GOLD] Refreshing transaction_save_potential...")
    with db.connect() as conn:
        if full:
            target_hashes = None
            silver_df = _fetch_silver_expenses(conn)
        else:
            if not hashes:
                print("  [GOLD] No hashes to refresh (save potential), skipping.")
                return 0
            target_hashes = set(str(h) for h in hashes)
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
            print("  [GOLD] No silver expense data to process (save potential).")
            return 0

        computed = compute_save_potential(
            silver_df,
            window_days=window_days,
            amount_col="amount_abs_eur",
            date_col="transaction_date",
            subcategory_col="subcategory",
            hash_col="transaction_hash",
            type_col="transaction_type",
            classification_col="classification",
            year_month_col="year_month",
        )

        if target_hashes is not None:
            computed = computed[
                computed["transaction_hash"].astype(str).isin(target_hashes)
            ]
        if len(computed) == 0:
            print("  [GOLD] No rows to upsert (save potential) after filtering.")
            return 0

        return _upsert_to_gold(conn, computed)


def _fetch_target_info(conn, hashes: Set[str]):
    """Return (subcategories, min_date, max_date) for the given hashes in silver."""
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
    return (subcats, min(dates), max(dates))


def _fetch_silver_expenses(
    conn,
    *,
    subcategories: Optional[set] = None,
    date_from=None,
    date_to=None,
    window_days: int = 365,
) -> pd.DataFrame:
    """Fetch EXPENSE rows with fields needed for save potential."""
    from datetime import datetime, timedelta

    cursor = conn.cursor()
    conditions = ["transaction_type = 'EXPENSE'"]
    params = []

    if subcategories is not None and len(subcategories) > 0:
        placeholders = ", ".join(["%s"] * len(subcategories))
        conditions.append(f"subcategory IN ({placeholders})")
        params.extend(subcategories)

    if date_from is not None and date_to is not None:
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
               amount_abs_eur, subcategory, classification, year_month
        FROM silver.transactions
        WHERE {where}
        """
    cursor.execute(query, params)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _upsert_to_gold(conn, df: pd.DataFrame) -> int:
    """UPSERT into gold.transaction_save_potential."""
    from datetime import datetime
    from psycopg2.extras import execute_batch

    df = df.copy()
    df["computed_at"] = datetime.now()

    cols = [
        "transaction_hash",
        "transaction_date",
        "subcategory",
        "classification",
        "amount_abs_eur",
        "avoidability",
        "month_txn_count",
        "hist_avg_monthly_count",
        "freq_ratio",
        "freq_excess",
        "amount_z_score",
        "amt_excess",
        "save_potential_score",
        "save_potential_label",
        "save_potential_reason",
        "computed_at",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df = df.where(pd.notna(df), None)

    update_set = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in cols if c != "transaction_hash"
    )
    placeholders = ", ".join(["%s"] * len(cols))
    cols_str = ", ".join([f'"{c}"' for c in cols])
    query = f"""
        INSERT INTO gold.transaction_save_potential ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (transaction_hash) DO UPDATE SET {update_set}
        """
    cursor = conn.cursor()
    execute_batch(cursor, query, df.values.tolist(), page_size=1000)
    conn.commit()
    return len(df)
