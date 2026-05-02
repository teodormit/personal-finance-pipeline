"""
Gold Notability Loader
======================

Refreshes gold.transaction_notability from silver.transactions.
Supports full refresh (all EXPENSE rows) or incremental upsert (only specified hashes).

STALENESS: If silver.subcategory or amount is corrected for an existing row,
re-run with full=True or include that hash in the targeted refresh.
"""

import sys
from pathlib import Path
from typing import Optional, Set

import pandas as pd

_src_root = Path(__file__).resolve().parent.parent
if str(_src_root) not in sys.path:
    sys.path.insert(0, str(_src_root))

from loaders.gold_refresher import GoldRefresher
from transformers.notable_transactions_transformer import compute_notability


class NotabilityRefresher(GoldRefresher):
    """Refresher for gold.transaction_notability."""

    gold_table = "gold.transaction_notability"
    print_label = "transaction_notability"
    silver_extra_columns: list[str] = []  # base columns are sufficient
    json_columns = ["extra_stats"]
    gold_columns = [
        "transaction_hash",
        "transaction_date",
        "subcategory",
        "amount_abs_eur",
        "hist_window_days",
        "hist_n_txns",
        "hist_avg_amount_eur",
        "hist_std_amount_eur",
        "hist_max_amount_eur",
        "amount_z_score",
        "is_new_subcategory",
        "is_new_subcategory_max",
        "notability_score",
        "notability_label",
        "notability_reason",
        "computed_at",
        "extra_stats",
    ]

    def compute(self, silver_df: pd.DataFrame, *, window_days: int) -> pd.DataFrame:
        return compute_notability(
            silver_df,
            window_days=window_days,
            amount_col="amount_abs_eur",
            date_col="transaction_date",
            subcategory_col="subcategory",
            hash_col="transaction_hash",
            type_col="transaction_type",
        )


def refresh_notability_for_hashes(
    db,
    hashes: Optional[Set[str]] = None,
    *,
    full: bool = False,
    window_days: int = 365,
) -> int:
    """Refresh gold.transaction_notability for given hashes (or all if full=True).

    Public function preserved for backwards compatibility with existing
    callers in BaseLoader and run_pipeline.py.
    """
    return NotabilityRefresher().refresh(
        db, hashes=hashes, full=full, window_days=window_days
    )


def main():
    """Standalone entry point for manual refresh (`python -m loaders.gold_notable_loader --full`).

    For normal use prefer `python scripts/run_pipeline.py --refresh-gold notability`.
    """
    import argparse

    from utils.db_connector import get_db_connector

    parser = argparse.ArgumentParser(description="Refresh gold.transaction_notability")
    parser.add_argument(
        "--full", action="store_true", help="Full refresh of all EXPENSE rows"
    )
    parser.add_argument(
        "--window-days", type=int, default=365, help="Lookback window in days"
    )
    args = parser.parse_args()

    db = get_db_connector()
    n = refresh_notability_for_hashes(
        db, hashes=None, full=args.full, window_days=args.window_days
    )
    print(f"\n[GOLD] Refreshed {n:,} rows in gold.transaction_notability")
    return n
