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

from loaders.gold_refresher import GoldRefresher
from transformers.save_potential_transformer import compute_save_potential


class SavePotentialRefresher(GoldRefresher):
    """Refresher for gold.transaction_save_potential."""

    gold_table = "gold.transaction_save_potential"
    print_label = "transaction_save_potential"
    # Save-potential needs classification (avoidability) and year_month (frequency).
    silver_extra_columns = ["classification", "year_month"]
    json_columns: list[str] = []
    gold_columns = [
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

    def compute(self, silver_df: pd.DataFrame, *, window_days: int) -> pd.DataFrame:
        return compute_save_potential(
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


def refresh_save_potential_for_hashes(
    db,
    hashes: Optional[Set[str]] = None,
    *,
    full: bool = False,
    window_days: int = 365,
) -> int:
    """Refresh gold.transaction_save_potential for given hashes (or all if full=True).

    Public function preserved for backwards compatibility with existing
    callers in BaseLoader and run_pipeline.py.
    """
    return SavePotentialRefresher().refresh(
        db, hashes=hashes, full=full, window_days=window_days
    )
