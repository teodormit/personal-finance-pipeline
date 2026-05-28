"""
Tableau Public anonymized export
=================================

Exports silver + gold data as a CSV suitable for embedding in a Tableau Public
workbook. Applies two anonymization transformations before writing:

  1. Payee genericization — payees are replaced with stable rank-based aliases
     ("Merchant #1", "Merchant #2", …) ordered by descending frequency. NULL /
     empty payees remain NULL. The ranking is deterministic within a single run
     but intentionally NOT stored, so the mapping resets on each export (which
     is fine — Tableau Public is a snapshot, not a live source).

  2. Amount scaling — amount_eur and amount_abs_eur are multiplied by SCALE.
     The scale factor is intentionally omitted from the CSV; it exists only here.

Columns dropped entirely (identifying or internal-only):
  description, source_record_id, category_id, account_name, labels,
  amount_bgn, amount_abs_bgn, amount, amount_abs, created_at, created_by,
  source_raw_id, transaction_id, transaction_hash

Output: data/exports/tableau_public_<YYYY-MM-DD>.csv

Run:
  python scripts/export_tableau_public.py
  docker compose run --rm pipeline python scripts/export_tableau_public.py
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))

from utils.db_connector import get_db_connector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCALE = 0.73  # amount multiplier — keeps spend magnitudes plausible but untrue

DROP_COLS = {
    "description",
    "source_record_id",
    "category_id",
    "account_name",
    "labels",
    "amount_bgn",
    "amount_abs_bgn",
    "amount",
    "amount_abs",
    "created_at",
    "created_by",
    "source_raw_id",
    "transaction_id",
    "transaction_hash",
    # gold join keys (already represented by transaction_date + subcategory in output)
    "t_hash_n",
    "t_hash_s",
}

# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

QUERY = """
SELECT
    t.*,
    n.notability_score,
    n.notability_label,
    n.notability_reason,
    n.hist_n_txns,
    n.hist_avg_amount_eur,
    n.hist_std_amount_eur,
    n.amount_z_score          AS notability_z_score,
    n.is_new_subcategory,
    n.is_new_subcategory_max,
    s.save_potential_score,
    s.save_potential_label,
    s.save_potential_reason,
    s.avoidability,
    s.freq_excess,
    s.amt_excess,
    s.month_txn_count,
    s.hist_avg_monthly_count
FROM silver.transactions t
LEFT JOIN gold.transaction_notability  n ON t.transaction_hash = n.transaction_hash
LEFT JOIN gold.transaction_save_potential s ON t.transaction_hash = s.transaction_hash
ORDER BY t.transaction_date, t.transaction_id
"""


# ---------------------------------------------------------------------------
# Anonymization helpers
# ---------------------------------------------------------------------------

def _genericize_payees(series: pd.Series) -> pd.Series:
    """Replace real payees with frequency-ranked aliases; keep NULL as NULL."""
    non_null = series.dropna()
    non_null = non_null[non_null.str.strip() != ""]
    if non_null.empty:
        return series.where(series.isna(), "")

    counts = non_null.value_counts()
    alias_map = {payee: f"Merchant #{rank}" for rank, payee in enumerate(counts.index, start=1)}

    def _map(v):
        if pd.isna(v) or str(v).strip() == "":
            return None
        return alias_map.get(v, None)

    return series.map(_map)


def _scale_amounts(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("amount_eur", "amount_abs_eur"):
        if col in df.columns:
            df[col] = (df[col] * SCALE).round(2)
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    db = get_db_connector()

    print("Connecting to database and fetching data...")
    with db.connect() as conn:
        df = pd.read_sql(QUERY, conn)

    print(f"Fetched {len(df):,} rows.")

    # Drop internal / identifying columns (only those that exist in the result)
    cols_to_drop = [c for c in DROP_COLS if c in df.columns]
    df = df.drop(columns=cols_to_drop)

    # Genericize payees
    if "payee" in df.columns:
        df["payee"] = _genericize_payees(df["payee"])

    # Scale amounts
    df = _scale_amounts(df)

    # Write output
    out_dir = _project_root / "data" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"tableau_public_{date.today().isoformat()}.csv"
    df.to_csv(out_path, index=False)

    print(f"Exported {len(df):,} rows → {out_path}")
    print(f"Columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
