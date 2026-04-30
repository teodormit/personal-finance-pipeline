"""
Notable Transactions Transformer
================================

PURPOSE: Score each EXPENSE transaction by how "surprising" it is vs. your
historical spending in the same subcategory over the prior 365 days.

INPUT: DataFrame from silver.transactions (or equivalent) with columns:
    transaction_hash, transaction_date, transaction_type, amount_abs_eur,
    subcategory, etc.

OUTPUT: DataFrame with notability scores, labels, and reasons suitable for
gold.transaction_notability.

HISTORICAL WINDOW: For each transaction at date D, baseline = all OTHER
EXPENSE rows in the same subcategory where:
    - transaction_date >= D - 365 days
    - strictly before current row in (transaction_date, transaction_hash) order

This 365-day cutoff reduces inflation bias from very old prices.
"""

import numpy as np
import pandas as pd
from typing import Optional


# Output columns aligned with gold.transaction_notability
OUTPUT_COLUMNS = [
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
    "extra_stats",
]


def compute_notability(
    df: pd.DataFrame,
    *,
    window_days: int = 365,
    amount_col: str = "amount_abs_eur",
    date_col: str = "transaction_date",
    subcategory_col: str = "subcategory",
    hash_col: str = "transaction_hash",
    type_col: str = "transaction_type",
) -> pd.DataFrame:
    """
    Compute notability scores for each EXPENSE transaction.

    Filters to EXPENSE rows, then for each row computes historical stats
    from prior rows in the same subcategory within the rolling window.
    Output columns match gold.transaction_notability schema.

    Parameters
    ----------
    df : pd.DataFrame
        Source data (e.g. from silver.transactions)
    window_days : int
        Lookback window in days (default 365)
    amount_col, date_col, subcategory_col, hash_col, type_col : str
        Column names for mapping

    Returns
    -------
    pd.DataFrame
        One row per EXPENSE transaction with notability fields.
    """
    expense = df[df[type_col].astype(str).str.upper() == "EXPENSE"].copy()
    if len(expense) == 0:
        return _empty_output()

    # Ensure datetime for date arithmetic
    expense = expense.copy()
    if not pd.api.types.is_datetime64_any_dtype(expense[date_col]):
        expense[date_col] = pd.to_datetime(expense[date_col], errors="coerce")
    expense = expense[expense[date_col].notna()]

    # Ensure amounts are numeric
    expense[amount_col] = pd.to_numeric(expense[amount_col], errors="coerce")
    expense = expense[expense[amount_col].notna()]

    # Sort for deterministic window: subcategory, date, hash
    expense = expense.sort_values(
        [subcategory_col, date_col, hash_col], kind="mergesort"
    ).reset_index(drop=True)

    # Compute historical stats per row (two-pointer per subcategory)
    results = _compute_rolling_stats(
        expense,
        amount_col=amount_col,
        date_col=date_col,
        subcategory_col=subcategory_col,
        hash_col=hash_col,
        window_days=window_days,
    )

    # Derive labels and composite score
    out = _derive_labels_and_score(results, amount_col, window_days)
    out = _compute_extra_features(out)  # Extensibility hook
    return out[OUTPUT_COLUMNS]


def _compute_rolling_stats(
    df: pd.DataFrame,
    *,
    amount_col: str,
    date_col: str,
    subcategory_col: str,
    hash_col: str,
    window_days: int,
) -> pd.DataFrame:
    """
    For each row, compute hist_n_txns, hist_avg, hist_std, hist_max
    from prior rows in same subcategory within [date - window_days, date).
    """
    dates = pd.to_datetime(df[date_col]).values.astype("datetime64[D]")
    amounts = df[amount_col].values.astype(np.float64)
    subcats = df[subcategory_col].fillna("").values
    hashes = df[hash_col].astype(str).values

    n = len(df)
    hist_n = np.zeros(n, dtype=np.int64)
    hist_sum = np.zeros(n)
    hist_sumsq = np.zeros(n)
    hist_max = np.full(n, np.nan)

    window_days_td = np.timedelta64(window_days, "D")

    for subcat in np.unique(subcats):
        if subcat == "":
            continue
        mask = subcats == subcat
        idx = np.where(mask)[0]
        sub_dates = dates[idx]
        sub_amounts = amounts[idx]
        sub_hashes = hashes[idx]
        sub_n = len(idx)

        left = 0
        for i in range(sub_n):
            di = sub_dates[i]
            cutoff = di - window_days_td
            # Advance left: drop rows outside window
            while left < i and sub_dates[left] < cutoff:
                left += 1
            # Window = [left, i): indices strictly before i, within [cutoff, di)
            # Also exclude same-day rows with hash >= current (strictly before = date<hash order)
            cnt = 0
            s = 0.0
            sq = 0.0
            mx = np.nan
            for j in range(left, i):
                if sub_dates[j] >= cutoff:
                    cnt += 1
                    s += sub_amounts[j]
                    sq += sub_amounts[j] ** 2
                    mx = sub_amounts[j] if np.isnan(mx) else max(mx, sub_amounts[j])
            hist_n[idx[i]] = cnt
            hist_sum[idx[i]] = s
            hist_sumsq[idx[i]] = sq
            hist_max[idx[i]] = mx

    result = df[[hash_col, date_col, subcategory_col, amount_col]].copy()
    result["hist_window_days"] = window_days
    result["hist_n_txns"] = hist_n
    with np.errstate(divide="ignore", invalid="ignore"):
        result["hist_avg_amount_eur"] = np.where(
            hist_n > 0, hist_sum / hist_n, np.nan
        )
        # Population std (ddof=0) for consistency with historical avg
        variance = np.where(
            hist_n >= 2,
            (hist_sumsq / hist_n) - (hist_sum / hist_n) ** 2,
            np.nan,
        )
    result["hist_std_amount_eur"] = np.where(variance > 0, np.sqrt(variance), np.nan)
    result["hist_max_amount_eur"] = hist_max
    return result


def _derive_labels_and_score(
    df: pd.DataFrame, amount_col: str, window_days: int
) -> pd.DataFrame:
    """Add amount_z_score, flags, notability_score, notability_label, notability_reason."""
    n = df["hist_n_txns"]
    avg = df["hist_avg_amount_eur"]
    std = df["hist_std_amount_eur"]
    mx = df["hist_max_amount_eur"]
    amt = df[amount_col]

    # Z-score: (x - mean) / std when std > 0
    z = np.where(
        (std > 0) & (std.notna()),
        (amt - avg) / std,
        np.nan,
    )
    df["amount_z_score"] = z

    df["is_new_subcategory"] = (n == 0) | (n.isna())
    df["is_new_subcategory_max"] = (amt > mx.fillna(-np.inf)) & (
        (n > 0) | (n == 0)
    )
    # When new subcategory, hist_max is NaN so amt > NaN is False; correct it
    df.loc[df["is_new_subcategory"], "is_new_subcategory_max"] = True

    # Composite notability score for ranking (higher = more notable)
    base_score = np.where(np.isnan(z) | (z < 0), 0, np.maximum(z, 0))
    df["notability_score"] = (
        base_score
        + np.where(df["is_new_subcategory"], 4.0, 0)
        + np.where(df["is_new_subcategory_max"] & ~df["is_new_subcategory"], 2.0, 0)
    )

    # Human-readable label (priority: New Category > Insufficient History > z-based > New Record > Normal)
    df["notability_label"] = np.where(
        df["is_new_subcategory"],
        "New Category",
        np.where(
            np.isnan(z),
            "Insufficient History",
            np.where(
                z >= 3,
                "Extreme Outlier",
                np.where(
                    z >= 2,
                    "High Outlier",
                    np.where(
                        z >= 1,
                        "Above Average",
                        np.where(
                            df["is_new_subcategory_max"],
                            "New Record",
                            "Normal",
                        ),
                    ),
                ),
            ),
        ),
    )

    # Reason string for tooltips
    def reason_row(row):
        if row["is_new_subcategory"]:
            return f"First time spending in {row['subcategory']}"
        if row["is_new_subcategory_max"]:
            return f"Largest ever {row['subcategory']} transaction"
        z_val = row["amount_z_score"]
        if pd.notna(z_val) and z_val >= 2:
            avg_val = row["hist_avg_amount_eur"]
            avg_str = f"€{avg_val:.0f}" if pd.notna(avg_val) else "N/A"
            return f"{z_val:.1f}σ above your usual {row['subcategory']} spend (avg {avg_str})"
        return None

    df["notability_reason"] = df.apply(reason_row, axis=1)
    df["extra_stats"] = [{} for _ in range(len(df))]  # Extensibility placeholder
    # Normalize transaction_date to date for DB alignment
    date_col_name = "transaction_date"
    if date_col_name in df.columns and pd.api.types.is_datetime64_any_dtype(
        df[date_col_name]
    ):
        df[date_col_name] = df[date_col_name].dt.date
    return df
# (This line appeared to be a leftover lint artifact and is not valid Python code.
# It should be removed for clean, working code. No statement is needed here.)


def _compute_extra_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extensibility hook for future statistical enrichments.
    Add new metrics here without changing core logic.
    """
    # Placeholder: extra_stats stays as empty dict
    return df


def _empty_output() -> pd.DataFrame:
    """Return empty DataFrame with correct columns."""
    return pd.DataFrame(columns=OUTPUT_COLUMNS)
