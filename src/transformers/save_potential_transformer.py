"""
Save Potential Transformer
===========================

Scores each EXPENSE transaction for "save potential" using:
  - avoidability (classification: WANT / NEED / MUST)
  - frequency excess vs prior monthly counts in the same subcategory
  - amount excess (positive z-score from 365-day subcategory history)

Weights (avoidability dominant, then frequency, then amount):
  save_score = avoidability * 3 + freq_excess * 2 + amt_excess * 1
"""

import numpy as np
import pandas as pd

from transformers.notable_transactions_transformer import _compute_rolling_stats

# Tunable weights (plan)
WEIGHT_AVOIDABILITY = 3.0
WEIGHT_FREQ = 2.0
WEIGHT_AMT = 1.0
FREQ_EXCESS_CAP = 3.0
AMT_EXCESS_CAP = 5.0

# Classification -> avoidability (0..1). Missing defaults to NEED.
AVOIDABILITY = {
    "WANT": 1.0,
    "NEED": 0.4,
    "MUST": 0.05,
}

SAVE_OUTPUT_COLUMNS = [
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
]


def compute_save_potential(
    df: pd.DataFrame,
    *,
    window_days: int = 365,
    amount_col: str = "amount_abs_eur",
    date_col: str = "transaction_date",
    subcategory_col: str = "subcategory",
    hash_col: str = "transaction_hash",
    type_col: str = "transaction_type",
    classification_col: str = "classification",
    year_month_col: str = "year_month",
) -> pd.DataFrame:
    """
    One row per EXPENSE with save potential fields aligned with gold.transaction_save_potential.
    """
    expense = df[df[type_col].astype(str).str.upper() == "EXPENSE"].copy()
    if len(expense) == 0:
        return pd.DataFrame(columns=SAVE_OUTPUT_COLUMNS)

    if not pd.api.types.is_datetime64_any_dtype(expense[date_col]):
        expense[date_col] = pd.to_datetime(expense[date_col], errors="coerce")
    expense = expense[expense[date_col].notna()]

    expense[amount_col] = pd.to_numeric(expense[amount_col], errors="coerce")
    expense = expense[expense[amount_col].notna()]

    if year_month_col not in expense.columns:
        expense[year_month_col] = pd.to_datetime(expense[date_col]).dt.strftime("%Y-%m")

    expense = expense.sort_values(
        [subcategory_col, date_col, hash_col], kind="mergesort"
    ).reset_index(drop=True)

    # Z-score from same rolling window as notability
    stats = _compute_rolling_stats(
        expense,
        amount_col=amount_col,
        date_col=date_col,
        subcategory_col=subcategory_col,
        hash_col=hash_col,
        window_days=window_days,
    )
    n = stats["hist_n_txns"]
    avg = stats["hist_avg_amount_eur"]
    std = stats["hist_std_amount_eur"]
    amt = stats[amount_col]
    z = np.where(
        (std > 0) & (std.notna()),
        (amt - avg) / std,
        np.nan,
    )
    stats["amount_z_score"] = z
    z_clean = np.where(np.isnan(z), 0.0, np.maximum(z, 0.0))
    stats["amt_excess"] = np.clip(z_clean, 0.0, AMT_EXCESS_CAP)

    # Avoidability from classification (default NEED)
    def _avoidability(val):
        if pd.isna(val):
            return AVOIDABILITY["NEED"]
        s = str(val).strip().upper()
        if s in ("NAN", "NONE", ""):
            return AVOIDABILITY["NEED"]
        return AVOIDABILITY.get(s, AVOIDABILITY["NEED"])

    stats["classification"] = expense[classification_col].values
    stats["avoidability"] = expense[classification_col].map(_avoidability).values

    # Monthly frequency: counts per (year_month, subcategory)
    month_counts = (
        expense.groupby([year_month_col, subcategory_col], dropna=False)
        .size()
        .reset_index(name="month_txn_count")
    )
    month_counts["_month_start"] = pd.to_datetime(
        month_counts[year_month_col].astype(str) + "-01", errors="coerce"
    )

    hist_avg_map = {}
    for _, mr in month_counts.iterrows():
        ym = mr[year_month_col]
        sub = mr[subcategory_col]
        cur_start = mr["_month_start"]
        if pd.isna(cur_start):
            hist_avg_map[(ym, sub)] = np.nan
            continue
        win_start = cur_start - pd.Timedelta(days=window_days)
        prior = month_counts[
            (month_counts[subcategory_col] == sub)
            & (month_counts["_month_start"] < cur_start)
            & (month_counts["_month_start"] >= win_start)
        ]
        hist_avg_map[(ym, sub)] = (
            prior["month_txn_count"].mean() if len(prior) else np.nan
        )

    mcount_map = month_counts.set_index([year_month_col, subcategory_col])[
        "month_txn_count"
    ].to_dict()

    def _row_freq(row):
        ym = row[year_month_col]
        sub = row[subcategory_col]
        key = (ym, sub)
        mcount = int(mcount_map.get(key, 0))
        havg = hist_avg_map.get(key, np.nan)
        if pd.notna(havg) and havg > 0:
            fr = mcount / havg
            fe = min(max(fr - 1.0, 0.0), FREQ_EXCESS_CAP)
            return mcount, havg, fr, fe
        return mcount, havg, np.nan, 0.0

    freq_df = expense.apply(
        lambda r: pd.Series(
            _row_freq(r),
            index=[
                "month_txn_count",
                "hist_avg_monthly_count",
                "freq_ratio",
                "freq_excess",
            ],
        ),
        axis=1,
    )
    stats["month_txn_count"] = freq_df["month_txn_count"].values
    stats["hist_avg_monthly_count"] = freq_df["hist_avg_monthly_count"].values
    stats["freq_ratio"] = freq_df["freq_ratio"].values
    stats["freq_excess"] = freq_df["freq_excess"].values.astype(float)

    fe_arr = stats["freq_excess"].values.astype(float)
    av = stats["avoidability"].values.astype(float)
    ae = stats["amt_excess"].values.astype(float)
    score = av * WEIGHT_AVOIDABILITY + fe_arr * WEIGHT_FREQ + ae * WEIGHT_AMT
    stats["save_potential_score"] = score

    stats["save_potential_label"] = np.where(
        score >= 5.0,
        "High Save Potential",
        np.where(
            score >= 3.0,
            "Medium Save Potential",
            np.where(score >= 1.0, "Low Save Potential", "Minimal"),
        ),
    )

    def _reason(r):
        parts = []
        cls_val = str(r["classification"]).strip() if pd.notna(r["classification"]) else ""
        if cls_val.upper() == "WANT":
            parts.append("Discretionary (WANT)")
        elif cls_val.upper() == "NEED":
            parts.append("Necessary (NEED)")
        elif cls_val.upper() == "MUST":
            parts.append("Obligation (MUST)")
        else:
            parts.append("Necessary (NEED, default)")

        if pd.notna(r["freq_ratio"]) and r["freq_ratio"] > 1.01:
            parts.append(f"{r['freq_ratio']:.1f}x usual {r['subcategory']} frequency this month")
        if pd.notna(r["amount_z_score"]) and r["amount_z_score"] >= 1.0:
            parts.append(f"{r['amount_z_score']:.1f}σ above avg {r['subcategory']} spend")
        return " + ".join(parts) if len(parts) > 1 else parts[0]

    stats["save_potential_reason"] = stats.apply(_reason, axis=1)

    # Normalize date
    if pd.api.types.is_datetime64_any_dtype(stats[date_col]):
        stats[date_col] = stats[date_col].dt.date

    out = stats[
        [
            hash_col,
            date_col,
            subcategory_col,
            "classification",
            amount_col,
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
        ]
    ].copy()
    out.columns = [
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
    ]
    return out[SAVE_OUTPUT_COLUMNS]
