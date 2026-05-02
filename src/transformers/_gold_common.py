"""
Shared helpers for gold-layer transformers.

Hosts logic used by more than one of the gold scoring transformers
(notable_transactions, save_potential, …) so neither has to import
private symbols from the other.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_rolling_stats(
    df: pd.DataFrame,
    *,
    amount_col: str,
    date_col: str,
    subcategory_col: str,
    hash_col: str,
    window_days: int,
) -> pd.DataFrame:
    """For each row, compute hist_n_txns, hist_avg, hist_std, hist_max
    from prior rows in the same subcategory within [date - window_days, date).

    The two-pointer scan is intentional rather than a pandas
    `groupby().rolling()` because:
      - The baseline must be deterministic given the (date, hash) sort
        order — including how same-day ties are broken — and the rolling
        primitive does not expose a tie-break hook.
      - The window is "strictly before the current row" (excluding the
        current day's same-hash record), which is awkward to express via
        rolling time-window APIs.

    The function is stable and well-tested; do not refactor in place
    without re-checking byte-equivalence of gold scoring.
    """
    # hash_col is part of the function signature for API symmetry with the
    # caller's sort key, but we don't actually need the hash values inside
    # the rolling computation — sort order alone gives us determinism.
    _ = hash_col

    dates = pd.to_datetime(df[date_col]).values.astype("datetime64[D]")
    amounts = df[amount_col].values.astype(np.float64)
    subcats = df[subcategory_col].fillna("").values

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
        sub_n = len(idx)

        left = 0
        for i in range(sub_n):
            di = sub_dates[i]
            cutoff = di - window_days_td
            # Advance left: drop rows outside window.
            while left < i and sub_dates[left] < cutoff:
                left += 1
            # Window = [left, i): indices strictly before i, within [cutoff, di).
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
        # Population std (ddof=0) for consistency with the historical avg.
        variance = np.where(
            hist_n >= 2,
            (hist_sumsq / hist_n) - (hist_sum / hist_n) ** 2,
            np.nan,
        )
    result["hist_std_amount_eur"] = np.where(variance > 0, np.sqrt(variance), np.nan)
    result["hist_max_amount_eur"] = hist_max
    return result
