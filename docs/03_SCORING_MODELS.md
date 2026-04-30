# Scoring Models

## Purpose
Gold scoring tables surface transaction-level intelligence beyond raw spending totals.

## Model 1: Transaction Notability
Table: `gold.transaction_notability`

### Question Answered
"Which transactions are unusual vs my normal trend?"

### Inputs
- `transaction_hash`
- `transaction_date`
- `subcategory`
- `amount_abs_eur`

### Window
- 365-day rolling lookback per subcategory.
- Baseline includes prior rows only in deterministic order `(transaction_date, transaction_hash)`.

### Core Components
- `amount_z_score = (amount - hist_avg) / hist_std` when `hist_std > 0`
- New-category and new-record flags

### Final Ranking
`notability_score = max(z, 0) + 4*new_subcategory + 2*new_record`

### Labels
- New Category
- Insufficient History
- Extreme Outlier (z >= 3)
- High Outlier (z >= 2)
- Above Average (z >= 1)
- New Record
- Normal

---

## Model 2: Transaction Save Potential
Table: `gold.transaction_save_potential`

### Question Answered
"Where could I have reasonably saved money?"

### Inputs
- `classification` (`WANT`, `NEED`, `MUST`)
- monthly transaction frequency in subcategory
- amount z-score (same 365-day baseline concept)

### Classification Map (Avoidability)
- WANT = 1.0
- NEED = 0.4
- MUST = 0.05
- Missing/unmapped defaults to NEED behavior

### Frequency Signal
- `month_txn_count` for `(year_month, subcategory)`
- `hist_avg_monthly_count` from prior months in 365-day window
- `freq_ratio = month_txn_count / hist_avg_monthly_count`
- `freq_excess = max(freq_ratio - 1, 0)` (capped at 3.0)

### Amount Signal
- `amt_excess = max(amount_z_score, 0)` (capped at 5.0)

### Final Ranking
`save_potential_score = 3*avoidability + 2*freq_excess + 1*amt_excess`

### Labels
- High Save Potential (>= 5)
- Medium Save Potential (>= 3)
- Low Save Potential (>= 1)
- Minimal (< 1)

---

## Operational Notes
- Incremental runs upsert only newly inserted expense hashes.
- Full refresh scripts recompute all expense rows.
- Any formula/weight update should be logged in `docs/05_DECISIONS_LOG.md` and `docs/06_CHANGELOG.md`.
