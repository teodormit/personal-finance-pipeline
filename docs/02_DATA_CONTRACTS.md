# Data Contracts

## Canonical Business Key
- `transaction_hash` (SHA-256) is the cross-layer dedupe and join key.
- Generated from normalized date + amount + category + description components.

## Core Expectations by Layer

### Staging
- Temporary shape for ingestion and quick validation.
- Can be truncated and rebuilt each run.

### Bronze (`bronze.transactions_raw`)
- Append-only immutable raw archive.
- Keeps ingestion metadata (`ingestion_batch_id`, timestamps, source markers).

### Silver (`silver.transactions`)
- One canonical analytical row per unique `transaction_hash`.
- Required analytical fields include:
  - `transaction_date`
  - `transaction_type`
  - `amount_abs_eur`
  - `subcategory`
  - `year_month`
  - `classification` (enriched via category mapping)

### Gold Notability (`gold.transaction_notability`)
Required conceptual fields:
- Key + context: `transaction_hash`, `transaction_date`, `subcategory`, `amount_abs_eur`
- Historical stats: `hist_window_days`, `hist_n_txns`, `hist_avg_amount_eur`, `hist_std_amount_eur`, `hist_max_amount_eur`
- Score outputs: `amount_z_score`, `notability_score`, `notability_label`, `notability_reason`

### Gold Save Potential (`gold.transaction_save_potential`)
Required conceptual fields:
- Key + context: `transaction_hash`, `transaction_date`, `subcategory`, `classification`, `amount_abs_eur`
- Components: `avoidability`, `month_txn_count`, `hist_avg_monthly_count`, `freq_ratio`, `freq_excess`, `amount_z_score`, `amt_excess`
- Score outputs: `save_potential_score`, `save_potential_label`, `save_potential_reason`

## Temporal Window Contract
- Both models default to a 365-day lookback (`window_days=365`).
- Incremental loaders fetch a widened silver slice from `min(target_date)-365` through `max(target_date)` for relevant subcategories.

## Classification Contract
- Source of truth: `silver.category_mapping` joined in loaders.
- Expected values: `WANT`, `NEED`, `MUST`.
- Save-potential fallback for missing/unmapped classification defaults to NEED behavior.

## Idempotency Contract
- Incremental silver load inserts only unknown `transaction_hash` values.
- Gold loaders use UPSERT on `transaction_hash`.
