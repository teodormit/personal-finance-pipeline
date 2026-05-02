# System Map

## High-Level Flow
1. Extract data (API or file)
2. Transform with Pandas (`ExpenseTransformer`)
3. Load to PostgreSQL layers
4. Compute gold scoring outputs
5. Visualize in Tableau

## Runtime Entry Points
- `scripts/run_pipeline.py` � unified runner for full and incremental loads, and gold-only full rebuilds via `--refresh-gold {notability,save-potential,both}`.
- `scripts/inspect_incremental_load.py` � dry-run inspection utility.

## Main Code Domains
- `src/extractors/`
  - `budgetbakers_extractor.py` � REST extraction and normalization.
  - `api_field_mapper.py` � maps raw API fields to transformer schema.
- `src/transformers/`
  - `expense_transformer.py` � core 9-step cleaning/enrichment.
  - `notable_transactions_transformer.py` � notability scoring.
  - `save_potential_transformer.py` � save-potential scoring.
- `src/loaders/`
  - `initial_load.py` � full load path (truncate/reload silver).
  - `incremental_load.py` � append-only + dedupe path.
  - `gold_notable_loader.py` � gold upsert for notability.
  - `gold_save_potential_loader.py` � gold upsert for save potential.
- `src/utils/`
  - `db_connector.py` � DB connection manager.
  - `hash_generator.py` � `transaction_hash` generation.

## Data Layers
- `staging` � transient landing table(s), truncated each run.
- `bronze` � immutable raw archive.
- `silver` � cleaned + deduped analytical base.
- `gold` � transaction-level intelligence tables.
- `metadata` � pipeline run and quality telemetry.

## Gold Refresh Behavior
- Incremental runs refresh gold for newly inserted expense hashes.
- Initial/full runs rebuild gold fully.
- Manual full refresh scripts exist for corrective recomputation.
