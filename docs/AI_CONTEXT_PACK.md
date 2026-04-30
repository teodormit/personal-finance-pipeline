# AI Context Pack

Use this file first when handing the repository to another AI.

## Project Snapshot
- Domain: personal finance ETL + analytics warehouse
- Core stack: Python, Pandas, PostgreSQL, Docker, Tableau
- Data architecture: `staging -> bronze -> silver -> gold -> metadata`

## Primary Entry Points
- `scripts/run_pipeline.py` (main runner)
- `src/loaders/incremental_load.py`
- `src/loaders/initial_load.py`
- `src/transformers/expense_transformer.py`
- `src/transformers/notable_transactions_transformer.py`
- `src/transformers/save_potential_transformer.py`

## Current Gold Models
1. `gold.transaction_notability`
   - score: `max(z,0) + 4*new_subcategory + 2*new_record`
   - lookback default: 365 days
2. `gold.transaction_save_potential`
   - score: `3*avoidability + 2*freq_excess + 1*amt_excess`
   - avoidability map: WANT=1.0, NEED=0.4, MUST=0.05

## Runtime Behavior (Important)
- Incremental silver loads dedupe by `transaction_hash`.
- Gold refresh in incremental mode only upserts targeted new expense hashes.
- Gold loaders fetch a widened silver slice (`min_date - window_days` to `max_date`) for historical context.
- Initial/full loads run full gold refreshes.

## Key Assumptions
- `silver.category_mapping` is maintained and used to populate classification.
- `transaction_hash` remains stable and is used as join/upsert key.
- Window default remains 365 unless intentionally overridden.

## Validation Commands
```bash
python -m pytest tests/test_notable_transactions_transformer.py -v
python -m pytest tests/test_save_potential_transformer.py -v
python scripts/run_pipeline.py --mode incremental
```

## Handoff Checklist
- Confirm branch and cleanliness (`git status`)
- Read `docs/05_DECISIONS_LOG.md`
- Read `docs/04_RUNBOOK.md`
- Read `README.md` Run Pipeline section
- Verify whether task impacts formulas, refresh behavior, or contracts
