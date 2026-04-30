# Decisions Log

## 2026-04 — Gold notability model added
- Decision: Compute transaction-level notability in `gold.transaction_notability`.
- Why: Surface unusual spending beyond simple amount sorting.
- Key design:
  - 365-day rolling subcategory baseline
  - z-score + novelty flags
  - incremental upsert + optional full refresh script
- Tradeoff: Existing old rows are not automatically re-scored on incremental runs.

## 2026-04 — Gold save-potential model added
- Decision: Compute `gold.transaction_save_potential` with avoidability-dominant weighting.
- Why: Identify transactions with practical savings opportunity, not only statistical surprise.
- Formula: `3*avoidability + 2*freq_excess + 1*amt_excess`
- Tradeoff: Weight choices are heuristic and require periodic calibration.

## 2026-04 — Post-commit gold refresh strategy
- Decision: Refresh gold after silver commit and keep failures non-fatal.
- Why: Preserve silver consistency even if scoring layer has transient issues.
- Tradeoff: Gold can briefly lag silver until next successful run/manual refresh.

## 2026-04 — Incremental slice strategy for gold loaders
- Decision: For targeted hashes, fetch silver rows for affected subcategories between `min_date-365` and `max_date`.
- Why: Preserve sufficient historical context while keeping refresh scoped.
- Tradeoff: Loader logic is more complex than full-table recompute.

## Revisit Triggers
- Material score drift observed in dashboard behavior
- Increased data volume impacting refresh performance
- Significant changes to category mapping policy
- Need for stricter consistency guarantees between silver and gold
