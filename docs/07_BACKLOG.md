# Backlog

## High Priority
1. Add model version fields to gold tables (`model_version`, `computed_with_window_days`).
2. Add integration tests for gold loader incremental slice correctness.
3. Add explicit warning suppression/fix for numerical edge cases in variance sqrt path.
4. Add dashboard QA checklist for score distribution sanity.

## Medium Priority
1. Add PR template requiring formula/weight impact notes.
2. Introduce docs automation checklist in CI (broken links, stale commands).
3. Add data-quality monitoring around classification completeness.
4. Add richer `metadata` table for gold refresh row counts and duration.

## Low Priority
1. Build monthly snapshot exports for dashboard archive.
2. Add optional model experimentation mode in notebooks.
3. Add lightweight benchmark script for scoring performance trends.

## Future Exploration
- Dynamic weights by category family.
- Externalized model config file (YAML/JSON) for easier non-code tuning.
- Explainability fields per score component for BI hover cards.
