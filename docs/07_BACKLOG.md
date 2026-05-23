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

## Phase A Remaining
1. **Dockerize the pipeline** (Phase A §3.3) — add a `pipeline` service to `docker-compose.yml` + a `Dockerfile` so the Python pipeline runs in a container alongside Postgres. Read up on Docker volumes, service networking (`postgres:5432` vs `localhost:5432`), and the dev-vs-production image trade-off (volume-mount vs copy-code-in) before implementing. Three approaches evaluated; Approach B (volume-mount, development-friendly) recommended as starting point. Revisit when Prefect orchestration arrives in Phase B — that's when a fully self-contained image becomes necessary.

## Future Exploration
- Dynamic weights by category family.
- Externalized model config file (YAML/JSON) for easier non-code tuning.
- Explainability fields per score component for BI hover cards.
