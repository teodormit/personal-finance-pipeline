# Changelog

## 2026-04-30

### Added
- Gold scoring table and refresh flow for transaction notability.
- Gold scoring table and refresh flow for transaction save potential.
- Standalone full-refresh scripts:
  - `scripts/refresh_gold_notability.py`
  - `scripts/refresh_gold_save_potential.py`
- Unit test coverage for both scoring transformers.
- Project knowledge-base docs under `docs/`.

### Changed
- `scripts/run_pipeline.py` preflight includes `gold` schema requirement.
- `initial_load.py` now triggers full gold refreshes after silver load.
- `incremental_load.py` now triggers incremental gold refreshes after silver commit.

### Notes
- Incremental runs upsert gold only for newly inserted expense hashes.
- Full refresh scripts should be used after major remaps/corrections.
