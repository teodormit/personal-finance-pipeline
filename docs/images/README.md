# README images

Visual assets referenced by the top-level `README.md`.

## `dashboard.png` — dashboard preview

The dashboard preview image displayed near the top of the main README, linked
to the published Tableau Public visualization.

**Procedure:**
1. Generate an anonymized extract with `python scripts/anonymization_finance_data.py`.
   The published dashboard must be built on anonymized data only; real financial
   figures are never published.
2. Build and publish the dashboard on Tableau Public.
3. Capture the published view at a wide aspect ratio (approximately 1600×900)
   and save it in this directory as `dashboard.png`.
4. In the top-level `README.md`, enable the dashboard block and replace the
   placeholder with the published visualization URL.

Keep the file size modest (under approximately 500 KB; compress if necessary).
An optional short looping `.gif` demonstrating filtering and tooltips may be
added as an enhancement.
