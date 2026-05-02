# Project Overview

## Mission
Build and operate a reliable, self-hosted financial data warehouse that centralizes personal financial data — starting with expenses and income from BudgetBakers, expanding to investments and other financial datasets over time — and makes it auditable, analytics-ready, and visualizable in Tableau.

## Long-Term Vision
This warehouse is designed with a 30-year horizon. The current expense + income pipeline is the foundation layer. Future phases will add investment tracking, net worth snapshots, and integration with additional data sources. All architecture decisions should preserve extensibility: new transaction types, new schemas, and new gold models should be addable without breaking existing pipelines. Open-source and self-hosted infrastructure is a first-class constraint throughout.

## Current State
- API + file extraction paths are live.
- Core medallion pipeline is live: `staging -> bronze -> silver -> gold`.
- Incremental and full load modes are both supported.
- Gold intelligence layer is active with two transaction-level models:
  - `gold.transaction_notability`
  - `gold.transaction_save_potential`
- Unit tests cover both scoring transformers.

## What Is Production-Ready
- Incremental ingestion with deduplication by `transaction_hash`.
- Category/classification enrichment via `silver.category_mapping`.
- Automated gold refresh after successful loads.
- Metadata run logging to `metadata.pipeline_runs`.

## What Is Experimental / Improving
- Save-potential score calibration over time (weights are configurable in code).
- Better diagnostics around sparse-history subcategories.
- Model-version fields in gold tables (planned).

## Near-Term Priorities
1. Keep README + docs in sync with behavior changes.
2. Add integration tests for gold loader SQL slices.
3. Add model versioning columns to gold tables.
4. Add dashboard-level QA checks for labels and score drift.
5. Define release notes cadence for formula changes.
