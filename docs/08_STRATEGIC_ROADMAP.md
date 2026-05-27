# Strategic Roadmap

**Purpose:** Honest assessment of where this warehouse stands today and what it needs to become a 30-year-durable financial data platform that doubles as a senior-grade DE portfolio piece. Written from the joint perspective of a data architect and a personal-finance practitioner.

**How to read this:** Section 1 grades what you have. Section 2 names what blocks the long-term vision. Section 3 is the strategic upgrade list (DE craft). Section 4 is the personal-finance domain expansion. Section 5 is the recommended phasing. Section 6 is the open questions for you to decide.

**Decisions already made (2026-05-01):**
- Currency: BGN peg is permanently fixed by law and BGN accounts are closed. All transactions going forward are EUR. No FX ingestion, no FX history table, no multi-currency investment complexity. EUR is the single analytical currency.
- dbt: of interest long-term, deferred until foundation is solid.
- Data quality framework: deprioritized for now — no concrete checks defined yet; left in plan for later.

---

## 1. Current State — What You've Actually Built (and Should Be Credited For)

You have a real, working medallion warehouse. That alone puts this ahead of 80% of "personal data project" attempts on GitHub. Specifically, the things done well:

- **Medallion layering is genuine, not cosmetic.** Bronze is append-only with `ingestion_batch_id` lineage. Silver enforces uniqueness on `transaction_hash`. Schemas are role-separated (`staging` / `bronze` / `silver` / `gold` / `metadata`). This is the correct foundation.
- **Single-transaction multi-layer commit** in [`incremental_load.py:127-140`](src/loaders/incremental_load.py#L127-L140). Staging, bronze, and silver are written under one connection and rolled back together. This is mature behavior — many production pipelines don't do this.
- **Content-based deduplication via SHA-256** is the right call. Hash key is stable across re-imports.
- **Two gold scoring models** with documented formulas, weights, and labels (`notability` and `save_potential`). Most personal finance trackers stop at a pivot table.
- **Pipeline run logging** to `metadata.pipeline_runs` works and has duration/row counts/status.
- **Pragmatic operational details**: account filter presets handled the BGN→EUR account migration cleanly. BGN accounts are now closed; EUR is the single currency going forward.
- **Documentation density is unusually high.** `docs/` has overview, system map, contracts, scoring models, runbook, decisions log, changelog, backlog, AI context pack. You already think like a senior engineer about documentation.
- **Tests exist** for both scoring transformers — they're the most formula-sensitive code, so this is the right place to start.

What this signals about you: you're not just learning DE patterns, you're applying the right ones. The gaps below are not "you did this wrong" — they're "here's the next layer of seniority."

### 1.1 To do after 14.05.2026
  1. Audit trail (metadata.transaction_audit) — trigger-based change log on silver.transactions capturing every out-of-band UPDATE/DELETE. No SCD2 (see §2.1). Phase A Step 1.                                                                                                                
  2. ExpenseTransformer tests — the biggest remaining coverage gap. The core 9-step cleaning logic has zero unit tests.
  3. Alembic baseline — schema version control so future DB changes are tracked and repeatable. Phase A Step 5.

---

## 2. Critical Gaps — What Blocks the 30-Year Vision

These are not nice-to-haves. These are the items that, left unaddressed, will become expensive to fix later.

### 2.1 Designed-but-not-built immutability *(resolved 2026-05-17 — audit log, no SCD2)*
The architecture doc committed to Type 2 SCD (`version`, `is_current`, `valid_from`, `valid_to`) plus a `metadata.transaction_audit` table. Today, if you correct a wrong category in silver, the original value is gone — no audit trail, no record of what you changed.

**Decision:** close this gap with the audit log alone — `metadata.transaction_audit`, written by an `AFTER UPDATE/DELETE` trigger on `silver.transactions` — and **drop SCD Type 2**. Rationale:
- The dataset is small and corrections are infrequent. SCD2 would force a mandatory `WHERE is_current = TRUE` on every silver query and break the `transaction_hash` unique constraint (multiple live versions per hash) — a permanent complexity tax for a point-in-time query that would rarely be run.
- The audit log answers every realistic question ("what did I change, when, from what to what") without that tax. History is reconstructable by replaying the log.
- If true point-in-time querying is ever needed, dbt (Phase C) has native `snapshot` SCD2 support — better to adopt it there than hand-roll it now and rip it out later.

The trigger captures all out-of-band corrections, including manual SQL edits; pipeline ingestion is excluded via a session flag. INSERTs are not audited — `bronze.transactions_raw` plus `created_at`/`created_by` already lineage ingestion. See `docs/05_DECISIONS_LOG.md` (2026-05-17).

### 2.2 No income gold layer despite income in silver
`silver.transactions` already holds income rows. But there are zero gold models for them. Net worth, savings rate, income volatility, YoY income growth, withholding analysis — all blocked. This is the gap `CLAUDE.md` already flags but it's not yet on any roadmap.

### 2.3 No account balances over time
The original architecture doc designed `gold.account_balance_history`. It was never built. Without it, you cannot compute net worth, runway, or "am I spending more than I earn this quarter" without recomputing from scratch each query. This is foundational for any personal finance use case beyond expense reporting.

### 2.4 No data quality framework — the framework you designed isn't wired up *(deprioritized)*
`bronze.has_quality_issues` exists but is hardcoded `False`. `metadata.data_quality_issues` table exists but nothing writes to it. The "DQ-001 through DQ-010" rules in the architecture doc were never implemented.

**Status:** Deprioritized — no concrete checks are well-defined yet. Left in the plan as a future item. When concrete checks are identified (not_null, unique, referential integrity on subcategory→category_mapping, row-count reconciliation), this becomes straightforward to wire up.

### 2.5 No orchestration despite Prefect being in requirements
You import nothing from Prefect anywhere in the code. Pipeline is run manually. This is fine for 2026 but in year 5 you will have forgotten to run it for two weeks at some point. A 30-year pipeline must self-trigger and self-alert.

### 2.6 Single-source coupling
Every layer assumes BudgetBakers. The transformer renames `note → description` and `category → subcategory` based on Wallet's column names. The extractor is `BudgetBakersExtractor`. To add a bank API, a brokerage, or a crypto exchange, you'd duplicate the loader. This blocks the income/investment expansion stated as the goal.

### 2.7 No automated schema migration
Schema lives in `init_scripts/` numbered SQL files which only run when the Postgres container is initialized for the first time. There's already a `06_alter_silver_schema.sql`, which shows the pain — schema changes in a running warehouse are awkward. You need Alembic or a similar migration framework before changing silver further.

### 2.8 No CI / pre-commit / test enforcement
Tests exist but nothing runs them. A future change to the scoring formula could silently break notability, and you'd find out in Tableau three weeks later.

### 2.9 No backup strategy
A 30-year warehouse on a single Docker volume on a single laptop is one disk failure away from total loss of your financial history. WAL archiving + offsite encrypted backups (`pg_dump` to encrypted file, rclone to cold storage) needs to exist before you accumulate data you can't reconstruct from BudgetBakers alone.

---

## 3. Architecture Upgrades — DE Craft and Portfolio Strength

These are the moves that mark the transition from "someone learning DE" to "senior DE." They also dramatically improve usability.

### 3.1 Adopt dbt for the silver→gold transformations *(deferred — after foundation is solid)*
**What it changes:** The Pandas-based gold loaders become SQL models. Every transformation is version-controlled, tested, and lineage-tracked. dbt generates documentation automatically.

**Why it matters strategically:**
- It's the single most-recognized tool in modern data engineering. Hiring managers will scan a portfolio for it.
- It replaces your hand-rolled `gold_*_loader.py` files with declarative SQL that's easier to reason about.
- Built-in tests cover the data quality framework gap (2.4) without writing any new framework.
- Auto-generated lineage docs satisfy the "30-year future me must understand this" requirement.

**Why it works here specifically:** your gold scoring is already SQL-shaped (window functions over silver). Pandas adds nothing the database can't do faster. dbt's incremental models map cleanly onto your "incremental upsert by hash" pattern.

**Pragmatic scope:** keep Pandas for extract+silver-load (file parsing and API normalization). Hand silver→gold to dbt entirely. Earmarked for Phase C.

### 3.2 Refactor extractors into a source-agnostic interface
Today: `BudgetBakersExtractor.extract()` returns a DataFrame.
Tomorrow: `Source` ABC with `extract(date_from, date_to) -> DataFrame` and a `to_canonical(df) -> DataFrame` mapping step. New sources implement one method each.

This is a small refactor now (one source, two extraction modes) that pays off the day you add a bank, brokerage, or crypto exchange. Doing it now is cheap; doing it after three sources have been bolted on is expensive.

### 3.3 Move the pipeline itself into Docker
Currently only Postgres is containerized. The Python pipeline runs on the host. For 30-year reproducibility (and for the portfolio story) the pipeline should be a container too. Add a `pipeline` service to `docker-compose.yml` with a `Dockerfile`, and a single `docker compose run pipeline incremental` command becomes the entry point.

### 3.4 Adopt orchestration (Prefect or Dagster)
Prefect is already in requirements. Wire it up. A flow that runs `incremental` daily, checks rows loaded, and alerts on failure is ~50 lines. Dagster is more modern and has tighter dbt integration; Prefect is what was already chosen. Either works.

### 3.5 Add CI: GitHub Actions running pytest + sqlfluff
A ~10-line workflow file. Catches formula regressions and schema drift on every PR. Visible green checkmarks on the repo. Massively improves the portfolio story at near-zero cost. (Add dbt build to this when dbt is adopted in Phase C.)

### 3.6 Schema migrations via Alembic
The `init_scripts/` folder is for one-time setup. As soon as you add a column to silver, you need migration files. Alembic gives you forward/backward migration scripts and a current-version tracker.

### 3.7 Add a config layer for reference data
Currently `silver.category_mapping` is bootstrapped from inline `INSERT INTO ... VALUES` in the SQL init script, with manual override `UPDATE` statements. Move classifications to a YAML file in the repo. The mapping table loads from YAML on each run. Then reclassifying bar/cafe as WANT or NEED is a one-line code change with full git history, not a hand-edited SQL file.

### 3.8 Type hints everywhere + ruff
Mixed today. Adding strict type hints + running ruff in CI is a low-effort, high-signal credibility upgrade.

---

## 4. Personal-Finance Domain Expansion — What's Possible

Here are the analytics that become possible once the gaps in §2 are closed. All amounts are EUR going forward.

### 4.1 The income side (mirrors of your gold expense models)
- `gold.income_stability` — variance in monthly income, predictable vs variable income split
- `gold.savings_rate` — `(income − expenses) / income` per month, with rolling 12-month smoothing
- `gold.income_source_concentration` — what % of your income comes from your top source (a financial-resilience metric)

### 4.2 Net worth and balances (requires §2.3 first)
- Daily account balance snapshots (computed from the transaction stream + manual opening balances)
- `gold.net_worth_daily` — sum across accounts, plus investment positions when those land
- Net worth growth rate, contribution vs market gain decomposition once investments are in

### 4.3 Cash flow and forecasting
- Recurring transaction detection — find subscriptions, regular bills, predictable transfers. Powers a "subscription audit" report (high-value personal finance use case).
- 12-month forward cash flow forecast based on detected recurring patterns + average discretionary spend
- Runway months: `cash_balance / avg_monthly_burn`

### 4.4 Investment layer (the big one)
This is its own design exercise but the shape:
- A new schema, e.g. `investments`, with `positions` (point-in-time holdings) and `transactions` (buys/sells/dividends/fees)
- A `prices` table fed from a free API (yfinance, Alpha Vantage) — EUR-denominated or EUR-converted at ingestion time
- Gold models: time-weighted return per holding, money-weighted return overall, allocation drift vs target, dividend yield realized
- All amounts in EUR, which is already the single analytical currency

### 4.5 Tax preparation support
- Annual income aggregation by source (wage / dividends / interest / capital gains / rental)
- Deductible-tagged transactions (charitable giving, work expenses, etc.) — would need a `tax_deductible BOOLEAN` column in silver
- Year-end "tax pack" report: a single Tableau dashboard you take to your accountant

### 4.6 Goal tracking
- FIRE number progress (`net_worth / (annual_expenses * 25)`)
- Emergency fund coverage (months of expenses in liquid accounts)
- Debt paydown projections at current rate
- Custom savings goals with current/target/ETA columns

### 4.7 Inflation-adjusted views
- Join silver with a HICP/CPI table; show "real" trends across decades, not nominal
- This is the kind of feature that only makes sense at 30-year scale, and only the most thoughtful personal-finance dashboards have it

### 4.8 Reconciliation against bank statements
- Monthly job: compare aggregated silver totals per account per month against statement, alert on mismatch
- Catches missed transactions, double-entries, and BudgetBakers categorization drift

---

## 5. Recommended Phased Roadmap

The principle: every phase delivers a working improvement and unblocks the next one.

### Phase A: Foundation hardening
**Goal:** make the warehouse safe to grow.
1. ✅ **Done (2026-05-17)** — Audit trail: `metadata.transaction_audit` change log written by `AFTER UPDATE/DELETE` trigger on `silver.transactions` (§2.1). No SCD Type 2 — see decision log.
2. ✅ **Done (2026-05-22)** — DIY schema migration runner: `scripts/migrate.py` tracks applied migrations in `metadata.schema_migrations` with SHA-256 checksums. Alembic rejected (no SQLAlchemy models in project). Baselined on the live DB 2026-05-25 (§3.6).
3. 🟡 **Partial** — `.github/workflows/ci.yml` runs `pytest` on every push and PR (added 2026-05-13). `sqlfluff` for SQL linting is **not** wired up yet (§3.5).
4. ✅ **Done (2026-05-27)** — Weekly `pg_dump` via `scripts/backup.ps1` registered in Windows Task Scheduler; rclone uploads each dump to `gdrive:Finance Backups/` after every successful run. 45-day local retention. Setup instructions in `docs/04_RUNBOOK.md §Backup` (§2.9).
5. ✅ **Done (2026-05-25)** — Pipeline moved into Docker (§3.3). Single image, one-shot containers via `docker compose run --rm pipeline ...`, host-Python dev path preserved. See `docs/05_DECISIONS_LOG.md` (2026-05-25).

**Why first:** none of these change behavior, all of them protect from data loss and silent breakage as scope expands.

**Phase A status:** Complete (2026-05-27). `sqlfluff` CI linting dropped from scope — no concrete rules defined yet. Phase B can start.

### Phase B: Income gold + account balances
**Goal:** unlock the full transaction picture — income, not just expenses.
1. Build income-side gold models using the current Python/SQL pattern: `gold.income_stability`, `gold.savings_rate` (§4.1).
2. Build `gold.account_balance_history` (§2.3, §4.2).
3. Refactor extractor into source-agnostic interface (§3.2) — no new source needed yet, just establish the shape.
4. Add Prefect orchestration with daily run + failure alert (§3.4).

**Why second:** you have a tested, audited foundation, and the income models use the same pattern as the existing gold models — low risk, high analytical value.

### Phase C: dbt migration + first non-Wallet source
**Goal:** modernize the transformation layer and prove multi-source extensibility.
1. Introduce dbt; reimplement existing gold models as dbt models (§3.1).
2. Add dbt tests covering the data quality framework (§2.4).
3. Add CI: `dbt build` step to the existing GitHub Actions workflow.
4. Add one non-Wallet source: a bank Open Banking integration. This forces the source abstraction (§3.2) to be tested against a real second source.

**Why third:** the foundation is solid, income models prove the pattern works, and dbt makes everything that follows cheaper to write and safer to change.

### Phase D: Investment layer
**Goal:** turn this into a real wealth-tracking system.
1. Design `investments` schema: `positions`, `transactions`, `prices`, `corporate_actions`.
2. Pick a broker source (Trading212, Interactive Brokers, Revolut Stocks — depends on what you actually use).
3. Add yfinance or Alpha Vantage for daily EUR prices.
4. Gold models: TWR, MWR, allocation drift, dividends realized.
5. Net worth gold table joining accounts + investments (§4.2).

### Phase E: Polish and personal-finance specials *(ongoing)*
- Recurring transaction detection and subscription audit
- Tax pack dashboard (§4.5)
- FIRE / emergency-fund / goal tracking (§4.6)
- Inflation-adjusted views using HICP (§4.7)
- Reconciliation jobs (§4.8)
- Config layer for reference data / YAML-driven category mapping (§3.7)

---

## 6. Open Questions

These are the remaining forks that will shape the roadmap when they come up.

1. **Orchestration: Prefect (already in requirements) or Dagster (more modern, better dbt integration)?** Either works; Prefect is the path of least resistance since it's already chosen.

2. **Open Banking provider for Phase C.** Depends on your country and banks. GoCardless Bank Account Data, Tink, or direct bank scraping. Worth a research session when Phase C is near.

3. **How much investment detail?** If you're a passive investor with a few ETFs, the investment layer can be lean. If you actively trade or have complex holdings, Phase D is a major project on its own.

4. **Family / multi-entity: now or later?** Modeling an `owner` / `account_holder` dimension now is cheap; retrofitting is expensive. Consider adding in Phase A if joint finances are on the horizon.

5. **Categorization ML?** A medium-term opportunity: train a classifier on your hand-categorized history to auto-suggest categories for new transactions. Out of scope for now, but high value if the manual categorization burden grows.

---

## 7. Closing Argument

You're at the inflection point where this stops being "a script that loads CSVs" and starts being "a personal financial system I can rely on for decades." The temptation will be to keep adding features in the current architecture because it works. Resist that.

The single most valuable thing you can do next is **Phase A** — not because it's exciting, but because everything in Phases B/C/D becomes far cheaper and safer once it's in place. Type 2 SCD, audit, backups, CI, and Docker-encapsulated runs are the unglamorous work that distinguishes a portfolio that says "I built a thing" from one that says "I built a thing the way a senior engineer would."

The personal-finance domain opportunities (§4) are where this project becomes *visibly different* from every other "I tracked my expenses" GitHub repo. Net worth, savings rate, investment performance, tax pack — these are the screenshots that make a recruiter pause. None of them are technically hard once Phase A and B are done. They're hard because nobody else has the foundation to support them.

You have the foundation. Phase A protects it. Phases B–D let it grow. Phase E is where you reap.

---

*Author: Claude (strategic analysis session, 2026-04-30). Decisions incorporated: 2026-05-01.*
