# Simplification Report — How the Refactor Works and Why

**Author:** Claude (sessions of 2026-05-01 and 2026-05-02)
**Branch:** `feature/code-optimization`
**Audience:** You, the project owner — strong on BI / data viz / SQL, four months into data engineering, learning Python software-engineering patterns.

This document is the *learning artifact* of the refactor. It explains:

1. What the codebase does (the data flow, the contracts).
2. What changed in the refactor and why each decision was made.
3. The software-engineering principles applied — in plain language, with line-level references back to the actual code, and with analogies to BI concepts you already know well.

Read this once end-to-end. Then keep it as a reference when you come back in three weeks and ask *"why did we do this again?"* It is the answer to that question.

---

## Part 1 — What the Codebase Does

### The medallion at a glance

You have a single Python pipeline that takes transactions out of BudgetBakers (either via the REST API or via an Excel/CSV export) and lands them in PostgreSQL through three quality tiers:

```
            ┌─────────────────────────────────────────────────────────┐
            │                Pipeline (Python, src/)                   │
            │                                                          │
  raw input │  Extract → Transform → Stage → Bronze → Silver → Gold   │
  ─────────►│                                                          │─► Tableau
            └─────────────────────────────────────────────────────────┘
                              │              │        │       │
                              ▼              ▼        ▼       ▼
                     staging.raw_     bronze.    silver.   gold.transaction_
                     transactions     transactions_raw  transactions   notability /
                                                                       save_potential
```

The four PostgreSQL schemas have explicit, unchanging roles:

- **`staging`** — disposable. Truncated and reloaded every run. This is the "scratch pad" for the latest batch.
- **`bronze`** — immutable archive. Append-only. Every row that ever entered the warehouse stays here forever, with a `source_file`, a `batch_id`, and an `ingestion_timestamp`. If silver gets corrupted, bronze is the backup.
- **`silver`** — the analytical truth. Cleaned, deduped, type-checked, enriched (year/month/quarter columns derived, EUR conversions, classification looked up from `category_mapping`). One row per logical transaction, identified by a SHA-256 `transaction_hash`. **This is what Tableau and most analysis lives on.**
- **`gold`** — derived intelligence. One row per `transaction_hash` with a *score and a label*. Two metrics today:
  - `gold.transaction_notability` — "how surprising was this transaction vs. my own history?" (z-score-based).
  - `gold.transaction_save_potential` — "where could I have reasonably saved money?" (avoidability × frequency × amount).

### The two transaction-loading paths

Two ways data arrives in silver:

- **Initial load** (`InitialDataLoader`): truncates silver, re-inserts everything from a single big export. Used once, when you set up the project. Effectively retired now that silver is populated through 2026-03-17.
- **Incremental load** (`IncrementalDataLoader`): runs daily/weekly. Pulls only new transactions (using `MAX(silver.transaction_date) + 1` as the watermark), deduplicates against existing silver hashes, appends only the new rows.

Both loaders end the same way: refresh the gold tables for the affected hashes, log to `metadata.pipeline_runs`.

### The deduplication contract

This is *the most important invariant* in the warehouse:

```
transaction_hash = SHA256(transaction_date + amount + category + description)
```

It is computed in `src/transformers/expense_transformer.py` after cleaning. Silver enforces uniqueness on it. Gold upserts on it. As long as the hash is stable across re-imports of the same row, you can re-run the pipeline freely without creating duplicates.

### The gold scoring contracts

Both gold tables follow the same pattern: take a slice of EXPENSE rows from silver, compute a score per row, UPSERT into gold. The two metrics differ only in *what* they compute:

**Notability** (z-score):
```
For each EXPENSE row at date D in subcategory S:
    history = all OTHER EXPENSE rows in S where transaction_date >= D - 365 days
              (strictly before the current row in (date, hash) sort order)
    z = (current_amount - mean(history)) / stddev(history)
    notability_score = max(z, 0) + 4*is_new_subcategory + 2*is_new_record
```

Labels (priority order): New Category > Insufficient History > Extreme Outlier (z≥3) > High Outlier (z≥2) > Above Average (z≥1) > New Record > Normal.

**Save Potential**:
```
For each EXPENSE row:
    avoidability ∈ {WANT=1.0, NEED=0.4, MUST=0.05}  (from silver.classification)
    freq_excess  = max(this_month_count / avg_monthly_count_prior_365d - 1, 0)  (capped at 3)
    amt_excess   = max(amount_z_score, 0)                                       (capped at 5)
    save_potential_score = 3*avoidability + 2*freq_excess + 1*amt_excess
```

Labels: High (≥5) / Medium (≥3) / Low (≥1) / Minimal.

These are the "math facts" the warehouse runs on. **Nothing in the refactor changed any of them.** That's the whole point of having a snapshot test.

---

## Part 2 — The Simplification at a Glance

Seven commits on `feature/code-optimization`, each independently revertable:

| # | Step | Commit | What it did |
|---|---|---|---|
| 1 | Archive stale scripts | `8a97af7` | Moved 3 BGN→EUR-era debugging utilities to `scripts/archive/`. |
| 2 | Consolidate refresh entry points | `4e53e27` | Folded `refresh_gold_*.py` scripts into `run_pipeline.py --refresh-gold {notability,save-potential,both}`. |
| 3a | Extract pure helpers into BaseLoader | `a672324` | Pulled `_log_pipeline_run`, `_display_summary`, `_bulk_insert`, `_update_category_mapping`, gold-refresh wrappers into `src/loaders/base_loader.py`. |
| 3b | Collapse layer loaders into BaseLoader | `cd8005d` | Moved `_extract_from_file`, `_transform`, `_load_staging`, `_load_bronze`, `_prepare_silver_df`, and the entire `load()` orchestration into BaseLoader. Both subclasses now share the same single-connection-with-rollback transaction strategy. |
| 4 | Extract GoldRefresher | `a451f78` | Same treatment for the gold loaders: shared base class with `refresh()` template, subclasses provide compute function + column lists. |
| 5+6+7 | Simplify gold transformers | `f2dbd78` | Moved `compute_rolling_stats` to a shared module; vectorized save-potential's frequency logic via merge; replaced nested `np.where` label chains with `np.select`. |

**Cumulative result:**

- Source: ~430 net lines removed across `src/`.
- Tests: 21 → 67 passing tests.
- Behaviour: byte-identical (verified by snapshot test against pre-refactor baseline + read-only smoke against live silver).

---

## Part 3 — Software Engineering Principles, in Plain Language

This is the section that matters. Each principle below is named, defined briefly, and then *shown in action* in your repo with line references.

### 3.1 DRY — Don't Repeat Yourself

> Every piece of knowledge should have a single, unambiguous, authoritative representation in the system.

**The simplest version:** if you find yourself copy-pasting code, you've created two places that need to be kept in sync forever. That's a bug factory.

**Where it shows up here:** before the refactor, [`incremental_load.py`](../src/loaders/incremental_load.py) and `initial_load.py` (pre-refactor) both contained:

- An identical `_log_pipeline_run` (~28 lines each)
- An identical `_bulk_insert` (~20 lines each)
- An identical category-mapping SQL block (~22 lines each)
- A 95% identical `_load_staging`, `_load_bronze`

If you'd needed to add a new column to `metadata.pipeline_runs` (say `cpu_time_seconds`), you'd have had to edit both files and remember to keep them aligned. With the refactor, you change `BaseLoader._log_pipeline_run` once.

**BI analogy:** it's the same reason you don't want the same calculated field defined in 14 different Tableau workbooks. One source of truth.

The trade-off DRY introduces: when something is shared, a change to it affects every caller. This is good when the duplicates *should* stay aligned (logging format), bad when they were only "incidentally similar" and might diverge later (don't DRY two functions just because they're 80% the same — make sure they're 80% the same *for the same reason*).

### 3.2 The Template Method pattern

> Define the skeleton of an algorithm in a base class, but leave specific steps to subclasses.

**This is the most important pattern in the refactor.** Both [`BaseLoader.load()`](../src/loaders/base_loader.py) and [`GoldRefresher.refresh()`](../src/loaders/gold_refresher.py) use it.

In `BaseLoader.load()`:

```python
def load(self) -> bool:
    self._print_header()
    raw_df = self._extract()             # ← subclass-specific
    transformed_df = self._transform(raw_df)
    transformed_df = self._post_transform(transformed_df)  # ← subclass-specific
    conn = self.db.connect()
    try:
        self._load_staging(transformed_df, conn)
        self._load_bronze(transformed_df, conn)
        self._load_silver(transformed_df, conn)             # ← subclass-specific
        self._update_category_mapping(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    self._refresh_gold_notability()
    self._refresh_gold_save_potential()
    self._log_pipeline_run()
    return True
```

The *outline* of "extract → transform → stage → bronze → silver → commit → refresh gold → log" is fixed. The three places where the two loaders genuinely differ — `_extract` (API+watermark vs file-only), `_post_transform` (account filter or not), `_load_silver` (dedupe vs truncate-and-reload) — are subclass overrides.

**Why this is powerful:** anyone reading [`incremental_load.py`](../src/loaders/incremental_load.py) sees only what makes incremental load *different* from the template. The template is somewhere else, in one place. This is what makes the file go from 513 lines to 258.

**BI analogy:** it's like a Tableau dashboard *template* — the layout (KPI tile / line chart / map) is fixed, the data sources can change per use case. You don't rebuild the layout every time.

The same pattern in `GoldRefresher`:

```python
def refresh(self, db, *, hashes, full, window_days):
    # Skeleton: fetch silver slice → compute → filter to target hashes → upsert
    silver_df = self._fetch_silver_expenses(...)
    computed = self.compute(silver_df, window_days=window_days)  # ← subclass-specific
    if target_hashes:
        computed = computed[computed["transaction_hash"].isin(target_hashes)]
    return self._upsert_to_gold(conn, computed)
```

Notability and save-potential differ only in the `compute()` override and a few class attributes (`gold_table`, `gold_columns`, `silver_extra_columns`, `json_columns`). The compute body is in the *transformers*; the refresher is just the wiring.

### 3.3 Inheritance vs. Composition (and when each is right)

**Inheritance** says: "B is-a A." A `IncrementalDataLoader` IS-A `BaseLoader`. They share a contract.

**Composition** says: "B has-a A." A `IncrementalDataLoader` HAS-A `DatabaseConnection`. They are partners.

A common mistake is reaching for inheritance every time you see shared behaviour. Inheritance is right when:
- The subclass really *is* a specialization of the base (a Loader IS a kind of Loader).
- The base class describes a *fixed contract* that every subclass must honour.
- The relationship is permanent — subclasses don't need to swap out their parent.

In this refactor we used inheritance for `BaseLoader` and `GoldRefresher` because both fit those criteria.

We used composition (not inheritance) for things like:
- The `DatabaseConnection` (every loader *has* one, but isn't one).
- The `ExpenseTransformer` (every loader *has* one).
- The compute functions in `GoldRefresher` — `NotabilityRefresher.compute()` *calls* `compute_notability(...)`, it doesn't subclass it.

A useful rule of thumb: **when you can satisfy a need with composition, prefer it.** Inheritance is a stronger commitment, and over-using it produces deep brittle hierarchies.

### 3.4 Open/Closed Principle

> Software entities should be open for extension but closed for modification.

In English: when you need new behaviour, you should be able to *add* code, not edit existing code.

**How the refactor enables this:**

Imagine adding a third gold metric — say, `gold.transaction_categorical_drift` — that flags transactions whose category seems wrong relative to history. To add it:

```python
class CategoricalDriftRefresher(GoldRefresher):
    gold_table = "gold.transaction_categorical_drift"
    print_label = "transaction_categorical_drift"
    silver_extra_columns = ["category", "payee"]
    gold_columns = ["transaction_hash", "drift_score", "drift_label", "computed_at", ...]

    def compute(self, silver_df, *, window_days):
        return compute_categorical_drift(silver_df, window_days=window_days)
```

That's it. ~30 lines. The `GoldRefresher` base class needs **zero changes**. The `BaseLoader.load()` method needs **zero changes**. You'd just add a `_refresh_gold_categorical_drift` method to BaseLoader (5 lines) and wire it into `load()`.

Compare to before: you'd have copied 200 lines from `gold_save_potential_loader.py`, search-and-replaced the metric name, and now you have three places to keep in sync.

This is what "closed for modification, open for extension" buys you.

### 3.5 Encapsulation — keep details where they belong

> Hide what shouldn't be visible. Expose only the contract.

Before the refactor, `save_potential_transformer.py` had this line near the top:

```python
from transformers.notable_transactions_transformer import _compute_rolling_stats
```

That underscore prefix is Python's convention for "private — don't import this from outside." But save-potential needed the function, so it reached across the wall and grabbed it. This is what's called a **tight coupling smell**: two modules know more about each other's internals than they should.

Step 5 fixed this by moving `_compute_rolling_stats` to a third module ([`src/transformers/_gold_common.py`](../src/transformers/_gold_common.py)) and renaming it `compute_rolling_stats` (no underscore — it's now a clearly public shared API). Both transformers import it from there. Neither one reaches into the other's private space.

**BI analogy:** it's the difference between a published Tableau Data Source (anyone can connect to it) and a workbook's local extract (which other workbooks shouldn't be reading directly).

### 3.6 Separation of concerns — even within one method

The `_log_pipeline_run` method is small but illustrates a subtle, important design choice:

```python
def _log_pipeline_run(self) -> None:
    ...
    with self.db.connect() as conn:   # ← fresh connection, NOT the main one
        cursor = conn.cursor()
        cursor.execute(query, list(log_data.values()))
```

The pipeline-run log uses a **separate, fresh database connection** — not the one that runs staging/bronze/silver. Why?

Because if the main transaction fails and rolls back, **you still want a record that the pipeline tried to run and failed**. If the log used the same connection, the rollback would erase the log entry too. The metadata table would lie about what happened. By giving the log its own connection, the log's success is independent of the main transaction's success.

This is "separation of concerns" applied at the transaction level: data writes (staging/bronze/silver) belong in one transaction; observability writes (logging) belong in another. They have different durability requirements.

### 3.7 Vectorization — using the language of the runtime

This is the principle behind Step 6, the riskiest step.

The original save-potential frequency code did this:

```python
freq_df = expense.apply(
    lambda r: pd.Series(_row_freq(r), ...),
    axis=1,
)
```

`apply(..., axis=1)` runs a Python function once per row. For 5,000 rows that's 5,000 Python function calls. Each one does dict lookups, arithmetic, branching. It's correct but it's slow, and — more importantly here — *each iteration's logic is trapped inside a lambda*, so a reader has to mentally reconstruct what's happening.

The vectorized replacement:

```python
expense_freq = expense.merge(
    month_counts[[year_month_col, subcategory_col, "month_txn_count", "hist_avg_monthly_count"]],
    on=[year_month_col, subcategory_col],
    how="left",
)
mcount = expense_freq["month_txn_count"].astype(int).values
havg   = expense_freq["hist_avg_monthly_count"].values
valid_havg = (havg > 0) & ~np.isnan(havg)
with np.errstate(divide="ignore", invalid="ignore"):
    freq_ratio = np.where(valid_havg, mcount / havg, np.nan)
freq_excess = np.where(np.isnan(freq_ratio), 0.0, np.clip(freq_ratio - 1.0, 0.0, FREQ_EXCESS_CAP))
```

Two things happened:

1. **Speed.** A pandas `.merge()` is implemented in C and runs in microseconds. `np.where` and `np.clip` operate on whole arrays at once, again in C. We swapped 5,000 Python function calls for two merges and three array ops.
2. **Readability** (the bigger win). The math is now *visible at the source level*: "merge to get the counts, ratio them where valid, clip the excess." No hidden lambda, no dict construction, no per-row branching.

**BI analogy:** it's the difference between a Tableau "row-level calculation" with a CASE WHEN inside a loop vs. doing a JOIN against a pre-aggregated dimension table. The pre-aggregated approach is what databases are built for. Pandas is the same way: it's built for set operations, not row loops.

### 3.8 Declarative over imperative — `np.select` and the label hierarchy

Imperative: "do this, then check that, then if X do Y, otherwise…"
Declarative: "here is the list of conditions and outcomes; pick the first one that matches."

The notability label code was deeply nested imperative:

```python
df["notability_label"] = np.where(
    df["is_new_subcategory"], "New Category",
    np.where(np.isnan(z), "Insufficient History",
        np.where(z >= 3, "Extreme Outlier",
            np.where(z >= 2, "High Outlier",
                np.where(z >= 1, "Above Average",
                    np.where(df["is_new_subcategory_max"], "New Record", "Normal"))))))
```

To read this, you have to mentally walk down the nest. To change the priority order, you have to re-nest the structure. To add a new label, you have to insert another `np.where` at the right depth.

After Step 7:

```python
def _assign_notability_label(*, is_new_subcat, z, is_new_max):
    conditions = [is_new_subcat, np.isnan(z), z >= 3, z >= 2, z >= 1, is_new_max]
    choices    = ["New Category", "Insufficient History", "Extreme Outlier",
                  "High Outlier", "Above Average", "New Record"]
    return np.select(conditions, choices, default="Normal")
```

The priority order *is the order of the list*. `np.select` picks the first match. Adding a label is appending one entry to each list. The math doesn't change at all — `np.select` is implemented as a chain of `np.where` under the hood — but the *intent* is now visible without parsing nested parentheses.

This is what we mean by "code that explains itself."

### 3.9 Snapshot / golden-file testing

Steps 5 and 7 were structural refactors. Step 6 vectorized the math — and *math refactors are dangerous*, because the formulas must produce identical numbers.

The existing 14 transformer tests checked properties — "WANT scores higher than NEED" — but didn't lock down exact byte-for-byte values. So I added [`tests/test_save_potential_snapshot.py`](../tests/test_save_potential_snapshot.py).

The pattern:

1. Construct a fixed input DataFrame (10 rows: 3 subcategories, 3 months, mixed classifications, edge cases for capping).
2. Run the *current* code on it; capture every output column value.
3. Encode those values as the test's expected values.
4. Make the change.
5. Run the test. If any number drifted, the test fails immediately.

This is also called *golden-file testing*. It's the safety net that turns "I think the math is unchanged" into "I have a test that proves it."

**BI analogy:** it's like saving a known-good version of a Tableau dashboard's underlying data extract, then after a refactor checking that the dashboard still produces identical totals. If the totals drift by a cent, you investigate.

### 3.10 Test isolation via transaction rollback

The Step 3b integration tests touched the live database. We did *not* want them committing test rows into your real silver or bronze. The pattern was:

```python
@pytest.fixture
def conn(db):
    c = db.connect()
    try:
        yield c                  # test runs here, possibly inserting/updating rows
    finally:
        c.rollback()             # everything is discarded
        c.close()
```

Postgres is "transactional by default" — every statement is part of an open transaction until you explicitly commit. If you don't commit and you rollback, the database forgets everything you did. That gives us a free, completely safe sandbox: the test can `INSERT INTO silver.transactions`, `SELECT` the inserted row, assert the helper worked correctly, and then the rollback erases it.

**Verified:** after running the integration tests, `silver.transactions` was still at exactly 5,745 rows — same as before the test run.

This pattern is much better than the alternative (a separate test database that needs setup/teardown). It runs against the *real* schema, with the *real* constraints, in milliseconds.

### 3.11 Mocking — testing without dependencies

The 14 unit tests for `BaseLoader` and the 17 for `GoldRefresher` use `unittest.mock.MagicMock` to fake the database connection. Why?

Because most of what those tests need to verify is **the SQL string we built**, **the values we passed to the cursor**, and **the order in which we called things**. None of that requires a real database to verify — it requires a fake "cursor" that records what was called on it.

```python
def test_log_pipeline_run_inserts_expected_row(loader):
    fake_conn = MagicMock()
    fake_cursor = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    loader.db.connect.return_value.__enter__.return_value = fake_conn

    loader._log_pipeline_run()

    sql, values = fake_cursor.execute.call_args.args
    assert sql.startswith("INSERT INTO metadata.pipeline_runs")
    assert "something exploded" in values
```

This test runs in milliseconds, has no external dependencies, can run in CI without a database, and *still* catches: a typo in the SQL, a missing column in the value list, a wrong ordering. Mocks let you test logic in isolation.

The general rule:

- **Unit test** with mocks when you want to test *what* you do (SQL strings, control flow, call order).
- **Integration test** with rollback when you want to test *that the SQL actually works* (correct columns, NOT NULL constraints satisfied, types compatible).

Both are valuable. The two suites cover each other's blind spots.

---

## Part 4 — File-by-File Walkthrough

This section explains *what each file in `src/loaders/` and `src/transformers/` is responsible for now*, after the refactor.

### `src/loaders/base_loader.py` — 465 lines

The shared engine for every staging/bronze/silver loader. Contains:

- **Constructor (`__init__`)** sets up `self.db`, `self.transformer`, `self.batch_id`, `self.run_stats`. The `source_file_name` is the only required argument.
- **`load()`** — the template method. The skeleton of every load run.
- **`_print_header()`** — banners for console output.
- **`_extract` and `_load_silver`** — abstract methods that raise `NotImplementedError`. Subclasses must override.
- **`_extract_from_file(path)`** — reads xlsx/csv into a DataFrame. Shared because both subclasses can read files.
- **`_transform(df)`** — runs the `ExpenseTransformer`. Same for both subclasses.
- **`_post_transform(df)`** — *hook* for subclasses to add a step (incremental uses this for the account filter; initial doesn't).
- **`_load_staging(df, conn)`** — TRUNCATE + INSERT into `staging.raw_transactions`.
- **`_load_bronze(df, conn)`** — append-only INSERT into `bronze.transactions_raw`.
- **`_silver_columns()`** — the whitelist of silver columns (ordered).
- **`_prepare_silver_df(df)`** — applies renames + adds metadata columns; produces a silver-ready DataFrame.
- **`_bulk_insert(df, schema, table, conn=None)`** — batch INSERT helper using `psycopg2.extras.execute_batch`.
- **`_update_category_mapping(conn)`** — backfills `silver.category` and `silver.classification` from the lookup table.
- **`_refresh_gold_notability(...)` and `_refresh_gold_save_potential(...)`** — non-fatal wrappers around the gold refreshers.
- **`_log_pipeline_run()`** — writes to `metadata.pipeline_runs` on a fresh connection.
- **`_display_summary(...)`** — banner for end-of-run output.

### `src/loaders/incremental_load.py` — 258 lines

Subclass of `BaseLoader`. Provides:

- **`__init__`** — stores incremental-specific options (source, account_filter, from_date, to_date), then calls `super().__init__(...)`.
- **`_extract`** — dispatches to `_extract_from_api` or the inherited `_extract_from_file(self.file_path)`.
- **`_extract_from_api`** — pulls from BudgetBakers using `MAX(silver.transaction_date) + 1d` as the watermark.
- **`_get_last_silver_date`** — reads the watermark.
- **`_post_transform(df)`** — applies the account filter.
- **`_load_silver(df, conn)`** — queries existing hashes, filters out duplicates, inserts only new rows, tracks `_new_expense_hashes` for downstream gold refresh.
- **`_refresh_gold_notability` / `_refresh_gold_save_potential`** — pass `hashes=self._new_expense_hashes` to the inherited helper (targeted refresh, not full).

That's it. Everything else is inherited.

### `src/loaders/initial_load.py` — 140 lines

Subclass of `BaseLoader`. Provides:

- **`__init__`** — stores `self.file_path`, calls `super().__init__(...)`.
- **`_extract`** — calls inherited `_extract_from_file(self.file_path)`.
- **`_load_silver(df, conn)`** — exports duplicate-hash records to CSV, TRUNCATEs silver, inserts everything (no dedupe).
- **`_export_duplicate_hashes(df)`** — the only initial-only helper. Writes a review CSV to `data/inspection/`.
- **`_refresh_gold_*`** — pass `full=True` (full rebuild after a full silver reload).

### `src/loaders/gold_refresher.py` — 237 lines

The shared engine for gold-table refreshes. Mirror of `BaseLoader` for the silver→gold direction. Contains:

- **Class attributes** (the "data shape" definition): `gold_table`, `print_label`, `silver_extra_columns`, `gold_columns`, `json_columns`. Subclasses override these.
- **`compute(silver_df, *, window_days)`** — abstract; subclass calls the metric-specific compute function.
- **`refresh(db, *, hashes, full, window_days)`** — the template method. Routes between full and incremental, fetches the right slice, calls compute, filters, upserts.
- **`_fetch_target_info(conn, hashes)`** — for an incremental refresh, looks up the affected subcategories and date range.
- **`_fetch_silver_expenses(conn, ...)`** — SELECT from silver, parameterized by `silver_extra_columns`.
- **`_upsert_to_gold(conn, df)`** — INSERT … ON CONFLICT DO UPDATE, parameterized by `gold_columns` and `json_columns`.

### `src/loaders/gold_notable_loader.py` — 105 lines

Subclass of `GoldRefresher`. Just data declarations and one compute call:

```python
class NotabilityRefresher(GoldRefresher):
    gold_table = "gold.transaction_notability"
    print_label = "transaction_notability"
    silver_extra_columns = []
    json_columns = ["extra_stats"]
    gold_columns = ["transaction_hash", "transaction_date", ..., "extra_stats"]

    def compute(self, silver_df, *, window_days):
        return compute_notability(silver_df, window_days=window_days, ...)
```

Plus `refresh_notability_for_hashes(...)` — the public function that existing callers (`BaseLoader._refresh_gold_notability`, `run_pipeline.py --refresh-gold notability`) use. It just instantiates the class and calls `.refresh(...)`.

### `src/loaders/gold_save_potential_loader.py` — 78 lines

Same shape as the notable loader. Notable differences:

- `silver_extra_columns = ["classification", "year_month"]` because save-potential needs them.
- `json_columns = []` — no extra_stats here.
- `compute()` calls `compute_save_potential(...)`.

### `src/transformers/_gold_common.py` — 103 lines

Shared module for transformer helpers used by more than one gold metric. Today: just `compute_rolling_stats`. The leading underscore in the module name (`_gold_common`) signals "private to the transformers package."

The two-pointer scan inside `compute_rolling_stats` is the most algorithmically subtle code in the whole pipeline. The docstring explains *why it stays as a hand-rolled loop and not a `groupby().rolling()`*: deterministic ordering by `(date, hash)` and the "strictly before" semantics aren't expressible cleanly in pandas' rolling primitive.

### `src/transformers/notable_transactions_transformer.py` — 215 lines

Computes notability scores. Uses `compute_rolling_stats` from `_gold_common`. Contains:

- `compute_notability(df, **kwargs)` — public entry point.
- `_derive_labels_and_score(df, ...)` — z-score, flags, composite score, label, reason.
- `_assign_notability_label(...)` — the new `np.select` helper from Step 7.
- `_compute_extra_features(df)` — extensibility hook (does nothing today).

### `src/transformers/save_potential_transformer.py` — 256 lines

Computes save-potential scores. Uses `compute_rolling_stats` for the amount z-score; computes monthly frequency via the new vectorized merge approach. Contains:

- `compute_save_potential(df, **kwargs)` — public entry point.
- `_avoidability(val)` — maps WANT/NEED/MUST to a numeric weight.
- `_assign_save_potential_label(score)` — the `np.select` helper for the 4-tier label.
- `_reason(r)` — the human-readable tooltip text for `gold.save_potential_reason`.

---

## Part 5 — What We Did *Not* Change, and Why

Equally important to know.

- **No SQL semantics changed.** Every INSERT, UPDATE, ON CONFLICT, and SELECT produces the same rows it did before. The integration tests verified this; the snapshot test verified the gold formulas; the live-DB smoke verified end-to-end on real silver data.
- **No formula coefficients changed.** Avoidability is still 1.0/0.4/0.05. Notability composite is still `max(z, 0) + 4*new_subcategory + 2*new_record`. Save-potential is still `3*avoidability + 2*freq_excess + 1*amt_excess`.
- **No schema change.** No table got a new column, no column got a new type, no constraint changed. All schema concerns are deferred to Phase A (Type 2 SCD, Alembic, etc.) per the strategic roadmap.
- **No new external dependencies.** Same `requirements.txt`. Same Python version.
- **No dbt yet.** dbt is Phase C in your roadmap. The transformers stay in Python so the day we move silver→gold to dbt, the move is straightforward — clean pandas with shared helpers translates almost line-for-line into dbt models with `{{ ref(...) }}` and `WHERE`.
- **The two-pointer scan in `compute_rolling_stats` stays.** It's the single most subtle piece of code in the pipeline, and the simpler-looking `groupby().rolling()` alternative doesn't honour the deterministic ordering we need. The docstring now explains why.
- **`initial_load.py`'s transaction model was unified with incremental** (single connection + rollback), changing its old per-layer-connection pattern. You signed off on this because `initial_load` is effectively retired.
- **The latent "if input has both `note` AND `description`, bronze crashes" brittleness still exists.** It existed before the refactor. It would only fire if `ExpenseTransformer` ever produced both columns simultaneously, which it doesn't. Worth a future hardening fix, not on the critical path.

---

## Part 6 — How to Think About the Next Refactor

A few principles to carry forward:

1. **Have a snapshot test before you touch the math.** Step 6 was the one place the math could shift, and the snapshot was what made it safe. If you're ever refactoring a formula again — for performance, readability, or to port to dbt — capture the current output first.

2. **Ship in slices.** Steps 3a / 3b were one big refactor split into two commits. If 3b had introduced a bug, you'd have reverted 3b and kept the 3a wins. Independent commits are cheaper to reason about than one big "refactor" PR.

3. **Tests are documentation.** `tests/test_save_potential_snapshot.py` reads like a specification: "given this input, here is exactly what the math should output." If a future refactor breaks a number, the test tells you precisely which one and to what.

4. **The right time to abstract is when the third copy appears, not the second.** Two similar copies might be coincidence. Three is a pattern. The refactor here was triggered because the two loaders had drifted *enough* over time to make changes risky — that's the "cost of duplication exceeded the cost of abstraction" moment.

5. **Inheritance is a long-term marriage.** Don't subclass unless the relationship is durable. `IncrementalDataLoader IS-A BaseLoader` will be true forever. `IncrementalDataLoader IS-A SourceConfigurable` would be a weak premise that might not survive Phase B.

6. **Read the code in the order a new reader would.** When a file goes from 510 to 140 lines, that's not just deletion — it's *reordering of attention*. The 140 lines are the parts of this loader that are interesting and unique. The 370 deleted lines were the ones that distracted from understanding.

---

## Closing

The math is unchanged. The behaviour is unchanged. What changed is *where each piece of knowledge lives* — and therefore *what you have to read* to understand any single piece of the pipeline.

You can now ask:

- *"How is the pipeline-run logged?"* — read one method in `BaseLoader`.
- *"How are duplicate transactions handled in incremental mode?"* — read `IncrementalDataLoader._load_silver`.
- *"How does notability compute its z-score?"* — read `compute_rolling_stats` and `_derive_labels_and_score`.

Each question now has *one* answer in *one* place. That's the entire point of the refactor. Everything in Part 3 is a tool for getting closer to that — DRY, the template method, encapsulation, vectorization. They're not academic. They are the reasons your codebase is now half its previous size and twice as testable.

Read this once. Come back to it next time you reach for a copy-paste. The right next move will be clearer.
