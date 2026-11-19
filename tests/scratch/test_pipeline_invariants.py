"""
Pipeline invariants on the scratch database (spec 001, Tier 2).

A module-scoped fixture runs the real loaders once:
    InitialDataLoader(A) → IncrementalDataLoader(file, B) → same B again
and records row counts around each run. Tests I1–I8 assert invariants on the
resulting state; I9 loads the collision batch C last.

These tests lock in CURRENT behavior. Known defects are marked
xfail(strict=True) with a one-line diagnosis, so fixing a defect turns the
check red (XPASS) until the marker is removed — the fix must be deliberate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from synthetic_data import (
    MAPPED_SUBCATEGORIES,
    TRANSFER_NOTE,
    UNMAPPED_SUBCATEGORY,
    row_count,
    write_batches,
)

pytestmark = pytest.mark.scratch

GOLD_TABLES = ("gold.transaction_notability", "gold.transaction_save_potential")

COUNT_SQL = {
    "silver": "SELECT COUNT(*) FROM silver.transactions",
    "silver_expense": "SELECT COUNT(*) FROM silver.transactions WHERE transaction_type = 'EXPENSE'",
    "bronze": "SELECT COUNT(*) FROM bronze.transactions_raw",
    "gold_notability": "SELECT COUNT(*) FROM gold.transaction_notability",
    "gold_save_potential": "SELECT COUNT(*) FROM gold.transaction_save_potential",
}


@dataclass
class LoadRun:
    label: str
    batch_id: str
    before: dict
    after: dict
    error: Exception | None = None


@dataclass
class PipelineState:
    runs: dict = field(default_factory=dict)
    paths: dict = field(default_factory=dict)


def _query_all(db, sql, params=None):
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def _scalar(db, sql, params=None):
    return _query_all(db, sql, params)[0][0]


def _counts(db) -> dict:
    return {k: _scalar(db, q) for k, q in COUNT_SQL.items()}


def _run_load(db, label, loader) -> LoadRun:
    before = _counts(db)
    error = None
    try:
        loader.load()
    except Exception as e:  # recorded; asserted by the tests, not raised here
        error = e
    return LoadRun(label, str(loader.batch_id), before, _counts(db), error)


@pytest.fixture(scope="module")
def state(db, tmp_path_factory) -> PipelineState:
    from loaders.incremental_load import IncrementalDataLoader
    from loaders.initial_load import InitialDataLoader

    out = tmp_path_factory.mktemp("pfp_fixture")  # system temp, outside the repo
    paths = write_batches(out)
    st = PipelineState(paths=paths)
    st.runs["A"] = _run_load(db, "A", InitialDataLoader(file_path=str(paths["A"])))
    for label in ("B1", "B2"):
        st.runs[label] = _run_load(
            db,
            label,
            IncrementalDataLoader(source="file", file_path=str(paths["B"]), account_filter="all"),
        )
    return st


# --------------------------------------------------------------------------- #
# Sanity: the fixture loads themselves succeeded
# --------------------------------------------------------------------------- #
def test_fixture_loads_succeeded(state):
    for label in ("A", "B1"):
        run = state.runs[label]
        assert run.error is None, f"load {label} raised: {run.error!r}"
    assert state.runs["A"].after["silver"] > 0
    assert state.runs["B1"].after["silver"] > state.runs["A"].after["silver"]


# --------------------------------------------------------------------------- #
# I1 run-twice idempotency
# --------------------------------------------------------------------------- #
def test_i1_rerun_is_idempotent(state):
    b2 = state.runs["B2"]
    assert b2.error is None, f"identical rerun of B raised: {b2.error!r}"
    for key in ("silver", "silver_expense", "gold_notability", "gold_save_potential"):
        assert b2.after[key] == b2.before[key], (
            f"{key} changed on identical rerun: {b2.before[key]} -> {b2.after[key]}"
        )
    # Bronze is an append-only archive: it grows by exactly B's row count.
    assert b2.after["bronze"] - b2.before["bronze"] == row_count("B")


# --------------------------------------------------------------------------- #
# I2 uniqueness of the dedup key
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("table", ["silver.transactions", *GOLD_TABLES])
def test_i2_hash_unique(db, state, table):
    total, distinct = _query_all(
        db, f"SELECT COUNT(*), COUNT(DISTINCT transaction_hash) FROM {table}"
    )[0]
    assert total == distinct, f"{table}: {total} rows but {distinct} distinct hashes"


# --------------------------------------------------------------------------- #
# I3 gold completeness / no orphans
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("table", GOLD_TABLES)
def test_i3_gold_matches_silver_expense(db, state, table):
    orphans = _scalar(
        db,
        f"""
        SELECT COUNT(*) FROM {table} g
        WHERE NOT EXISTS (
            SELECT 1 FROM silver.transactions s
            WHERE s.transaction_hash = g.transaction_hash
              AND s.transaction_type = 'EXPENSE')
        """,
    )
    assert orphans == 0, f"{table}: {orphans} gold rows not backed by a silver EXPENSE row"
    gold_n = _scalar(db, f"SELECT COUNT(*) FROM {table}")
    expense_n = _scalar(db, COUNT_SQL["silver_expense"])
    assert gold_n == expense_n, f"{table}: {gold_n} rows vs {expense_n} silver EXPENSE rows"


# --------------------------------------------------------------------------- #
# I4 category mapping
# --------------------------------------------------------------------------- #
def test_i4_mapped_subcategories_have_category(db, state):
    missing = _scalar(
        db,
        """
        SELECT COUNT(*) FROM silver.transactions t
        JOIN silver.category_mapping m ON m.subcategory = t.subcategory
        WHERE t.category IS NULL
        """,
    )
    assert missing == 0
    for sub in MAPPED_SUBCATEGORIES:
        assert _scalar(
            db, "SELECT COUNT(*) FROM silver.transactions WHERE subcategory = %s", (sub,)
        ) > 0, f"fixture subcategory {sub!r} absent from silver"


def test_i4_unmapped_subcategory_stays_null(db, state):
    rows = _query_all(
        db,
        "SELECT category FROM silver.transactions WHERE subcategory = %s",
        (UNMAPPED_SUBCATEGORY,),
    )
    assert rows, "unmapped fixture row missing from silver"
    assert all(r[0] is None for r in rows)


# --------------------------------------------------------------------------- #
# I5 income semantics
# --------------------------------------------------------------------------- #
def test_i5_income_rows_typed_and_unclassified(db, state):
    rows = _query_all(
        db,
        "SELECT subcategory, classification, income_type FROM silver.transactions "
        "WHERE transaction_type = 'INCOME' ORDER BY subcategory",
    )
    assert len(rows) == 2, f"expected salary + refund, got {rows}"
    for sub, cls, itype in rows:
        assert cls is None, f"INCOME {sub!r} has classification {cls!r}"
        assert itype is not None, f"INCOME {sub!r} has NULL income_type"
    assert {(r[0], r[2]) for r in rows} == {("Wage, invoices", "REAL"), ("Groceries", "REFUND")}


# --------------------------------------------------------------------------- #
# I6 transfers are dropped (documents current behavior)
# --------------------------------------------------------------------------- #
def test_i6_transfer_dropped(db, state):
    for table in ("bronze.transactions_raw", "silver.transactions"):
        n = _scalar(db, f"SELECT COUNT(*) FROM {table} WHERE description = %s", (TRANSFER_NOTE,))
        assert n == 0, f"transfer row reached {table}"


# --------------------------------------------------------------------------- #
# I7 reconciliation: pipeline_runs vs actual rows (SUCCESS runs)
# --------------------------------------------------------------------------- #
def test_i7_pipeline_runs_reconcile_success(db, state):
    for label in ("A", "B1", "B2"):
        run = state.runs[label]
        rows = _query_all(
            db,
            "SELECT status, rows_loaded_bronze, rows_loaded_silver "
            "FROM metadata.pipeline_runs WHERE run_id = %s",
            (run.batch_id,),
        )
        assert len(rows) == 1, f"{label}: expected 1 pipeline_runs row, got {rows}"
        status, logged_bronze, logged_silver = rows[0]
        assert status == "SUCCESS"
        actual_bronze = _scalar(
            db,
            "SELECT COUNT(*) FROM bronze.transactions_raw WHERE ingestion_batch_id = %s",
            (run.batch_id,),
        )
        assert logged_bronze == actual_bronze, f"{label}: bronze logged {logged_bronze} vs {actual_bronze}"
        silver_delta = run.after["silver"] - run.before["silver"]
        assert logged_silver == silver_delta, f"{label}: silver logged {logged_silver} vs delta {silver_delta}"


# --------------------------------------------------------------------------- #
# I8 gold refresh equivalence (targeted incremental vs full rebuild)
# --------------------------------------------------------------------------- #
def _norm(v):
    """NaN never equals itself; normalize it so unchanged NaN cells don't show as diffs."""
    try:
        if v != v:  # Decimal('NaN') / float('nan')
            return "NaN"
    except Exception:
        pass
    return v


def _gold_snapshot(db, table) -> dict:
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} ORDER BY transaction_hash")
        cols = [d[0] for d in cur.description]
        return {
            row[cols.index("transaction_hash")]: {
                c: _norm(v) for c, v in zip(cols, row) if c != "computed_at"
            }
            for row in cur.fetchall()
        }
    finally:
        conn.close()


@pytest.mark.xfail(strict=True, reason="audit §3.1: targeted refresh ≠ full rebuild")
def test_i8_gold_refresh_equivalence(db, state):
    from loaders.gold_notable_loader import NotabilityRefresher
    from loaders.gold_save_potential_loader import SavePotentialRefresher

    before = {t: _gold_snapshot(db, t) for t in GOLD_TABLES}
    NotabilityRefresher().refresh(db, full=True)
    SavePotentialRefresher().refresh(db, full=True)
    after = {t: _gold_snapshot(db, t) for t in GOLD_TABLES}

    diffs = []
    for t in GOLD_TABLES:
        for h in sorted(set(before[t]) | set(after[t])):
            b, a = before[t].get(h), after[t].get(h)
            if b != a:
                cols = sorted(
                    c for c in (set(b or {}) | set(a or {}))
                    if (b or {}).get(c) != (a or {}).get(c)
                )
                diffs.append(
                    f"{t} {h[:12]} "
                    + ", ".join(f"{c}: {(b or {}).get(c)!r} -> {(a or {}).get(c)!r}" for c in cols)
                )
    if diffs:
        print(f"\nI8: {len(diffs)} gold rows differ between incremental and full refresh:")
        for d in diffs:
            print("  " + d)
    assert not diffs, f"{len(diffs)} gold rows differ (see printed diff)"


# --------------------------------------------------------------------------- #
# I10 gold numerics use NULL, not NaN (found by spec 001, fixed in spec 002)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("table", GOLD_TABLES)
def test_i10_gold_has_no_numeric_nan(db, state, table):
    numeric_cols = [
        r[0]
        for r in _query_all(
            db,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND data_type = 'numeric'",
            tuple(table.split(".")),
        )
    ]
    counts = {
        c: _scalar(db, f"SELECT COUNT(*) FROM {table} WHERE {c} = 'NaN'::numeric")
        for c in numeric_cols
    }
    nan_cols = {c: n for c, n in counts.items() if n}
    print(f"\nI10 {table}: NaN cells per column: {nan_cols}")
    assert not nan_cols


# --------------------------------------------------------------------------- #
# I9 in-batch hash collision (characterization of current behavior)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def collision_run(db, state) -> LoadRun:
    from loaders.incremental_load import IncrementalDataLoader

    loader = IncrementalDataLoader(
        source="file", file_path=str(state.paths["C"]), account_filter="all"
    )
    run = _run_load(db, "C", loader)
    state.runs["C"] = run
    return run


def test_i9_in_batch_collision_rolls_back(db, collision_run):
    run = collision_run
    assert run.error is not None, "loading two identical rows in one batch did not raise"
    for key in ("silver", "bronze"):
        assert run.after[key] == run.before[key], f"{key} changed despite rollback"
    rows = _query_all(
        db, "SELECT status FROM metadata.pipeline_runs WHERE run_id = %s", (run.batch_id,)
    )
    assert rows == [("FAILED",)], f"expected one FAILED pipeline_runs row, got {rows}"


@pytest.mark.xfail(
    strict=True,
    reason="pipeline_runs logs pre-rollback row counts on FAILED runs (rows_loaded_* not reset)",
)
def test_i7_pipeline_runs_reconcile_failed(db, collision_run):
    run = collision_run
    logged_bronze, logged_silver = _query_all(
        db,
        "SELECT rows_loaded_bronze, rows_loaded_silver FROM metadata.pipeline_runs WHERE run_id = %s",
        (run.batch_id,),
    )[0]
    actual_bronze = _scalar(
        db,
        "SELECT COUNT(*) FROM bronze.transactions_raw WHERE ingestion_batch_id = %s",
        (run.batch_id,),
    )
    silver_delta = run.after["silver"] - run.before["silver"]
    print(f"\nFAILED run logged bronze={logged_bronze} silver={logged_silver}; "
          f"actual bronze={actual_bronze} silver_delta={silver_delta}")
    assert (logged_bronze, logged_silver) == (actual_bronze, silver_delta)
