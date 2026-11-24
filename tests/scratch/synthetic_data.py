"""
Deterministic synthetic batches in the Wallet CSV-export shape
(date, note, type, payee, amount, labels, account, category, currency, payment) —
the shape BaseLoader._extract_from_file + ExpenseTransformer step 1 expect.

No real data: fictional notes/payees, Wallet subcategory names that the
public blueprint already seeds into silver.category_mapping.

Batches (see spec 001, Design §7):
  A  initial load: Jan–Feb 2025, 4 mapped subcategories + 1 unmapped,
     one INCOME, one refund (INCOME with an expense subcategory),
     one transfer (expected to be dropped by the transformer).
  B  incremental: new Groceries rows in Feb 2025 (month+subcategory already in A
     → save-potential month_txn_count drift), one LATE-dated Bar, cafe row
     before A's later Bar, cafe rows (notability baseline drift), one Fuel row.
  C  in-batch collision: two identical rows (same date/amount/subcategory/empty note).
"""

from __future__ import annotations

import random
from pathlib import Path

import pandas as pd

COLUMNS = ["date", "note", "type", "payee", "amount", "labels",
           "account", "category", "currency", "payment"]

ACCOUNT = "Synthetic Account EUR"
UNMAPPED_SUBCATEGORY = "Zz Synthetic Unmapped"
TRANSFER_NOTE = "synth-transfer-must-drop"
MAPPED_SUBCATEGORIES = ["Groceries", "Bar, cafe", "Energy, utilities", "Fuel"]

SEED = 20261119


def _row(date, category, amount, *, type_="Expenses", note="", payee="Synthetic Payee"):
    return {
        "date": date,
        "note": note,
        "type": type_,
        "payee": payee,
        "amount": f"{amount:.2f}",
        "labels": "",
        "account": ACCOUNT,
        "category": category,
        "currency": "EUR",
        "payment": "DEBIT_CARD",
    }


def _expenses(rng, prefix, category, dates, lo, hi):
    return [
        _row(d, category, -round(rng.uniform(lo, hi), 2), note=f"{prefix}-{category[:4]}-{i:02d}")
        for i, d in enumerate(dates)
    ]


def batch_a(rng: random.Random) -> list[dict]:
    rows = []
    rows += _expenses(rng, "a", "Groceries",
                      ["2025-01-03", "2025-01-09", "2025-01-15", "2025-01-21",
                       "2025-01-27", "2025-02-02", "2025-02-08", "2025-02-14"], 20, 60)
    rows += _expenses(rng, "a", "Bar, cafe",
                      ["2025-01-05", "2025-01-12", "2025-01-20", "2025-01-28",
                       "2025-02-04", "2025-02-11", "2025-02-18"], 3, 12)
    rows += _expenses(rng, "a", "Energy, utilities", ["2025-01-10", "2025-02-10"], 80, 120)
    rows += _expenses(rng, "a", "Fuel", ["2025-01-07", "2025-01-25", "2025-02-15"], 40, 70)
    rows.append(_row("2025-01-18", UNMAPPED_SUBCATEGORY, -15.00, note="a-unmapped"))
    rows.append(_row("2025-01-31", "Wage, invoices", 2500.00, type_="Income", note="a-salary"))
    rows.append(_row("2025-02-03", "Groceries", 12.50, type_="Income", note="a-refund"))
    rows.append(_row("2025-02-05", "Transfer", -100.00, type_="Transfer", note=TRANSFER_NOTE))
    return rows


def batch_b(rng: random.Random) -> list[dict]:
    rows = []
    rows += _expenses(rng, "b", "Groceries", ["2025-02-20", "2025-02-24", "2025-02-26"], 20, 60)
    # Late-dated: earlier than A's later Bar, cafe rows (01-12 … 02-18), large amount.
    rows.append(_row("2025-01-08", "Bar, cafe", -55.00, note="b-late-bar"))
    rows += _expenses(rng, "b", "Fuel", ["2025-02-27"], 40, 70)
    return rows


def batch_c() -> list[dict]:
    dup = _row("2025-03-10", "Groceries", -9.99, note="", payee="Synthetic Payee")
    return [dict(dup), dict(dup)]


def write_batches(out_dir: Path) -> dict[str, Path]:
    """Write A, B, C as CSV into out_dir (must be outside the repo). Returns paths."""
    rng = random.Random(SEED)
    batches = {"A": batch_a(rng), "B": batch_b(rng), "C": batch_c()}
    paths = {}
    for name, rows in batches.items():
        p = out_dir / f"batch_{name}.csv"
        pd.DataFrame(rows, columns=COLUMNS).to_csv(p, index=False)
        paths[name] = p
    return paths


def row_count(name: str) -> int:
    rng = random.Random(SEED)
    a, b = batch_a(rng), batch_b(rng)
    return {"A": len(a), "B": len(b), "C": 2}[name]
