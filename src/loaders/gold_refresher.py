"""
Shared base class for gold-table refreshers.

`gold_notable_loader` and `gold_save_potential_loader` both follow the
same pattern:

  1. If `full=True`, fetch all EXPENSE rows from silver.
     Otherwise, look up the target hashes' subcategories and date range,
     and fetch the relevant slice plus a `window_days` history baseline.
  2. Run a metric-specific `compute_*` function on the slice.
  3. If a target hash set was supplied, filter the computed rows to it.
  4. UPSERT the computed rows into the metric's gold table.

The differences are all data-shape:
  - which compute function to call (and its extra kwargs)
  - which silver columns to SELECT (e.g. save-potential needs
    `classification` + `year_month`)
  - which gold columns to UPSERT into (and the table name)
  - whether any column needs JSON serialization (notable has `extra_stats`)
  - print-label for log output

`GoldRefresher` captures the shared orchestration. Subclasses fill in
the differences via class attributes and a `compute()` override.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Optional, Set

import pandas as pd
from psycopg2.extras import execute_batch


# Columns every metric needs from silver (in addition to silver_extra_columns).
_BASE_SILVER_COLUMNS = [
    "transaction_hash",
    "transaction_date",
    "transaction_type",
    "amount_abs_eur",
    "subcategory",
]


class GoldRefresher:
    """Refresh a gold table from silver.transactions.

    Subclasses set the class-level attributes and override `compute()`.
    Then `refresh(db, hashes, full, window_days)` does the rest.
    """

    # ---- Subclass overrides ---------------------------------------------- #
    gold_table: str = ""
    """Fully-qualified target gold table, e.g. `gold.transaction_notability`."""

    print_label: str = ""
    """Short metric name used in console output, e.g. `transaction_notability`."""

    silver_extra_columns: list[str] = []
    """Extra columns the metric's compute function needs beyond _BASE_SILVER_COLUMNS."""

    gold_columns: list[str] = []
    """Whitelist of columns to UPSERT into the gold table (in order)."""

    json_columns: list[str] = []
    """Columns whose dict values must be JSON-serialized before insert."""

    def compute(self, silver_df: pd.DataFrame, *, window_days: int) -> pd.DataFrame:
        """Run the metric-specific compute_* function and return the scored DataFrame."""
        raise NotImplementedError("Subclasses must implement compute()")

    # ---- Public entrypoint ----------------------------------------------- #
    def refresh(
        self,
        db,
        *,
        hashes: Optional[Set[str]] = None,
        full: bool = False,
        window_days: int = 365,
    ) -> int:
        """Refresh `self.gold_table` for the given hashes (or all if `full=True`).

        Returns the number of rows upserted.
        """
        print(f"\n[GOLD] Refreshing {self.print_label}...")
        with db.connect() as conn:
            if full:
                target_hashes = None
                silver_df = self._fetch_silver_expenses(conn)
            else:
                if not hashes:
                    print(f"  [GOLD] No hashes to refresh ({self.print_label}), skipping.")
                    return 0
                target_hashes = {str(h) for h in hashes}
                target_info = self._fetch_target_info(conn, target_hashes)
                if not target_info:
                    print("  [GOLD] No target rows found in silver, skipping.")
                    return 0
                subcats, min_date, max_date = target_info
                silver_df = self._fetch_silver_expenses(
                    conn,
                    subcategories=subcats,
                    date_from=min_date,
                    date_to=max_date,
                    window_days=window_days,
                )

            if len(silver_df) == 0:
                print(f"  [GOLD] No silver expense data to process ({self.print_label}).")
                return 0

            computed = self.compute(silver_df, window_days=window_days)

            if target_hashes is not None:
                computed = computed[
                    computed["transaction_hash"].astype(str).isin(target_hashes)
                ]
            if len(computed) == 0:
                print(f"  [GOLD] No rows to upsert ({self.print_label}) after filtering.")
                return 0

            return self._upsert_to_gold(conn, computed)

    # ---- Shared SQL helpers ---------------------------------------------- #
    @staticmethod
    def _fetch_target_info(conn, hashes: Set[str]):
        """Return (subcategories, min_date, max_date) for the given hashes in silver,
        or None if nothing matches.

        Returning None (rather than empty sets) is the signal to refresh()
        that there is nothing to do. We treat "no rows" and "rows but no
        dates" the same: skip the refresh entirely.
        """
        cursor = conn.cursor()
        placeholders = ", ".join(["%s"] * len(hashes))
        cursor.execute(
            f"""
            SELECT DISTINCT subcategory, transaction_date
            FROM silver.transactions
            WHERE transaction_hash IN ({placeholders})
              AND transaction_type = 'EXPENSE'
            """,
            list(hashes),
        )
        rows = cursor.fetchall()
        if not rows:
            return None
        subcats = {r[0] for r in rows if r[0]}
        dates = [r[1] for r in rows if r[1]]
        if not dates:
            return None
        return (subcats, min(dates), max(dates))

    def _fetch_silver_expenses(
        self,
        conn,
        *,
        subcategories: Optional[set] = None,
        date_from=None,
        date_to=None,
        window_days: int = 365,
    ) -> pd.DataFrame:
        """SELECT EXPENSE rows from silver.transactions.

        With no filters: full table.
        With subcategories + date range: that slice, expanded back by
        `window_days` so the metric has its history baseline.
        """
        select_columns = _BASE_SILVER_COLUMNS + list(self.silver_extra_columns)
        select_clause = ", ".join(select_columns)

        conditions = ["transaction_type = 'EXPENSE'"]
        params: list = []

        if subcategories:
            placeholders = ", ".join(["%s"] * len(subcategories))
            conditions.append(f"subcategory IN ({placeholders})")
            params.extend(subcategories)

        if date_from is not None and date_to is not None:
            d_from = _coerce_to_date(date_from)
            d_to = _coerce_to_date(date_to)
            fetch_from = d_from - timedelta(days=window_days)
            conditions.append("transaction_date >= %s AND transaction_date <= %s")
            params.extend([fetch_from, d_to])

        query = (
            f"SELECT {select_clause} FROM silver.transactions "
            f"WHERE {' AND '.join(conditions)}"
        )
        cursor = conn.cursor()
        cursor.execute(query, params)
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=cols)

    def _upsert_to_gold(self, conn, df: pd.DataFrame) -> int:
        """UPSERT rows into self.gold_table on transaction_hash. Commits and
        returns row count.
        """
        df = df.copy()
        df["computed_at"] = datetime.now()

        # JSON-serialize any dict columns the metric flagged.
        for c in self.json_columns:
            if c in df.columns:
                df[c] = df[c].apply(
                    lambda x: json.dumps(x) if isinstance(x, dict) else "{}"
                )

        # Project onto the gold column whitelist (fill missing with None).
        for c in self.gold_columns:
            if c not in df.columns:
                df[c] = None
        df = df[self.gold_columns]
        df = df.where(pd.notna(df), None)

        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in self.gold_columns
            if c != "transaction_hash"
        )
        placeholders = ", ".join(["%s"] * len(self.gold_columns))
        cols_str = ", ".join([f'"{c}"' for c in self.gold_columns])
        query = (
            f"INSERT INTO {self.gold_table} ({cols_str}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT (transaction_hash) DO UPDATE SET {update_set}"
        )
        values = df.values.tolist()
        cursor = conn.cursor()
        execute_batch(cursor, query, values, page_size=1000)
        conn.commit()
        return len(values)


def _coerce_to_date(value):
    """Accept a date, datetime, or YYYY-MM-DD string; return a date."""
    if isinstance(value, str):
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    return value
