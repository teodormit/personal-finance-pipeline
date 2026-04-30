"""
Standalone full refresh for gold.transaction_save_potential.

Usage:
  python scripts/refresh_gold_save_potential.py --full
  python scripts/refresh_gold_save_potential.py --full --window-days 365
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))

from loaders.gold_save_potential_loader import refresh_save_potential_for_hashes
from utils.db_connector import get_db_connector


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Refresh gold.transaction_save_potential")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full refresh of all EXPENSE rows (required)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=365,
        help="Lookback window in days",
    )
    args = parser.parse_args()

    if not args.full:
        print("Use --full for full refresh. Incremental refresh runs with the pipeline.")
        sys.exit(1)

    db = get_db_connector()
    n = refresh_save_potential_for_hashes(
        db, hashes=None, full=True, window_days=args.window_days
    )
    print(f"\n[GOLD] Refreshed {n:,} rows in gold.transaction_save_potential")
    sys.exit(0)


if __name__ == "__main__":
    main()
