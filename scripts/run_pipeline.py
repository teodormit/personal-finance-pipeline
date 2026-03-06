"""
Unified Pipeline Entry Point
============================

Single command to run full or incremental expense pipeline.
Supports API extraction (BudgetBakers) and file-based extraction.

Usage:
  python scripts/run_pipeline.py --mode incremental
  python scripts/run_pipeline.py --mode full --source api
  python scripts/run_pipeline.py --mode full --source file --file data/raw/export.xlsx
"""

import argparse
import sys
from pathlib import Path

# Add src to path
_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))


def main():
    parser = argparse.ArgumentParser(
        description="Run personal finance expense pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_pipeline.py --mode incremental
  python scripts/run_pipeline.py --mode full --source api
  python scripts/run_pipeline.py --mode full --source file --file data/raw/export.xlsx
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Full load (truncate silver) or incremental (append only)",
    )
    parser.add_argument(
        "--source",
        choices=["api", "file"],
        default="api",
        help="Extract from BudgetBakers API or local file",
    )
    parser.add_argument(
        "--file",
        help="Path to source file (required when --source=file)",
    )
    parser.add_argument(
        "--from-date",
        help="Start date for API extraction (YYYY-MM-DD). Default: 1 year ago for full, last silver date for incremental",
    )
    parser.add_argument(
        "--to-date",
        help="End date for API extraction (YYYY-MM-DD). Default: today",
    )
    args = parser.parse_args()

    if args.source == "file" and not args.file:
        parser.error("--file is required when --source=file")

    if args.mode == "full":
        if args.source == "file":
            from loaders.initial_load import InitialDataLoader

            loader = InitialDataLoader(file_path=args.file)
            success = loader.load()
        else:
            # Full load from API: extract date range, save to temp file, run initial load
            from datetime import datetime, timedelta
            import uuid

            from extractors.budgetbakers_extractor import BudgetBakersExtractor
            from loaders.initial_load import InitialDataLoader

            date_to = datetime.now()
            date_from = date_to - timedelta(days=365)
            if args.from_date:
                date_from = datetime.strptime(args.from_date, "%Y-%m-%d")
            if args.to_date:
                date_to = datetime.strptime(args.to_date, "%Y-%m-%d")

            extractor = BudgetBakersExtractor()
            df = extractor.extract(date_from=date_from, date_to=date_to)
            if len(df) == 0:
                print("No data extracted from API.")
                sys.exit(0)

            tmp_dir = _project_root / "data" / "processed"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_file = tmp_dir / f"api_extract_{uuid.uuid4().hex[:8]}.csv"
            df.to_csv(tmp_file, index=False)
            try:
                loader = InitialDataLoader(file_path=str(tmp_file))
                success = loader.load()
            finally:
                tmp_file.unlink(missing_ok=True)
    else:
        from loaders.incremental_load import IncrementalDataLoader

        loader = IncrementalDataLoader(source=args.source, file_path=args.file)
        success = loader.load()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
