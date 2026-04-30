"""
Prefect Flow for Expense Pipeline
==================================

Orchestrates the expense ETL pipeline.

Schedule with:
  prefect deployment build flows/expense_pipeline_flow.py:expense_pipeline_flow --cron "0 6 * * *"

Run manually:
  python flows/expense_pipeline_flow.py
  python flows/expense_pipeline_flow.py --mode full
"""

import sys
from pathlib import Path

# Add src to path
_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))


def _run_incremental() -> bool:
    """Run incremental load from API."""
    from loaders.incremental_load import IncrementalDataLoader

    loader = IncrementalDataLoader(source="api")
    return loader.load()


def _run_full_from_api(from_date=None, to_date=None) -> bool:
    """Run full load from API."""
    from datetime import datetime, timedelta
    import uuid

    from extractors.budgetbakers_extractor import BudgetBakersExtractor
    from loaders.initial_load import InitialDataLoader

    date_to = to_date or datetime.now()
    date_from = from_date or (date_to - timedelta(days=365))
    extractor = BudgetBakersExtractor()
    df = extractor.extract(date_from=date_from, date_to=date_to)
    if len(df) == 0:
        return True
    tmp_dir = _project_root / "data" / "processed"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_file = tmp_dir / f"api_extract_{uuid.uuid4().hex[:8]}.csv"
    df.to_csv(tmp_file, index=False)
    try:
        loader = InitialDataLoader(file_path=str(tmp_file))
        return loader.load()
    finally:
        tmp_file.unlink(missing_ok=True)


def _run_flow_impl(mode: str) -> bool:
    """Internal implementation - runs without Prefect decorator."""
    if mode == "incremental":
        return _run_incremental()
    return _run_full_from_api()


try:
    from prefect import flow

    @flow(name="expense-pipeline", log_prints=True)
    def expense_pipeline_flow(mode: str = "incremental"):
        """
        Prefect flow for expense pipeline.

        Args:
            mode: 'incremental' (default) or 'full'
        """
        return _run_flow_impl(mode)

except ImportError:

    def expense_pipeline_flow(mode: str = "incremental"):
        """Run without Prefect (fallback when prefect not installed)."""
        return _run_flow_impl(mode)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    args = parser.parse_args()
    success = expense_pipeline_flow(mode=args.mode)
    sys.exit(0 if success else 1)
