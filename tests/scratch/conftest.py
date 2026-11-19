"""
Tier 2 fixtures. These tests write to a database, so they run ONLY under
scripts/check.py, which creates the scratch container and sets the env.
A plain `pytest tests/` skips them before any src/ import touches a DB.
"""

import os
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
for _p in (_ROOT / "src", _ROOT / "scripts"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


@pytest.fixture(scope="session")
def scratch_guard():
    if not os.environ.get("PFP_SCRATCH_RUN_ID"):
        pytest.skip("scratch tests run only via scripts/check.py")
    from scratch_db import assert_scratch_target

    assert_scratch_target()
    return True


@pytest.fixture(scope="session")
def db(scratch_guard):
    from utils.db_connector import get_db_connector

    return get_db_connector()
