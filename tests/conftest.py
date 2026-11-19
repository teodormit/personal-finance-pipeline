"""
Suite-wide safety net for the check harness (spec 001).

When scripts/check.py runs Tier 2 it sets PFP_SCRATCH_RUN_ID. In that mode,
every test in the session — including the live-DB integration modules — is
refused unless the environment provably points at the scratch database
(allowlist guard in scripts/scratch_db.py). Outside check.py this is a no-op.
"""

import os
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))


@pytest.fixture(scope="session", autouse=True)
def _scratch_mode_guard():
    if os.environ.get("PFP_SCRATCH_RUN_ID"):
        from scratch_db import assert_scratch_target

        assert_scratch_target()
    yield
