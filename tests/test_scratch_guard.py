"""Tier 1 unit tests for the scratch-DB allowlist guard (no database needed)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from scratch_db import ScratchGuardError, assert_scratch_target  # noqa: E402

RUN_ID = "0123456789abcdef0123456789abcdef"


def _env(**overrides):
    env = {
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": "55123",
        "POSTGRES_DB": f"pfp_scratch_{RUN_ID[:8]}",
        "POSTGRES_USER": "pipeline_admin",
        "POSTGRES_PASSWORD": "x",
        "PFP_SCRATCH_RUN_ID": RUN_ID,
    }
    env.update(overrides)
    return {k: v for k, v in env.items() if v is not None}


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def execute(self, sql, params=None):
        self.params = params

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._row)

    def close(self):
        self.closed = True


def _connect_returning(row):
    return lambda **kw: _FakeConn(row)


def _must_not_connect(**kw):
    raise AssertionError("guard attempted a connection before static checks failed")


def test_accepts_valid_scratch_target_with_sentinel():
    assert_scratch_target(_env(), connect=_connect_returning((1,)))


def test_rejects_live_database_name():
    with pytest.raises(ScratchGuardError, match="not a scratch database"):
        assert_scratch_target(_env(POSTGRES_DB="finance_warehouse"), connect=_must_not_connect)


def test_rejects_scratch_name_from_another_run():
    with pytest.raises(ScratchGuardError, match="does not belong to run"):
        assert_scratch_target(_env(POSTGRES_DB="pfp_scratch_ffffffff"), connect=_must_not_connect)


def test_rejects_live_port():
    with pytest.raises(ScratchGuardError, match="5432"):
        assert_scratch_target(_env(POSTGRES_PORT="5432"), connect=_must_not_connect)


def test_rejects_non_loopback_host():
    for host in ("localhost", "postgres", "0.0.0.0", "192.168.1.10"):
        with pytest.raises(ScratchGuardError, match="POSTGRES_HOST"):
            assert_scratch_target(_env(POSTGRES_HOST=host), connect=_must_not_connect)


def test_rejects_missing_env_var():
    with pytest.raises(ScratchGuardError, match="missing"):
        assert_scratch_target(_env(POSTGRES_PASSWORD=None), connect=_must_not_connect)


def test_rejects_missing_run_id():
    with pytest.raises(ScratchGuardError, match="missing"):
        assert_scratch_target(_env(PFP_SCRATCH_RUN_ID=None), connect=_must_not_connect)


def test_rejects_missing_sentinel():
    with pytest.raises(ScratchGuardError, match="sentinel"):
        assert_scratch_target(_env(), connect=_connect_returning(None))


def test_rejects_unreachable_scratch():
    def boom(**kw):
        raise OSError("connection refused")

    with pytest.raises(ScratchGuardError, match="cannot reach"):
        assert_scratch_target(_env(), connect=boom)
