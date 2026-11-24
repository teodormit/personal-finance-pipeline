"""
Throwaway Postgres for the check harness (and, later, the restore drill).

Two responsibilities:
  1. Lifecycle of a scratch `postgres:17` container: start on a random
     localhost port, wait until it accepts TCP, apply the public blueprint,
     write a run sentinel, remove.
  2. `assert_scratch_target()`: an ALLOWLIST guard that refuses to let any
     test or loader write unless the environment provably points at the
     scratch database this run created. The live warehouse can never satisfy
     it (wrong db name, port 5432, no sentinel).

Uses only the docker CLI (subprocess) and psycopg2.
"""

from __future__ import annotations

import os
import re
import secrets
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BLUEPRINT = PROJECT_ROOT / "docs" / "postgres_init_blueprint.sql"

IMAGE = "postgres:17"
# Must equal the blueprint's `\set admin_role pipeline_admin` (line 30),
# which overrides any `psql -v admin_role=...`.
SCRATCH_USER = "pipeline_admin"
SCRATCH_HOST = "127.0.0.1"
LIVE_PORT = 5432

SCRATCH_DB_RE = re.compile(r"^pfp_scratch_[0-9a-f]{8}$")
RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
ENV_KEYS = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)
RUN_ID_ENV = "PFP_SCRATCH_RUN_ID"


class ScratchGuardError(RuntimeError):
    """Raised when the environment does not point at this run's scratch DB."""


# --------------------------------------------------------------------------- #
# Guard
# --------------------------------------------------------------------------- #
def assert_scratch_target(env=None, *, connect=None, check_sentinel: bool = True) -> None:
    """Refuse unless ALL hold:

    - all five POSTGRES_* vars and PFP_SCRATCH_RUN_ID are set
    - POSTGRES_DB matches ^pfp_scratch_[0-9a-f]{8}$ and its suffix is the run id prefix
    - POSTGRES_PORT is an integer != 5432
    - POSTGRES_HOST is exactly 127.0.0.1
    - metadata.scratch_sentinel contains this run id (if check_sentinel)

    Static checks run first, so pointing this at the live env raises before
    any connection is attempted. `connect` is injectable for unit tests.
    """
    env = os.environ if env is None else env

    missing = [k for k in (*ENV_KEYS, RUN_ID_ENV) if not env.get(k)]
    if missing:
        raise ScratchGuardError(f"scratch env incomplete, missing: {missing}")

    run_id = env[RUN_ID_ENV]
    db = env["POSTGRES_DB"]
    host = env["POSTGRES_HOST"]
    port_raw = env["POSTGRES_PORT"]

    if not RUN_ID_RE.match(run_id):
        raise ScratchGuardError(f"{RUN_ID_ENV} is not a 32-hex run id: {run_id!r}")
    if not SCRATCH_DB_RE.match(db):
        raise ScratchGuardError(f"POSTGRES_DB {db!r} is not a scratch database name")
    if db != f"pfp_scratch_{run_id[:8]}":
        raise ScratchGuardError(f"POSTGRES_DB {db!r} does not belong to run {run_id[:8]}")
    try:
        port = int(port_raw)
    except ValueError:
        raise ScratchGuardError(f"POSTGRES_PORT {port_raw!r} is not an integer") from None
    if port == LIVE_PORT:
        raise ScratchGuardError("POSTGRES_PORT is 5432 (the live warehouse port)")
    if host != SCRATCH_HOST:
        raise ScratchGuardError(f"POSTGRES_HOST {host!r} is not {SCRATCH_HOST}")

    if not check_sentinel:
        return

    if connect is None:
        import psycopg2

        connect = psycopg2.connect
    try:
        conn = connect(
            host=host,
            port=port,
            dbname=db,
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"],
            connect_timeout=5,
        )
    except Exception as e:
        raise ScratchGuardError(f"cannot reach scratch DB for sentinel check: {e}") from e
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM metadata.scratch_sentinel WHERE run_id = %s",
                (run_id,),
            )
            found = cur.fetchone() is not None
        except Exception as e:
            raise ScratchGuardError(f"sentinel table unreadable: {e}") from e
    finally:
        conn.close()
    if not found:
        raise ScratchGuardError(f"sentinel for run {run_id[:8]} not found in metadata.scratch_sentinel")


# --------------------------------------------------------------------------- #
# Container lifecycle
# --------------------------------------------------------------------------- #
@dataclass
class ScratchDB:
    run_id: str
    container: str
    db: str
    password: str
    host: str = SCRATCH_HOST
    port: int = 0
    user: str = SCRATCH_USER

    def env(self) -> dict:
        """The env vars that point src/ (db_connector) at this scratch DB."""
        return {
            "POSTGRES_HOST": self.host,
            "POSTGRES_PORT": str(self.port),
            "POSTGRES_DB": self.db,
            "POSTGRES_USER": self.user,
            "POSTGRES_PASSWORD": self.password,
            RUN_ID_ENV: self.run_id,
        }

    def connect(self):
        import psycopg2

        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.db,
            user=self.user,
            password=self.password,
            connect_timeout=5,
        )


def _docker(*args: str, input_bytes: bytes | None = None, check: bool = True):
    proc = subprocess.run(
        ["docker", *args],
        input=input_bytes,
        capture_output=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"docker {' '.join(args[:2])} failed ({proc.returncode}): "
            f"{proc.stderr.decode(errors='replace').strip()}"
        )
    return proc


def start_scratch(timeout_s: int = 90) -> ScratchDB:
    """Start a scratch container on a random localhost port and wait for TCP readiness."""
    run_id = uuid.uuid4().hex
    short = run_id[:8]
    scratch = ScratchDB(
        run_id=run_id,
        container=f"pfp-scratch-{short}",
        db=f"pfp_scratch_{short}",
        password=secrets.token_hex(16),
    )
    _docker(
        "run", "-d", "--rm",
        "--name", scratch.container,
        "-e", f"POSTGRES_USER={scratch.user}",
        "-e", f"POSTGRES_PASSWORD={scratch.password}",
        "-e", f"POSTGRES_DB={scratch.db}",
        "-p", f"{SCRATCH_HOST}::5432",
        IMAGE,
    )
    try:
        out = _docker("port", scratch.container, "5432/tcp").stdout.decode().strip()
        # e.g. "127.0.0.1:55123" (one line per binding; we bound only 127.0.0.1)
        first = out.splitlines()[0]
        bound_host, _, bound_port = first.rpartition(":")
        if bound_host != SCRATCH_HOST:
            raise RuntimeError(f"scratch bound to unexpected host: {out!r}")
        scratch.port = int(bound_port)

        # TCP readiness inside the container. The image's init phase runs a
        # temporary server with no TCP listener, so -h 127.0.0.1 only succeeds
        # once the real server is up (avoids racing the init restart).
        deadline = time.monotonic() + timeout_s
        while True:
            ready = _docker(
                "exec", scratch.container,
                "pg_isready", "-h", "127.0.0.1", "-U", scratch.user, "-d", scratch.db,
                check=False,
            )
            if ready.returncode == 0:
                break
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"scratch Postgres not ready after {timeout_s}s "
                    f"(container {scratch.container})"
                )
            time.sleep(1)

        # Host-side port forward can lag the in-container listener briefly.
        while True:
            try:
                scratch.connect().close()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"scratch port {scratch.port} not reachable from host after {timeout_s}s"
                    )
                time.sleep(1)
    except Exception:
        remove_scratch(scratch)
        raise
    return scratch


def apply_blueprint(scratch: ScratchDB, path: Path = BLUEPRINT) -> None:
    """Pipe the blueprint through psql inside the container (it uses psql meta-commands)."""
    proc = _docker(
        "exec", "-i", scratch.container,
        "psql", "-q", "-v", "ON_ERROR_STOP=1", "-U", scratch.user, "-d", scratch.db,
        input_bytes=path.read_bytes(),
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "blueprint failed to apply:\n" + proc.stderr.decode(errors="replace")
        )


def write_sentinel(scratch: ScratchDB) -> None:
    conn = scratch.connect()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS metadata.scratch_sentinel (run_id text NOT NULL)")
        cur.execute("INSERT INTO metadata.scratch_sentinel (run_id) VALUES (%s)", (scratch.run_id,))
        conn.commit()
    finally:
        conn.close()


def remove_scratch(scratch: ScratchDB) -> None:
    _docker("rm", "-f", scratch.container, check=False)
