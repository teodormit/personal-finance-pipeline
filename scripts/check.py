"""
check — deterministic grader for this repo (spec 001).

  python scripts/check.py            # Tier 1 then Tier 2; exit 0 only if both pass
  python scripts/check.py --tier 1   # fast path: no database
  python scripts/check.py --keep     # leave the scratch container up for debugging

Tier 1: compileall + unit tests with the DB forced unreachable.
Tier 2: throwaway Postgres (scripts/scratch_db.py) → blueprint → sentinel →
        pipeline invariants (tests/scratch) + the live-DB integration modules,
        all pointed at the scratch DB. Integration tests must PASS, not skip.

xfail is allowed; XPASS (strict) and any non-xfail skip in Tier 2 fail the check.
This script never imports src/ — every src/ import happens in a pytest
subprocess whose environment already points at the scratch DB.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

# Windows consoles default to cp1252, which cannot encode '≠'/'§' in xfail
# reasons; force UTF-8 so the summary never crashes (same fix as the other scripts).
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scratch_db import (  # noqa: E402
    RUN_ID_ENV,
    apply_blueprint,
    assert_scratch_target,
    remove_scratch,
    start_scratch,
    write_sentinel,
)

TIER2_TARGETS = [
    "tests/scratch",
    "tests/test_base_loader_integration.py",
    "tests/test_transaction_audit.py",
]


@dataclass
class TierResult:
    name: str
    ok: bool = False
    duration: float = 0.0
    counts: dict = field(default_factory=dict)
    xfails: list = field(default_factory=list)   # (test id, reason, captured output)
    problems: list = field(default_factory=list)  # human-readable failure lines


def _parse_junit(path: Path, result: TierResult) -> None:
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0, "xfailed": 0}
    if not path.exists():
        result.problems.append("no junit report produced (pytest crashed?)")
        result.counts = counts
        return
    for case in ET.parse(path).getroot().iter("testcase"):
        test_id = f"{case.get('classname')}::{case.get('name')}"
        if case.find("failure") is not None:
            counts["failed"] += 1
            msg = case.find("failure").get("message", "")
            result.problems.append(f"FAILED {test_id}: {msg[:300]}")
        elif case.find("error") is not None:
            counts["error"] += 1
            msg = case.find("error").get("message", "")
            result.problems.append(f"ERROR {test_id}: {msg[:300]}")
        elif case.find("skipped") is not None:
            sk = case.find("skipped")
            if sk.get("type") == "pytest.xfail":
                counts["xfailed"] += 1
                out = case.find("system-out")
                detail = (out.text or "").strip() if out is not None else ""
                result.xfails.append((test_id, sk.get("message", ""), detail))
            else:
                counts["skipped"] += 1
                result.problems.append(f"SKIPPED {test_id}: {sk.get('message', '')[:300]}")
        else:
            counts["passed"] += 1
    result.counts = counts


def _run_pytest(args: list[str], env: dict, result: TierResult, *, skips_fail: bool) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        junit = Path(tmp) / "junit.xml"
        cmd = [sys.executable, "-m", "pytest", *args, "-q", "-rxXfE", "-o", "junit_logging=system-out", f"--junitxml={junit}"]
        proc = subprocess.run(cmd, cwd=ROOT, env=env)
        _parse_junit(junit, result)
    c = result.counts
    if not skips_fail:
        # Tier 1 legitimately skips the live-DB integration modules.
        result.problems = [p for p in result.problems if not p.startswith("SKIPPED")]
    result.ok = (
        proc.returncode == 0
        and c.get("failed", 0) == 0
        and c.get("error", 0) == 0
        and (not skips_fail or c.get("skipped", 0) == 0)
    )
    if proc.returncode != 0 and not result.problems:
        result.problems.append(f"pytest exited {proc.returncode}")


def tier1() -> TierResult:
    res = TierResult("Tier 1 (no DB)")
    t0 = time.monotonic()
    env = os.environ.copy()
    env["POSTGRES_HOST"] = "127.0.0.1"
    env["POSTGRES_PORT"] = "1"
    env.pop(RUN_ID_ENV, None)

    comp = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "src", "scripts"], cwd=ROOT, env=env
    )
    if comp.returncode != 0:
        res.problems.append("compileall failed")
        res.duration = time.monotonic() - t0
        return res

    _run_pytest(["tests/", "-m", "not scratch"], env, res, skips_fail=False)
    res.duration = time.monotonic() - t0
    return res


def tier2(keep: bool) -> TierResult:
    res = TierResult("Tier 2 (scratch Postgres)")
    t0 = time.monotonic()
    scratch = None
    try:
        print("\n[check] starting scratch Postgres...", flush=True)
        scratch = start_scratch()
        print(f"[check] {scratch.container} on {scratch.host}:{scratch.port}", flush=True)
        apply_blueprint(scratch)
        write_sentinel(scratch)

        env = os.environ.copy()
        env.update(scratch.env())
        # Allowlist guard in-process before handing the env to pytest
        # (pytest re-checks it in a session fixture).
        assert_scratch_target(env)

        _run_pytest(TIER2_TARGETS, env, res, skips_fail=True)
    except Exception as e:
        res.ok = False
        res.problems.append(f"harness error: {type(e).__name__}: {e}")
    finally:
        if scratch is not None:
            if keep:
                print(
                    "\n[check] --keep: scratch left running\n"
                    f"  container: {scratch.container}\n"
                    f"  host={scratch.host} port={scratch.port} db={scratch.db} "
                    f"user={scratch.user} password={scratch.password}\n"
                    f"  remove with: docker rm -f {scratch.container}"
                )
            else:
                remove_scratch(scratch)
    res.duration = time.monotonic() - t0
    return res


def _summary(results: list[TierResult], total: float) -> None:
    print("\n" + "=" * 70)
    print("CHECK SUMMARY")
    print("=" * 70)
    for r in results:
        c = r.counts
        print(
            f"{'PASS' if r.ok else 'FAIL'}  {r.name:<28} {r.duration:6.1f}s  "
            f"passed={c.get('passed', 0)} failed={c.get('failed', 0)} "
            f"errors={c.get('error', 0)} skipped={c.get('skipped', 0)} "
            f"xfailed={c.get('xfailed', 0)}"
        )
        for test_id, reason, detail in r.xfails:
            print(f"      xfail  {test_id}\n             reason: {reason}")
            noise = ("Database connector", "[GOLD]", "  [GOLD]")
            lines = [ln for ln in detail.splitlines() if ln.strip() and not ln.startswith(noise)]
            for line in lines[:25]:
                print(f"             | {line}")
        for p in r.problems:
            print(f"      !! {p}")
    ok = all(r.ok for r in results)
    print("-" * 70)
    print(f"RESULT: {'PASS' if ok else 'FAIL'}   total {total:.1f}s")


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic repo check (spec 001)")
    parser.add_argument("--tier", choices=["1", "2", "all"], default="all")
    parser.add_argument("--keep", action="store_true", help="leave the scratch container running")
    args = parser.parse_args()

    t0 = time.monotonic()
    results: list[TierResult] = []
    if args.tier in ("1", "all"):
        results.append(tier1())
    if args.tier in ("2", "all") and (args.tier == "2" or results[-1].ok):
        results.append(tier2(args.keep))
    elif args.tier == "all":
        print("\n[check] Tier 1 failed — skipping Tier 2.")
    _summary(results, time.monotonic() - t0)
    return 0 if results and all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
