#!/usr/bin/env python3
"""
test_all.py -- runs every standalone test file in dhan/test/ and prints one
consolidated pass/fail summary.

Each test file below is run as its OWN fresh subprocess, not imported into
this process. That's deliberate, not incidental: 6 of the 7 files apply
process-wide mocks at import time (patch.object(rt, ..., ...).start(),
never matched with a .stop()) scoped to that file's own assumptions --
e.g. test_targets.py permanently patches _load_uc_cache to always return
{}, test_batch_concurrency.py and test_exit_stage_timing.py each patch
_hold_until to a no-op, etc. Importing two of these into the same process
would let one file's global patches leak into another file's scenarios,
silently changing what they're actually testing. Running each as its own
subprocess is exactly the isolation every file already gets when run
individually (`python3.11 dhan/test/test_X.py`) -- this script just
automates running all of them and reading all the exit codes in one place,
instead of merging their code (and their mocking) together.

Usage:
    python dhan/test/test_all.py          # quiet -- full output only on failure
    python dhan/test/test_all.py -v       # stream every file's full output live

Exit 0 only if every test file exits 0.
"""

import subprocess
import sys
import time
from pathlib import Path

_DIR  = Path(__file__).resolve().parent          # dhan/test/ -- where the sibling test files live
_ROOT = _DIR.parent.parent                        # project root -- used as each subprocess's cwd

# Explicit, ordered list rather than a glob over test_*.py -- a new test
# file should be added here deliberately (and reviewed for what it mocks),
# not picked up silently, and this keeps a stable, readable run order.
TEST_FILES = [
    "test_auth_renew.py",
    "test_targets.py",
    "test_batch_concurrency.py",
    "test_parallel_orders.py",
    "test_uc_staged_entry.py",
    "test_exit_stage_timing.py",
    "test_order_update_feed.py",
]

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def run_one(path: Path) -> tuple[bool, float, str]:
    start = time.monotonic()
    # stderr merged INTO stdout (not captured separately and concatenated
    # after) so the combined output preserves real chronological order --
    # several test files intentionally print an expected error/rejection
    # message mid-run (e.g. test_auth_renew.py's failure-path scenario) as
    # part of a passing test; capturing the streams separately would push
    # that message after the file's real final "PASSED" line and make the
    # one-line preview below misleading, even though the exit code is fine.
    proc = subprocess.run(
        [sys.executable, str(path)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(_ROOT),
    )
    elapsed = time.monotonic() - start
    return proc.returncode == 0, elapsed, proc.stdout


def main() -> int:
    verbose = "-v" in sys.argv or "--verbose" in sys.argv
    print(f"Running {len(TEST_FILES)} test file(s) from {_DIR} "
          f"(each in its own subprocess)...\n")

    results: list[tuple[str, bool, float, str]] = []
    for name in TEST_FILES:
        path = _DIR / name
        label = f"--- {name} "
        print(label + "-" * max(1, 70 - len(label)))

        if not path.exists():
            print(f"  !! FILE NOT FOUND: {path}")
            results.append((name, False, 0.0, "file not found"))
            continue

        ok, elapsed, output = run_one(path)
        if verbose or not ok:
            print(output.rstrip())
        else:
            last_line = next((l for l in reversed(output.splitlines()) if l.strip()), "")
            print(f"  {last_line}")
        print(f"  [{'exit 0' if ok else 'exit != 0'}, {elapsed:.1f}s]\n")
        results.append((name, ok, elapsed, output))

    print(f"{'='*70}\nSUMMARY\n{'='*70}")
    for name, ok, elapsed, _ in results:
        status = PASS if ok else FAIL
        print(f"  {status}  {name:32s} ({elapsed:.1f}s)")

    n_pass = sum(1 for _, ok, *_ in results if ok)
    n_total = len(results)
    print(f"\n{n_pass}/{n_total} test file(s) passed"
          f"{'.' if n_pass == n_total else ' -- see full output above for the failing file(s).'}")

    return 0 if n_pass == n_total else 1


if __name__ == "__main__":
    sys.exit(main())
