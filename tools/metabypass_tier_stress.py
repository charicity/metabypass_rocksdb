#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Repeat tiered storage concurrency/recovery tests in isolated processes."""

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


CASES = [
    "TieredMigrationDoesNotBlockAsyncWrites",
    "TieredConcurrentReadsDuringPressureAndFlush",
    "TieredPressureEvictsWithoutMemtableFlush",
    "TieredRestoreRetryDoesNotChangePublishedSource",
    "TieredRandomizedRoundTrip",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--repeat", type=int, default=10)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--case", action="append")
    parser.add_argument("--tmp-parent")
    args = parser.parse_args()
    if args.repeat < 1 or args.workers < 1:
        parser.error("repeat and workers must be positive")
    binary = str(args.binary.resolve())
    cases = args.case or CASES
    if args.all_cases:
        listing = subprocess.check_output(
            [binary, "--gtest_filter=MetaBypassTest.Tiered*", "--gtest_list_tests"],
            text=True,
        )
        cases = [line.strip().split()[0] for line in listing.splitlines()
                 if line.startswith("  Tiered")]
        if not cases:
            parser.error("no tiered tests found in binary")
    started = time.monotonic()

    def run(case):
        with tempfile.TemporaryDirectory(prefix="mb-tier-stress-", dir=args.tmp_parent) as directory:
            env = dict(os.environ, TEST_TMPDIR=directory)
            process = subprocess.Popen(
                [binary, "--gtest_filter=MetaBypassTest." + case],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                start_new_session=os.name == "posix",
            )
            timeout = False
            try:
                output, _ = process.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                timeout = True
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                output, _ = process.communicate()
            return {
                "case": case,
                "exit_code": process.returncode,
                "timeout": timeout,
                "failure_output": output[-12000:] if process.returncode else "",
            }

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(run, cases * args.repeat))
    failures = [r for r in results if r["exit_code"] or r["timeout"]]
    print(json.dumps({
        "binary": binary,
        "repeat": args.repeat,
        "workers": args.workers,
        "tmp_parent": args.tmp_parent,
        "case_timeout_seconds": 60,
        "elapsed_seconds": time.monotonic() - started,
        "total": len(results),
        "passed": len(results) - len(failures),
        "failures": failures,
    }, indent=2))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
