#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Measure tiered writes separately from their final remote barrier."""

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--fast-parent", default="/dev/shm")
    parser.add_argument("--slow-parent", default="/tmp")
    parser.add_argument("--num", type=int, default=2000)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    binary = str(args.binary.resolve())
    results = []

    def run(command):
        start = time.monotonic()
        output = subprocess.run(command, text=True, capture_output=True, timeout=180)
        if output.returncode:
            raise RuntimeError(output.stdout + output.stderr)
        return {
            "command": command,
            "elapsed_seconds": time.monotonic() - start,
            "stdout": output.stdout,
            "metrics": {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", output.stdout)},
        }

    for delay in (0, 1000):
        for repeat in range(args.repeat):
            for mode in ("baseline_fast", "baseline_slow", "direct_backup", "tiered", "tiered_small"):
                with tempfile.TemporaryDirectory(prefix="mb-tier-fast-", dir=args.fast_parent) as fast:
                    with tempfile.TemporaryDirectory(prefix="mb-tier-slow-", dir=args.slow_parent) as slow:
                        index = str(Path(fast) / "index")
                        data = str(Path(fast if mode == "baseline_fast" else slow) / "data")
                        backup = str(Path(slow) / "backup")
                        command = [binary, "--db=" + index,
                                   "--metabypass_data_dir=" + data,
                                   "--metabypass_backup_dir=" + backup,
                                   "--num=" + str(args.num), "--value_size=1024",
                                   "--sync=false", "--metabypass_slow_write_delay_us=" +
                                   str(0 if mode == "baseline_fast" else delay)]
                        if mode.startswith("tiered"):
                            command += ["--metabypass_staging_dir=" + str(Path(fast) / "staging"),
                                        "--metabypass_staging_capacity=" + str(65536 if mode == "tiered_small" else 4194304)]
                        row = {"mode": mode, "delay_us": delay, "repeat": repeat}
                        row["write"] = run(command + ["--metabypass_mode=" +
                                                       ("baseline" if mode.startswith("baseline") else "write")])
                        row["slow_bytes"] = sum(p.stat().st_size for p in Path(slow).rglob("*") if p.is_file())
                        row["fast_bytes"] = sum(p.stat().st_size for p in Path(fast).rglob("*") if p.is_file())
                        if mode.startswith("tiered"):
                            # No oracle or writer state is passed to Restore.
                            shutil.rmtree(index)
                            shutil.rmtree(Path(fast) / "staging")
                            row["restore"] = run(command + ["--metabypass_mode=restore"])
                            row["verify"] = run(command + ["--metabypass_mode=verify"])
                        results.append(row)
    print(json.dumps({
        "binary": binary, "num": args.num, "value_size": 1024,
        "repeat": args.repeat, "fast_parent": args.fast_parent,
        "slow_parent": args.slow_parent,
        "warning": "tmpfs vs disk and latency injection are not physical SSD/HDD or power-loss tests",
        "results": results,
    }, indent=2))


if __name__ == "__main__":
    main()
