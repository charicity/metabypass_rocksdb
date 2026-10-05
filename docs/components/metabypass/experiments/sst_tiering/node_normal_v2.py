#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Normal-v1 measurements with an independent bounded-v2 preparation helper.

Only preparation argv changes. Frozen workload, policy, orchestration and
all cgroup/OOM/seed/inode/timeout safeguards are inherited unchanged.
"""

import json
from pathlib import Path
import signal
import sys

import node_clone_bounded as clone
import node_normal as normal


class NormalRunnerV2(normal.NormalRunner):
    def __init__(self, args):
        super().__init__(args)
        self.report["preparation_protocol"] = clone.PREPARATION_PROTOCOL
        self.report["limits"]["preparation"] = {
            "buffer_bytes": clone.BUFFER_BYTES, "sync_batch_bytes": clone.SYNC_BATCH_BYTES,
            "python_flags": ["-I", "-S", "-B"], "directory_fsync": False,
            "copy_kernel_memory_bounded": False}
        sources = self.report["driver_identity"]["source_sha256"]
        for path in (Path(clone.__file__).resolve(), Path(__file__).resolve()):
            sources[str(path)] = normal.node_run.digest(path)

    def execute_checked(self, row, command, dirs, timeout, preparation=False):
        if preparation:
            expected = [sys.executable, str(Path(normal.node_run.__file__).resolve()),
                        "--clone", *map(str, (*self.seed, *dirs))]
            if command != expected:
                raise normal.trade.SafetyStop("unexpected preparation command; bounded override refused")
            command = [sys.executable, "-I", "-S", "-B", str(Path(clone.__file__).resolve()),
                       "--clone", *map(str, (*self.seed, *dirs))]
        return super().execute_checked(row, command, dirs, timeout, preparation=preparation)


def main():
    args = normal.parse_args()
    runner = NormalRunnerV2(args)

    def stop(signum, _frame):
        if runner.active:
            normal.node_run.kill_owned_group(runner.active)
        raise normal.trade.SafetyStop("controller received signal " + str(signum))

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        runner.run()
    except Exception as error:
        runner.report.update(status="blocked_or_failed", error=str(error))
    finally:
        runner.finalize()
    print(json.dumps({"status": runner.report["status"], "run_uuid": runner.run_id,
                      "output": args.output, "error": runner.report.get("error"),
                      "final_integrity_errors": runner.report.get("final_integrity_errors")}))
    return 0 if runner.report["status"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
