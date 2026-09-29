# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Small harness checks; no RocksDB workloads or builds are started here."""
import os
from pathlib import Path
import sys
import tempfile
import unittest

from run import (allocated_bytes, backpressure_report_markdown, execute,
                 RunFailure, summarize, validate_result)


class RunnerTest(unittest.TestCase):
    def backpressure_result(self):
        return {"status": "OK", "exit": 0, "metrics": {
            "successful_ops": 16384, "foreground_us": 100, "sync_us": 20,
            "close_us": 10, "total_us": 130, "p99_ns": 5,
            "cpu_user_us": 10, "cpu_system_us": 5, "peak_rss_kib": 1000,
            "recovery_points": 1, "foreground_backpressure_us": 4,
            "backpressure_us": 5, "queue_peak_bytes": 1024,
            "queue_capacity_bytes": 2 * 1024**2, "injected_wait_us": 1000,
            "delayed_append_calls": 1, "delayed_sync_calls": 0,
            "delayed_fsync_calls": 0, "delayed_named_sync_calls": 0}}

    def check_backpressure_write(self, result):
        validate_result(result, "write", 16384, "current", "backpressure")

    def test_backpressure_requires_positive_foreground_and_total_wait(self):
        for metric in ("foreground_backpressure_us", "backpressure_us"):
            with self.subTest(metric=metric):
                result = self.backpressure_result()
                result["metrics"][metric] = 0
                with self.assertRaises(RunFailure):
                    self.check_backpressure_write(result)

    def test_backpressure_rejects_over_capacity_peak(self):
        result = self.backpressure_result()
        result["metrics"]["queue_peak_bytes"] = 2 * 1024**2 + 1
        with self.assertRaises(RunFailure):
            self.check_backpressure_write(result)

    def test_backpressure_requires_all_metrics(self):
        for metric in ("foreground_backpressure_us", "backpressure_us",
                       "queue_peak_bytes", "queue_capacity_bytes",
                       "injected_wait_us", "delayed_append_calls"):
            with self.subTest(metric=metric):
                result = self.backpressure_result()
                del result["metrics"][metric]
                with self.assertRaises(RunFailure):
                    self.check_backpressure_write(result)

    def test_backpressure_rejects_wrong_operation_count(self):
        result = self.backpressure_result()
        result["metrics"]["successful_ops"] = 16383
        with self.assertRaises(RunFailure):
            self.check_backpressure_write(result)

    def test_backpressure_report_has_no_three_way_speed_ratio(self):
        report = {"state": "complete", "summary": {
            "samples": 1, "metrics": self.backpressure_result()["metrics"]}}
        markdown = backpressure_report_markdown(report)
        self.assertIn("mechanism_validation", markdown)
        self.assertIn("Foreground backpressure us", markdown)
        self.assertNotIn("ops/s", markdown)
        self.assertNotIn("relative to", markdown)
        self.assertNotIn("baseline", markdown)
        self.assertNotIn("upstream", markdown)

    def test_failed_verification_excludes_fast_write(self):
        def row(foreground, sync, valid=True):
            return {"phase": "measured", "scenario": "control",
                    "variant": "current", "valid": valid, "allocated_bytes": 4096,
                    "write": {"metrics": {"successful_ops": 100,
                                          "foreground_us": foreground,
                                          "sync_us": sync, "close_us": 0,
                                          "total_us": foreground + sync}},
                    "verify": {"metrics": {"restore_us": 1}}}
        rows = [row(1, 100), row(100, 1), row(90, 90), row(0, 0, False)]
        group = summarize(rows)["control"]["current"]
        self.assertEqual(group["samples"], 3)
        self.assertEqual(group["metrics"]["total_us"]["median"], 101)
        self.assertNotEqual(group["metrics"]["total_us"]["median"],
                            group["metrics"]["foreground_us"]["median"] +
                            group["metrics"]["sync_us"]["median"])

    def test_timeout_keeps_output_and_failure(self):
        command = [sys.executable, "-c",
                   "import signal; print('started', flush=True); signal.pause()"]
        result = execute(command, 0.5)
        self.assertTrue(result["timed_out"])
        self.assertIn("started", result["stdout"])
        self.assertNotEqual(result["exit"], 0)
        with self.assertRaises(RunFailure):
            validate_result(result, "write", 1, "current")

    def test_zero_exit_without_success_is_rejected(self):
        result = execute([sys.executable, "-c", "print('not a benchmark result')"], 5)
        with self.assertRaises(RunFailure):
            validate_result(result, "verify", 1, "baseline")

    def test_physical_space_deduplicates_hardlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first"
            first.write_bytes(b"x" * 8192)
            os.link(first, root / "second")
            self.assertEqual(allocated_bytes(root),
                             (root.stat().st_blocks + first.stat().st_blocks) * 512)


if __name__ == "__main__":
    unittest.main()
