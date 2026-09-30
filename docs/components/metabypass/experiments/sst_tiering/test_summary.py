#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Check that compact latency summaries preserve short tail spikes."""

import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest import mock


ROOT = Path(__file__).parent
RUN_SPEC = importlib.util.spec_from_file_location("run", ROOT / "run.py")
RUN = importlib.util.module_from_spec(RUN_SPEC)
RUN_SPEC.loader.exec_module(RUN)
with mock.patch.dict(sys.modules, {"run": RUN}):
    SPEC = importlib.util.spec_from_file_location(
        "sst_summary", ROOT / "summarize_standard.py")
    SUMMARY = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(SUMMARY)


def example_trial(repeat, workload, mode, percent, p99_values):
    space = {
        "logical_bytes_by_path_category": {"ssd_sst": 1024},
        "physical_allocated_by_category_unique_inodes": {"ssd_sst": 4096},
        "physical_allocated_exclusive_category_bytes": {"ssd_sst": 4096},
        "physical_allocated_unique_bytes": 4096,
        "hardlinked_extra_paths": 0,
        "unique_inodes": 1,
    }
    events = []
    for index, p99 in enumerate(p99_values):
        window = {field: 0 for field in SUMMARY.WINDOW_FIELDS}
        window.update(event="window", phase="measured", window=index,
                      elapsed_us=1000000, phase_elapsed_us=(index + 1) * 1000000,
                      ops=100, gets=100, hot_gets=0, cold_gets=100,
                      get_p50_us=5, get_p95_us=8, get_p99_us=p99,
                      cold_p50_us=5, cold_p95_us=8, cold_p99_us=p99,
                      get_ops_per_sec=100, publish_lag_us=50)
        events.append(window)
    events.extend(({"event": "phase_end", "name": "measured",
                    "elapsed_us": 2000000, "ok": True},
                   {"event": "summary", "publish_lag_us": 50,
                    "ok": True}))
    stdout = "".join("MB_SST_JSON " + json.dumps(event) + "\n"
                     for event in events)
    return {
        "repeat": repeat, "workload": workload, "mode": mode,
        "budget_percent": percent, "budget_bytes": (percent or 0) * 1024,
        "command": ["fake-db-bench", "--seed=17"], "valid": True,
        "exit_code": 0, "timed_out": False,
        "mechanism_observed": mode != "disabled",
        "mechanism_basis": ("none" if mode == "disabled" else
                            "measured_sampling" if mode == "observe" else
                            "measured_migration"),
        "distribution": {"valid": True}, "elapsed_s": 2,
        "sampled_reads_measured": 0, "migrations_fill": 0,
        "migrations_warmup": 0, "migrations_measured": 0,
        "hotspot_adaptation_us": None, "cpu_seconds_sampled": 1,
        "peak_rss_kib": 1000, "space": space, "events": events,
        "stdout": stdout,
    }


class SummaryTest(unittest.TestCase):
    def report(self):
        rows = [example_trial(repeat, workload, mode, percent,
                              ((10 + 10 * repeat), (1000 + 200 * repeat)))
                for repeat in range(3) for workload in RUN.WORKLOADS
                for mode, percent in RUN.CONFIGS]
        return {
            "profile": "standard", "repeats": 3, "status": "complete",
            "binary": {"sha256": "a", "sha256_at_end": "a"},
            "preflight": {"capacity_pressure_covered": True},
            "mechanism_coverage": {
                "all_adaptive_observed": True,
                "all_observe_sampled": True,
                "disabled_without_sampling": True},
            "configurations": [
                {"mode": mode, "budget_percent": percent}
                for mode, percent in RUN.CONFIGS],
            "workloads": list(RUN.WORKLOADS), "trials": rows,
            "same_device": True, "source": {}, "protocol": "test",
            "settings": {}, "benchmark_overrides": {}, "seed": 17,
            "read_delay_us": 0, "roots": {}, "host": {},
            "baseline_sst_logical_bytes": 1024, "budgets": {},
        }

    def test_group_uses_trial_window_medians_and_keeps_spikes(self):
        result = SUMMARY.summarize_report(self.report(), "raw-hash")
        self.assertEqual(105, result["trial_count"])
        self.assertEqual(35, result["group_count"])
        group = next(row for row in result["groups"] if row["workload"] ==
                     "uniform" and row["mode"] == "disabled")
        self.assertEqual({"median": 610, "min": 505, "max": 715,
                          "count": 3}, group["median_window_get_p99_us"])
        self.assertEqual({"median": 1200, "min": 1000, "max": 1400,
                          "count": 3}, group["max_window_get_p99_us"])
        self.assertTrue(all(row["space"]["ssd_physical_saved_vs_disabled_bytes"]
                            is None for row in result["trials"]))

    def test_subset_metadata_cannot_claim_a_complete_standard_matrix(self):
        report = self.report()
        report["workloads"] = ["uniform"]
        report["configurations"] = [
            {"mode": "disabled", "budget_percent": None}]
        report["trials"] = [row for row in report["trials"] if
                            row["workload"] == "uniform" and
                            row["mode"] == "disabled"]
        with self.assertRaisesRegex(ValueError, "full default"):
            SUMMARY.summarize_report(report, "raw-hash")

    def test_missing_trial_rejected_from_full_matrix(self):
        report = self.report()
        report["trials"].pop()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            SUMMARY.summarize_report(report, "raw-hash")


if __name__ == "__main__":
    unittest.main()
