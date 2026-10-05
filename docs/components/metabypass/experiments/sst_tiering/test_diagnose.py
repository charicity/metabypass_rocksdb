#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Diagnostic dataset controls, phase boundaries and fixed plan regression tests."""

import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import node_diagnose as DIAG
import node_run as RUN


def events(counts=(0, 0, 0)):
    result = [{"event": "ready", "steady_time_us": 1}]
    for index, (phase, count) in enumerate(zip(DIAG.FORMAL_DURATIONS, counts)):
        counters = {key: count for key in DIAG.MIGRATION_COUNTERS}
        counters.update(observed_promotions=100 + index, observed_demotions=200 + index)
        result.append({"event": "window", "phase": phase,
                       "stats": {key: 999 for key in DIAG.MIGRATION_COUNTERS}})
        result.append({"event": "phase_end", "phase": phase, "stats": counters,
                       "elapsed_us": 1_000_000, "steady_time_us": (index + 1) * 1_000_000})
    return result


class DiagnosticTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ssd, self.hdd = self.root / "ssd", self.root / "hdd"
        self.ssd.mkdir()
        self.hdd.mkdir()

    def fixture(self):
        index = self.ssd / "index"
        index.mkdir()
        (index / "IDENTITY").write_text("test-id\n")
        (index / "000001.sst").write_bytes(b"a" * 4096)
        store = self.hdd / "backup" / "sst-store"
        store.mkdir(parents=True)
        (store / "1-2-88.sst").write_bytes(b"b" * 4096)
        payload = b"MBS1 test-id\n1 4096 77 1 1-1-77.sst\n2 4096 88 0 1-2-88.sst\n"
        (index / "SST-PLACEMENT").write_bytes(payload + f"CRC {DIAG.node_cache.crc32c(payload)}\n".encode())
        return self.ssd, self.hdd

    def test_fixed_cli_settings_and_two_reversed_orders(self):
        arguments = []
        values = {"binary": "/bin/true", "source-manifest": self.root / "source.json",
                  "ssd-root": self.ssd, "hdd-root": self.hdd,
                  "seed-ssd": self.ssd, "seed-hdd": self.hdd,
                  "cgroup": self.root / "cg", "output": self.root / "report.json"}
        for key, value in values.items():
            arguments.extend(("--" + key, str(value)))
        args = DIAG.parse_args(arguments)
        self.assertEqual(("small", "0,1,2,3", True, "uniform", 1800),
                         (args.profile, args.cpu_list, args.keep_data, args.workload, args.deadline_seconds))
        self.assertEqual(list(reversed(DIAG.arm_order(1))), DIAG.arm_order(2))
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            DIAG.parse_args(arguments + ["--deadline-seconds", "1801"])
        with self.assertRaises(ValueError):
            DIAG.arm_order(3)
        runner = DIAG.DiagnosticRunner.__new__(DIAG.DiagnosticRunner)
        runner.args, runner.keys, runner.budget, runner.rate = args, 1_000_000, 9_500_000, 112
        command = runner.command("run", (self.ssd, self.hdd), mode="adaptive", workload="uniform",
                                 budget=runner.budget, rate=runner.rate, warmup=30, measured=30, saturated=15)
        for flag in ("--num=1000000", "--value_size=1024", "--metabypass_sst_cache_bytes=65536",
                     "--metabypass_queue_capacity=2097152", "--metabypass_sst_target_ops_per_sec=112",
                     "--metabypass_sst_warmup_seconds=30", "--metabypass_sst_measure_seconds=30",
                     "--metabypass_sst_saturated_seconds=15"):
            self.assertIn(flag, command)

    def test_independent_clone_matches_common_closed_placement_and_allocation(self):
        source = self.fixture()
        baseline = DIAG.placement_snapshot(source)
        destination = (self.root / "clone-ssd", self.root / "clone-hdd")
        RUN.clone_dataset(zip(source, destination))
        actual = DIAG.placement_snapshot(destination)
        DIAG.require_same_start(baseline, actual)
        self.assertEqual([1], actual["hot_tables"])
        self.assertEqual([2], actual["cold_tables"])
        self.assertNotEqual((source[0] / "index" / "000001.sst").stat().st_ino,
                            (destination[0] / "index" / "000001.sst").stat().st_ino)
        for key, change in (("placement_sha256", "changed"), ("hot_tables", [2]),
                            ("ssd_sst_allocated_bytes", 0), ("identity_sha256", "other")):
            mismatched = copy.deepcopy(actual)
            mismatched[key] = change
            with self.subTest(key=key), self.assertRaises(ValueError):
                DIAG.require_same_start(baseline, mismatched)

    def test_invalid_placement_or_missing_cold_object_is_rejected(self):
        self.fixture()
        (self.hdd / "backup" / "sst-store" / "1-2-88.sst").unlink()
        with self.assertRaises(ValueError):
            DIAG.placement_snapshot((self.ssd, self.hdd))

    def test_migration_deltas_use_previous_phase_end_including_saturation(self):
        deltas = DIAG.phase_deltas(events((1, 3, 8)))
        self.assertIsNone(deltas["warmup"]["delta"])
        self.assertEqual(2, deltas["measured"]["delta"]["demotions"])
        self.assertEqual(5, deltas["saturated"]["delta"]["demotions"])
        self.assertEqual(2_000_000, deltas["saturated"]["start_monotonic_us"])
        for broken in (events((1, 0, 8)), events()[:-1], events() + [events()[-1]]):
            with self.subTest(broken=broken), self.assertRaises(ValueError):
                DIAG.phase_deltas(broken)

    def test_static_allows_observations_but_rejects_actual_copy_and_migration(self):
        snapshot = DIAG.placement_snapshot(self.fixture())
        DIAG.require_static(events(), snapshot, snapshot)
        for key in DIAG.MIGRATION_COUNTERS:
            actual = events()
            for event in actual:
                if event.get("event") == "phase_end":
                    event["stats"][key] = 1
            with self.subTest(key=key), self.assertRaises(ValueError):
                DIAG.require_static(actual, snapshot, snapshot)
        changed = copy.deepcopy(snapshot)
        changed["ssd_sst_allocated_bytes"] += 4096
        with self.assertRaises(ValueError):
            DIAG.require_static(events(), snapshot, changed)
        startup_copy = events()
        for event in startup_copy:
            if event.get("event") == "phase_end":
                event["stats"]["copied_bytes"] = 4096
        DIAG.require_all_ssd_observe(startup_copy)
        with self.assertRaises(ValueError):
            DIAG.require_static(startup_copy, snapshot, snapshot)
        for phase in ("measured", "saturated"):
            formal_copy = copy.deepcopy(startup_copy)
            for event in formal_copy:
                if event.get("event") == "phase_end" and \
                        list(DIAG.FORMAL_DURATIONS).index(event["phase"]) >= list(DIAG.FORMAL_DURATIONS).index(phase):
                    event["stats"]["copied_bytes"] += 4096
            with self.subTest(phase=phase), self.assertRaises(ValueError):
                DIAG.require_all_ssd_observe(formal_copy)

    def test_dynamic_coverage_requires_migrations_in_each_formal_phase(self):
        for counts, expected in (((1, 3, 8), True), ((1, 1, 8), False), ((1, 3, 3), False)):
            with self.subTest(counts=counts):
                coverage = DIAG.dynamic_coverage(DIAG.phase_deltas(events(counts)))
                self.assertEqual(expected, coverage["covered"])
                self.assertEqual((counts[2] - counts[1]) * 2,
                                 coverage["phases"]["saturated"]["actual_migrations"])

    def test_phase_io_does_not_mix_measured_and_saturated_samples(self):
        path = self.root / "samples.jsonl"
        records = []
        for stamp, read_bytes in ((1.1, 100), (1.9, 200), (2.1, 300), (2.9, 700)):
            counters = [0] * 11
            counters[2], counters[9] = read_bytes, int(stamp * 1000)
            records.append({"monotonic_s": stamp, "proc_io": {"read_bytes": read_bytes},
                            "device_io": {"sdc": {"counters": counters}}})
        path.write_text("".join(json.dumps(r) + "\n" for r in records))
        io = DIAG.phase_io(path, DIAG.phase_deltas(events()))
        self.assertEqual(100, io["measured"]["proc_io_delta"]["read_bytes"])
        self.assertEqual(400, io["saturated"]["proc_io_delta"]["read_bytes"])
        self.assertEqual("insufficient_samples", io["warmup"]["status"])

    def test_reclaim_failcnt_is_recorded_without_rejecting_success(self):
        runner = DIAG.DiagnosticRunner.__new__(DIAG.DiagnosticRunner)
        runner.args = SimpleNamespace(workload="uniform")
        runner.budget, runner.rate = 10, 112
        runner.deadline = RUN.Deadline(1800)
        successful = {"valid": True, "events": [], "oom_kill_delta": 0, "memory_failcnt_delta": 1_000_000}
        with mock.patch.object(runner, "check_idle"), mock.patch.object(runner, "command", return_value=[]), \
                mock.patch.object(runner, "execute", return_value=successful), \
                mock.patch.object(RUN, "phase_validation", return_value=[]):
            result = runner.benchmark_round("test", (self.ssd, self.hdd), RUN.time.monotonic(),
                                            "observe", DIAG.FORMAL_DURATIONS)
        self.assertTrue(result["valid"])
        self.assertEqual(1_000_000, result["memory_failcnt_delta"])


if __name__ == "__main__":
    unittest.main()
