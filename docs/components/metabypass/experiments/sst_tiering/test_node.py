#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Meaningful node ownership, deadline, copy, space and report protocol tests."""

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


RUN = module("node_run")
SUMMARY = module("node_summary")
CACHE = module("node_cache")


class NodeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ssd, self.hdd = self.root / "ssd", self.root / "hdd"
        self.ssd.mkdir()
        self.hdd.mkdir()

    def ack(self, seq, stamp=100, size=1040):
        return {"seq": seq, "steady_time_us": stamp, "business_bytes": size}

    def test_deadline_denies_new_round_and_reserves_full_plan(self):
        deadline = RUN.Deadline(100, now=10)
        self.assertEqual(100, deadline.remaining(now=10))
        deadline.require(79, now=30)
        for cost, now in ((80, 30), (0, 110), (0, 111)):
            with self.subTest(cost=cost, now=now), self.assertRaises(RuntimeError):
                deadline.require(cost, now=now)

    def test_cleanup_requires_matching_owned_direct_child_and_success(self):
        owned = RUN.create_owned(self.ssd, "trial", "uuid")
        sibling = self.ssd / "old-experiment"
        sibling.mkdir()
        (sibling / "evidence").write_text("keep")
        self.assertFalse(RUN.cleanup_owned(owned, self.ssd, "uuid", False))
        self.assertTrue(owned.exists())
        with self.assertRaises(ValueError):
            RUN.cleanup_owned(owned, self.ssd, "different", True)
        with self.assertRaises(ValueError):
            RUN.cleanup_owned(owned, self.root, "uuid", True)
        alias = self.ssd / "alias"
        alias.symlink_to(owned)
        with self.assertRaises(ValueError):
            RUN.cleanup_owned(alias, self.ssd, "uuid", True)
        self.assertTrue(RUN.cleanup_owned(owned, self.ssd, "uuid", True))
        self.assertEqual("keep", (sibling / "evidence").read_text())

    def test_cleanup_does_not_reuse_existing_directory(self):
        RUN.create_owned(self.ssd, "trial", "uuid")
        with self.assertRaises(FileExistsError):
            RUN.create_owned(self.ssd, "trial", "uuid")

    def test_kill_checks_starttime_and_process_group(self):
        identity = {"pid": 100, "starttime_ticks": 200}
        for current in ({"pid": 100, "starttime_ticks": 201, "pgrp": 100},
                        {"pid": 100, "starttime_ticks": 200, "pgrp": 99}, None):
            with mock.patch.object(RUN, "proc_identity", return_value=current), \
                    mock.patch.object(RUN.os, "killpg") as killer:
                self.assertFalse(RUN.kill_owned_group(identity))
                killer.assert_not_called()
        with mock.patch.object(RUN, "proc_identity", return_value={
                "pid": 100, "starttime_ticks": 200, "pgrp": 100}), \
                mock.patch.object(RUN.os, "killpg") as killer:
            self.assertTrue(RUN.kill_owned_group(identity))
            killer.assert_called_once_with(100, RUN.signal.SIGKILL)

    def test_space_hardlink_deduplicates_device_inode_and_counts_paths(self):
        original = self.ssd / "one.sst"
        original.write_bytes(b"x" * 8192)
        os.link(original, self.hdd / "two.sst")
        (self.hdd / "ignored.sst").symlink_to(original)
        measured = RUN.space_snapshot(self.ssd, self.hdd)
        self.assertTrue(measured["complete"])
        self.assertEqual(8192, measured["logical_bytes"]["ssd_sst"])
        self.assertEqual(8192, measured["logical_bytes"]["hdd_sst"])
        self.assertEqual(1, measured["hardlinked_extra_paths"])
        physical = original.stat().st_blocks * 512
        self.assertEqual(physical, measured["physical_category_bytes"]["hdd_sst"])
        self.assertEqual(physical, measured["physical_category_bytes"]["ssd_sst"])
        expected = sum(p.stat().st_blocks * 512 for p in (self.ssd, self.hdd)) + physical
        self.assertEqual(expected, measured["physical_global_unique_bytes"])

    def test_space_missing_root_is_incomplete_never_savings_zero(self):
        measured = RUN.space_snapshot(self.ssd, self.hdd / "missing")
        self.assertFalse(measured["complete"])
        self.assertGreater(measured["scan_error_count"], 0)

    def test_clone_reconstructs_internal_hardlinks_without_seed_sharing(self):
        source = self.ssd / "one.sst"
        source.write_bytes(b"original")
        source.chmod(0o444)
        os.link(source, self.hdd / "linked.sst")
        dest1, dest2 = self.root / "copyssd", self.root / "copyhdd"
        result = RUN.clone_dataset(((self.ssd, dest1), (self.hdd, dest2)))
        self.assertEqual(1, result["reconstructed_internal_links"])
        self.assertEqual(8, result["ordinary_copy_bytes"])
        self.assertNotEqual(source.stat().st_ino, (dest1 / "one.sst").stat().st_ino)
        self.assertEqual((dest1 / "one.sst").stat().st_ino, (dest2 / "linked.sst").stat().st_ino)
        (dest1 / "one.sst").write_bytes(b"changed")
        self.assertEqual(b"original", source.read_bytes())
        dest3, dest4 = self.root / "secondssd", self.root / "secondhdd"
        RUN.clone_dataset(((self.ssd, dest3), (self.hdd, dest4)))
        self.assertNotEqual((dest1 / "one.sst").stat().st_ino, (dest3 / "one.sst").stat().st_ino)

    def test_fault_deadline_uses_driver_ready_despite_controller_lag(self):
        event = {"steady_time_us": 1050000}
        ready = RUN.ready_clock(event, 1.0, 1.75)
        self.assertEqual(61.05, ready + 60)
        self.assertNotEqual(61.75, ready + 60)
        for stamp in (999998, 1750002, "1050000", float("nan")):
            with self.subTest(stamp=stamp), self.assertRaises(ValueError):
                RUN.ready_clock({"steady_time_us": stamp}, 1.0, 1.75)

    def test_rpo_zero_loss_and_marker_ahead_map_last_actual_ack(self):
        acks = [self.ack(1, 100), self.ack(2, 200)]
        for marker in (2, 3):
            with self.subTest(marker=marker):
                result = RUN.rpo_result(acks, marker, 300)
                self.assertEqual(0, result["rpo_us"])
                self.assertEqual(0, result["lost_ack_batches"])
                self.assertEqual(2, result["last_recovered_ack"]["seq"])
                self.assertEqual(marker > 2, result["marker_beyond_last_ack"])

    def test_rpo_lost_tail_uses_last_recovered_ack_not_first_lost(self):
        acks = [self.ack(1, 100), self.ack(2, 200), self.ack(3, 250)]
        result = RUN.rpo_result(acks, 1, 300)
        self.assertEqual(200, result["rpo_us"])
        self.assertEqual(2, result["lost_ack_batches"])
        self.assertEqual(2080, result["lost_business_bytes"])
        self.assertEqual(2, result["first_lost_ack"]["seq"])
        self.assertEqual(3, result["last_lost_ack"]["seq"])
        result = RUN.rpo_result(acks, 0, 300)
        self.assertIsNone(result["rpo_us"])
        self.assertEqual(200, result["rpo_lower_bound_us"])

    def test_histogram_merge_uses_counts_and_rejects_schema_mismatch(self):
        histogram = lambda buckets: {"buckets": buckets, "count": sum(buckets), "max_us": 7}
        result = SUMMARY.merge_histograms([histogram([99, 0, 1]), histogram([0, 0, 100])], [1, 4, 8])
        self.assertEqual(200, result["count"])
        self.assertEqual(8, result["p50_us"])
        self.assertEqual(8, result["p99_us"])
        self.assertEqual(7, result["max_us"])
        self.assertIsNone(SUMMARY.merge_histograms([histogram([0, 0, 0])], [1, 4, 8])["p99_us"])
        with self.assertRaises(ValueError):
            SUMMARY.merge_histograms([histogram([1, 2])], [1, 4, 8])
        with self.assertRaises(ValueError):
            SUMMARY.merge_histograms([{"buckets": [1], "count": 2}], [1])

    def placement_fixture(self):
        index, backup = self.ssd / "index", self.hdd / "backup"
        index.mkdir()
        (backup / "sst-store").mkdir(parents=True)
        (index / "IDENTITY").write_text("identity\n")
        (index / "000001.sst").write_bytes(b"h" * 4096)
        (index / "000002.sst").write_bytes(b"retired" * 1024)
        (index / "000003.sst").write_bytes(b"u" * 4096)
        (backup / "sst-store" / "1-1-10.sst").write_bytes(b"protected copy" * 1024)
        (backup / "sst-store" / "1-2-20.sst").write_bytes(b"c" * 8192)
        data = b"MBS1 identity\n1 4096 10 1 1-1-10.sst\n2 8192 20 0 1-2-20.sst\n"
        (index / "SST-PLACEMENT").write_bytes(data + b"CRC " + str(CACHE.crc32c(data)).encode() + b"\n")
        stats = {"protected_bytes": 12288, "unprotected_bytes": 4096}
        return index, backup, stats

    def test_crc32c_and_placement_reject_bad_crc_identity_duplicate_and_path(self):
        self.assertEqual(0xe3069283, CACHE.crc32c(b"123456789"))
        def encode(data):
            return data + b"CRC " + str(CACHE.crc32c(data)).encode() + b"\n"
        good = b"MBS1 owner\n1 10 22 1 1-1-22.sst\n"
        self.assertTrue(CACHE.parse_placement(encode(good), "owner")[1]["hot"])
        for data, identity in ((good, "different"), (good + good.split(b"\n")[1] + b"\n", "owner"),
                               (b"MBS1 owner\n1 10 22 0 ../outside.sst\n", "owner")):
            with self.subTest(data=data), self.assertRaises(ValueError):
                CACHE.parse_placement(encode(data), identity)
        with self.assertRaises(ValueError):
            CACHE.parse_placement(encode(good).replace(b"10 22", b"11 22"), "owner")

    def test_residency_routes_hot_cold_and_unprotected_once_per_table(self):
        index, backup, stats = self.placement_fixture()
        routed, _signatures, _placement, expected = CACHE.routed_tables(index, backup, "adaptive", stats)
        self.assertEqual({1, 2, 3}, set(routed))
        self.assertEqual(str(index / "000001.sst"), routed[1]["path"])
        self.assertEqual(str(backup / "sst-store" / "1-2-20.sst"), routed[2]["path"])
        self.assertEqual("ssd_unprotected", routed[3]["source"])
        self.assertEqual(16384, expected)
        def measured(path, signature):
            return {"logical_bytes": signature[2], "resident_bytes": signature[2] // 2,
                    "resident_fraction": 0.5, "pages": signature[2] // 4096}
        with mock.patch.object(CACHE, "mincore_file", side_effect=measured) as query:
            result = CACHE.sample_residency(index, backup, "adaptive", stats)
        self.assertTrue(result["complete"])
        self.assertEqual(3, query.call_count)
        self.assertEqual(16384, result["covered_logical_bytes"])
        self.assertEqual(0.5, result["resident_fraction"])
        queried = [str(call.args[0]) for call in query.call_args_list]
        self.assertNotIn(str(index / "000002.sst"), queried)
        self.assertNotIn(str(backup / "sst-store" / "1-1-10.sst"), queried)

    def test_residency_race_and_unmatched_retired_tables_are_partial(self):
        index, backup, stats = self.placement_fixture()
        with mock.patch.object(CACHE, "mincore_file", side_effect=FileNotFoundError("racing unlink")):
            result = CACHE.sample_residency(index, backup, "adaptive", stats)
        self.assertFalse(result["complete"])
        self.assertIsNone(result["resident_fraction"])
        self.assertEqual(3, len(result["errors"]))
        (index / "000004.sst").write_bytes(b"retired or unpublished")
        result = CACHE.sample_residency(index, backup, "adaptive", stats)
        self.assertFalse(result["complete"])
        self.assertIsNone(result["resident_fraction"])
        self.assertIn("differs from live-byte", result["errors"][0]["error"])

    def test_linux_mincore_does_not_write_or_read_data_to_measure_pages(self):
        if sys.platform != "linux":
            self.skipTest("Linux mincore")
        source = self.ssd / "immutable.sst"
        data = b"a" * 5000
        source.write_bytes(data)
        source.chmod(0o444)
        before = CACHE.file_signature(source)
        with mock.patch.object(CACHE.os, "read", side_effect=AssertionError("must not read SST data")):
            result = CACHE.mincore_file(source, before)
        self.assertEqual(5000, result["logical_bytes"])
        self.assertGreaterEqual(result["resident_bytes"], 0)
        self.assertLessEqual(result["resident_bytes"], 5000)
        self.assertEqual(before, CACHE.file_signature(source))
        self.assertEqual(data, source.read_bytes())

    def test_cache_gate_requires_postwarm_complete_highcoverage_nonfull_samples(self):
        samples = self.root / "cache-samples.jsonl"
        def cache(complete=True, coverage=1, fraction=0.5, stamp=200):
            return {"complete": complete, "coverage_fraction": coverage, "resident_fraction": fraction,
                    "started_monotonic_us": stamp}
        row = {"samples_path": str(samples), "events": [
            {"event": "phase_end", "phase": "warmup", "steady_time_us": 100},
            {"event": "phase_end", "phase": "saturated", "steady_time_us": 300}]}
        for measurement in (cache(stamp=50), cache(complete=False), cache(coverage=0.89), cache(fraction=0.91)):
            samples.write_text(json.dumps({"monotonic_s": 0.0002, "sst_cache": measurement,
                                           "proc_status": {"RssAnon": 1024}}) + "\n")
            self.assertFalse(RUN.cache_gate(row)["passed"])
        samples.write_text(json.dumps({"monotonic_s": 0.0002, "sst_cache": cache(),
                                       "proc_status": {"RssAnon": 1024}}) + "\n")
        result = RUN.cache_gate(row)
        self.assertTrue(result["passed"])
        self.assertEqual(1048576, result["minimum_saturated_anonymous_rss_bytes"])

    def test_nonfinite_protocol_json_is_rejected(self):
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                json.loads('{"elapsed_us":' + value + '}', parse_constant=RUN.reject_json_constant)

    def test_rate_and_paired_counterbalanced_order(self):
        self.assertEqual(70, RUN.choose_rate([200, 100]))
        self.assertEqual(1, RUN.choose_rate([1, 1]))
        for rates in ([0, 100], [float("nan"), 1], [1]):
            with self.assertRaises(ValueError):
                RUN.choose_rate(rates)
        plan = RUN.plan_order()
        self.assertEqual(18, len(plan))
        self.assertEqual([(1, "uniform", "disabled"), (1, "uniform", "adaptive")], plan[:2])
        self.assertEqual([(2, "uniform", "adaptive"), (2, "uniform", "disabled")], plan[6:8])

    def test_space_primary_scope_excludes_seed_and_run_scope_includes_it(self):
        trial_ssd = self.ssd / "trial"
        trial_hdd = self.hdd / "trial"
        trial_ssd.mkdir()
        trial_hdd.mkdir()
        (trial_hdd / "active.blob").write_bytes(b"x" * 8192)
        (self.hdd / "seed.blob").write_bytes(b"x" * 16384)
        result = RUN.space_snapshot(trial_ssd, trial_hdd, self.hdd, self.ssd)
        self.assertEqual(8192, result["logical_bytes"]["hdd_blob"])
        self.assertEqual(24576, result["run_scope"]["logical_bytes"]["hdd_blob"])

    def test_phase_validation_rejects_truncated_and_bad_histogram(self):
        buckets = [1] + [0] * 192
        histogram = {"count": 1, "buckets": buckets}
        empty = {"count": 0, "buckets": [0] * 193}
        phase = {"event": "phase_end", "phase": "measured", "elapsed_us": 120_000_000,
                 "ok": True, "ops": 1,
                 "get": {"count": 1, "service": histogram, "response": histogram},
                 "put": {"count": 0, "service": empty, "response": empty},
                 "delete": {"count": 0, "service": empty, "response": empty}}
        row = {"events": [{"event": "histogram_schema", "upper_bounds_us": list(range(1, 194))}, phase]}
        self.assertEqual([], RUN.phase_validation(row, {"measured": 120}))
        phase["elapsed_us"] = 119_000_000
        self.assertTrue(RUN.phase_validation(row, {"measured": 120}))
        phase["elapsed_us"] = 120_000_000
        phase["get"]["response"] = {"count": 1, "buckets": [0] * 193}
        self.assertTrue(RUN.phase_validation(row, {"measured": 120}))

    def test_mechanism_requires_reads_and_phase_migration_or_settled_cold(self):
        warm = {"sampled_reads": 10, "demotions": 1}
        end = {"sampled_reads": 20, "demotions": 1, "protected_bytes": 100, "ssd_bytes": 50}
        row = {"events": [{"event": "phase_end", "phase": "warmup", "stats": warm},
                           {"event": "phase_end", "phase": "measured", "stats": end}]}
        result = RUN.mechanism_coverage(row)
        self.assertTrue(result["covered"])
        self.assertEqual("warmup_settled_cold", result["basis"])
        end["sampled_reads"] = 10
        self.assertFalse(RUN.mechanism_coverage(row)["covered"])
        end.update(sampled_reads=20, protected_bytes=50, ssd_bytes=100)
        self.assertFalse(RUN.mechanism_coverage(row)["covered"])
        end["demotions"] = 2
        self.assertEqual("measured_migration", RUN.mechanism_coverage(row)["basis"])

    def test_incomplete_exit_space_returns_unknown_not_zero(self):
        samples = self.root / "samples.jsonl"
        samples.write_text(json.dumps({"monotonic_s": 1, "phase": "measured", "space": {
            "complete": False, "physical_category_bytes": {}, "scan_cpu_s": 0,
            "scan_wall_s": 0}}) + "\n")
        result = SUMMARY.sampled_space({"samples_path": str(samples), "events": [],
                                       "exit_space": {"complete": False}})
        self.assertEqual(1, result["incomplete_runtime_space_samples"])
        self.assertIsNone(result["exit_ssd_sst_allocated_bytes"])
        self.assertIsNone(result["sampled_peak_ssd_sst_allocated_bytes"])

    def prepared_runner(self):
        cg = self.root / "cg"
        cg.mkdir()
        (cg / "tasks").write_text("")
        runner = RUN.Runner.__new__(RUN.Runner)
        runner.args = SimpleNamespace(cgroup=str(cg), space_interval=5, output=str(self.root / "result.json"))
        runner.run_id = "uuid"
        runner.deadline = RUN.Deadline(10)
        runner.calibration_end = runner.deadline.end
        runner.control = self.root / "control"
        runner.control.mkdir()
        runner.ssd, runner.hdd = self.ssd, self.hdd
        runner.devices = [self.ssd.stat().st_dev, self.hdd.stat().st_dev]
        runner.report = {"commands": []}
        runner.active = None
        return runner, cg

    def calibration_fake(self, gates, profile="legacy", sst_bytes=200 * 1024 * 1024):
        runner, _cg = self.prepared_runner()
        runner.args.keep_data = False
        runner.args.binary = sys.executable
        runner.args.profile = profile
        runner.report.update(calibration=[], trials=[], rpo_samples=[])
        runner.deadline = RUN.Deadline(14400)
        runner.calibration_end = runner.deadline.start + 1800
        runner.preflight = mock.Mock()
        runner.deadline.require = mock.Mock(side_effect=RuntimeError("formal sentinel"))
        commands = []
        def execute(name, command, dirs, **kwargs):
            commands.append((name, command, kwargs, runner.calibration_end))
            flags = dict(item[2:].split("=", 1) for item in command if item.startswith("--") and "=" in item)
            events = []
            if flags.get("metabypass_sst_action") == "run":
                events.append({"event": "histogram_schema", "upper_bounds_us": list(range(1, 194))})
                for phase_name, seconds in (("warmup", int(flags["metabypass_sst_warmup_seconds"])),
                                            ("measured", int(flags["metabypass_sst_measure_seconds"])),
                                            ("saturated", int(flags["metabypass_sst_saturated_seconds"]))):
                    count = int(seconds > 0)
                    hist = {"count": count, "buckets": [count] + [0] * 192}
                    empty = {"count": 0, "buckets": [0] * 193}
                    events.append({"event": "phase_end", "phase": phase_name, "elapsed_us": seconds * 1000000,
                        "ok": True, "ops": count, "get": {"count": count, "service": hist, "response": hist},
                        "put": {"count": 0, "service": empty, "response": empty},
                        "delete": {"count": 0, "service": empty, "response": empty},
                        "stats": {"sampled_reads": 10 if phase_name == "saturated" or phase_name == "measured" and seconds else 1,
                                  "demotions": 2 if phase_name == "saturated" or phase_name == "measured" and seconds else 1,
                                  "protected_bytes": 100, "ssd_bytes": 50}})
            sample_path = runner.control / (name + ".jsonl")
            sample_path.write_text("\n".join(json.dumps({"device_io": {
                "8:1": {"counters": [0, 0, count] + [0] * 8},
                "8:2": {"counters": [0, 0, count] + [0] * 8}}}) for count in (0, 1)) + "\n")
            elapsed = sum(e["elapsed_us"] for e in events if e.get("event") == "phase_end") / 1000000 + 27 if profile == "small" else 1
            return {"name": name, "valid": True, "events": events, "elapsed_s": elapsed,
                    "samples_path": str(sample_path), "exit_space": {"complete": True,
                    "file_paths": {"ssd_sst": 4}, "logical_bytes": {"ssd_sst": sst_bytes}}}
        runner.execute = execute
        cache_results = [{"passed": passed, "minimum_saturated_anonymous_rss_bytes": 1024 * 1024} for passed in gates]
        return runner, commands, cache_results

    def test_small_constructor_deadline_and_rejects_unchanged_96m_group(self):
        source = self.root / "source.json"
        source.write_text('{"head":"fake"}')
        cg = self.root / "private-cg"
        cg.mkdir()
        (cg / "tasks").write_text("")
        (cg / "memory.limit_in_bytes").write_text(str(96 * 1024 * 1024))
        (cg / "memory.swappiness").write_text("0")
        args = SimpleNamespace(profile="small", deadline_seconds=5400, ssd_root=str(self.ssd),
            hdd_root=str(self.hdd), output=str(self.root / "new.json"), binary=sys.executable,
            source_manifest=str(source), cgroup=str(cg), keep_data=False, space_interval=10)
        runner = RUN.Runner(args)
        self.assertEqual(5400, runner.deadline.end - runner.deadline.start)
        self.assertEqual(900, runner.calibration_end - runner.deadline.start)
        self.assertEqual("small", runner.report["profile"])
        with mock.patch.object(RUN.os, "sched_getaffinity", return_value=set(range(96))), \
                self.assertRaisesRegex(ValueError, "48 MiB"):
            runner.preflight()
        self.assertEqual(str(96 * 1024 * 1024), (cg / "memory.limit_in_bytes").read_text())
        runner.keys = 1000000
        command = runner.command("run", (runner.ssd, runner.hdd))
        self.assertIn("--metabypass_queue_capacity=2097152", command)
        self.assertIn("--metabypass_sst_warmup_seconds=30", command)
        self.assertIn("--metabypass_sst_measure_seconds=60", command)
        self.assertIn("--metabypass_sst_saturated_seconds=15", command)

    def test_timing_probe_marker_zero_does_not_claim_rpo_zero(self):
        runner, _cg = self.prepared_runner()
        runner.args.profile = "small"
        runner.args.binary = sys.executable
        runner.args.keep_data = False
        runner.keys, runner.budget, runner.rate = 1000000, 10000, 100
        runner.copy = mock.Mock(return_value={"elapsed_s": 1})
        writer = {"fault_injected": True, "timed_out": False, "parse_errors": [], "elapsed_s": 37}
        restore = {"valid": True, "elapsed_s": 62, "events": [
            {"event": "recovered", "recovered_marker": 0, "rto_us": 50000000}]}
        runner.execute = mock.Mock(side_effect=[writer, restore])
        with mock.patch.object(RUN, "phase_validation", return_value=[]), \
                mock.patch.object(RUN, "rpo_result", side_effect=AssertionError("probe must not compute RPO")):
            row = runner.recovery_sample((runner.ssd, runner.hdd), "timing-probe", "adaptive", 10, 10,
                                        100, 900, calibration=True, timing_only=True)
        self.assertTrue(row["valid"])
        self.assertTrue(row["timing_only"])
        self.assertFalse(row["included_in_formal_rpo"])
        self.assertEqual(0, row["recovered_marker"])
        self.assertNotIn("rpo_us", row)
        self.assertEqual(10, row["read_seconds"])
        self.assertTrue(all(call.kwargs["calibration"] for call in runner.execute.call_args_list))
        restore_command = runner.execute.call_args_list[1].args[1]
        self.assertIn("--metabypass_sst_warmup_seconds=10", restore_command)

    def test_small_fixed_profile_limits_and_actual_forecast(self):
        small = RUN.PROFILES["small"]
        self.assertEqual(1000000, small["keys"])
        self.assertEqual(48 * 1024 * 1024, small["memory_bytes"])
        self.assertEqual(2 * 1024 * 1024, small["queue_bytes"])
        self.assertEqual((5400, 900, 30, 60, 15), tuple(small[k] for k in
                         ("deadline_s", "calibration_s", "warmup_s", "measured_s", "saturated_s")))
        self.assertAlmostEqual(472.086, RUN.round_forecast([
            {"preparation_s": 77.526, "elapsed_s": 244.560}], 90, 210))
        cost = RUN.recovery_forecast(13, 27, 52, 90)
        self.assertEqual(272, cost["total_s"])
        self.assertEqual(147, cost["writer_timeout_s"])
        self.assertEqual(142, cost["restore_timeout_s"])
        with self.assertRaises(ValueError):
            RUN.recovery_forecast(1, float("nan"), 2, 60)

    def test_small_no_cache_evidence_blocks_without_growth_or_probe(self):
        runner, commands, gates = self.calibration_fake([False, False], "small", 1024 * 1024)
        runner.recovery_sample = mock.Mock()
        with mock.patch.object(RUN, "cache_gate", side_effect=gates), self.assertRaisesRegex(RuntimeError, "small cache evidence"):
            runner.run()
        self.assertEqual(["seed-1000000"], [name for name, _cmd, _kw, _end in commands if name.startswith("seed-")])
        self.assertEqual(1, sum(name == "cal-1000000-observe" for name, _cmd, _kw, _end in commands))
        runner.recovery_sample.assert_not_called()
        self.assertEqual([], runner.report["trials"])
        self.assertEqual(1000000, runner.keys)

    def test_small_probe_budget_full_matrix_and_per_trial_cache_gate(self):
        cache_results = [{"passed": True, "minimum_saturated_anonymous_rss_bytes": 1024 * 1024,
                          "measurement_status": "non_full_cache_observed", "cache_dominated": False}
                         for _ in range(20)]
        cache_results[3] = {**cache_results[3], "passed": False, "measurement_status": "cache_dominated", "cache_dominated": True}
        runner, commands, _gates = self.calibration_fake([], "small", 1024 * 1024)
        runner.deadline.require = mock.Mock()
        runner.recovery_sample = mock.Mock(side_effect=lambda *args, **kwargs: {
            "valid": True, "preparation_s": 13, "writer": {"elapsed_s": 37},
            "restore": {"elapsed_s": 62}, "timing_only": kwargs.get("timing_only", False),
            "mode": args[2], "cut_seconds": args[3], "read_seconds": args[4]})
        with mock.patch.object(RUN, "cache_gate", side_effect=cache_results) as gate:
            runner.run()
        self.assertEqual(18, len(runner.report["trials"]))
        self.assertEqual(6, len(runner.report["rpo_samples"]))
        self.assertTrue(runner.report["recovery_probe"]["timing_only"])
        self.assertEqual(7, runner.recovery_sample.call_count)
        probe = runner.recovery_sample.call_args_list[0]
        self.assertEqual(("adaptive", 10, 10), probe.args[2:5])
        self.assertTrue(probe.kwargs["calibration"])
        self.assertEqual([60, 75, 90, 60, 75, 90], [call.args[3] for call in runner.recovery_sample.call_args_list[1:]])
        self.assertTrue(all(call.args[4] == 60 for call in runner.recovery_sample.call_args_list[1:]))
        self.assertTrue(all(call.kwargs == {"phase": "measured"} for call in gate.call_args_list[2:]))
        self.assertEqual("complete_with_invalid_comparisons", runner.report["status"])
        self.assertEqual(17, sum(row["performance_valid"] for row in runner.report["trials"]))
        self.assertTrue(runner.report["calibration_pressure"]["accepted"])
        self.assertLess(runner.report["calibration_pressure"]["sst_to_available_cache_ratio"], 1.5)
        required = runner.report["budget_forecast"]["remaining_required_s"]
        self.assertEqual(required, runner.deadline.require.call_args_list[0].args[0])
        costs = [row["total_s"] for row in runner.report["budget_forecast"]["rpo_samples"]]
        self.assertEqual([sum(costs[i:]) for i in range(6)], [call.args[0] for call in runner.deadline.require.call_args_list[-6:]])
        bench = [cmd for _name, cmd, _kw, _end in commands if any(flag.startswith("--num=") for flag in cmd)]
        self.assertTrue(all("--num=1000000" in cmd and "--metabypass_queue_capacity=2097152" in cmd for cmd in bench))
        self.assertTrue(all("--metabypass_sst_cache_bytes=65536" in cmd for cmd in bench))

    def test_partial_sample_log_retains_prior_sample_and_reports_missing(self):
        path = self.root / "partial.jsonl"
        path.write_text('{"monotonic_s":1}\n{"monotonic_s":')
        samples, errors = SUMMARY.load_samples(path)
        self.assertEqual([{"monotonic_s": 1}], samples)
        self.assertEqual(2, errors[0]["line"])
        measured = SUMMARY.sampled_space({"samples_path": str(path), "events": [], "exit_space": {"complete": False}})
        self.assertTrue(measured["sample_log_errors"])
        self.assertIsNone(measured["exit_ssd_sst_allocated_bytes"])
        path.unlink()
        samples, errors = SUMMARY.load_samples(path)
        self.assertEqual([], samples)
        self.assertTrue(errors)

    def test_malformed_histogram_preserves_valid_sibling_phase(self):
        histogram = {"count": 1, "buckets": [1, 0], "max_us": 1}
        empty = {"count": 0, "buckets": [0, 0], "max_us": 0}
        phase = {"event": "phase_end", "phase": "saturated", "elapsed_us": 1000000,
                 "ops": 1, "get": {"count": 1, "service": histogram, "response": histogram},
                 "put": {"count": 0, "service": empty, "response": empty},
                 "delete": {"count": 0, "service": empty, "response": empty}}
        broken = {**phase, "phase": "measured", "get": {"count": 1, "service": {"count": 2, "buckets": [1, 0]}, "response": histogram}}
        events = [{"event": "histogram_schema", "upper_bounds_us": [1, 2]}, broken, phase]
        self.assertFalse(SUMMARY.phase_result(events, "measured")["valid"])
        self.assertIsNone(SUMMARY.phase_result(events, "measured")["throughput_ops_s"])
        sibling = SUMMARY.phase_result(events, "saturated")
        self.assertTrue(sibling["valid"])
        self.assertEqual(1, sibling["throughput_ops_s"])

    def test_calibration_cache_failure_retries_once_6m_with_same_deadline(self):
        runner, commands, gates = self.calibration_fake([False, False, True, True])
        with mock.patch.object(RUN, "cache_gate", side_effect=gates), self.assertRaisesRegex(RuntimeError, "formal sentinel"):
            runner.run()
        seeds = [name for name, _cmd, _kw, _end in commands if name.startswith("seed-")]
        self.assertEqual(["seed-4000000", "seed-6000000"], seeds)
        self.assertEqual([False, True], [p["accepted"] for p in runner.report["calibration_attempts"]])
        self.assertTrue(all(kw.get("calibration") for _name, _cmd, kw, _end in commands))
        self.assertEqual({runner.calibration_end}, {end for _name, _cmd, _kw, end in commands})
        self.assertTrue((runner.ssd / "cal-4000000-disabled").exists())
        self.assertTrue((runner.ssd / "cal-4000000-adaptive").exists())
        self.assertFalse((runner.ssd / "cal-6000000-disabled").exists())
        self.assertEqual(6_000_000, runner.keys)

    def test_calibration_6m_cache_failure_blocks_formal_without_third_seed(self):
        runner, commands, gates = self.calibration_fake([False, False, False, False])
        with mock.patch.object(RUN, "cache_gate", side_effect=gates), self.assertRaisesRegex(RuntimeError, "6M calibration"):
            runner.run()
        self.assertEqual(2, len([name for name, _cmd, _kw, _end in commands if name.startswith("seed-")]))
        runner.deadline.require.assert_not_called()
        self.assertEqual([], runner.report["trials"])

    def test_execute_timeout_kills_owned_group_and_preserves_sample(self):
        runner, _cg = self.prepared_runner()
        fake = self.root / "blocked.py"
        fake.write_text("import signal; signal.pause()\n")
        row = runner.execute("timeout", [sys.executable, str(fake)], (self.ssd, self.hdd), timeout=0.2)
        self.assertTrue(row["timed_out"])
        self.assertFalse(row["valid"])
        self.assertEqual(-9, row["exit_code"])
        self.assertTrue(Path(row["samples_path"]).exists())
        self.assertIsNone(RUN.proc_identity(row["process_identity"]["pid"]))

    def test_helper_affinity_is_set_before_tasks_and_exec_without_pid_change(self):
        _runner, cg = self.prepared_runner()
        operations = []
        effective = set(range(96))
        def bind(pid, cpus):
            self.assertEqual(0, pid)
            self.assertEqual("", (cg / "tasks").read_text())
            operations.append("affinity")
            effective.clear()
            effective.update(cpus)
        def execute(binary, arguments):
            self.assertEqual(str(os.getpid()), (cg / "tasks").read_text())
            operations.append("exec")
            self.assertEqual(("benchmark", ["benchmark", "--flag=value"]), (binary, arguments))
        with mock.patch.object(RUN.os, "sched_getaffinity", side_effect=lambda _pid: effective), \
                mock.patch.object(RUN.os, "sched_setaffinity", side_effect=bind), \
                mock.patch.object(RUN.os, "execvp", side_effect=execute), \
                mock.patch("builtins.print") as output:
            RUN.cgroup_exec([str(cg / "tasks"), "--cpu-list", "0-3", "--", "benchmark", "--flag=value"])
        self.assertEqual(["affinity", "exec"], operations)
        evidence = json.loads(output.call_args.args[0][12:])
        self.assertEqual([0, 1, 2, 3], evidence["child_effective_cpu_list"])
        self.assertTrue(evidence["applied_before_cgroup"])

    def test_helper_disallowed_or_malformed_cpu_list_never_writes_tasks(self):
        _runner, cg = self.prepared_runner()
        cases = ([str(cg / "tasks"), "--cpu-list", "0-3", "--", "benchmark"],
                 [str(cg / "tasks"), "--cpu-list", "1;bad", "--", "benchmark"],
                 [str(cg / "tasks"), "--cpu-list", "4-3", "--", "benchmark"],
                 [str(cg / "tasks"), "--cpu-list", "4", "--unknown", "benchmark"],
                 [str(cg / "tasks"), "--cpu-list", "4", "--"], [])
        with mock.patch.object(RUN.os, "sched_getaffinity", return_value={4, 5}), \
                mock.patch.object(RUN.os, "sched_setaffinity") as bind, \
                mock.patch.object(RUN.os, "execvp") as execute:
            for arguments in cases:
                with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                    RUN.cgroup_exec(arguments)
                self.assertEqual("", (cg / "tasks").read_text())
            bind.assert_not_called()
            execute.assert_not_called()

    def test_disallowed_small_cpus_block_preflight_and_execute_before_launch(self):
        runner, cg = self.prepared_runner()
        runner.args.profile = "small"
        with mock.patch.object(RUN.os, "sched_getaffinity", return_value={4, 5, 6, 7}), \
                mock.patch.object(RUN.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "outside parent allowed"):
                runner.preflight()
            with self.assertRaisesRegex(ValueError, "outside parent allowed"):
                runner.execute("not-started", ["benchmark"], (self.ssd, self.hdd))
            launch.assert_not_called()
        self.assertEqual([], runner.report["commands"])
        self.assertEqual("", (cg / "tasks").read_text())

    def test_legacy_default_inherits_affinity_without_binding(self):
        _runner, cg = self.prepared_runner()
        with mock.patch.object(RUN.os, "sched_getaffinity", return_value={4, 5}), \
                mock.patch.object(RUN.os, "sched_setaffinity") as bind, \
                mock.patch.object(RUN.os, "execvp") as execute, mock.patch("builtins.print"):
            self.assertIsNone(RUN.cpu_affinity("legacy")["requested_cpu_list"])
            RUN.cgroup_exec([str(cg / "tasks"), "--", "benchmark"])
            bind.assert_not_called()
            execute.assert_called_once_with("benchmark", ["benchmark"])

    def test_affinity_failure_or_effective_mismatch_never_joins_cgroup(self):
        _runner, cg = self.prepared_runner()
        for failure in (OSError("denied"), None):
            with self.subTest(failure=failure), \
                    mock.patch.object(RUN.os, "sched_getaffinity", return_value={0, 1, 2, 3, 4}), \
                    mock.patch.object(RUN.os, "sched_setaffinity", side_effect=failure), \
                    mock.patch.object(RUN.os, "execvp") as execute, \
                    self.assertRaises((OSError, ValueError)):
                RUN.cgroup_exec([str(cg / "tasks"), "--cpu-list", "0-3", "--", "benchmark"])
            self.assertEqual("", (cg / "tasks").read_text())
            execute.assert_not_called()

    def test_small_exec_actual_affinity_and_child_thread_inheritance(self):
        runner, cg = self.prepared_runner()
        runner.args.profile = "small"
        cpu = min(os.sched_getaffinity(0))
        runner.args.cpu_list = str(cpu)
        fake = self.root / "affinity.py"
        fake.write_text('import json, os, threading\n'
            'seen=[]\n'
            'def inspect(): seen.append(sorted(os.sched_getaffinity(0)))\n'
            'thread=threading.Thread(target=inspect); thread.start(); thread.join()\n'
            'print("MB_SST_JSON " + json.dumps({"protocol":"node_v1","event":"summary",'
            '"ok":True,"main_cpus":sorted(os.sched_getaffinity(0)),"thread_cpus":seen}),flush=True)\n')
        row = runner.execute("affinity", [sys.executable, str(fake)], (self.ssd, self.hdd))
        self.assertTrue(row["valid"])
        self.assertEqual([cpu], row["cpu_affinity"]["child_effective_cpu_list"])
        summary = next(event for event in row["events"] if event["event"] == "summary")
        self.assertEqual([cpu], summary["main_cpus"])
        self.assertEqual([[cpu]], summary["thread_cpus"])
        self.assertEqual(str(row["process_identity"]["pid"]), (cg / "tasks").read_text())
        self.assertEqual([cpu], runner.report["source_identity"]["cpu_affinity"]["child_effective_cpu_list"])

    def test_kmem_missing_files_remain_unknown_in_samples_and_summary(self):
        _runner, cg = self.prepared_runner()
        snapshot = RUN.sample_cgroup(cg, include_slabinfo=True)
        self.assertEqual("unknown", snapshot["kmem_telemetry_status"])
        for key in ("memory.kmem.usage_in_bytes", "memory.kmem.max_usage_in_bytes", "memory.kmem.failcnt"):
            self.assertIsNone(snapshot[key])
        self.assertEqual("unknown", snapshot["slabinfo"]["status"])
        self.assertIsNone(snapshot["slabinfo"]["raw"])
        summary = SUMMARY.sampled_memory({"cgroup_start": snapshot, "cgroup_end": snapshot}, [{"cgroup": snapshot}])
        self.assertEqual("unknown", summary["kmem_measurement_status"])
        for key in ("baseline_kmem_usage_bytes", "sampled_peak_kmem_usage_bytes",
                    "sampled_peak_kmem_increase_bytes", "kmem_failcnt_delta", "sampled_peak_fd_count"):
            self.assertIsNone(summary[key])

    def test_kmem_baseline_residual_counts_with_empty_tasks_and_no_counter_reset(self):
        runner, cg = self.prepared_runner()
        baseline = {"memory.usage_in_bytes": 30 * 1048576, "memory.limit_in_bytes": 48 * 1048576,
                    "memory.kmem.usage_in_bytes": 29 * 1048576,
                    "memory.kmem.max_usage_in_bytes": 45 * 1048576, "memory.kmem.failcnt": 11}
        for key, value in baseline.items():
            (cg / key).write_text(str(value))
        (cg / "memory.kmem.slabinfo").write_text("slabinfo - version: 2.1\nradix_tree_node ...\n")
        snapshot = RUN.sample_cgroup(cg, include_slabinfo=True)
        budget = RUN.memory_budget(snapshot)
        self.assertEqual(18 * 1048576, budget["remaining_headroom_bytes"])
        self.assertEqual(29 * 1048576, budget["baseline_kmem_usage_bytes"])
        self.assertEqual("", (cg / "tasks").read_text())
        end = {**snapshot, "memory.kmem.usage_in_bytes": 30 * 1048576, "memory.kmem.failcnt": 13}
        summary = SUMMARY.sampled_memory({"cgroup_start": snapshot, "cgroup_end": end,
            "memory_budget_start": budget}, [{"cgroup": {**snapshot, "memory.kmem.usage_in_bytes": 34 * 1048576}}])
        self.assertEqual(34 * 1048576, summary["sampled_peak_kmem_usage_bytes"])
        self.assertEqual(5 * 1048576, summary["sampled_peak_kmem_increase_bytes"])
        self.assertEqual(45 * 1048576, summary["cgroup_lifetime_max_kmem_usage_bytes"])
        self.assertEqual(2, summary["kmem_failcnt_delta"])
        self.assertEqual("11", (cg / "memory.kmem.failcnt").read_text())

    def test_proc_resources_current_and_missing_process_preserve_unknown(self):
        evidence = RUN.proc_resources(os.getpid())
        self.assertEqual("known", evidence["status"])
        self.assertGreater(evidence["fd_count"], 0)
        self.assertGreaterEqual(evidence["thread_count"], 1)
        self.assertEqual(sorted(os.sched_getaffinity(0)), evidence["allowed_cpu_list"])
        for thread in evidence["threads"]:
            self.assertIsInstance(thread["last_processor_cpu"], int)
            self.assertTrue(thread["allowed_cpu_list"])
        missing = RUN.proc_resources(2147483647)
        self.assertEqual("unknown", missing["status"])
        self.assertIsNone(missing["fd_count"])
        self.assertIsNone(missing["thread_count"])
        self.assertIsNone(missing["allowed_cpu_list"])

    def test_execute_fault_preserves_external_ack_and_cgroup_helper_pid(self):
        runner, cg = self.prepared_runner()
        fake = self.root / "fake.py"
        fake.write_text('import json, signal, time\n'
            'def emit(e):\n'
            ' e["protocol"]="node_v1"; print("MB_SST_JSON " + json.dumps(e), flush=True)\n'
            'ready={"protocol":"node_v1","event":"ready","steady_time_us":time.monotonic_ns()//1000}\n'
            'ack={"protocol":"node_v1","event":"ack","seq":1,"steady_time_us":time.monotonic_ns()//1000,"business_bytes":1040}\n'
            'print("MB_SST_JSON "+json.dumps(ready)+"\\nMB_SST_JSON "+json.dumps(ack),flush=True)\n'
            'signal.pause()\n')
        row = runner.execute("fault", [sys.executable, str(fake)], (self.ssd, self.hdd), fault_seconds=0)
        self.assertTrue(row["fault_injected"])
        self.assertFalse(row["timed_out"])
        self.assertEqual(-9, row["exit_code"])
        self.assertEqual(str(row["process_identity"]["pid"]), (cg / "tasks").read_text())
        acks = [json.loads(line) for line in Path(row["acks_path"]).read_text().splitlines()]
        self.assertEqual(1, len(acks))
        self.assertEqual(1040, acks[0]["business_bytes"])
        self.assertTrue(Path(row["stdout_path"]).read_text().startswith("MB_SST_JSON "))


if __name__ == "__main__":
    unittest.main()
