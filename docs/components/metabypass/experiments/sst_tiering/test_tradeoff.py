#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Frozen tradeoff command, seed, phase evidence and failure protocol tests."""

import copy
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import node_run as RUN
import node_tradeoff as TRADE


def protocol_events(workload="uniform", capacity=1000, mixed=True, sampled=(10, 20, 30)):
    result = [{"event": "histogram_schema", "upper_bounds_us": list(range(1, 194))},
              {"event": "ready", "steady_time_us": 1}]
    stamp = 1
    for index, (phase, seconds) in enumerate(TRADE.DURATIONS[workload].items()):
        ops = seconds * TRADE.TARGET_RATE if phase != "saturated" else 500
        def operation(count):
            hist = {"count": count, "buckets": [count] + [0] * 192, "max_us": 1}
            return {"count": count, "service": copy.deepcopy(hist), "response": copy.deepcopy(hist)}
        counters = {key: 0 for key in TRADE.node_diagnose.MIGRATION_COUNTERS}
        counters.update(protected_bytes=1000 if mixed else 0, unprotected_bytes=0,
                        ssd_bytes=600 if mixed else 0, reserved_bytes=0, pending_delete_bytes=0,
                        sampled_reads=sampled[index], migration_errors=0, ssd_capacity_bytes=capacity,
                        hdd_bytes=1000 if mixed else 0)
        stamp += seconds * 1_000_000
        event = {"event": "phase_end", "phase": phase, "elapsed_us": seconds * 1_000_000,
                 "steady_time_us": stamp, "stats": counters, "ops": ops, "ok": True,
                 "target_ops_per_sec": TRADE.TARGET_RATE if phase != "saturated" else 0,
                 "scheduled_ops": ops, "unfinished_ops": 0, "late_ops": 0,
                 "late_fraction": 0 if phase != "saturated" else None,
                 "get": operation(ops), "put": operation(0), "delete": operation(0)}
        result += [{**copy.deepcopy(event), "event": "window"}, event]
        if workload == "switch" and phase == "measured":
            result.insert(-2, {"event": "hotspot_switch", "elapsed_us": seconds * 500_000})
    result.append({"event": "summary", "ok": True, "stats": copy.deepcopy(counters)})
    return result


class TradeoffTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ssd, self.hdd = self.root / "ssd", self.root / "hdd"
        self.ssd.mkdir()
        self.hdd.mkdir()
        self.seed = (self.ssd / "seed", self.hdd / "seed")
        for path in self.seed:
            path.mkdir()
        index = self.seed[0] / "index"
        index.mkdir()
        (index / "IDENTITY").write_text("immutable-owner\n")
        for number in range(1, 5):
            (index / f"{number:06}.sst").write_bytes(bytes([number]) * 4096)
        (self.seed[1] / "data").mkdir()
        (self.seed[1] / "data" / "blob").write_bytes(b"immutable blob")
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text("{}\n")
        self.cgroup = self.root / "cg"
        self.cgroup.mkdir()
        (self.cgroup / "tasks").write_text("")
        (self.cgroup / "memory.oom_control").write_text("oom_kill 0\nunder_oom 0\n")

    def arguments(self, *extra):
        values = {"binary": "/bin/true", "source-manifest": self.manifest,
                  "ssd-root": self.ssd, "hdd-root": self.hdd,
                  "seed-ssd": self.seed[0], "seed-hdd": self.seed[1],
                  "cgroup": self.cgroup, "output": self.root / "report.json"}
        arguments = [item for name, value in values.items() for item in ("--" + name, str(value))]
        return arguments + list(extra)

    def runner(self, *extra):
        return TRADE.TradeoffRunner(TRADE.parse_args(self.arguments(*extra)))

    def fake_execute(self, runner, failures=None, oom=None):
        calls, sources = [], []
        failures, oom = set(failures or ()), set(oom or ())

        def execute(name, command, dirs, timeout=480):
            calls.append((name, command, timeout))
            if "--clone" in command:
                split = command.index("--clone")
                source = tuple(map(Path, command[split + 1:split + 3]))
                sources.append(source)
                RUN.clone_dataset(zip(source, dirs))
                events = [{"event": "summary", "ok": name not in failures}]
            else:
                flags = dict(item[2:].split("=", 1) for item in command[1:])
                events = protocol_events(flags["metabypass_sst_workload"],
                                         int(flags["metabypass_sst_capacity_bytes"]),
                                         mixed=flags["metabypass_sst_mode"] == "adaptive")
            samples = runner.control / (name + ".samples.jsonl")
            samples.write_text("")
            return {"name": name, "command": command, "events": events,
                    "valid": name not in failures and name not in oom, "oom_kill_delta": int(name in oom),
                    "elapsed_s": 1, "exit_code": 0 if name not in failures else 1,
                    "timed_out": False, "samples_path": str(samples),
                    "exit_space": RUN.space_snapshot(*dirs)}

        return execute, calls, sources

    def test_default_cli_freezes_28_rounds_and_exact_reverse_repeat(self):
        args = TRADE.parse_args(self.arguments())
        self.assertEqual(("small", "0,1,2,3", True, 7200, 2),
                         (args.profile, args.cpu_list, args.keep_data, args.deadline_seconds, args.repeats))
        self.assertEqual(list(TRADE.DEFAULT_STRATEGIES), args.strategies)
        self.assertNotIn("conservative90", args.strategies)
        plan = TRADE.trial_plan(args.strategies, args.workloads, args.repeats)
        self.assertEqual(28, len(plan))
        pairs = lambda rows: [(r["workload"], r["strategy"]) for r in rows]
        self.assertEqual(list(reversed(pairs(plan[:14]))), pairs(plan[14:]))
        self.assertEqual({"warmup": 30, "measured": 45, "saturated": 15}, TRADE.DURATIONS["uniform"])
        self.assertEqual({"warmup": 45, "measured": 120, "saturated": 15}, TRADE.DURATIONS["switch"])

    def test_conservative90_remains_explicit_with_unchanged_parameters_and_effective81_budget(self):
        args = TRADE.parse_args(self.arguments("--strategies", "disabled_ssd", "conservative90",
                                              "--workloads", "uniform", "switch", "--repeats", "2"))
        self.assertEqual(8, len(TRADE.trial_plan(args.strategies, args.workloads, args.repeats)))
        configuration = TRADE.policy_configuration("conservative90", 10000)
        self.assertEqual("metabypass-sst-tradeoff-policies-v3", configuration["version"])
        self.assertEqual("metabypass-sst-tradeoff-v3", TRADE.PROTOCOL)
        self.assertEqual((90, 9000, 8100, 81), (configuration["capacity_percent"], configuration["capacity_bytes"],
                                              configuration["effective_budget_bytes"], configuration["effective_budget_percent"]))
        parameters = {**TRADE.DEFAULT_PARAMETERS, "demote_rounds": 8,
                      "min_residency_ms": 30000, "replacement_margin": 1.0}
        self.assertEqual(parameters, configuration["parameters"])

    def test_cli_filters_preserve_order_and_reject_invalid_or_duplicate_choices(self):
        args = TRADE.parse_args(self.arguments("--strategies", "default90", "disabled_ssd",
                                              "--workloads", "switch", "--repeats", "1"))
        plan = TRADE.trial_plan(args.strategies, args.workloads, args.repeats)
        self.assertEqual([("switch", "default90"), ("switch", "disabled_ssd")],
                         [(r["workload"], r["strategy"]) for r in plan])
        for extra in (("--deadline-seconds", "10801"), ("--space-interval", "11"),
                      ("--repeats", "3"), ("--workloads", "scan"),
                      ("--strategies", "default50", "default50"), ("--workloads", "uniform", "uniform")):
            with self.subTest(extra=extra), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                TRADE.parse_args(self.arguments(*extra))
        mixed = TRADE.parse_args(self.arguments("--workloads", "mixed"))
        self.assertEqual(["mixed"], mixed.workloads)
        self.assertEqual(TRADE.DURATIONS["uniform"], TRADE.DURATIONS["mixed"])

    def test_effective_budget_matches_native_reserve_rounding(self):
        for baseline in (1, 199, 1001, 123456789):
            for name in TRADE.POLICIES:
                with self.subTest(baseline=baseline, strategy=name):
                    config = TRADE.policy_configuration(name, baseline)
                    cap = config["capacity_bytes"]
                    native = cap - cap // 100 * 10 - cap % 100 * 10 // 100
                    self.assertEqual(native, config["effective_budget_bytes"])
        self.assertEqual(81, TRADE.policy_configuration("default90", 10000)["effective_budget_percent"])
        self.assertFalse(TRADE.policy_configuration("disabled_ssd", 10000)["budget_applies"])

    def test_all_policy_flags_reach_argv_once_with_frozen_policy_parameters(self):
        runner = TRADE.TradeoffRunner.__new__(TRADE.TradeoffRunner)
        runner.args = TRADE.parse_args(self.arguments())
        runner.keys, runner.rate = TRADE.KEYS, TRADE.TARGET_RATE
        for name in TRADE.POLICIES:
            with self.subTest(strategy=name):
                config = TRADE.policy_configuration(name, 10000)
                command = runner.trial_command(self.seed, config, "switch")
                pairs = [item[2:].split("=", 1) for item in command[1:]]
                self.assertEqual(len(pairs), len(set(key for key, _ in pairs)))
                flags = dict(pairs)
                for key, value in config["parameters"].items():
                    self.assertEqual(str(value), flags[TRADE.PARAMETER_FLAGS[key]])
                for key, expected in (("num", "1000000"), ("value_size", "1024"),
                                      ("metabypass_queue_capacity", "2097152"),
                                      ("metabypass_sst_cache_bytes", "65536"),
                                      ("metabypass_sst_target_ops_per_sec", "112"),
                                      ("metabypass_sst_warmup_seconds", "45"),
                                      ("metabypass_sst_measure_seconds", "120"),
                                      ("metabypass_sst_saturated_seconds", "15"),
                                      ("metabypass_sst_read_delay_us", "0")):
                    self.assertEqual(expected, flags[key])
                expected = (2, 3, 10000, 0.25)
                if name.startswith("conservative"):
                    expected = (2, 8, 30000, 1.0)
                elif name.startswith("fast_promote"):
                    expected = (1, 3, 10000, 0.25)
                elif name.startswith("fast_swap"):
                    expected = (1, 1, 3000, 0.10)
                elif name.startswith("aggressive"):
                    expected = (1, 1, 0, 0.0)
                self.assertEqual(expected, (
                    int(flags["metabypass_sst_promote_rounds"]),
                    int(flags["metabypass_sst_demote_rounds"]),
                    int(flags["metabypass_sst_residency_ms"]),
                    float(flags["metabypass_sst_replacement_margin"])))

    def test_nine_aggressive_profiles_keep_common_parameters_and_fixed_budget_rounding(self):
        expected_names = (
            "fast_promote65", "fast_swap65", "aggressive65",
            "fast_promote80", "fast_swap80", "aggressive80",
            "fast_promote90", "fast_swap90", "aggressive90")
        self.assertEqual(expected_names, TRADE.AGGRESSIVE_STRATEGIES)
        variants = (("fast_promote", (1, 3, 10000, 0.25)),
                    ("fast_swap", (1, 1, 3000, 0.10)),
                    ("aggressive", (1, 1, 0, 0.0)))
        changed = ("promote_rounds", "demote_rounds", "min_residency_ms", "replacement_margin")
        for percent in (65, 80, 90):
            for prefix, values in variants:
                name = prefix + str(percent)
                with self.subTest(strategy=name):
                    params = {**TRADE.DEFAULT_PARAMETERS, **dict(zip(changed, values))}
                    for baseline in (199, 1001, 22670692):
                        config = TRADE.policy_configuration(name, baseline)
                        capacity = baseline // 100 * percent + baseline % 100 * percent // 100
                        effective = capacity - capacity // 100 * 10 - capacity % 100 * 10 // 100
                        self.assertEqual(("adaptive", percent, capacity, effective),
                                         (config["mode"], config["capacity_percent"],
                                          config["capacity_bytes"], config["effective_budget_bytes"]))
                        self.assertEqual(params, config["parameters"])

    def test_explicit_aggressive_matrix_is_36_rounds_and_reverses_entire_repeat(self):
        args = TRADE.parse_args(self.arguments(
            "--strategies", *TRADE.AGGRESSIVE_STRATEGIES,
            "--workloads", "uniform", "switch", "--repeats", "2",
            "--deadline-seconds", "10800"))
        plan = TRADE.trial_plan(args.strategies, args.workloads, args.repeats)
        first = [(w, s) for w in ("uniform", "switch") for s in TRADE.AGGRESSIVE_STRATEGIES]
        self.assertEqual(36, len(plan))
        self.assertEqual(first, [(r["workload"], r["strategy"]) for r in plan[:18]])
        self.assertEqual(first[::-1], [(r["workload"], r["strategy"]) for r in plan[18:]])
        self.assertEqual({1}, {r["repeat"] for r in plan[:18]})
        self.assertEqual({2}, {r["repeat"] for r in plan[18:]})
        self.assertEqual((10800, 480, 112, 1000000),
                         (args.deadline_seconds, TRADE.ROUND_LIMIT_S, TRADE.TARGET_RATE, TRADE.KEYS))
        self.assertFalse(set(args.strategies) & set(TRADE.DEFAULT_STRATEGIES))
        self.assertEqual(28, len(TRADE.trial_plan(
            TRADE.DEFAULT_STRATEGIES, TRADE.DEFAULT_WORKLOADS, 2)))

    def test_aggressive_zero_residency_and_margin_reach_command_without_fallback(self):
        runner = self.runner("--strategies", "aggressive65", "--workloads", "uniform", "--repeats", "1")
        for name in ("aggressive65", "aggressive80", "aggressive90"):
            with self.subTest(strategy=name):
                command = runner.trial_command(self.seed, TRADE.policy_configuration(name, 22670692), "uniform")
                for flag, expected in (("metabypass_sst_promote_rounds", "1"),
                                       ("metabypass_sst_demote_rounds", "1"),
                                       ("metabypass_sst_residency_ms", "0"),
                                       ("metabypass_sst_replacement_margin", "0.0")):
                    self.assertEqual(["--" + flag + "=" + expected],
                                     [arg for arg in command if arg.startswith("--" + flag + "=")])

    def test_extended_deadline_preserves_7200_default_and_rejects_outside_10800(self):
        self.assertEqual(7200, TRADE.parse_args(self.arguments()).deadline_seconds)
        for deadline in (7201, 10800):
            self.assertEqual(deadline, TRADE.parse_args(
                self.arguments("--deadline-seconds", str(deadline))).deadline_seconds)
        for deadline in (0, -1, 10801):
            with self.subTest(deadline=deadline), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                TRADE.parse_args(self.arguments("--deadline-seconds", str(deadline)))

    def test_mixed_placement_with_zero_new_migrations_is_covered(self):
        events = protocol_events()
        coverage = TRADE.mixed_coverage(events, TRADE.node_diagnose.phase_deltas(events))
        self.assertTrue(coverage["covered"])
        for phase in ("measured", "saturated"):
            self.assertEqual(0, coverage["phases"][phase]["actual_migrations"])
            self.assertEqual(10, coverage["phases"][phase]["sampled_reads_delta"])
            self.assertGreater(coverage["phases"][phase]["mixed_observation_count"], 0)

    def test_protected_hot_backups_do_not_count_as_cold_coverage(self):
        events = protocol_events()
        for event in events:
            if "stats" in event:
                event["stats"]["ssd_bytes"] = 1000
                event["stats"]["hdd_bytes"] = 1000
        coverage = TRADE.mixed_coverage(events, TRADE.node_diagnose.phase_deltas(events))
        self.assertFalse(coverage["covered"])
        self.assertEqual(0, coverage["phases"]["measured"]["mixed_observation_count"])

    def test_missing_sampled_reads_or_unprotected_placement_remains_uncovered(self):
        for sampled, unprotected, error in (((10, 10, 20), 0, 0), ((10, 20, 30), 1, 0),
                                            ((10, 20, 30), 0, 1)):
            events = protocol_events(sampled=sampled)
            for event in events:
                if "stats" in event:
                    event["stats"].update(unprotected_bytes=unprotected, migration_errors=error)
            with self.subTest(sampled=sampled, unprotected=unprotected, error=error):
                self.assertFalse(TRADE.mixed_coverage(events, TRADE.node_diagnose.phase_deltas(events))["covered"])

    def test_reserved_and_pending_charges_are_removed_from_hot_placement(self):
        stats = {"protected_bytes": 1000, "unprotected_bytes": 0, "ssd_bytes": 800,
                 "reserved_bytes": 50, "pending_delete_bytes": 150}
        self.assertEqual({"hot_bytes": 600, "cold_bytes": 400, "total_bytes": 1000, "unprotected_bytes": 0},
                         TRADE.placement_bytes(stats))
        for update in ({"ssd_bytes": 100}, {"ssd_bytes": 1500}, {"reserved_bytes": -1}, {"ssd_bytes": None}):
            with self.subTest(update=update):
                self.assertIsNone(TRADE.placement_bytes({**stats, **update}))

    def test_seed_metadata_does_not_read_payload_and_detects_write(self):
        before = TRADE.tree_metadata(self.seed)
        with mock.patch.object(Path, "read_bytes", side_effect=AssertionError("payload read")):
            self.assertEqual(before, TRADE.tree_metadata(self.seed))
        (self.seed[1] / "data" / "blob").write_bytes(b"changed")
        self.assertNotEqual(TRADE.metadata_digest(before), TRADE.metadata_digest(TRADE.tree_metadata(self.seed)))

    def test_seed_symlink_and_clones_sharing_seed_or_previous_inodes_are_rejected(self):
        metadata = TRADE.tree_metadata(self.seed)
        dest = (self.root / "clone-ssd", self.root / "clone-hdd")
        RUN.clone_dataset(zip(self.seed, dest))
        clone = TRADE.tree_metadata(dest)
        inodes = TRADE.require_independent_inodes(metadata, clone)
        with self.assertRaises(TRADE.SafetyStop):
            TRADE.require_independent_inodes(metadata, clone, inodes)
        with self.assertRaises(TRADE.SafetyStop):
            TRADE.require_independent_inodes(metadata, metadata)
        (self.seed[1] / "alias").symlink_to(self.seed[1] / "data")
        with self.assertRaises(ValueError):
            TRADE.tree_metadata(self.seed)

    def test_retired_inode_reuse_allowed_but_still_live_sharing_rejected(self):
        runner = self.runner("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
        previous = self.root / "old-trial"
        previous.mkdir()
        retired = previous / "retired.log"
        retired.write_text("old WAL")
        stale_inode = (retired.stat().st_dev, retired.stat().st_ino)
        retired.unlink()
        current = self.root / "current-trial"
        current.mkdir()
        runner.report["trials"][0]["directories"] = [str(previous)]
        active = runner.prior_trial_inodes((current,))
        self.assertNotIn(stale_inode, active)
        # Simulate the filesystem assigning the retired inode to a new copy.
        metadata = [{"dev": stale_inode[0], "ino": stale_inode[1]}]
        TRADE.require_independent_inodes([], metadata, active)
        live = previous / "live.log"
        live.write_text("live WAL")
        os.link(live, current / "shared.log")
        with self.assertRaises(TRADE.SafetyStop):
            TRADE.require_independent_inodes([], TRADE.tree_metadata((current,)),
                                            runner.prior_trial_inodes((current,)))

    def test_helper_replacement_or_deletion_stops_frozen_run_and_is_checked_at_end(self):
        for change in ("replace", "delete"):
            with self.subTest(change=change):
                arguments = self.arguments("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
                arguments[arguments.index("--output") + 1] = str(self.root / (change + "-helper.json"))
                runner = TRADE.TradeoffRunner(TRADE.parse_args(arguments))
                helper = self.root / (change + "-helper.py")
                helper.write_text("print('frozen helper')\n")
                runner.report["driver_identity"]["source_sha256"] = {str(helper): RUN.digest(helper)}
                runner.check_identity()
                if change == "replace":
                    helper.write_text("print('replacement helper')\n")
                else:
                    helper.unlink()
                with self.assertRaises(TRADE.SafetyStop):
                    runner.check_identity()
                runner.finalize()
                self.assertEqual("blocked_or_failed", runner.report["status"])
                self.assertTrue(runner.report["final_integrity_errors"])

    def test_all_filtered_rounds_copy_original_seed_and_keep_distinct_data(self):
        runner = self.runner("--strategies", "disabled_ssd", "default50")
        execute, calls, sources = self.fake_execute(runner)
        with mock.patch.object(runner, "preflight"), mock.patch.object(runner, "execute", side_effect=execute):
            runner.run()
        runner.finalize()
        self.assertEqual("complete", runner.report["status"])
        self.assertEqual(8, len(runner.report["trials"]))
        self.assertEqual(16, len(calls))
        self.assertEqual([self.seed] * 8, sources)
        self.assertTrue(runner.report["seed"]["final_immutable_check"])
        self.assertEqual(runner.report["seed"]["metadata_sha256_start"], runner.report["seed"]["metadata_sha256_end"])
        first_inodes = set()
        for row in runner.report["trials"]:
            self.assertTrue(row["valid"])
            self.assertTrue(row["independent_inode_check"]["passed"])
            self.assertIn("phase_migration_deltas", row)
            self.assertIn("phase_io", row)
            self.assertIn("closed_snapshot", row)
            self.assertIn("space", row)
            file = Path(row["directories"][0]) / "index" / "000001.sst"
            first_inodes.add((file.stat().st_dev, file.stat().st_ino))
        self.assertEqual(8, len(first_inodes))

    def test_ordinary_round_failure_is_retained_and_next_independent_round_runs(self):
        runner = self.runner("--strategies", "disabled_ssd", "default50", "--workloads", "uniform", "--repeats", "1")
        first = runner.report["trials"][0]["name"]
        execute, calls, sources = self.fake_execute(runner, failures={first})
        with mock.patch.object(runner, "preflight"), mock.patch.object(runner, "execute", side_effect=execute):
            runner.run()
        runner.finalize()
        self.assertEqual("complete_with_failures", runner.report["status"])
        self.assertEqual(("failed", "complete"), tuple(r["status"] for r in runner.report["trials"]))
        self.assertTrue(Path(runner.report["trials"][0]["directories"][0]).is_dir())
        self.assertEqual(2, len(sources))
        self.assertEqual(4, len(calls))
        self.assertIn("events", runner.report["trials"][0])

    def test_clone_or_benchmark_oom_stops_and_records_all_unexecuted_rows(self):
        for copy_oom in (False, True):
            with self.subTest(copy_oom=copy_oom):
                # Each subcase has a new output path; owned data roots are new.
                arguments = self.arguments("--strategies", "disabled_ssd", "default50",
                                           "--workloads", "uniform", "--repeats", "1")
                arguments[arguments.index("--output") + 1] = str(self.root / f"oom-{copy_oom}.json")
                runner = TRADE.TradeoffRunner(TRADE.parse_args(arguments))
                first = runner.report["trials"][0]["name"]
                execute, calls, sources = self.fake_execute(runner, oom={first + "-copy" if copy_oom else first})
                with mock.patch.object(runner, "preflight"), mock.patch.object(runner, "execute", side_effect=execute), \
                        self.assertRaises(TRADE.SafetyStop):
                    runner.run()
                runner.report["status"] = "blocked_or_failed"
                runner.finalize()
                self.assertEqual(("stopped", "not_run"), tuple(r["status"] for r in runner.report["trials"]))
                self.assertEqual(1 if copy_oom else 2, len(calls))
                self.assertEqual(1, len(sources))

    def test_child_without_identity_or_oom_delta_still_stops_on_cgroup_oom(self):
        runner = self.runner("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
        row = runner.report["trials"][0]
        def early_failure(*args, **kwargs):
            (self.cgroup / "memory.oom_control").write_text("oom_kill 1\nunder_oom 0\n")
            return {"name": row["name"] + "-copy", "exit_code": -9, "valid": False}
        with mock.patch.object(runner, "execute", side_effect=early_failure), self.assertRaises(TRADE.SafetyStop):
            runner.execute_checked(row, ["clone"], self.seed, timeout=480, preparation=True)
        self.assertEqual(1, row["preparation"]["oom_kill_delta"])
        self.assertEqual(1, row["independent_oom_checks"][0]["oom_kill_delta"])
        self.assertFalse(row["preparation"]["valid"])

    def test_ordinary_early_failure_without_oom_delta_remains_independent(self):
        runner = self.runner("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
        row = runner.report["trials"][0]
        actual = {"name": row["name"], "exit_code": 1, "valid": False}
        with mock.patch.object(runner, "execute", return_value=actual):
            result = runner.execute_checked(row, ["benchmark"], self.seed, timeout=480)
        self.assertIs(actual, result)
        self.assertEqual(0, result["oom_kill_delta"])
        self.assertEqual(0, row["independent_oom_checks"][0]["oom_kill_delta"])
        self.assertFalse(result["valid"])

    def test_cgroup_occupation_blocks_clone_and_benchmark_before_launch(self):
        runner = self.runner("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
        row = runner.report["trials"][0]
        (self.cgroup / "tasks").write_text("123\n")
        with mock.patch.object(runner, "execute") as execute:
            with self.assertRaises(TRADE.SafetyStop):
                runner.copy_round(row)
            with self.assertRaises(TRADE.SafetyStop):
                runner.benchmark_round(row, self.seed)
            execute.assert_not_called()

    def test_seed_and_identity_change_after_round_stop_following_round(self):
        for changed in ("seed", "manifest"):
            with self.subTest(changed=changed):
                args = self.arguments("--strategies", "disabled_ssd", "default50",
                                      "--workloads", "uniform", "--repeats", "1")
                args[args.index("--output") + 1] = str(self.root / (changed + "-change.json"))
                runner = TRADE.TradeoffRunner(TRADE.parse_args(args))
                execute, calls, sources = self.fake_execute(runner)
                def changing_execute(name, command, dirs, timeout=480):
                    actual = execute(name, command, dirs, timeout)
                    if "--clone" not in command:
                        target = self.seed[1] / "data" / "blob" if changed == "seed" else self.manifest
                        target.write_text("modified immutable input\n")
                    return actual
                with mock.patch.object(runner, "preflight"), \
                        mock.patch.object(runner, "execute", side_effect=changing_execute), self.assertRaises(TRADE.SafetyStop):
                    runner.run()
                runner.report["status"] = "blocked_or_failed"
                runner.finalize()
                self.assertEqual(2, len(calls))
                self.assertEqual("not_run", runner.report["trials"][1]["status"])
                self.assertIn("final_integrity_errors", runner.report)

    def test_round_timeout_includes_clone_and_never_shortens_fixed_phases(self):
        runner = self.runner("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
        row = runner.report["trials"][0]
        row["round_started_monotonic_s"] = 0
        with mock.patch.object(TRADE.time, "monotonic", return_value=400), \
                mock.patch.object(runner, "execute") as execute, self.assertRaises(RuntimeError):
            runner.benchmark_round(row, self.seed)
        execute.assert_not_called()

    def test_failed_partial_trial_keeps_window_events_and_valid_sibling_phase(self):
        runner = self.runner("--strategies", "default50", "--workloads", "switch", "--repeats", "1")
        row = runner.report["trials"][0]
        row["events"] = protocol_events("switch")
        row["events"] = [e for e in row["events"] if e.get("phase") != "saturated" and e.get("event") != "summary"]
        runner.collect_evidence(row, self.seed)
        self.assertTrue(row["phases"]["measured"]["valid"])
        self.assertIsNone(row["phases"]["saturated"])
        self.assertIn("phase_migration_deltas", [e["kind"] for e in row["evidence_errors"]])
        self.assertEqual(2, sum(e["event"] == "window" for e in row["events"]))
        self.assertEqual(1, sum(e["event"] == "hotspot_switch" for e in row["events"]))
        self.assertFalse(row["mixed_placement_coverage"]["covered"])

    def test_reported_capacity_mismatch_invalidates_trial(self):
        runner = self.runner("--strategies", "default50", "--workloads", "uniform", "--repeats", "1")
        row = runner.report["trials"][0]
        row.update(effective_configuration=TRADE.policy_configuration("default50", 16384),
                   mode="adaptive", round_started_monotonic_s=RUN.time.monotonic())
        actual = {"valid": True, "events": protocol_events(capacity=123), "oom_kill_delta": 0,
                  "name": row["name"], "timed_out": False}
        with mock.patch.object(runner, "execute", return_value=actual), \
                mock.patch.object(runner, "collect_evidence"), self.assertRaises(RuntimeError):
            runner.benchmark_round(row, self.seed)
        self.assertFalse(row["valid"])
        self.assertIn("reported capacity does not match actual strategy command", row["phase_validation_errors"])

    def test_finalize_keeps_unexecuted_plan_when_preflight_fails(self):
        runner = self.runner()
        runner.report.update(status="blocked_or_failed", error="preflight unavailable")
        runner.finalize()
        saved = json.loads(Path(runner.args.output).read_text())
        self.assertEqual(28, len(saved["trials"]))
        self.assertTrue(all(r["status"] == "not_run" for r in saved["trials"]))
        self.assertEqual(saved["binary_sha256_start"], saved["binary_sha256_end"])
        self.assertEqual(saved["source_manifest_sha256_start"], saved["source_manifest_sha256_end"])


if __name__ == "__main__":
    unittest.main()
