#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Safety and statistical contract tests; these do not benchmark the engine."""
import copy
import errno
import io
import json
import math
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import run
import summary


def event(kind, **fields):
    return {"protocol": run.PROTOCOL, "event": kind, **fields}


def sample(group="D", number=1, throughput=100):
    ops = int(throughput)
    return {"phase": "measured", "valid": True, "status": "pass",
            "spec": {"scenario": "insert-1k-1t", "group": group, "round": number},
            "driver": {"events": [event("sample", name="foreground", ops=ops, elapsed_us=1000000,
                                                ops_per_sec=ops, p50_us=1, p95_us=1, p99_us=1,
                                                hist_us=[[1, ops]]),
                                   event("phase", name="lifecycle", elapsed_us=1000000),
                                   event("stats", flushes=1, compactions=0)]}}


class ContractTests(unittest.TestCase):
    def test_sample_verification_commands_and_event_identity(self):
        spec = {"group": "S", "workload": "mixed", "keys": 16, "value_size": 1024,
                "threads": 2, "seed": 17, "verify_samples": 8}
        verification = event("verification", verification_protocol="metabypass-key-sample-v1",
                             mode="sample", population=16, requested=8, actual=8,
                             derived_seed=17 ^ 0xd1b54a32d192ed03,
                             keys=[0, 1, 3, 7, 8, 9, 11, 15], live_checked=6,
                             deleted_checked=1, unwritten_checked=1, full_keycount_checked=False)
        for action in ("verify", "restore", "append-reopen"):
            done = event("done", status="pass", variant="S", action=action, workload="mixed", verified_keys=8)
            self.assertIn("--verify-samples=8", run.driver_command("/binary", {}, spec, action))
            run.validate_events([verification, done], action, "S", "mixed", spec)
            for field, value in (("actual", 16), ("actual", True), ("derived_seed", 0),
                                 ("verification_protocol", "unknown"), ("full_keycount_checked", True),
                                 ("keys", [0] * 8), ("keys", [0, 1, 3, 6, 8, 9, 11, 15]),
                                 ("deleted_checked", -1), ("requested", 9)):
                with self.subTest(action=action, field=field, value=value):
                    with self.assertRaises(run.RunFailure):
                        run.validate_events([{**verification, field: value}, done], action, "S", "mixed", spec)
            for events in ([done], [verification, verification, done],
                           [verification, {**done, "verified_keys": 16}]):
                with self.assertRaises(run.RunFailure):
                    run.validate_events(events, action, "S", "mixed", spec)
            full = {key: value for key, value in spec.items() if key != "verify_samples"}
            self.assertFalse(any(flag.startswith("--verify-samples=")
                                 for flag in run.driver_command("/historical-binary", {}, full, action)))
            with self.assertRaises(run.RunFailure):
                run.validate_events([verification, done], action, "S", "mixed", full)

    def test_telemetry_pf_exiting_requires_same_sample_owned_identity(self):
        pid = 1871853
        expected = {"pid": pid, "starttime_ticks": 1201369035, "pgrp": pid,
                    "session": pid, "state": "R", "cpu_ticks": 3292}
        for failure in (None, "live", "identity", "global", "cgroup", "foreign_child"):
            with self.subTest(failure=failure):
                flags = 4194304 if failure == "live" else 4194308
                observed = {**expected, "flags": flags}
                after = {**observed, "starttime_ticks": 9} if failure == "identity" else observed
                process = mock.Mock(pid=pid + 1 if failure == "foreign_child" else pid)
                process.poll.return_value = None
                fields = ["0"] * 20
                fields[0], fields[2], fields[3], fields[6] = "R", str(pid), str(pid), str(flags)
                fields[11], fields[19] = "3292", str(expected["starttime_ticks"])
                stat = str(pid) + " (mb_native) " + " ".join(fields)
                def read(path):
                    path = str(path)
                    if path == f"/proc/{pid}/io" or (failure == "global" and path == "/proc/diskstats") or \
                            (failure == "cgroup" and path == "/fake-cgroup/memory.stat"):
                        raise PermissionError(errno.EACCES, "Permission denied", path)
                    return stat if path == f"/proc/{pid}/stat" else "readable\n"
                with mock.patch.object(run.Path, "read_text", read), \
                        mock.patch.object(run, "proc_identity", side_effect=[observed, after]):
                    snapshot = run.telemetry(pid, Path("/fake-cgroup"), process, expected)
                self.assertEqual(bool(snapshot["errors"]), failure is not None)
                self.assertEqual(snapshot["process_observation"]["sampled_stat"]["flags"], flags)
                if failure in (None, "global", "cgroup"):
                    error = snapshot["expected_exit_errors"][0]
                    self.assertEqual(error["reason"], "same_identity_pf_exiting")
                    self.assertEqual(error["errno"], errno.EACCES)
                else:
                    self.assertNotIn("expected_exit_errors", snapshot)

    def test_telemetry_only_same_sample_exit_races_are_expected(self):
        pid = 603760
        expected = {"pid": pid, "starttime_ticks": 1199844536, "pgrp": pid,
                    "session": pid, "state": "R", "cpu_ticks": 7}
        def stat_text(state):
            fields = ["0"] * 20
            fields[0], fields[2], fields[3] = state, str(pid), str(pid)
            fields[11], fields[19] = "7", str(expected["starttime_ticks"])
            return str(pid) + " (mb_native) " + " ".join(fields)
        for state, exit_codes, expected_race in (("Z", [None, 0], True),
                                                  ("X", [None, None], True),
                                                  ("R", [None, None], False)):
            with self.subTest(state=state):
                process = mock.Mock(pid=pid)
                process.poll.side_effect = exit_codes
                observed = {**expected, "state": state}
                def read(path):
                    path = str(path)
                    if path == f"/proc/{pid}/io":
                        raise PermissionError(errno.EACCES, "Permission denied", path)
                    if path == f"/proc/{pid}/stat":
                        return stat_text(state)
                    if path == f"/proc/{pid}/status":
                        return "State:\t" + state + "\n"
                    return "readable\n"
                with mock.patch.object(run.Path, "read_text", read), \
                        mock.patch.object(run, "proc_identity", side_effect=[expected, observed]):
                    snapshot = run.telemetry(pid, process=process, expected_identity=expected)
                self.assertEqual(bool(snapshot["errors"]), not expected_race)
                self.assertEqual(bool(snapshot.get("expected_exit_errors")), expected_race)
                if expected_race:
                    self.assertTrue(snapshot["process_exited"])
                    self.assertEqual(snapshot["expected_exit_errors"][0]["errno"], errno.EACCES)
                    self.assertIn(f"/proc/{pid}/io", snapshot["expected_exit_errors"][0]["message"])
                else:
                    process.returncode = 0
                    self.assertTrue(snapshot["errors"])  # A later exit does not reclassify it.
        process = mock.Mock(pid=pid)
        process.poll.return_value = 0
        def disappeared(path):
            if str(path).startswith(f"/proc/{pid}/"):
                raise FileNotFoundError(errno.ENOENT, "No such process", str(path))
            return "readable\n"
        with mock.patch.object(run.Path, "read_text", disappeared), \
                mock.patch.object(run, "proc_identity", return_value=None):
            snapshot = run.telemetry(pid, process=process, expected_identity=expected)
        self.assertFalse(snapshot["errors"])
        self.assertEqual(len(snapshot["expected_exit_errors"]), 4)
        self.assertEqual(snapshot["process_observation"]["exit_code_after"], 0)

    def test_telemetry_exit_race_never_excuses_global_cgroup_or_identity_errors(self):
        pid = 101
        expected = {"pid": pid, "starttime_ticks": 10, "pgrp": pid,
                    "session": pid, "state": "Z", "cpu_ticks": 0}
        for failure in ("global", "cgroup", "identity"):
            with self.subTest(failure=failure):
                process = mock.Mock(pid=pid)
                process.poll.return_value = 0
                after = {**expected, "starttime_ticks": 11} if failure == "identity" else expected
                def read(path):
                    path = str(path)
                    if path == f"/proc/{pid}/io" or (failure == "global" and path == "/proc/diskstats") or \
                            (failure == "cgroup" and path == "/fake-cgroup/memory.stat"):
                        raise PermissionError(errno.EACCES, "Permission denied", path)
                    if path == f"/proc/{pid}/stat":
                        fields = ["0"] * 20
                        fields[0], fields[2], fields[3], fields[19] = "Z", str(pid), str(pid), "10"
                        return str(pid) + " (mb_native) " + " ".join(fields)
                    return "readable\n"
                with mock.patch.object(run.Path, "read_text", read), \
                        mock.patch.object(run, "proc_identity", side_effect=[expected, after]):
                    snapshot = run.telemetry(pid, "/fake-cgroup", process, expected)
                self.assertTrue(snapshot["errors"])
                if failure == "identity":
                    self.assertFalse(snapshot.get("expected_exit_errors"))
                    self.assertTrue(any("identity changed" in value for value in snapshot["errors"]))
                else:
                    self.assertEqual(len(snapshot["expected_exit_errors"]), 1)
                    self.assertTrue(any(("diskstats" if failure == "global" else "memory.stat") in value
                                        for value in snapshot["errors"]))

    def test_unlimited_deadline_and_plan_preserve_null_caps_without_json_infinity(self):
        deadline = run.Deadline(None)
        self.assertTrue(math.isinf(deadline.remaining()))
        deadline.require(math.inf)
        with mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(run.main(["plan", "--no-time-limits"]), 0)
            plan = run.strict_json(output.getvalue())
        self.assertIsNone(plan["budget_seconds"])
        self.assertEqual(plan["stage_budget_seconds"], {key: None for key in run.STAGES})
        self.assertEqual(plan["time_limit_policy"]["mode"], "unlimited")
        self.assertTrue(plan["time_limit_policy"]["explicit_no_time_limits"])
        self.assertEqual(plan["time_limit_policy"]["per_driver_timeout_mode"], "inactivity")
        self.assertEqual(len(plan["core"]), 120)
        self.assertEqual(len(plan["specials"]), 21)
        with tempfile.TemporaryDirectory() as temp:
            experiment = object.__new__(run.Experiment)
            experiment.output = Path(temp)
            experiment.deadline = deadline
            experiment.report = {}
            experiment.save()
            self.assertIsNone(run.strict_json((Path(temp) / "results.json").read_text())["remaining_seconds"])

    def test_explicit_stage_budget_override_validates_and_preserves_provenance(self):
        proposed = {"preflight": 1200, "calibration": 3300, "core": 10200,
                    "specials": 6900, "recovery": 3600, "reserve": 3600}
        self.assertEqual(run.validate_stage_budgets(proposed), proposed)
        default, provenance = run.stage_budget_configuration()
        self.assertEqual(default, run.STAGES)
        self.assertIsNone(provenance)
        invalid = [None, [], {**proposed, "extra": 1},
                   {key: value for key, value in proposed.items() if key != "reserve"},
                   {**proposed, "reserve": 3601}]
        invalid += [{**proposed, "preflight": value} for value in
                    (True, "1200", 0, -1, float("nan"), float("inf"))]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                run.validate_stage_budgets(value)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "budgets.json"
            raw = json.dumps(proposed, indent=2) + "\n"
            path.write_text(raw)
            actual, provenance = run.stage_budget_configuration(path)
            self.assertEqual(actual, proposed)
            self.assertEqual(provenance["raw_json"], raw)
            self.assertEqual(provenance["sha256"], run.digest(path))
            self.assertEqual(provenance["path"], str(path.resolve()))

    def test_deadline_enforces_remaining_and_rejects_bad_values(self):
        clock = [10]
        deadline = run.Deadline(5, lambda: clock[0])
        deadline.require(4)
        clock[0] = 15
        with self.assertRaises(run.RunFailure):
            deadline.require()
        for value in (float("nan"), float("inf"), -1, 0):
            with self.assertRaises(ValueError):
                run.Deadline(value)

    def test_nonfinite_and_truncated_json_rejected(self):
        for value in ('{"x":NaN}', '{"x":Infinity}', '{"x":', '<xml/>'):
            with self.assertRaises(ValueError):
                run.strict_json(value)

    def test_unknown_and_old_protocol_rejected(self):
        for protocol in ("metabypass-quick-v1", "node_v1", None):
            with self.assertRaises(ValueError):
                summary.summarize({"protocol": protocol})

    def test_done_requires_matching_action_and_unique_success(self):
        done = event("done", status="pass", variant="D", action="verify", workload="insert")
        run.validate_events([done], "verify", "D", "insert")
        for events in ([], [done, done], [{**done, "status": "failed"}],
                       [{**done, "protocol": "metabypass-quick-v1"}],
                       [{**done, "variant": "U"}], [done, event("error", message="broken")]):
            with self.assertRaises(run.RunFailure):
                run.validate_events(events, "verify", "D", "insert")

    def test_natural_success_without_requested_crash_gate_is_not_pass(self):
        done = event("done", status="pass", variant="S", action="crashwrite", workload="overwrite")
        for events, injected, exit_code in (([done], False, 0),
                                           ([event("ready", stage="write")], True, -9),
                                           ([event("ready", stage="publish")], False, 0)):
            with self.assertRaises(run.CoverageFailure):
                run.validate_crash_boundary(events, "publish", injected, exit_code)
        run.validate_crash_boundary([event("ready", stage="publish")], "publish", True, -9)

    def test_warmup_not_measured_and_gc_has_three_generations(self):
        base = sample()["driver"]["events"][0]
        warmup = {**base, "name": "warmup", "ops": 1, "hist_us": [[1000, 1]]}
        done = event("done", status="pass", variant="D", action="run", workload="insert")
        run.validate_events([warmup, base, done], "run", "D", "insert")
        gc_done = {**done, "workload": "gc"}
        run.validate_events([base, base, base, gc_done], "run", "D", "gc")
        row = sample()
        row["driver"]["events"].insert(0, warmup)
        self.assertEqual(summary.row_metrics(row)["p99_us"], 1)

    def test_histogram_percentile_merges_counts(self):
        self.assertEqual(summary.histogram_percentile([[1, 1000], [1000, 1]], .99), 1)
        self.assertEqual(summary.histogram_percentile([[1, 1], [1000, 1000]], .99), 1000)
        for histogram in ([], [[1, -1]], [[float("nan"), 1]], [[1, 1.2]]):
            with self.assertRaises(ValueError):
                summary.histogram_percentile(histogram, .99)

    def test_failed_samples_never_enter_statistics_or_repair_rounds(self):
        rows = [sample("D", n, 80) for n in (1, 2, 3)] + [sample("B", n, 100) for n in (1, 2, 3)]
        rows[1].update(valid=False, status="failed")
        replacement = sample("D", 2, 10000)
        replacement["phase"] = "retest"
        rows.append(replacement)
        result = summary.summarize({"protocol": run.PROTOCOL, "profile": "formal", "state": "incomplete", "runs": rows})
        comparison = next(row for row in result["comparisons"] if row["candidate"] == "D" and row["reference"] == "B")
        self.assertFalse(comparison["complete"])
        self.assertEqual(comparison["paired_rounds"], [1, 3])
        self.assertEqual(len(result["omitted"]), 1)

    def test_paired_round_ratios_not_ratio_of_medians(self):
        rows = [sample("D", n, value) for n, value in enumerate((1, 100, 100), 1)]
        rows += [sample("B", n, value) for n, value in enumerate((1, 10000, 100), 1)]
        result = summary.summarize({"protocol": run.PROTOCOL, "profile": "formal", "runs": rows})
        comparison = next(row for row in result["comparisons"] if row["candidate"] == "D" and row["reference"] == "B")
        self.assertEqual(comparison["degradation"]["ops_per_sec"]["median"], 0)
        self.assertTrue(result["ranking_allowed"])
        blocked = summary.summarize({"protocol": run.PROTOCOL, "profile": "formal", "runs": rows,
                                    "blocked_groups": {"D": "restore failed"}})
        self.assertFalse(blocked["ranking_allowed"])

    def test_matrix_freezes_counts_rotates_groups_and_preserves_sizes(self):
        rows = run.matrix("formal", 786432, 256)
        self.assertEqual(len(rows), 120)
        for scenario in ("insert-1k-1t", "insert-1k-4t", "insert-4k-1t"):
            selected = [row for row in rows if row["scenario"] == scenario]
            self.assertEqual({row["keys"] * row["value_size"] for row in selected}, {768 * run.MIB})
            for n in (1, 2, 3):
                group_order = [row["group"] for row in selected if row["round"] == n]
                self.assertEqual(group_order[0], run.GROUPS[n - 1])
        reads = [row for row in rows if row.get("static")]
        self.assertEqual({row["keys"] for row in reads}, {12 * run.GIB // 1024})
        concurrent = [row for row in rows if row["scenario"] == "randomread-4t"]
        self.assertEqual({row["group"] for row in concurrent}, {"U", "B", "S"})
        specials = run.special_matrix()
        self.assertEqual(len(specials), 21)
        for row in specials:
            if row["workload"] == "gc":
                self.assertEqual(row["ops"], row["keys"])
                self.assertEqual(row.get("seconds", 0), 0)

    def test_owned_boundary_cannot_reuse_or_escape_or_follow_symlink(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = run.create_owned(root, "one", "id")
            run.check_owned(path, root, "id")
            with self.assertRaises(FileExistsError):
                run.create_owned(root, "one", "id")
            for name in ("../escape", "a/b", ".."):
                with self.assertRaises(run.RunFailure):
                    run.create_owned(root, name, "id")
            with self.assertRaises(run.RunFailure):
                run.check_owned(path, root, "other")
            link = root / "link"
            link.symlink_to(path, target_is_directory=True)
            with self.assertRaises(run.RunFailure):
                run.check_owned(link, root, "id")
            run.cleanup_owned(path, root, "id")
            self.assertFalse(path.exists())

    def test_owner_marker_outside_engine_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            experiment = object.__new__(run.Experiment)
            experiment.ssd = root / "ssd"
            experiment.hdd = root / "hdd"
            experiment.output = root / "out"
            for path in (experiment.ssd, experiment.hdd, experiment.output):
                path.mkdir()
            experiment.run_id = "test"
            paths = experiment.paths("test")
            for key in ("index", "data", "backup", "staging"):
                self.assertEqual(list(paths[key].iterdir()), [])
                self.assertTrue((paths[key].parent / run.OWNER).is_file())
            experiment.remove_data(paths)

    def test_special_restores_before_cleanup_and_core_does_not_restore(self):
        for stage, workload in (("specials", "gc"), ("core", "overwrite")):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                experiment = object.__new__(run.Experiment)
                experiment.ssd, experiment.hdd, experiment.output = root / "ssd", root / "hdd", root / "out"
                for path in (experiment.ssd, experiment.hdd, experiment.output):
                    path.mkdir()
                experiment.run_id = "test"
                experiment.deadline = run.Deadline(60)
                experiment.rows, experiment.static, experiment.static_verified = [], {}, set()
                experiment.report = {"blocked_groups": {}, "failure_ledger": []}
                experiment.save = lambda: None
                calls = []
                stats = event("stats", flushes=3, compactions=1, wal_bytes=1024,
                              flush_data_bytes=1024, compaction_output_bytes=1024,
                              mirrored_wal_bytes=1024, recovery_points=1, peak_queued_bytes=1024,
                              backpressure_micros=0, candidate_copied_bytes=1024, hdd_extents=1,
                              peak_staging_bytes=1024, staging_spill_bytes=0, staging_backpressure_micros=0)
                def invoke(spec, paths, action, **_):
                    calls.append(action)
                    directory = experiment.output / str(len(calls))
                    directory.mkdir()
                    (paths["index"] / "OPTIONS-000001").write_text("effective options\n")
                    return {"events": [stats], "elapsed_s": 1}, directory
                def restore(spec, paths, row):
                    self.assertIn("space_after", row)
                    self.assertTrue(row["effective_options"])
                    calls.append("restore")
                    row["recovery"] = {"events": []}
                    row["append_reopen"] = {"events": []}
                    return []
                original_cleanup = experiment.remove_data
                def cleanup(paths):
                    calls.append("cleanup")
                    original_cleanup(paths)
                experiment.invoke, experiment.restore_gate, experiment.remove_data = invoke, restore, cleanup
                spec = {"stage": stage, "scenario": "test", "round": 1, "group": "S",
                        "workload": workload, "keys": 32, "ops": 32, "value_size": 1024, "gc": False}
                row = experiment.sample(spec)
                self.assertTrue(row["valid"])
                if stage == "specials":
                    self.assertLess(calls.index("restore"), calls.index("cleanup"))
                    self.assertIn("recovery", row)
                    self.assertIn("append_reopen", row)
                else:
                    self.assertNotIn("restore", calls)

    def test_pid_reuse_or_foreign_group_never_killed(self):
        identity = {"pid": 100, "starttime_ticks": 10, "pgrp": 100}
        for current in (None, {**identity, "starttime_ticks": 11}, {**identity, "pgrp": 200}):
            with mock.patch.object(run, "proc_identity", return_value=current), \
                    mock.patch.object(run.os, "killpg") as kill:
                self.assertFalse(run.kill_owned_group(identity))
                kill.assert_not_called()

    def test_space_snapshot_deduplicates_hardlinks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = root / "original"
            original.write_bytes(b"x" * 100)
            os.link(original, root / "link")
            result = run.space_snapshot([root])
            self.assertEqual(result["logical_bytes"], 100)
            self.assertEqual(result["files"], 1)

    def test_staging_quota_is_failure_and_unhit_gc_is_incomplete(self):
        spec = {"group": "S", "workload": "gc", "stage": "specials", "staging_capacity": 1024}
        stats = event("stats", peak_staging_bytes=1025)
        with self.assertRaises(run.DriverFailure):
            run.mechanism_coverage(spec, [stats])
        result = run.mechanism_coverage({**spec, "staging_capacity": 2048}, [stats])
        self.assertFalse(result["complete"])
        self.assertTrue(any("reclaimed" in item for item in result["missing"]))


if __name__ == "__main__":
    unittest.main()
