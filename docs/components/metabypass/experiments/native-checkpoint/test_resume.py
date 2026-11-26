#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Resume boundaries; no engine process or benchmark is started."""
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

import analyze
import resume
import run
import summary
from test_runner import sample


def source_fixture():
    counts = {"write_keys": 809103, "sync_ops": 128}
    frozen = {"protocol": run.PROTOCOL, "profile": "formal", "counts": counts,
              "core": run.matrix("formal", **counts), "specials": run.special_matrix("formal")}
    rows = [{"phase": "calibration", "spec": {"stage": "calibration", "group": group,
                                             "workload": workload},
             "status": "pass", "valid": True}
            for workload in ("insert", "sync") for group in run.GROUPS]
    for spec in frozen["core"] + frozen["specials"][:3]:
        rows.append({"phase": "measured", "spec": resume.normalize_spec(spec, 6500000),
                     "status": "pass", "valid": True})
    rows[-1].update(status="incomplete", valid=False, error="required telemetry could not be collected")
    report = {"protocol": run.PROTOCOL, "profile": "formal", "state": "incomplete",
              "time_limit_policy": {"explicit_no_time_limits": True}, "blocked_groups": {},
              "calibration": {"counts": counts}, "runs": rows,
              "failure_ledger": [{"sample": len(rows), "error": rows[-1]["error"]}],
              "sst_capacity_freeze": {"budget_bytes": 6500000, "sst_files": 16,
                                      "disabled_logical_sst_bytes": 10000000}}
    return report, frozen


class ResumeTests(unittest.TestCase):
    def test_calibration_json_roundtrip_preserves_command_values(self):
        paths = {"index": "/ssd/native-id/sample-D-index/db",
                 "state": "/results/original/sample-D.state",
                 "backup": "/hdd/native-id/sample-D-backup/db"}
        spec = {"stage": "calibration", "scenario": "calibrate-insert", "round": 0,
                "group": "D", "workload": "insert", "keys": 524288,
                "value_size": 1024, "threads": 1}
        binary = "/build/current/mb_native"
        original = run.driver_command(binary, paths, spec, "run", fresh=True)
        restored = json.loads(json.dumps({"spec": spec, "paths": paths}, sort_keys=True))
        reordered = run.driver_command(binary, restored["paths"], restored["spec"], "run", fresh=True)
        self.assertNotEqual(original, reordered)
        self.assertEqual(resume.command_identity(original), resume.command_identity(reordered))
        self.assertNotEqual(resume.command_identity(original),
                            resume.command_identity(["/another/binary", *original[1:]]))
        self.assertNotEqual(resume.command_identity(original),
                            resume.command_identity([item.replace("--keys=524288", "--keys=524289")
                                                     for item in original]))
        for invalid in (original + ["--keys=524288"], original + ["--keys"],
                        original + ["keys=524288"], original + ["--=value"],
                        original + ["--keys="], original + ["--bad key=value"]):
            with self.subTest(invalid=invalid[-1]), self.assertRaises(run.RunFailure):
                resume.command_identity(invalid)

    def test_blocked_static_retest_never_validates_or_invokes_database(self):
        experiment = object.__new__(resume.ResumeExperiment)
        experiment.report = {"blocked_groups": {"D": "prior functional failure"}}
        experiment.rows = []
        experiment.deadline = run.Deadline(None)
        experiment.save = mock.Mock()
        experiment.validate_static, experiment.invoke = mock.Mock(), mock.Mock()
        spec = {"stage": "reserve", "scenario": "randomread", "round": 1,
                "group": "D", "workload": "randomread", "static": True}
        row = experiment.sample(spec, "retest")
        self.assertEqual(row["status"], "incomplete")
        self.assertFalse(row["valid"])
        self.assertEqual(experiment.rows, [row])
        experiment.validate_static.assert_not_called()
        experiment.invoke.assert_not_called()

    def test_resume_skips_successes_and_preserves_original_incomplete(self):
        report, frozen = source_fixture()
        original = copy.deepcopy(report)
        plan = resume.remaining_plan(report, frozen)
        self.assertEqual(len(plan["successful_original_indexes"]), 65)
        self.assertEqual(len(plan["pending_original_specs"]), 6)
        self.assertEqual(len(plan["independent_attempt_specs"]), 1)
        self.assertEqual(plan["independent_attempt_specs"][0]["sst_budget"], 6500000)
        self.assertEqual(report, original)
        seen = {resume.identity(r["spec"]) for r in report["runs"] if r["phase"] == "measured"}
        self.assertTrue(all(resume.identity(s) not in seen for s in plan["pending_original_specs"]))

    def test_resume_rejects_changed_frozen_specs_duplicate_rounds_and_capacity(self):
        for failure in ("frozen", "row", "duplicate", "capacity", "protocol"):
            with self.subTest(failure=failure):
                report, frozen = source_fixture()
                if failure == "frozen":
                    frozen["specials"][8]["keys"] //= 2
                elif failure == "row":
                    report["runs"][8]["spec"]["threads"] = 3
                elif failure == "duplicate":
                    report["runs"].append(copy.deepcopy(report["runs"][8]))
                elif failure == "capacity":
                    report["sst_capacity_freeze"]["budget_bytes"] += 1
                else:
                    report["protocol"] = "metabypass-native-perf-v1"
                with self.assertRaises(run.RunFailure):
                    resume.remaining_plan(report, frozen)

    def test_resume_before_first_sst_sample_derives_budget_during_execution(self):
        report, frozen = source_fixture()
        report["runs"] = [row for row in report["runs"] if row["spec"]["stage"] != "specials"]
        del report["sst_capacity_freeze"]
        plan = resume.remaining_plan(report, frozen)
        self.assertEqual(plan["pending_original_specs"], frozen["specials"])
        self.assertFalse(plan["independent_attempt_specs"])
        experiment = object.__new__(run.Experiment)
        experiment.sst_budget = None
        experiment.report = {"blocked_groups": {}}
        experiment.sample = mock.Mock()
        experiment.sample_sst(plan["pending_original_specs"][0])
        disabled = plan["pending_original_specs"][0]
        experiment.sample.assert_called_once_with(disabled, "measured")
        experiment.sst_budget = 6500000
        observe = plan["pending_original_specs"][1]
        experiment.sample_sst(observe)
        experiment.sample.assert_called_with({**observe, "sst_budget": 6500000}, "measured")

    def test_resume_rejects_source_hash_and_foreign_ownership(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source.json"
            source.write_text('{"state":"incomplete"}')
            with self.assertRaises(run.RunFailure):
                resume.checked_json(source, "0" * 64)
            container = run.create_owned(root, "old", "source-id")
            with self.assertRaises(run.RunFailure):
                run.check_owned(container, root, "new-id")
            marker = json.loads((container / run.OWNER).read_text())
            marker["uid"] = os.getuid() + 1
            (container / run.OWNER).write_text(json.dumps(marker))
            with self.assertRaises(run.RunFailure):
                run.check_owned(container, root, "source-id")

    def test_adopted_static_space_uses_authenticated_source_device_roles(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            experiment = object.__new__(resume.ResumeExperiment)
            experiment.ssd, experiment.hdd = root / "new-ssd", root / "new-hdd"
            old = {"ssd": root / "old-ssd", "hdd": root / "old-hdd"}
            experiment.evidence = SimpleNamespace(source_roots=old)
            paths = {}
            for role, label in (("index", "ssd"), ("backup", "hdd")):
                path = old[label] / role / "db"
                path.mkdir(parents=True)
                (path / "payload").write_bytes(b"x" * (10 if label == "ssd" else 20))
                paths[role] = path
            with self.assertRaises(run.RunFailure):
                experiment.physical_space(paths)
            experiment.adopted_static_paths = set(paths.values())
            spaces = experiment.physical_space(paths)
            self.assertEqual(spaces["ssd"]["logical_bytes"], 10)
            self.assertEqual(spaces["hdd"]["logical_bytes"], 20)

    def test_attempts_do_not_fill_original_round_or_alias_source_raw_links(self):
        originals = [sample("D", n, 90) for n in (1, 2, 3)]
        originals += [sample("U", n, 100) for n in (1, 2, 3)]
        originals[0].update(valid=False, status="incomplete", error="telemetry")
        attempt = sample("D", 1, 1000)
        attempt["phase"] = "attempt"
        report = {"protocol": run.PROTOCOL, "profile": "formal", "state": "incomplete",
                  "runs": originals + [attempt]}
        result = summary.summarize(report)
        comparison = next(c for c in result["comparisons"] if c["candidate"] == "D" and c["reference"] == "U")
        self.assertEqual(comparison["paired_rounds"], [2, 3])
        self.assertFalse(comparison["complete"])
        self.assertEqual(len(result["independent_attempts"]), 1)
        roots = ["/remote/original", "/remote/resumed"]
        self.assertEqual(analyze.raw_link("/remote/original/0001-run/result.json", Path("/tmp/archive/resumed"), roots),
                         "[raw](../original/0001-run/result.json)")
        self.assertEqual(analyze.raw_link("/remote/resumed/0001-run/result.json", Path("/tmp/archive/resumed"), roots),
                         "[raw](0001-run/result.json)")


if __name__ == "__main__":
    unittest.main()
