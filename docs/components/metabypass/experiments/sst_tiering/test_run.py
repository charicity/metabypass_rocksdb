#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Protocol tests with a fake db_bench; no RocksDB build is required."""

import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("run.py")
SPEC = importlib.util.spec_from_file_location("sst_tiering_run", SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)

FAKE_BENCH = '''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

flags = dict(item[2:].split("=", 1) for item in sys.argv[1:])
db = Path(flags["db"])
db.mkdir()
(db.parents[1] / "external-marker").write_text("preserve")
backup = Path(flags["metabypass_backup_dir"])
mode = flags["metabypass_sst_mode"]
if mode == "adaptive" and os.environ.get("FAKE_SLEEP_ADAPTIVE") == "1":
    import time
    time.sleep(2)
for i in range(8 if mode != "adaptive" else 4):
    (db / (str(i) + ".sst")).write_bytes(b"x" * 2048)
if mode == "adaptive":
    store = backup / "sst-store"
    store.mkdir()
    for i in range(4, 8):
        (store / (str(i) + ".sst")).write_bytes(b"x" * 2048)
else:
    work = backup / "work"
    work.mkdir()
    (work / "0.sst").hardlink_to(db / "0.sst")

def emit(item):
    print("MB_SST_JSON " + json.dumps(item), flush=True)

keys = int(flags["num"])
has_warmup = int(flags["metabypass_sst_warmup_ops"]) > 0
has_measure = int(flags["metabypass_sst_measure_ops"]) > 0
fill_only = os.environ.get("FAKE_FILL_ONLY") == "1"
warmup_only = os.environ.get("FAKE_WARMUP_ONLY") == "1"
no_migration = os.environ.get("FAKE_NO_MIGRATION") == "1"
no_measured_samples = os.environ.get("FAKE_NO_MEASURED_SAMPLES") == "1"
fill_demotions = int(mode == "adaptive" and fill_only)
warmup_demotions = fill_demotions + int(
    mode == "adaptive" and has_warmup and not fill_only and not no_migration)
measured_demotions = warmup_demotions + int(
    mode == "adaptive" and has_measure and
    flags["metabypass_sst_workload"] == "switch" and
    not fill_only and not warmup_only and not no_migration)
measured_promotions = int(
    mode == "adaptive" and has_measure and not fill_only and
    not warmup_only and not no_migration and
    os.environ.get("FAKE_NO_POST_SWITCH_PROMOTION") != "1")
warmup_samples = int(mode != "disabled" and has_warmup) * 5
measured_samples = warmup_samples + int(
    mode != "disabled" and has_measure and not no_measured_samples) * 10
def emit_stats(name, samples, promotions, demotions):
    end_hot = (name == "measured" and mode == "adaptive" and
               flags["metabypass_sst_workload"] == "mixed" and
               os.environ.get("FAKE_MIXED_END_HOT") == "1")
    emit({"event": "phase_stats", "name": name,
          "sampled_reads": samples, "promotions": promotions,
          "demotions": demotions,
          "ssd_sst_bytes": 16384 if end_hot or not (
              promotions + demotions) else 8192,
          "hdd_sst_bytes": 16384, "protected_bytes": 16384,
          "unprotected_bytes": 0, "reserved_bytes": 0,
          "pending_delete_bytes": 0})

emit({"event": "phase", "name": "fill", "keys": keys})
emit({"event": "phase_end", "name": "fill", "ok": True})
emit_stats("fill", 0, 0, fill_demotions)
emit({"event": "phase", "name": "warmup"})
emit({"event": "phase_end", "name": "warmup", "ok": True})
emit_stats("warmup", warmup_samples, 0, warmup_demotions)
emit({"event": "phase", "name": "measured"})
if has_measure:
    if flags["metabypass_sst_workload"] == "switch":
        emit({"event": "window", "sampled_reads_delta":
              0 if mode == "disabled" or no_measured_samples else 5,
              "promotions_delta": 0,
              "demotions_delta": measured_demotions - warmup_demotions,
              "phase_elapsed_us": 500000})
        emit({"event": "hotspot_switch", "elapsed_us": 1000000})
    emit({"event": "window", "sampled_reads_delta":
          0 if mode == "disabled" or no_measured_samples else
          (5 if flags["metabypass_sst_workload"] == "switch" else 10),
          "promotions_delta": measured_promotions, "demotions_delta": 0,
          "phase_elapsed_us": 2000000})
emit({"event": "phase_end", "name": "measured", "ok": True})
emit_stats("measured", measured_samples, measured_promotions,
           measured_demotions)
emit({"event": "verification", "keys": keys, "ok": True})
fail = mode == "adaptive" and os.environ.get("FAKE_FAIL_ADAPTIVE") == "1"
hot_gets = (0 if flags["metabypass_sst_workload"] == "uniform" else 800) if has_measure else 0
cold_gets = (1000 if flags["metabypass_sst_workload"] == "uniform" else 200) if has_measure else 0
cold_mask = (0x300 if os.environ.get("FAKE_SKEWED_COLD") == "1" else 0x3ff) if has_measure else 0
emit({"event": "summary", "ok": not fail,
      "sampled_reads": measured_samples,
      "measured_hot_gets": hot_gets, "measured_cold_gets": cold_gets,
      "measured_cold_key_mod10_mask": cold_mask,
      "promotions": measured_promotions, "demotions": measured_demotions})
if fail:
    print("intentional adaptive failure", file=sys.stderr)
    raise SystemExit(7)
'''


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / "fake_db_bench.py"
        self.binary.write_text(FAKE_BENCH)
        self.binary.chmod(0o755)
        self.ssd = self.root / "ssd"
        self.hdd = self.root / "hdd"
        self.output = self.root / "result.json"

    def invoke(self, repeats=1, profile="smoke", workload="uniform", extra=()):
        argv = [str(SCRIPT), "--binary", str(self.binary),
                "--ssd-root", str(self.ssd), "--hdd-root", str(self.hdd),
                "--output", str(self.output), "--profile", profile,
                "--repeats", str(repeats), "--workloads", workload,
                "--cache-bytes", "1024", *extra]
        configs = (("disabled", None), ("observe", 25),
                   ("adaptive", 25))
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(RUNNER, "CONFIGS", configs), \
                mock.patch.object(RUNNER, "source_identity", return_value={
                    "head": "fake", "source_sha256": "fake"}):
            return RUNNER.main()

    def report(self):
        return json.loads(self.output.read_text())

    def test_frozen_budget_repeats_and_owned_cleanup(self):
        self.invoke(repeats=3)
        report = self.report()
        self.assertEqual("complete", report["status"])
        self.assertEqual(9, len(report["trials"]))
        self.assertEqual(16384, report["baseline_sst_logical_bytes"])
        self.assertEqual(4096, report["budgets"]["25"])
        self.assertTrue(all(row["budget_bytes"] == 4096 for row in
                            report["trials"] if row["mode"] != "disabled"))
        self.assertTrue(all(row["valid"] for row in report["trials"]))
        self.assertTrue((self.ssd / "external-marker").exists())
        self.assertEqual(["external-marker"],
                         sorted(path.name for path in self.ssd.iterdir()))
        self.assertEqual([], list(self.hdd.iterdir()))
        adaptive = [row for row in report["space_comparisons"] if
                    row["mode"] == "adaptive"]
        self.assertTrue(all(row["ssd_sst_category_allocated_delta_bytes"] > 0
                            for row in adaptive))
        self.assertTrue(all(row["ssd_physical_saved_bytes"] is None for
                            row in adaptive))

    def test_hardlink_dedup_across_categories(self):
        self.ssd.mkdir()
        self.hdd.mkdir()
        (self.ssd / "1.sst").write_bytes(b"x" * 8192)
        (self.hdd / "1.sst").hardlink_to(self.ssd / "1.sst")
        result = RUNNER.space(self.ssd, self.hdd)
        self.assertEqual(8192, result["logical_bytes_by_path_category"][
            "ssd_sst"])
        self.assertEqual(8192, result["logical_bytes_by_path_category"][
            "hdd_sst"])
        self.assertEqual(1, result["hardlinked_extra_paths"])
        self.assertLess(result["physical_allocated_unique_bytes"], sum(
            result["physical_allocated_by_category_unique_inodes"].values()))

    def test_failure_keeps_raw_log_and_failed_trial_data(self):
        with mock.patch.dict(os.environ, {"FAKE_FAIL_ADAPTIVE": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke()
        report = self.report()
        self.assertEqual("failed_trials", report["status"])
        self.assertEqual(3, len(report["trials"]))
        failed = report["trials"][-1]
        self.assertFalse(failed["valid"])
        self.assertEqual(7, failed["exit_code"])
        self.assertIn("intentional adaptive failure", failed["stderr"])
        self.assertIn("MB_SST_JSON", failed["stdout"])
        self.assertTrue(Path(failed["ssd_dir"]).exists())
        self.assertFalse(Path(report["trials"][0]["ssd_dir"]).exists())

    def test_timeout_and_missing_mechanism_are_not_success(self):
        timeout = RUNNER.execute([sys.executable, "-c",
                                  "import time; time.sleep(2)"], 0.02)
        self.assertTrue(timeout["timed_out"])
        self.assertNotEqual(0, timeout["exit_code"])
        with mock.patch.dict(os.environ, {"FAKE_NO_MIGRATION": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke()
        report = self.report()
        self.assertEqual("coverage_incomplete", report["status"])
        self.assertTrue(all(row["valid"] for row in report["trials"]))
        self.assertFalse(report["mechanism_coverage"][
            "all_adaptive_observed"])

    def test_skewed_cold_key_distribution_is_rejected(self):
        with mock.patch.dict(os.environ, {"FAKE_SKEWED_COLD": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke()
        report = self.report()
        self.assertEqual("failed_trials", report["status"])
        self.assertEqual(3, len(report["trials"]))
        self.assertTrue(all(not row["valid"] for row in report["trials"]))
        self.assertEqual(0x300,
                         report["trials"][0]["distribution"][
                             "cold_key_mod10_mask"])

    def test_timed_out_trial_is_recorded_and_retained(self):
        short = RUNNER.PROFILES["smoke"].copy()
        short["timeout_s"] = 0.5
        with mock.patch.dict(RUNNER.PROFILES, {"smoke": short}), \
                mock.patch.dict(os.environ, {"FAKE_SLEEP_ADAPTIVE": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke()
        report = self.report()
        self.assertEqual("failed_trials", report["status"])
        self.assertEqual(3, len(report["trials"]))
        timed_out = report["trials"][-1]
        self.assertTrue(timed_out["timed_out"])
        self.assertFalse(timed_out["valid"])
        self.assertTrue(Path(timed_out["ssd_dir"]).exists())

    def test_switch_response_uses_post_switch_promotion(self):
        self.invoke(workload="switch")
        report = self.report()
        self.assertEqual(1, report["mechanism_coverage"][
            "switch_trials_with_post_switch_promotion"])
        adaptive = [row for row in report["trials"] if row["mode"] ==
                    "adaptive"]
        self.assertEqual(1000000, adaptive[0]["hotspot_adaptation_us"])

    def test_switch_without_promotion_keeps_valid_coverage(self):
        with mock.patch.dict(os.environ,
                             {"FAKE_NO_POST_SWITCH_PROMOTION": "1"}):
            self.invoke(workload="switch")
        report = self.report()
        self.assertEqual("complete", report["status"])
        self.assertEqual(0, report["mechanism_coverage"][
            "switch_trials_with_post_switch_promotion"])
        adaptive = [row for row in report["trials"] if row["mode"] ==
                    "adaptive"]
        self.assertIsNone(adaptive[0]["hotspot_adaptation_us"])
        self.assertTrue(adaptive[0]["mechanism_observed"])

    def test_warmup_migration_with_measured_samples_and_cold_is_covered(self):
        with mock.patch.dict(os.environ, {"FAKE_WARMUP_ONLY": "1"}):
            self.invoke()
        report = self.report()
        self.assertEqual("complete", report["status"])
        adaptive = report["trials"][-1]
        self.assertEqual(1, adaptive["migrations_warmup"])
        self.assertEqual(0, adaptive["migrations_measured"])
        self.assertEqual(10, adaptive["sampled_reads_measured"])
        self.assertGreater(adaptive["phase_evidence"][
            "cold_live_bytes_lower_bound"], 0)
        self.assertTrue(adaptive["mechanism_observed"])
        self.assertEqual("warmup_settled_cold", adaptive["mechanism_basis"])

    def test_measured_migration_covers_mixed_even_if_end_is_all_hot(self):
        with mock.patch.dict(os.environ, {"FAKE_MIXED_END_HOT": "1"}):
            self.invoke(workload="mixed")
        report = self.report()
        self.assertEqual("complete", report["status"])
        adaptive = report["trials"][-1]
        self.assertEqual(0, adaptive["phase_evidence"][
            "cold_live_bytes_lower_bound"])
        self.assertGreater(adaptive["migrations_measured"], 0)
        self.assertEqual("measured_migration", adaptive["mechanism_basis"])
        self.assertTrue(adaptive["mechanism_observed"])

    def test_warmup_only_without_cold_end_is_not_covered(self):
        with mock.patch.dict(os.environ, {"FAKE_WARMUP_ONLY": "1",
                                        "FAKE_MIXED_END_HOT": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke(workload="mixed")
        report = self.report()
        adaptive = report["trials"][-1]
        self.assertEqual("coverage_incomplete", report["status"])
        self.assertEqual(0, adaptive["migrations_measured"])
        self.assertGreater(adaptive["migrations_warmup"], 0)
        self.assertEqual(0, adaptive["phase_evidence"][
            "cold_live_bytes_lower_bound"])
        self.assertEqual("none", adaptive["mechanism_basis"])

    def test_audit_reparses_stdout_without_changing_original(self):
        with mock.patch.dict(os.environ, {"FAKE_MIXED_END_HOT": "1"}):
            self.invoke(workload="mixed")
        original = self.report()
        adaptive = original["trials"][-1]
        adaptive["events"] = []
        adaptive["mechanism_observed"] = False
        adaptive["valid"] = False
        original["status"] = "coverage_incomplete"
        self.output.write_text(json.dumps(original))
        before = hashlib.sha256(self.output.read_bytes()).hexdigest()
        audit_path = self.root / "audit.json"
        subprocess.run([sys.executable, str(SCRIPT.with_name(
            "audit_existing.py")), "--input", str(self.output),
            "--output", str(audit_path)], check=True, capture_output=True)
        audit = json.loads(audit_path.read_text())
        self.assertEqual(before, hashlib.sha256(self.output.read_bytes()).hexdigest())
        self.assertEqual(before, audit["source_json_sha256"])
        self.assertEqual("complete", audit["reparsed_status"])
        self.assertEqual(1, audit["changed_mechanism_verdicts"])
        self.assertEqual(1, audit["changed_validity_verdicts"])
        self.assertEqual("measured_migration", audit["trials"][-1][
            "mechanism_basis"])

    def test_audit_rejects_missing_trial_even_if_remaining_ones_pass(self):
        self.invoke()
        original = self.report()
        original["trials"].pop()
        self.output.write_text(json.dumps(original))
        audit_path = self.root / "audit-missing.json"
        subprocess.run([sys.executable, str(SCRIPT.with_name(
            "audit_existing.py")), "--input", str(self.output),
            "--output", str(audit_path)], check=True, capture_output=True)
        audit = json.loads(audit_path.read_text())
        self.assertFalse(audit["trial_matrix_complete"])
        self.assertEqual("incomplete_trial_matrix", audit["reparsed_status"])

    def test_audit_rejects_binary_changed_during_trials(self):
        self.invoke()
        original = self.report()
        original["binary"]["sha256_at_end"] = "0" * 64
        self.output.write_text(json.dumps(original))
        audit_path = self.root / "audit-binary-changed.json"
        subprocess.run([sys.executable, str(SCRIPT.with_name(
            "audit_existing.py")), "--input", str(self.output),
            "--output", str(audit_path)], check=True, capture_output=True)
        audit = json.loads(audit_path.read_text())
        self.assertTrue(audit["trial_matrix_complete"])
        self.assertTrue(all(row["reparsed_valid"] for row in audit["trials"]))
        self.assertFalse(audit["binary_hashes_match"])
        self.assertEqual(original["binary"]["sha256"],
                         audit["source_binary_sha256"])
        self.assertEqual("0" * 64, audit["source_binary_sha256_at_end"])
        self.assertEqual("binary_changed", audit["reparsed_status"])

    def test_fill_only_migration_does_not_prove_read_workload(self):
        with mock.patch.dict(os.environ, {"FAKE_FILL_ONLY": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke()
        report = self.report()
        self.assertEqual("coverage_incomplete", report["status"])
        adaptive = report["trials"][-1]
        self.assertEqual(1, adaptive["migrations_fill"])
        self.assertEqual(0, adaptive["migrations_warmup"])
        self.assertEqual(0, adaptive["migrations_measured"])
        self.assertEqual(10, adaptive["sampled_reads_measured"])
        self.assertGreater(adaptive["phase_evidence"][
            "cold_live_bytes_lower_bound"], 0)
        self.assertFalse(adaptive["mechanism_observed"])

    def test_missing_measured_samples_is_not_covered(self):
        with mock.patch.dict(os.environ,
                             {"FAKE_NO_MEASURED_SAMPLES": "1"}):
            with self.assertRaises(SystemExit):
                self.invoke()
        report = self.report()
        self.assertEqual("coverage_incomplete", report["status"])
        adaptive = report["trials"][-1]
        self.assertEqual(0, adaptive["sampled_reads_measured"])
        self.assertFalse(adaptive["mechanism_observed"])

    def test_rejects_insufficient_repeats_and_conflicting_roots(self):
        with open(os.devnull, "w") as sink, contextlib.redirect_stderr(sink):
            with self.assertRaises(SystemExit):
                self.invoke(repeats=2, profile="standard")
            with self.assertRaises(SystemExit):
                self.invoke(extra=("--hdd-root", str(self.ssd)))
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
