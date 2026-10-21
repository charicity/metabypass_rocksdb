#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Comparison protocol tests, without compiling or benchmarking RocksDB."""
import copy
import unittest

import compare


def mixed_final_put_keys(spec):
    # Replay the driver's fixed-operation xorshift64 stream to budget live new
    # values conservatively; repeated Put/Delete of one key counts only once.
    def random(value):
        mask = (1 << 64) - 1
        value = (value ^ (value << 13)) & mask
        value ^= value >> 7
        return (value ^ (value << 17)) & mask

    mutations = {}
    for worker in range(spec["threads"]):
        lo = spec["keys"] * worker // spec["threads"]
        hi = spec["keys"] * (worker + 1) // spec["threads"]
        quota = spec["ops"] * (worker + 1) // spec["threads"] - spec["ops"] * worker // spec["threads"]
        rng = spec["seed"] + worker + 1
        for _ in range(quota):
            rng = random(rng)
            key = lo + rng % (hi - lo)
            rng = random(rng)
            choice = rng % 100
            if choice >= 90:
                mutations[key] = choice < 95
    return sum(mutations.values())


class ComparisonTests(unittest.TestCase):
    def report(self, profile="formal", write_bytes=4 * compare.runner.GIB, seconds=30, verify_samples=0):
        return {"profile": profile, "state": "completed", "arms": list(compare.ARMS),
                "matrix_scope": compare.matrix_scope(write_bytes, seconds, verify_samples),
                "runs": [{"valid": True, "status": "pass", "spec": spec,
                          "driver": {"events": [{"event": "sample", "name": "foreground",
                                                   "ops": 1, "elapsed_us": 1,
                                                   "hist_us": [[1, 1]]}]}}
                         for spec in compare.matrix(profile, write_bytes, seconds, verify_samples)]}

    def test_sample_policy_is_uniform_and_wrong_policy_blocks_ranking(self):
        for profile in ("formal", "smoke"):
            plan = compare.matrix(profile, 8 * compare.runner.GIB, 180, 4096)
            self.assertEqual(len(plan), 162)
            self.assertTrue(all(row["verify_samples"] == 4096 for row in plan))
            for row in plan:
                self.assertIn("--verify-samples=4096", compare.runner.driver_command("/binary", {}, row, "verify"))
        report = self.report(write_bytes=8 * compare.runner.GIB, seconds=180, verify_samples=4096)
        summary = compare.summarize(report)
        self.assertTrue(summary["ranking_allowed"])
        self.assertEqual(summary["matrix_scope"]["verification_protocol"], "metabypass-key-sample-v1")
        report["runs"][0]["spec"]["verify_samples"] = 2048
        self.assertFalse(compare.summarize(report)["ranking_allowed"])
        report = self.report(verify_samples=4096)
        del report["matrix_scope"]["verify_samples"]
        self.assertFalse(compare.summarize(report)["ranking_allowed"])
        self.assertNotIn("verify_samples", compare.matrix()[0])
        self.assertNotIn("verify_samples", compare.matrix_scope())

    def test_matrix_has_independent_three_round_pairs(self):
        self.assertEqual(compare.ARMS, ("original1s", "newfalse5s", "newtrue5s"))
        for profile in ("formal", "smoke"):
            with self.subTest(profile=profile):
                rows = compare.matrix(profile)
                self.assertEqual(len(rows), 162)
                self.assertEqual(len({(r["scenario"], r["round"], r["arm"]) for r in rows}), 162)
                self.assertEqual(len({r["scenario"] for r in rows}), 18)
                for arm in compare.ARMS:
                    self.assertEqual(sum(r["arm"] == arm for r in rows), 54)
                for number in range(1, 4):
                    order = compare.ARMS[number - 1:] + compare.ARMS[:number - 1]
                    for scenario in {r["scenario"] for r in rows}:
                        paired = [r for r in rows if r["scenario"] == scenario and r["round"] == number]
                        self.assertEqual(tuple(r["arm"] for r in paired), order)
                        self.assertEqual({r["seed"] for r in paired}, {20261020 + number})
                for spec in rows:
                    if spec["arm"] == "original1s":
                        self.assertNotIn("direct_write", spec)
                        self.assertNotIn("checkpoint_interval_ms", spec)
                        self.assertNotIn("mirror_interval_ms", spec)
                    else:
                        self.assertEqual(spec["checkpoint_interval_ms"], 5000)
                        self.assertEqual(spec["mirror_interval_ms"], 1000)
                        self.assertEqual(spec["direct_write"], spec["arm"] == "newtrue5s")
        rows = compare.matrix()
        self.assertTrue(all(r["gc_wait_seconds"] == 5 for r in rows))
        self.assertTrue(all(r["seconds"] == 0 for r in rows if r["workload"] == "insert"))

    def test_smoke_mixed_writes_exceed_saturated_capacity_without_changing_formal(self):
        smoke = compare.matrix("smoke")
        self.assertTrue(all(row["keys"] == 8192 and row["ops"] == 8192 for row in smoke))
        self.assertTrue(all(row["keys"] * row["value_size"] == 8 * compare.runner.MIB for row in smoke))
        self.assertTrue(all(row["seconds"] == 0 for row in smoke))
        self.assertTrue(all(row["staging_capacity"] == 16 * compare.runner.MIB
                            for row in smoke if row["staging_case"] == "ample"))
        saturated = [row for row in smoke if row["workload"] == "mixed" and row["staging_case"] == "saturated"]
        # All arms use identical paired streams, so replay one per round/thread.
        distinct = {(row["round"], row["threads"]): row for row in saturated}
        counts = [mixed_final_put_keys(row) for row in distinct.values()]
        self.assertEqual(min(counts), 348)
        for row in saturated:
            self.assertEqual(row["staging_capacity"], 65536)
            self.assertGreater(mixed_final_put_keys(row) * row["value_size"], row["staging_capacity"])
        old_small = {**saturated[0], "keys": 512, "ops": 512}
        self.assertLess(mixed_final_put_keys(old_small) * old_small["value_size"], old_small["staging_capacity"])
        for row in compare.matrix():
            self.assertEqual(row["keys"] * row["value_size"], 4 * compare.runner.GIB)
            self.assertEqual(row["ops"], row["keys"])
            self.assertEqual(row["seconds"], 0 if row["workload"] == "insert" else 30)

    def test_legacy_command_omits_new_flags(self):
        rows = compare.matrix("smoke")
        original = next(r for r in rows if r["arm"] == "original1s")
        direct = next(r for r in rows if r["arm"] == "newtrue5s")
        for spec in (original, direct):
            command = compare.runner.driver_command("/binary", {}, spec, "run")
            self.assertEqual(any(c.startswith("--direct-write=") for c in command), spec is direct)
            self.assertEqual(any(c.startswith("--checkpoint-interval-ms=") for c in command), spec is direct)
            self.assertIn("--sync=false", command)

    def test_staging_coverage_is_observed_not_assumed(self):
        spec = next(r for r in compare.matrix("smoke") if r["staging_case"] == "ample")
        row = {"spec": spec, "driver": {"events": [{"event": "stats", "peak_staging_bytes": 10,
                                                    "staging_spill_bytes": 0}]}}
        self.assertTrue(compare.coverage(row, "smoke")["complete"])
        row["driver"]["events"][0]["staging_spill_bytes"] = 1
        self.assertFalse(compare.coverage(row, "smoke")["complete"])
        row["spec"] = {**spec, "staging_case": "saturated"}
        self.assertTrue(compare.coverage(row, "smoke")["complete"])

    def test_formal_requires_foreground_publication_and_duration(self):
        spec = next(r for r in compare.matrix() if r["arm"] == "newtrue5s")
        events = [{"event": "sample", "name": "foreground", "elapsed_us": 15_000_000},
                  {"event": "stats", "boundary": "foreground_begin", "recovery_points": 2},
                  {"event": "stats", "boundary": "foreground_end", "recovery_points": 4}]
        row = {"spec": spec, "driver": {"events": events}}
        self.assertTrue(compare.coverage(row, "formal")["complete"])
        events[-1]["recovery_points"] = 3
        self.assertFalse(compare.coverage(row, "formal")["complete"])
        events[-1]["recovery_points"] = 4
        events[0]["elapsed_us"] = 1
        self.assertFalse(compare.coverage(row, "formal")["complete"])

    def test_summary_preserves_failures_and_rejects_duplicate_rounds(self):
        report = {"profile": "formal", "state": "incomplete", "runs": [
            {"valid": False, "status": "incomplete", "error": "coverage failed"}]}
        result = compare.summarize(report)
        self.assertFalse(result["ranking_allowed"])
        self.assertEqual(result["omitted"][0]["error"], "coverage failed")
        report = self.report()
        report["runs"].append(copy.deepcopy(report["runs"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            compare.summarize(report)

    def test_complete_formal_summary_allows_only_two_comparisons_per_scenario(self):
        for write_bytes, seconds in ((4 * compare.runner.GIB, 30), (8 * compare.runner.GIB, 60)):
            with self.subTest(write_bytes=write_bytes, seconds=seconds):
                result = compare.summarize(self.report(write_bytes=write_bytes, seconds=seconds))
                self.assertTrue(result["ranking_allowed"])
                self.assertEqual(result["arms"], list(compare.ARMS))
                self.assertEqual(result["matrix_scope"], compare.matrix_scope(write_bytes, seconds))
                self.assertEqual(len(result["comparisons"]), 36)
                self.assertTrue(all(c["paired_rounds"] == [1, 2, 3] and c["complete"]
                                    for c in result["comparisons"]))
                self.assertEqual({(c["candidate"], c["reference"], c["meaning"])
                                  for c in result["comparisons"]},
                                 {("newtrue5s", "newfalse5s", "direct_write_same_interval"),
                                  ("newtrue5s", "original1s", "overall_revision_and_interval")})
                self.assertEqual(result["omitted"], [])

    def test_summary_blocks_missing_failed_extra_and_mismatched_samples(self):
        for problem in ("missing", "failed", "extra", "seed", "interval", "workload", "arms", "scope"):
            with self.subTest(problem=problem):
                report = self.report()
                if problem == "missing":
                    report["runs"].pop()
                elif problem == "failed":
                    report["runs"][0].update(valid=False, status="failed", error="test failure")
                elif problem == "extra":
                    extra = copy.deepcopy(report["runs"][0])
                    extra["spec"]["scenario"] = "unexpected"
                    report["runs"].append(extra)
                elif problem == "seed":
                    report["runs"][0]["spec"]["seed"] += 1
                elif problem == "interval":
                    report["runs"][1]["spec"]["checkpoint_interval_ms"] = 1000
                elif problem == "workload":
                    report["runs"][0]["spec"]["workload"] = "mixed"
                elif problem == "arms":
                    report["arms"].append("unexpected")
                else:
                    report["matrix_scope"]["samples"] = 216
                result = compare.summarize(report)
                self.assertFalse(result["ranking_allowed"])
                self.assertEqual(len(result["comparisons"]), 36)
                if problem in ("missing", "failed", "seed", "interval", "workload"):
                    self.assertTrue(any(not c["complete"] for c in result["comparisons"]))
                if problem in ("failed", "extra", "seed", "interval", "workload"):
                    self.assertTrue(result["omitted"])

    def test_summary_blocks_smoke_and_uncompleted_reports(self):
        for profile, state in (("smoke", "completed"), ("formal", "incomplete"), ("formal", "running")):
            with self.subTest(profile=profile, state=state):
                report = self.report(profile)
                report["state"] = state
                self.assertFalse(compare.summarize(report)["ranking_allowed"])


if __name__ == "__main__":
    unittest.main()
