# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

import copy
import unittest

from run import COMPARISON_KEYS, PROTOCOL, compare, sequence_age


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.left = {
            "sample": "left",
            "protocol": PROTOCOL,
            "status": "passed",
            "initial": {
                "initial_verified": True,
                "dataset_sha256": "a" * 64,
                "dataset_logical_sha256": "b" * 64,
                "initial_sha256": "b" * 64,
                "initial_records": 1024,
            },
            "identity": {"core_commit": "old-core"},
            "comparison": dict.fromkeys(COMPARISON_KEYS, "matching"),
            "ops_per_second": 110,
        }
        self.right = copy.deepcopy(self.left)
        self.right.update(sample="right", identity={"core_commit": "new-core"},
                          ops_per_second=100)

    def test_matching_initial_contents_across_cores(self):
        self.assertAlmostEqual(compare(self.left, self.right)["throughput_ratio"], 1.1)

    def test_unverified_contents(self):
        for key in ("initial_verified", "initial_sha256"):
            row = copy.deepcopy(self.right)
            del row["initial"][key]
            self.assertIsNone(compare(self.left, row)["throughput_ratio"])
        self.right["initial"]["initial_sha256"] = "c" * 64
        self.assertIn("verification missing", compare(self.left, self.right)["reason"])

    def test_independent_fixture_rejected(self):
        self.right["initial"].update(dataset_sha256="c" * 64,
                                     initial_sha256="d" * 64,
                                     dataset_logical_sha256="d" * 64)
        self.assertEqual(compare(self.left, self.right)["reason"], "initial datasets differ")

    def test_workload_and_observation_must_match(self):
        self.right["comparison"]["memory_gib"] = 8
        self.assertIsNone(compare(self.left, self.right)["throughput_ratio"])
        self.right["comparison"]["memory_gib"] = self.left["comparison"]["memory_gib"]
        for row in (self.left, self.right):
            del row["comparison"]["ycsb_commit"]
        self.assertIsNone(compare(self.left, self.right)["throughput_ratio"])
        self.right = copy.deepcopy(self.left)
        self.right["status"] = "failed"
        self.assertIsNone(compare(self.left, self.right)["throughput_ratio"])
        self.right["protocol"] = "legacy"
        self.assertIsNone(compare(self.left, self.right)["throughput_ratio"])

    def test_sequence_bounds(self):
        events = [
            dict(event="progress", sequence=seq, query_begin_ns=begin, query_end_ns=end)
            for seq, begin, end in ((100, 101, 102), (120, 201, 202), (140, 301, 302))
        ]
        fault = {"monotonic_ns": 300}
        self.assertEqual(sequence_age(events, 110, fault), dict(lower_ns=98, upper_ns=199))
        self.assertEqual(sequence_age(events, 120, fault), dict(lower_ns=None, upper_ns=99))


if __name__ == "__main__":
    unittest.main()
