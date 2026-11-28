# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

import argparse
import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from run import (COMPARISON_KEYS, PROTOCOL, PUT_SPAN, add_sst_async_arguments,
                 checkpoint_timing, compare, sequence_age)


def write_timing_log(path, events):
    path.write_text("host message\n" + "".join(
        json.dumps(event, separators=(",", ":")) + "\n" for event in events))


def capture_timing_event(left, right, trigger="periodic"):
    return dict(event="mb_checkpoint_timing", trigger=trigger, gate_present=False,
                success=True, capture_begin_ns=left, capture_end_ns=right,
                live_files_ns=right - left, sequence_lower_bound=10)


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

    def test_checkpoint_timing_mode_must_match(self):
        self.assertAlmostEqual(compare(self.left, self.right)["throughput_ratio"], 1.1)
        self.left["checkpoint_timing"] = {"puts": {"put_samples": 110}}
        for left, right in ((self.left, self.right), (self.right, self.left)):
            with self.subTest(left=left["sample"]):
                result = compare(left, right)
                self.assertEqual(result["reason"], "checkpoint timing mode differs")
                self.assertIsNone(result["throughput_ratio"])
                self.assertIsNone(result["throughput_change_pct"])
        self.right["checkpoint_timing"] = {"puts": {"put_samples": 100}}
        result = compare(self.left, self.right)
        self.assertIsNone(result["reason"])
        self.assertAlmostEqual(result["throughput_ratio"], 1.1)

    def test_sst_async_options(self):
        parser = argparse.ArgumentParser()
        add_sst_async_arguments(parser)
        self.assertEqual(vars(parser.parse_args([])),
                         dict(sst_async_min_level=-1, sst_async_delay_ms=1000))
        args = parser.parse_args(["--sst-async-min-level", "5", "--sst-async-delay-ms", "25000"])
        self.assertEqual(vars(args), dict(sst_async_min_level=5, sst_async_delay_ms=25000))

    def test_sequence_bounds(self):
        events = [
            dict(event="progress", sequence=seq, query_begin_ns=begin, query_end_ns=end)
            for seq, begin, end in ((100, 101, 102), (120, 201, 202), (140, 301, 302))
        ]
        fault = {"monotonic_ns": 300}
        self.assertEqual(sequence_age(events, 110, fault), dict(lower_ns=98, upper_ns=199))
        self.assertEqual(sequence_age(events, 120, fault), dict(lower_ns=None, upper_ns=99))

    def test_checkpoint_timing_intervals(self):
        spans = [(90, 100), (95, 106), (110, 120), (105, 125), (119, 144),
                 (121, 155), (151, 170), (173, 200), (185, 220), (197, 197)]
        gates = [dict(event="mb_checkpoint_timing", trigger=trigger,
                      gate_begin_ns=left, gate_end_ns=right)
                 for left, right, trigger in ((170, 210, "periodic"),
                                              (90, 110, "periodic"),
                                              (120, 150, "periodic"),
                                              (160, 169, "requested"),
                                              (220, 240, "periodic"))]
        waits = [dict(event="mb_write_gate_wait", wait_begin_ns=left,
                      wait_end_ns=right, wait_ns=right - left)
                 for left, right in ((103, 112), (130, 158), (148, 175),
                                     (175, 203), (151, 169))]
        with TemporaryDirectory() as directory:
            trace, log = Path(directory) / "spans", Path(directory) / "stderr"
            trace.write_bytes(b"".join(PUT_SPAN.pack(*span) for span in spans))
            write_timing_log(log, gates + waits)
            summary = checkpoint_timing(trace, log, 100, 200)

        self.assertEqual(summary["puts"],
                         dict(put_samples=8, max_put_ns=34, put_p99_ns=34))
        self.assertEqual(summary["overlap"],
                         dict(put_samples=6, max_put_ns=35, put_p99_ns=None))
        points = summary["checkpoints"]
        self.assertEqual([p["partial_window"] for p in points], [True, False, True])
        self.assertEqual([p["overlap"]["put_samples"] for p in points], [2, 3, 2])
        self.assertEqual(points[1]["puts_started_before_gate"], 2)
        self.assertEqual([p["waiting_puts"] for p in points], [1, 2, 2])
        self.assertEqual([p["longest_gate_wait_ns"] for p in points], [7, 20, 25])
        self.assertEqual(summary["unattributed_wait_records"], 1)

    def test_checkpoint_timing_p99_minimum_and_truncated_trace(self):
        with TemporaryDirectory() as directory:
            trace, log = Path(directory) / "spans", Path(directory) / "stderr"
            log.write_text('{"event":"mb_checkpoint_timing","trigger":"periodic",'
                           '"gate_begin_ns":100,"gate_end_ns":300}\n')
            spans = b"".join(PUT_SPAN.pack(101, 101 + duration)
                             for duration in range(1, 101))
            trace.write_bytes(spans[:-PUT_SPAN.size])
            self.assertIsNone(checkpoint_timing(trace, log, 100, 300)["overlap"]["put_p99_ns"])
            trace.write_bytes(spans)
            timing = checkpoint_timing(trace, log, 100, 300)
            self.assertEqual(timing["overlap"],
                             dict(put_samples=100, max_put_ns=100, put_p99_ns=99))
            trace.write_bytes(spans[:-1])
            with self.assertRaisesRegex(ValueError, "incomplete Put timing record"):
                checkpoint_timing(trace, log, 100, 300)
            log.write_text("")
            with self.assertRaisesRegex(ValueError, "no MB checkpoint timing events"):
                checkpoint_timing(trace, log, 100, 300)


    def test_checkpoint_timing_capture_intervals(self):
        spans = [(101, 101 + duration) for duration in range(1, 101)]
        spans += [(95, 180), (160, 170)]
        events = [capture_timing_event(140, 160), capture_timing_event(90, 130),
                  capture_timing_event(165, 175, "requested"),
                  capture_timing_event(210, 240)]
        with TemporaryDirectory() as directory:
            trace, log = Path(directory) / "spans", Path(directory) / "stderr"
            trace.write_bytes(b"".join(PUT_SPAN.pack(*span) for span in spans))
            write_timing_log(log, events)
            summary = checkpoint_timing(trace, log, 100, 200)

        empty = dict(put_samples=0, max_put_ns=None, put_p99_ns=None)
        self.assertEqual(summary["puts"],
                         dict(put_samples=100, max_put_ns=98, put_p99_ns=97))
        self.assertEqual(summary["overlap"], empty)
        self.assertEqual(summary["capture_overlap"],
                         dict(put_samples=101, max_put_ns=100, put_p99_ns=99))
        self.assertEqual(summary["unattributed_wait_records"], 0)
        first, second = summary["checkpoints"]
        self.assertTrue(first["partial_window"])
        self.assertFalse(second["partial_window"])
        self.assertEqual(first["capture_overlap"], summary["capture_overlap"])
        self.assertEqual(second["capture_overlap"],
                         dict(put_samples=62, max_put_ns=100, put_p99_ns=None))
        for point in (first, second):
            self.assertEqual(point["overlap"], empty)
            self.assertEqual(point["puts_started_before_gate"], 0)
            self.assertEqual(point["waiting_puts"], 0)
            self.assertIsNone(point["longest_gate_wait_ns"])

    def test_checkpoint_timing_rejects_invalid_capture(self):
        capture = capture_timing_event(100, 150)
        closed_gate = dict(event="mb_checkpoint_timing", trigger="periodic",
                           gate_begin_ns=160, gate_end_ns=180)
        gate_wait = dict(event="mb_write_gate_wait", wait_begin_ns=160,
                         wait_end_ns=180, wait_ns=20)
        cases = [
            ([dict(capture, gate_present="false")], "invalid checkpoint gate_present"),
            ([dict(capture, capture_begin_ns=None)], "invalid checkpoint capture interval"),
            ([dict(capture, capture_end_ns=99)], "invalid checkpoint capture interval"),
            ([dict(capture, trigger=None)], "invalid checkpoint timing trigger"),
            ([dict(capture, live_files_ns=49)], "invalid checkpoint capture fields"),
            ([dict(capture, sequence_lower_bound=True)], "invalid checkpoint capture fields"),
            ([dict(capture, success=1)], "invalid checkpoint capture fields"),
            ([capture, capture_timing_event(145, 175)], "checkpoint capture intervals overlap"),
            ([capture, closed_gate], "mixed checkpoint timing modes"),
            ([capture, gate_wait], "unexpected write gate waits"),
        ]
        with TemporaryDirectory() as directory:
            trace, log = Path(directory) / "spans", Path(directory) / "stderr"
            trace.write_bytes(b"")
            for events, message in cases:
                with self.subTest(events=events):
                    write_timing_log(log, events)
                    with self.assertRaisesRegex(ValueError, message):
                        checkpoint_timing(trace, log, 100, 200)
            log.write_text('{"event":"mb_checkpoint_timing","gate_present":false,')
            with self.assertRaises(json.JSONDecodeError):
                checkpoint_timing(trace, log, 100, 200)


if __name__ == "__main__":
    unittest.main()
