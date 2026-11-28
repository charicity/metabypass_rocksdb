#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

from argparse import Namespace
import csv
import json
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from sst_checkpoint_study import (level_rows, measurement_backup, parse_dump, phase,
                                   read_inventory, read_recovery_boundary, run_study,
                                   summarize, transient_files, validate_ssts,
                                   writer_publications)


MANIFEST_DUMP = (
    '{"EditNumber":0,"ColumnFamily":0,"Comparator":"leveldb.BytewiseComparator"}\n'
    '--------------- Column family "default"  (ID 0) --------------\n'
    'log number: 12\n'
    '--- level 0 --- version# 1 ---\n'
    ' 7:100[0 .. 5][a .. z]\n'
    '--- level 1 --- version# 1 ---\n'
    'next_file_number 12 last_sequence 5  prev_log_number 0 '
    'max_column_family 0 min_log_number_to_keep 0\n'
)


class CheckpointStudyTest(unittest.TestCase):
    def test_workloadd_capture_forwards_cgroup_and_archives_d_logs(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("driver", "ldb", "dataset", "workloadd"):
                (root / name).touch()
            front, back, cgroup = (root / name for name in ("front", "back", "cgroup"))
            for path in (front, back, cgroup):
                path.mkdir()
            output = root / "out"
            args = Namespace(
                driver=root / "driver", ldb=root / "ldb", dataset=root / "dataset",
                workload=root / "workloadd", output=output, front_root=front, back_root=back,
                cgroup=cgroup, memory_gib=8, threads=16, warmup_seconds=65, measure_seconds=180,
                checkpoint_ms=40000, timeout=1800, sst_async_min_level=-1,
                sst_async_delay_ms=1000, cleanup_data=False,
            )

            def prepare_runner_output(command, **kwargs):
                sample = command[command.index("--sample") + 1]
                self.assertTrue(sample.startswith("ycsb-d-"))
                self.assertEqual(command[command.index("--cgroup") + 1], str(cgroup))
                self.assertEqual(command[command.index("--memory-gib") + 1], "8")
                writer = back / sample / "writer"
                writer.mkdir(parents=True)
                (writer / "stdout.jsonl").touch()
                return child.return_value

            with patch("sst_checkpoint_study.subprocess.Popen") as child, \
                    patch("sst_checkpoint_study.summarize") as report:
                child.return_value.poll.return_value = 0
                child.return_value.returncode = 0
                child.side_effect = prepare_runner_output
                run_study(args)
                report.assert_called_once_with(output, root / "ldb")
            capture = json.loads((output / "capture.json").read_text())
            self.assertEqual(capture["workload"], "workloadd")
            self.assertTrue((output / "back/ycsb-d/writer/stdout.jsonl").is_file())

    def test_offline_summary_reads_d_and_legacy_a_archives(self):
        for capture_fields, sample, workload, boundary, boundary_type, manifest_seq in (
            ({}, "ycsb-a", "workloada", "4 12\n", "exact", ""),
            ({"workload": "workloadd"}, "ycsb-d", "workloadd",
             "MBR2 4 12\n", "lower_bound", ""),
            ({"workload": "workloadd"}, "ycsb-d", "workloadd",
             "MBR2 4 12 5\n", "lower_bound", "5"),
        ):
            with self.subTest(sample=sample, boundary=boundary), TemporaryDirectory() as directory:
                output = Path(directory)
                capture = dict(capture_fields, runner_returncode=0, final_latest="point-1")
                (output / "capture.json").write_text(json.dumps(capture))
                sample_path = output / "back" / sample
                writer = sample_path / "writer"
                writer.mkdir(parents=True)
                result = dict(status="passed", initial={"warmup_seconds": 1}, ops_per_second=1)
                (sample_path / "result.json").write_text(json.dumps(result))
                (writer / "process.json").write_text(json.dumps({"fault": {"monotonic_ns": 3*10**9}}))
                events = [
                    dict(event="backup", initial=True, completed=True, recovery_points=1, end_ns=1),
                    dict(event="measurement_start", monotonic_ns=10**9 + 1),
                    dict(event="measurement_end", monotonic_ns=2*10**9 + 1),
                ]
                (writer / "stdout.jsonl").write_text("".join(json.dumps(event) + "\n"
                                                            for event in events))
                point = output / "points/point-1"
                point.mkdir(parents=True)
                (point / "LATEST").write_text("point-1\n")
                (point / "RECOVERY").write_text(boundary)
                (point / "observation.json").write_text('{"observed_monotonic_ns": 1}')
                metadata = {"CURRENT": "MANIFEST-000001\n", "MANIFEST-000001": "fixture",
                            "OPTIONS-000002": "num_levels=2\n"}
                for name, contents in metadata.items():
                    (point / name).write_text(contents)
                (point / "INVENTORY").write_text("MBI7\n000007.sst 100\n000012.log 30\n" + "".join(
                    f"{name} {len(contents)}\n" for name, contents in metadata.items()))
                with patch("sst_checkpoint_study.subprocess.run", return_value=CompletedProcess(
                    args=["ldb"], returncode=0, stdout=MANIFEST_DUMP, stderr=""
                )):
                    report = json.loads(summarize(output, "ldb").read_text())
                self.assertEqual(report["workload"], workload)
                self.assertEqual(report["confirmed_points"], 1)
                with (output / "points.csv").open(newline="") as stream:
                    row, = csv.DictReader(stream)
                self.assertEqual(row["sequence"], "4")
                self.assertEqual(row["wal_floor"], "12")
                self.assertEqual(row["sequence_boundary_type"], boundary_type)
                self.assertEqual(row["manifest_sequence"], manifest_seq)

    def test_invalid_recovery_boundary(self):
        for boundary in (
            "", "5", "5 12 5", "MBR3 5 12", "MBR2 5", "MBR2 5 12 5 extra",
            "MBR2 5 12 4", f"MBR2 {1 << 56} 12", f"MBR2 5 12 {1 << 56}",
            f"MBR2 5 {1 << 64}", "MBR2 +5 12", "MBR2 5 -12",
        ):
            with self.subTest(boundary=boundary), TemporaryDirectory() as directory:
                path = Path(directory) / "RECOVERY"
                path.write_text(boundary)
                with self.assertRaisesRegex(ValueError, "invalid RECOVERY boundary"):
                    read_recovery_boundary(path)

    def test_same_number_move(self):
        rows = level_rows({11: (2, 10), 12: (0, 20)}, {11: (0, 10)}, 4)
        self.assertEqual([r["sst_bytes"] for r in rows], [20, 0, 10, 0])
        self.assertEqual([r["added_files"] for r in rows], [1, 0, 0, 0])
        self.assertEqual([r["added_bytes"] for r in rows], [20, 0, 0, 0])
        self.assertEqual([r["moved_in_bytes"] for r in rows], [0, 0, 10, 0])

    def test_dump_and_inventory_lengths(self):
        with self.assertRaisesRegex(ValueError, "missing final ldb MANIFEST summary"):
            parse_dump(MANIFEST_DUMP.rsplit("next_file_number", 1)[0])
        live, edits = parse_dump(MANIFEST_DUMP)
        self.assertEqual(len(edits), 1)
        self.assertEqual(live, {7: (0, 100)})
        validate_ssts(read_inventory("MBI7\n000007.sst 100\nCURRENT 16\n"), live)
        with self.assertRaisesRegex(ValueError, r"numbers/lengths differ: \[7\]"):
            validate_ssts(read_inventory("MBI7\n000007.sst 101\nCURRENT 16\n"), live)

    def test_transient_files(self):
        prior = {7: (0, 100)}
        replay = [
            {"AddedFiles": [{"FileNumber": 8, "Level": 0, "FileSize": 200},
                            {"FileNumber": 9, "Level": 1, "FileSize": 300},
                            {"FileNumber": 10, "Level": 0, "FileSize": 400}]},
            {"DeletedFiles": [{"FileNumber": 8, "Level": 0}, {"FileNumber": 7, "Level": 0}],
             "AddedFiles": [{"FileNumber": 8, "Level": 5, "FileSize": 200},
                            {"FileNumber": 7, "Level": 2, "FileSize": 100}]},
            {"DeletedFiles": [{"FileNumber": 8, "Level": 5}, {"FileNumber": 9, "Level": 1},
                              {"FileNumber": 7, "Level": 2}]},
        ]
        actual = transient_files(prior, {10: (0, 400)}, replay)
        self.assertEqual(actual, dict(transient_files=2, transient_bytes=500,
                                      transient_touch_l1plus_files=2,
                                      transient_touch_l1plus_bytes=500,
                                      transient_touch_l2plus_files=1,
                                      transient_touch_l2plus_bytes=200,
                                      transient_touch_l5plus_files=1,
                                      transient_touch_l5plus_bytes=200))

    def test_confirmations_and_phase(self):
        events = [
            dict(event="backup", initial=True, completed=True, recovery_points=1, end_ns=2000),
            dict(event="progress", recovery_points=3, last_publish_micros=20),
            dict(event="progress", recovery_points=3, last_publish_micros=20),
            dict(event="progress", recovery_points=4, last_publish_micros=60),
        ]
        initial, confirmed, publications = writer_publications(events, 60000, 3000)
        self.assertEqual((initial, confirmed, set(publications)), (1, 3, {3}))
        self.assertEqual(phase(1, initial, None, 3000, 8000, 50000), "initial")
        self.assertEqual(phase(2, initial, None, 3000, 8000, 50000), "unknown")
        self.assertEqual(phase(3, initial, publications[3]["last_publish_micros"] * 1000,
                               3000, 8000, 50000), "measurement")

    def test_measurement_end_confirms_point_and_earliest_snapshot_wins(self):
        backup = dict(event="backup", initial=True, completed=True, end_ns=2000,
                      recovery_points=1, last_publish_micros=1, stats_sample_monotonic_ns=1500)
        start = dict(event="measurement_start", recovery_points=2, last_publish_micros=4,
                     stats_sample_monotonic_ns=6000)
        progress = dict(event="progress", recovery_points=2, last_publish_micros=4,
                        stats_sample_monotonic_ns=8500)
        end = dict(event="measurement_end", recovery_points=3, last_publish_micros=12,
                   stats_sample_monotonic_ns=18000, last_checkpoint_sst_copy_bytes=25)
        late = dict(event="progress", recovery_points=4, last_publish_micros=19,
                    stats_sample_monotonic_ns=20000)
        _, confirmed, selected = writer_publications([backup, progress, start, end, late],
                                                    20000, 3000)
        self.assertEqual(confirmed, 3)
        self.assertEqual(selected, {1: backup, 2: start, 3: end})

    def test_measurement_counter_deltas_and_sampled_queue_peak(self):
        start = dict(monotonic_ns=1000, stats_sample_monotonic_ns=1100,
                     sst_async_copy_bytes=500, io_backup_write_bytes=600,
                     wal_backpressure_micros=200, wal_queued_bytes=22)
        end = dict(monotonic_ns=9000, stats_sample_monotonic_ns=9050,
                   sst_async_copy_bytes=800, io_backup_write_bytes=1000,
                   wal_backpressure_micros=205, wal_queued_bytes=45, peak_queued_bytes=100)
        events = [dict(event="progress", stats_sample_monotonic_ns=ns, wal_queued_bytes=size)
                  for ns, size in ((900, 901), (2000, 90), (9100, 92))]
        points = [dict(last_checkpoint_sst_copy_bytes=28, last_checkpoint_sst_ready_bytes=8)]
        stats = measurement_backup(start, end, events, points)
        self.assertEqual(stats["counter_deltas"], dict(sst_async_copy_bytes=300,
                                                      wal_backpressure_micros=5,
                                                      io_backup_write_bytes=400))
        self.assertEqual(stats["max_sampled_wal_queued_bytes"], 90)
        self.assertEqual(stats["process_queue_peak_upper_bound_bytes_at_measurement_end"], 100)
        self.assertEqual(stats["published_checkpoint_sst_copy_bytes"], 28)
        self.assertEqual(stats["published_checkpoint_sst_ready_bytes"], 8)


if __name__ == "__main__":
    unittest.main()
