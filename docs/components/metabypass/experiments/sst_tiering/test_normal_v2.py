#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Bounded copying and normal-v1 safety through the independent v2 entry."""

import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import node_clone_bounded as CLONE
import node_normal_v2 as V2
import test_normal as NORMAL_FIXTURE
import test_tradeoff as FIXTURE


class NormalV2Test(unittest.TestCase):
    def setUp(self):
        FIXTURE.TradeoffTest.setUp(self)

    def targets(self, name):
        return (self.ssd / name, self.hdd / name)

    def runner(self, *extra):
        return V2.NormalRunnerV2(V2.normal.parse_args(FIXTURE.TradeoffTest.arguments(self, *extra)))

    def test_exact_bytes_and_single_fixed_buffer_for_empty_small_and_large_files(self):
        sizes = (0, 1, CLONE.BUFFER_BYTES, CLONE.BUFFER_BYTES + 7, 2 * CLONE.SYNC_BATCH_BYTES + 31)
        for index, size in enumerate(sizes):
            (self.seed[1] / ("payload-%d" % index)).write_bytes(b"a" * size)
        original = CLONE.io.FileIO
        buffers = []

        class Reader:
            def __init__(self, *args, **kwargs):
                self.file = original(*args, **kwargs)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.file.close()
            def readinto(self, buffer):
                buffers.append((id(buffer), len(buffer)))
                return self.file.readinto(buffer)

        targets = self.targets("clone")
        with mock.patch.object(CLONE.io, "FileIO", Reader):
            result = CLONE.clone_dataset(zip(self.seed, targets))
        self.assertEqual(1, len({identity for identity, _ in buffers}))
        self.assertEqual({65536}, {size for _, size in buffers})
        self.assertEqual(65536, result["buffer_bytes"])
        for source, target in zip(self.seed, targets):
            for path in source.rglob("*"):
                if path.is_file():
                    self.assertEqual(path.read_bytes(), (target / path.relative_to(source)).read_bytes())

    def test_short_reads_and_short_writes_never_exceed_one_mib_before_sync(self):
        payload = self.seed[1] / "large"
        payload.write_bytes(b"x" * (2 * CLONE.SYNC_BATCH_BYTES + 231))
        original_reader, original_write = CLONE.io.FileIO, CLONE.os.write
        pending = [0]
        batches = []

        class Reader:
            def __init__(self, *args, **kwargs):
                self.file = original_reader(*args, **kwargs)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.file.close()
            def readinto(self, buffer):
                return self.file.readinto(memoryview(buffer)[:17003])

        def write(fd, view):
            n = original_write(fd, view[:12345])
            pending[0] += n
            self.assertLessEqual(pending[0], CLONE.SYNC_BATCH_BYTES)
            return n

        def sync(fd):
            batches.append(pending[0])
            pending[0] = 0

        with mock.patch.object(CLONE.io, "FileIO", Reader), \
                mock.patch.object(CLONE.os, "write", side_effect=write), \
                mock.patch.object(CLONE.os, "fdatasync", side_effect=sync):
            result = CLONE.clone_dataset(zip(self.seed, self.targets("clone")))
        self.assertEqual(0, pending[0])
        self.assertEqual(2, batches.count(CLONE.SYNC_BATCH_BYTES))
        self.assertIn(231, batches)
        self.assertEqual(len(batches), result["fdatasync_calls"])
        self.assertEqual(payload.read_bytes(), (self.hdd / "clone/large").read_bytes())

    def test_zero_progress_and_sync_failure_keep_partial_files_and_raise(self):
        source = self.seed[1] / "data/blob"
        for index, patcher in enumerate((mock.patch.object(CLONE.os, "write", return_value=0),
                                        mock.patch.object(CLONE.os, "fdatasync", side_effect=OSError("sync failed")))):
            target = self.root / ("partial-%d" % index)
            with patcher, self.assertRaises(OSError):
                CLONE.copy_file(source, target, source.lstat(), bytearray(CLONE.BUFFER_BYTES))
            self.assertTrue(target.is_file())
        self.assertEqual(b"immutable blob", source.read_bytes())

    def test_internal_cross_root_hardlinks_are_independent_across_seed_and_clones(self):
        original = self.seed[0] / "index/000001.sst"
        os.link(str(original), str(self.seed[0] / "index/alias"))
        os.link(str(original), str(self.seed[1] / "alias"))
        clone_inodes = []
        for name in ("first", "second"):
            targets = self.targets(name)
            result = CLONE.clone_dataset(zip(self.seed, targets))
            self.assertEqual(2, result["reconstructed_internal_links"])
            paths = [targets[0] / "index/000001.sst", targets[0] / "index/alias", targets[1] / "alias"]
            self.assertEqual(1, len({(p.stat().st_dev, p.stat().st_ino) for p in paths}))
            self.assertNotEqual(original.stat().st_ino, paths[0].stat().st_ino)
            clone_inodes.append(paths[0].stat().st_ino)
        self.assertNotEqual(*clone_inodes)

    def test_metadata_xattrs_and_owner_identity_are_preserved_without_seed_mutation(self):
        original = self.seed[0] / "index/000001.sst"
        supported = True
        try:
            os.setxattr(str(original), "user.bounded-test", b"metadata")
        except OSError as error:
            if error.errno not in (errno.ENOTSUP, errno.EOPNOTSUPP):
                raise
            supported = False
        # Linux user xattrs require write permission, even for the file owner.
        # Configure metadata while writable, then exercise a read-only seed.
        original.chmod(0o440)
        os.utime(str(original), ns=(1000000000, 2000000000))
        for source in self.seed:
            (source / CLONE.OWNER).write_text("seed-owner")
        targets = self.targets("clone")
        for target in targets:
            target.mkdir()
            (target / CLONE.OWNER).write_text("new-trial-owner")
        before = V2.normal.trade.tree_metadata(self.seed)
        CLONE.clone_dataset(zip(self.seed, targets))
        copied = targets[0] / "index/000001.sst"
        self.assertEqual(2000000000, copied.stat().st_mtime_ns)
        self.assertEqual(0o640, copied.stat().st_mode & 0o777)
        self.assertEqual(before, V2.normal.trade.tree_metadata(self.seed))
        if supported:
            self.assertEqual(b"metadata", os.getxattr(str(copied), "user.bounded-test"))
        self.assertTrue(all((target / CLONE.OWNER).read_text() == "new-trial-owner" for target in targets))

    def test_symlink_special_file_and_existing_target_are_rejected(self):
        original = self.seed[1] / "data/blob"
        for target in (self.seed[1], self.seed[1] / "nested", self.hdd):
            with self.subTest(overlap=str(target)), self.assertRaises(ValueError):
                CLONE.clone_dataset(((self.seed[1], target),))
        for name in ("symlink", "fifo", "existing", "root-link"):
            target = self.root / name
            with self.subTest(name=name):
                if name == "symlink":
                    bad = self.seed[1] / "bad"
                    bad.symlink_to(original)
                elif name == "fifo":
                    bad = self.seed[1] / "bad"
                    os.mkfifo(str(bad))
                elif name == "existing":
                    target.mkdir(); (target / "data").mkdir()
                    (target / "data/blob").write_text("do not overwrite")
                    bad = None
                else:
                    target.symlink_to(self.seed[1], target_is_directory=True)
                    bad = None
                with self.assertRaises((OSError, ValueError)):
                    CLONE.clone_dataset(((self.seed[1], target),))
                if bad is not None:
                    bad.unlink()
                if name == "existing":
                    self.assertEqual("do not overwrite", (target / "data/blob").read_text())

    def test_source_metadata_change_during_copy_is_rejected_with_partial_retained(self):
        source = self.seed[1] / "data/blob"
        target = self.root / "partial"
        original = CLONE.os.write
        def write(fd, view):
            written = original(fd, view)
            info = source.stat()
            os.utime(str(source), ns=(info.st_atime_ns, info.st_mtime_ns + 1))
            return written
        with mock.patch.object(CLONE.os, "write", side_effect=write), self.assertRaises(ValueError):
            CLONE.copy_file(source, target, source.lstat(), bytearray(CLONE.BUFFER_BYTES))
        self.assertTrue(target.is_file())

    def test_isolated_cli_emits_success_only_after_copy_and_preserves_failures(self):
        targets = self.targets("clone")
        argv = [sys.executable, "-I", "-S", "-B", str(Path(CLONE.__file__).resolve()),
                "--clone", *map(str, (*self.seed, *targets))]
        success = subprocess.run(argv, capture_output=True, text=True, timeout=10)
        self.assertEqual(0, success.returncode, success.stderr)
        event = json.loads(success.stdout.split("MB_SST_JSON ", 1)[1])
        self.assertTrue(event["ok"] and event["copy_complete"])
        self.assertEqual("bounded-v2", event["preparation_protocol"])
        self.assertFalse(event["directory_fsync"])
        failed = subprocess.run(argv, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(0, failed.returncode)
        self.assertNotIn("copy_complete", failed.stdout)
        self.assertTrue((targets[0] / "index/000001.sst").is_file())

    def test_only_preparation_argv_changes_with_unique_isolation_flags(self):
        runner = self.runner("--strategies", "default80", "--repeats", "1")
        row = runner.report["trials"][0]
        old = [sys.executable, str(Path(V2.normal.node_run.__file__).resolve()), "--clone",
               *map(str, (*self.seed, *self.seed))]
        with mock.patch.object(V2.normal.NormalRunner, "execute_checked", return_value={}) as execute:
            runner.execute_checked(row, old, self.seed, 480, preparation=True)
            actual = execute.call_args[0][1]
            self.assertEqual([sys.executable, "-I", "-S", "-B", str(Path(CLONE.__file__).resolve()),
                              "--clone", *map(str, (*self.seed, *self.seed))], actual)
            bench = runner.trial_command(self.seed, V2.normal.policy_configuration("default80", 10000), "mixed")
            runner.execute_checked(row, bench, self.seed, 480)
            self.assertEqual(bench, execute.call_args[0][1])
        with self.assertRaises(V2.normal.trade.SafetyStop):
            runner.execute_checked(row, ["wrong"], self.seed, 480, preparation=True)

    def test_eight_runtime_hashes_include_helper_and_detect_replacement_or_deletion(self):
        runner = self.runner("--strategies", "default80", "--repeats", "1")
        sources = runner.report["driver_identity"]["source_sha256"]
        self.assertEqual({"node_normal.py", "node_normal_v2.py", "node_clone_bounded.py", "node_tradeoff.py",
                          "node_run.py", "node_cache.py", "node_diagnose.py", "node_summary.py"},
                         {Path(p).name for p in sources})
        probe = self.root / "helper.py"
        probe.write_text("frozen")
        sources[str(probe)] = V2.normal.node_run.digest(probe)
        for deleted in (False, True):
            if deleted:
                probe.unlink()
            else:
                probe.write_text("replacement")
            with self.assertRaises(V2.normal.trade.SafetyStop):
                runner.check_identity()

    def test_inherited_early_oom_and_occupied_cgroup_stop_before_dirty_continuation(self):
        runner = self.runner("--strategies", "default80", "--repeats", "1")
        row = runner.report["trials"][0]
        def fail(*args, **kwargs):
            (self.cgroup / "memory.oom_control").write_text("oom_kill 1\nunder_oom 0\n")
            raise RuntimeError("child exited before identity")
        with mock.patch.object(runner, "execute", side_effect=fail), self.assertRaises(V2.normal.trade.SafetyStop):
            runner.copy_round(row)
        self.assertEqual(1, row["independent_oom_checks"][0]["oom_kill_delta"])
        (self.cgroup / "tasks").write_text("12345\n")
        with mock.patch.object(runner, "execute") as execute, self.assertRaises(V2.normal.trade.SafetyStop):
            runner.copy_round(row)
        execute.assert_not_called()

    def test_twenty_trials_keep_v1_protocol_flags_seed_and_independent_inode_safety(self):
        runner = self.runner()
        calls = []
        def execute(name, argv, dirs, timeout=480):
            calls.append(argv)
            if "--clone" in argv:
                split = argv.index("--clone")
                CLONE.clone_dataset(zip(map(Path, argv[split + 1:split + 3]), dirs))
                events = [{"event": "summary", "ok": True}]
            else:
                flags = dict(arg[2:].split("=", 1) for arg in argv[1:])
                events = NORMAL_FIXTURE.protocol_events(int(flags["metabypass_sst_capacity_bytes"]),
                                                       flags["metabypass_sst_mode"] == "adaptive")
            samples = runner.control / (name + ".samples.jsonl")
            samples.write_text("")
            return {"name": name, "command": argv, "events": events, "valid": True,
                    "oom_kill_delta": 0, "elapsed_s": 1, "exit_code": 0, "timed_out": False,
                    "samples_path": str(samples), "exit_space": V2.normal.node_run.space_snapshot(*dirs)}
        before = V2.normal.trade.tree_metadata(self.seed)
        with mock.patch.object(runner, "preflight"), mock.patch.object(runner, "execute", side_effect=execute):
            runner.run()
        runner.finalize()
        self.assertEqual("complete", runner.report["status"])
        self.assertEqual(V2.normal.PROTOCOL, runner.report["protocol"])
        self.assertEqual(V2.normal.POLICY_VERSION, runner.report["policy_version"])
        self.assertEqual("bounded-v2", runner.report["preparation_protocol"])
        self.assertEqual([65536, 1048576, ["-I", "-S", "-B"]],
                         [runner.report["limits"]["preparation"][k] for k in ("buffer_bytes", "sync_batch_bytes", "python_flags")])
        self.assertEqual(list(V2.normal.STRATEGIES) + list(reversed(V2.normal.STRATEGIES)),
                         [r["strategy"] for r in runner.report["trials"]])
        self.assertTrue(all(r["durations"] == V2.normal.DURATIONS and r["independent_inode_check"]["passed"]
                            for r in runner.report["trials"]))
        self.assertEqual(before, V2.normal.trade.tree_metadata(self.seed))
        self.assertTrue(runner.report["seed"]["final_immutable_check"])
        plain = V2.normal.NormalRunner.__new__(V2.normal.NormalRunner)
        plain.args = runner.args; plain.keys = runner.keys; plain.rate = runner.rate
        for row, argv in zip(runner.report["trials"], calls[1::2]):
            self.assertEqual(plain.trial_command(tuple(map(Path, row["directories"])),
                                                row["effective_configuration"], "mixed"), argv)


if __name__ == "__main__":
    unittest.main()
