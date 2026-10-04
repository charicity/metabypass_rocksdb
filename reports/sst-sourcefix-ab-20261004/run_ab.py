#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

"""Bounded A/B probe for the frozen small-SST memory investigation."""

import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import time
from types import SimpleNamespace


RUNNER_PATH = Path(
    "/home/lj/ssd/metabypass_test/sst-sourcefix-20261003-e1effa72/source/source/"
    "docs/components/metabypass/experiments/sst_tiering/node_run.py")
SEED_SSD = Path(
    "/home/lj/ssd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/"
    "data/node-sst-53d08154-15df-4ae7-8574-f04a94daed26/seed-1000000")
SEED_HDD = Path(
    "/home/lj/hdd/metabypass_test/sst-effects-small-20261003173152Z-558efb2e/"
    "data/node-sst-53d08154-15df-4ae7-8574-f04a94daed26/seed-1000000")
BINARY = Path(
    "/home/lj/ssd/metabypass_test/sst-effects-20261001T143951Z-89c50c28-3b783a5a/"
    "build-gflags/db_bench")
SOURCE_MANIFEST = Path(
    "/home/lj/ssd/metabypass_test/sst-sourcefix-20261003-e1effa72/meta/source-manifest.json")
EXPECTED_BINARY_SHA256 = "2f3c89fead39cb97bf69fd698797a24210892baf2def95c6d6f1fc47c4f0bd76"
EXPECTED_MANIFEST_SHA256 = "3a9e91e86fbd2743d473ef09199b490f1341db7e84a83a3e0535a2a081cceda2"
EXPECTED_RUNNER_SHA256 = "e1effa72cfe4945e805bdb23cb01b18b4346bb9eb2fc81948afdda1bbee627ee"
SEED_RUN_UUID = "53d08154-15df-4ae7-8574-f04a94daed26"
GROUPS = {
    "A": Path("/sys/fs/cgroup/memory/mbsst-source-a-20261004"),
    "B": Path("/sys/fs/cgroup/memory/mbsst-source-b-20261004"),
}
UNUSED_GROUP = Path("/sys/fs/cgroup/memory/mbsst-small-fixed-20261004")
LIMIT_BYTES = 48 * 1024 * 1024
BUDGET_BYTES = 11335346
RATE = 107
QUEUE_BYTES = 2 * 1024 * 1024


def load_runner():
    runner_dir = RUNNER_PATH.parent
    sys.path.insert(0, str(runner_dir))
    spec = importlib.util.spec_from_file_location("frozen_node_run", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def cgroup_preflight(mod, path):
    if not path.is_dir():
        raise RuntimeError("missing cgroup: " + str(path))
    tasks = (path / "tasks").read_text().split()
    procs = (path / "cgroup.procs").read_text().split()
    if tasks or procs:
        raise RuntimeError("cgroup is occupied: " + str(path))
    if not os.access(path / "tasks", os.W_OK):
        raise RuntimeError("tasks file is not writable: " + str(path))
    snapshot = mod.sample_cgroup(path, include_slabinfo=True)
    if snapshot.get("memory.limit_in_bytes") != LIMIT_BYTES or snapshot.get("memory.swappiness") != 0:
        raise RuntimeError("unexpected cgroup limit/swappiness: " + str(path))
    if snapshot.get("memory.usage_in_bytes") != 0 or snapshot.get("memory.kmem.usage_in_bytes") != 0:
        raise RuntimeError("nonzero fresh cgroup starting usage: " + str(path))
    if snapshot.get("oom_control", {}).get("oom_kill") != 0:
        raise RuntimeError("nonzero fresh cgroup OOM counter: " + str(path))
    return {"path": str(path), "tasks": tasks, "cgroup_procs": procs,
            "snapshot": snapshot, "tasks_writable": True,
            "child_directories": [p.name for p in path.iterdir() if p.is_dir()]}


def require_seed(mod):
    for path, required in ((SEED_SSD, ("index",)), (SEED_HDD, ("data", "backup"))):
        if not path.is_dir():
            raise RuntimeError("missing validated seed directory: " + str(path))
        owner = json.loads((path / mod.OWNER).read_text())
        if owner.get("host") != "8001" or owner.get("uid") != os.getuid() or \
                owner.get("run_uuid") != SEED_RUN_UUID:
            raise RuntimeError("seed ownership metadata mismatch: " + str(path))
        for child in required:
            if not (path / child).is_dir():
                raise RuntimeError("missing seed subtree: " + str(path / child))
    snapshot = mod.space_snapshot(SEED_SSD, SEED_HDD)
    if not snapshot.get("complete") or snapshot.get("file_paths", {}).get("ssd_sst", 0) < 4 or \
            snapshot.get("file_paths", {}).get("hdd_blob", 0) == 0:
        raise RuntimeError("validated seed space scan is incomplete or empty")
    return {"ssd": str(SEED_SSD), "hdd": str(SEED_HDD),
            "prior_seed_runner_valid": True, "prior_seed_binary_sha256": EXPECTED_BINARY_SHA256,
            "prior_local_raw_report": "reports/sst-effects-small-20261003173152Z-558efb2e/"
                                      "raw-node/ssd/task/control/runner.json",
            "run_uuid": SEED_RUN_UUID, "read_only": True, "space_snapshot": snapshot}


def db_command(mod, ssd, hdd):
    flags = {
        "metabypass_mode": "sst_tiering",
        "metabypass_sst_action": "run",
        "db": str(ssd / "index"),
        "metabypass_data_dir": str(hdd / "data"),
        "metabypass_backup_dir": str(hdd / "backup"),
        "num": 1000000,
        "value_size": 1024,
        "key_size": 16,
        "compression_type": "none",
        "metabypass_sst_mode": "adaptive",
        "metabypass_sst_capacity_bytes": BUDGET_BYTES,
        "metabypass_sst_workload": "uniform",
        "metabypass_sst_seed": 1,
        "metabypass_sst_warmup_seconds": 0,
        "metabypass_sst_measure_seconds": 10,
        "metabypass_sst_saturated_seconds": 0,
        "metabypass_sst_target_ops_per_sec": RATE,
        "metabypass_sst_flush_interval_ms": 10000,
        "metabypass_sst_window_ms": 1000,
        "metabypass_sst_cache_bytes": 65536,
        "metabypass_sst_flush_every": 65536,
        "metabypass_sst_verify_keys": 256,
        "metabypass_queue_capacity": QUEUE_BYTES,
        "metabypass_batch_bytes": 262144,
        "metabypass_interval_ms": 1000,
        "metabypass_sst_interval_ms": 1000,
        "metabypass_sst_half_life_ms": 10000,
        "metabypass_sst_residency_ms": 10000,
        "metabypass_sst_promote_rounds": 2,
        "metabypass_sst_demote_rounds": 3,
        "metabypass_sst_sample_one_in": 64,
    }
    return [str(BINARY), *["--%s=%s" % (key, value) for key, value in flags.items()]]


def row_oom(row):
    return int(row.get("oom_kill_delta") or 0) > 0 or \
        int(row.get("cgroup_end", {}).get("oom_control", {}).get("oom_kill", 0)) > \
        int(row.get("cgroup_start", {}).get("oom_control", {}).get("oom_kill", 0))


def summarize_groups(report):
    grouped = {key: {"commands": 0, "rows": [], "sample_count": 0,
                     "peak_cgroup_usage_bytes": None, "peak_cgroup_kmem_bytes": None,
                     "peak_cgroup_rss_bytes": None, "peak_cgroup_cache_bytes": None,
                     "peak_proc_vmrss_bytes": None, "peak_proc_rssanon_bytes": None,
                     "peak_proc_rssfile_bytes": None, "peak_fd_count": None,
                     "peak_thread_count": None, "distinct_last_processor_cpus": [],
                     "oom_kill_delta": 0, "failcnt_delta": 0}
               for key in GROUPS}
    for row in report.get("commands", []):
        wrapped = row.get("wrapped_command", [])
        text = " ".join(wrapped)
        label = "A" if str(GROUPS["A"] / "tasks") in text else \
                "B" if str(GROUPS["B"] / "tasks") in text else None
        if label is None:
            continue
        info = grouped[label]
        info["commands"] += 1
        info["rows"].append({"name": row.get("name"), "valid": row.get("valid"),
                             "exit_code": row.get("exit_code"), "timed_out": row.get("timed_out"),
                             "oom_kill_delta": row.get("oom_kill_delta"),
                             "memory_failcnt_delta": row.get("memory_failcnt_delta"),
                             "kmem_usage_delta_bytes": row.get("kmem_usage_delta_bytes"),
                             "cgroup_peak_usage_bytes": row.get("cgroup_end", {}).get("memory.max_usage_in_bytes"),
                             "cgroup_peak_kmem_bytes": row.get("cgroup_end", {}).get("memory.kmem.max_usage_in_bytes")})
        info["oom_kill_delta"] += int(row.get("oom_kill_delta") or 0)
        info["failcnt_delta"] += int(row.get("memory_failcnt_delta") or 0)
        if row.get("cgroup_end", {}).get("memory.max_usage_in_bytes") is not None:
            info["peak_cgroup_usage_bytes"] = max(
                info["peak_cgroup_usage_bytes"] or 0,
                row["cgroup_end"]["memory.max_usage_in_bytes"])
        if row.get("cgroup_end", {}).get("memory.kmem.max_usage_in_bytes") is not None:
            info["peak_cgroup_kmem_bytes"] = max(
                info["peak_cgroup_kmem_bytes"] or 0,
                row["cgroup_end"]["memory.kmem.max_usage_in_bytes"])
        for sample_path in [row.get("samples_path")]:
            if not sample_path or not Path(sample_path).is_file():
                continue
            with Path(sample_path).open() as stream:
                for line in stream:
                    try:
                        sample = json.loads(line)
                    except ValueError:
                        continue
                    info["sample_count"] += 1
                    cg = sample.get("cgroup", {})
                    stats = cg.get("stats", {})
                    for field, value in (
                            ("peak_cgroup_usage_bytes", cg.get("memory.usage_in_bytes")),
                            ("peak_cgroup_kmem_bytes", cg.get("memory.kmem.usage_in_bytes")),
                            ("peak_cgroup_rss_bytes", stats.get("rss")),
                            ("peak_cgroup_cache_bytes", stats.get("cache"))):
                        if value is not None:
                            info[field] = max(info[field] or 0, value)
                    status = sample.get("proc_status") or {}
                    for field, proc_name in (("peak_proc_vmrss_bytes", "VmRSS"),
                                             ("peak_proc_rssanon_bytes", "RssAnon"),
                                             ("peak_proc_rssfile_bytes", "RssFile")):
                        if status.get(proc_name) is not None:
                            info[field] = max(info[field] or 0, status[proc_name] * 1024)
                    resources = sample.get("proc_resources") or {}
                    for field, proc_name in (("peak_fd_count", "fd_count"),
                                             ("peak_thread_count", "thread_count")):
                        if resources.get(proc_name) is not None:
                            info[field] = max(info[field] or 0, resources[proc_name])
                    for thread in resources.get("threads", []):
                        cpu = thread.get("last_processor_cpu")
                        if cpu is not None and cpu not in info["distinct_last_processor_cpus"]:
                            info["distinct_last_processor_cpus"].append(cpu)
    for info in grouped.values():
        info["distinct_last_processor_cpus"].sort()
    return grouped


def main():
    mod = load_runner()
    if sha256(RUNNER_PATH) != EXPECTED_RUNNER_SHA256:
        raise RuntimeError("frozen node_run.py SHA mismatch")
    if sha256(BINARY) != EXPECTED_BINARY_SHA256:
        raise RuntimeError("db_bench SHA mismatch")
    if sha256(SOURCE_MANIFEST) != EXPECTED_MANIFEST_SHA256:
        raise RuntimeError("source manifest SHA mismatch")
    seed = require_seed(mod)
    baselines = {key: cgroup_preflight(mod, path) for key, path in GROUPS.items()}
    unused = cgroup_preflight(mod, UNUSED_GROUP)
    if Path("/sys/fs/cgroup/memory/mbsst-small-fixed-20261004/tasks").read_text().strip():
        raise RuntimeError("fixed full-test cgroup must stay empty")
    ssd_root = Path("/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/ssd-data")
    hdd_root = Path("/home/lj/hdd/metabypass_test/sst-sourcefix-ab-20261004/hdd-data")
    output = Path("/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/control/ab-report.json")
    for root in (ssd_root, hdd_root, output.parent):
        if not root.is_dir():
            raise RuntimeError("task-owned output parent missing: " + str(root))
    args = SimpleNamespace(binary=str(BINARY), source_manifest=str(SOURCE_MANIFEST),
                           ssd_root=str(ssd_root), hdd_root=str(hdd_root),
                           cgroup=str(GROUPS["A"]), output=str(output), profile="legacy",
                           cpu_list=None, deadline_seconds=720, space_interval=10,
                           keep_data=True)
    runner = mod.Runner(args)
    started = time.monotonic()
    runner.report.update(
        test_kind="bounded paired CPU-affinity A/B for cgroup-kmem overhead",
        ab_protocol={"pair_order": "A1,B1,A2,B2,A3,B3,A4,B4",
                     "max_trials_per_group": 4, "hard_deadline_seconds": 720,
                     "warmup_seconds": 0, "measured_seconds": 10,
                     "saturated_seconds": 0, "target_ops_per_sec": RATE,
                     "mode": "adaptive", "workload": "uniform", "budget_bytes": BUDGET_BYTES,
                     "queue_bytes": QUEUE_BYTES, "block_cache_bytes": 65536,
                     "keys": 1000000, "value_bytes": 1024, "key_bytes": 16,
                     "A_cpu_policy": "inherit allowed set; no CPU binding",
                     "B_cpu_policy": "set CPU list 0-3 before cgroup tasks write",
                     "fresh_copy_per_trial": True, "preserve_all_data": True,
                     "unused_fixed_cgroup": str(UNUSED_GROUP)},
        source_and_binary={"node_run_path": str(RUNNER_PATH),
                           "node_run_sha256": sha256(RUNNER_PATH),
                           "binary_path": str(BINARY),
                           "binary_sha256_start": sha256(BINARY),
                           "source_manifest_path": str(SOURCE_MANIFEST),
                           "source_manifest_sha256": sha256(SOURCE_MANIFEST)},
        seed=seed, cgroup_initial={"A": baselines["A"], "B": baselines["B"],
                                   "small_fixed_unused": unused},
        host={"hostname": os.uname().nodename, "uname": list(os.uname()),
              "uid": os.getuid(), "allowed_cpu_list": sorted(os.sched_getaffinity(0)),
              "start_utc": utc_now()},
        experiment_deadline_utc=(datetime.datetime.now(datetime.timezone.utc) +
                                 datetime.timedelta(seconds=720)).isoformat(timespec="seconds"),
        ab_pairs=[], group_stop_reasons={}, status="running")
    runner.save()
    active = {"A": True, "B": True}
    for pair in range(1, 5):
        pair_row = {"pair": pair, "order": []}
        runner.report["ab_pairs"].append(pair_row)
        for label in ("A", "B"):
            if not active[label]:
                pair_row["order"].append({"group": label, "status": "skipped_group_stopped"})
                continue
            group = GROUPS[label]
            runner.args.cgroup = str(group)
            runner.args.cpu_list = None if label == "A" else "0-3"
            name = "%s%d" % (label, pair)
            dirs = runner.directories("ab-" + name)
            pair_row["order"].append({"group": label, "name": name,
                                      "data_ssd": str(dirs[0]), "data_hdd": str(dirs[1]),
                                      "cgroup": str(group),
                                      "requested_cpu_list": None if label == "A" else [0, 1, 2, 3]})
            runner.save()
            copy_command = [sys.executable, str(RUNNER_PATH), "--clone",
                            str(SEED_SSD), str(SEED_HDD), str(dirs[0]), str(dirs[1])]
            try:
                copy_row = runner.execute(name + "-copy", copy_command, dirs, timeout=180)
            except Exception as error:
                pair_row["order"][-1].update(status="copy_controller_error", error=str(error))
                active[label] = False
                runner.report["group_stop_reasons"][label] = "copy controller error: " + str(error)
                runner.save()
                continue
            pair_row["order"][-1]["copy_row"] = name + "-copy"
            pair_row["order"][-1]["copy_valid"] = bool(copy_row.get("valid"))
            pair_row["order"][-1]["copy_oom_kill_delta"] = copy_row.get("oom_kill_delta")
            if row_oom(copy_row) or not copy_row.get("valid"):
                reason = "copy OOM" if row_oom(copy_row) else "copy failed/invalid"
                pair_row["order"][-1].update(status=reason)
                active[label] = False
                runner.report["group_stop_reasons"][label] = reason
                runner.save()
                continue
            try:
                run_row = runner.execute(name + "-run", db_command(mod, *dirs), dirs, timeout=120)
            except Exception as error:
                pair_row["order"][-1].update(status="run_controller_error", error=str(error))
                active[label] = False
                runner.report["group_stop_reasons"][label] = "run controller error: " + str(error)
                runner.save()
                continue
            run_row["phase_validation_errors"] = mod.phase_validation(run_row, {"measured": 10})
            runner.save()
            pair_row["order"][-1].update(run_row=name + "-run", run_valid=bool(run_row.get("valid")),
                                          measured_phase_errors=run_row["phase_validation_errors"],
                                          run_oom_kill_delta=run_row.get("oom_kill_delta"),
                                          run_exit_code=run_row.get("exit_code"))
            if row_oom(run_row) or not run_row.get("valid"):
                reason = "run OOM" if row_oom(run_row) else "run failed/invalid"
                pair_row["order"][-1]["status"] = reason
                active[label] = False
                runner.report["group_stop_reasons"][label] = reason
                runner.save()
                continue
            if run_row.get("phase_validation_errors"):
                pair_row["order"][-1]["status"] = "run completed with phase validation issues"
            else:
                pair_row["order"][-1]["status"] = "complete"
            tasks_left = (group / "tasks").read_text().split()
            if tasks_left:
                pair_row["order"][-1]["tasks_remaining_after_run"] = tasks_left
                active[label] = False
                runner.report["group_stop_reasons"][label] = "cgroup tasks not empty after trial"
            runner.save()
        runner.save()
    runner.report["binary_sha256_end"] = sha256(BINARY)
    runner.report["group_metrics"] = summarize_groups(runner.report)
    runner.report["finished_utc"] = utc_now()
    runner.report["elapsed_s"] = time.monotonic() - started
    runner.report["active_groups_at_end"] = active
    runner.report["status"] = "complete" if not runner.report["group_stop_reasons"] else "partial"
    if runner.report["binary_sha256_end"] != EXPECTED_BINARY_SHA256:
        runner.report["status"] = "blocked_or_failed"
        runner.report["error"] = "benchmark binary SHA changed"
    runner.save()
    print(json.dumps({"status": runner.report["status"], "output": str(output),
                      "elapsed_s": runner.report["elapsed_s"],
                      "group_stop_reasons": runner.report["group_stop_reasons"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BaseException as error:
        print("A/B controller failed: %s: %s" % (type(error).__name__, error), file=sys.stderr)
        raise
