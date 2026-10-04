#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Real-device, cgroup-isolated SST experiment. No privileged operations."""

import argparse
import hashlib
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import uuid

from node_cache import sample_residency

PROTOCOL = "metabypass-sst-node-v1"
OWNER = ".node-owner.json"
PROFILES = {
    "legacy": {"keys": 4000000, "memory_bytes": 96 * 1024 * 1024,
               "queue_bytes": 8 * 1024 * 1024, "deadline_s": 14400,
               "calibration_s": 1800, "warmup_s": 60, "measured_s": 120, "saturated_s": 30,
               "cpu_list": None},
    "small": {"keys": 1000000, "memory_bytes": 48 * 1024 * 1024,
              "queue_bytes": 2 * 1024 * 1024, "deadline_s": 5400,
              "calibration_s": 900, "warmup_s": 30, "measured_s": 60, "saturated_s": 15,
              "cpu_list": "0,1,2,3"},
}


def parse_cpu_list(value):
    if value is None:
        return None
    if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*", value):
        raise ValueError("CPU list must contain comma-separated CPU numbers or ranges")
    cpus = set()
    for item in value.split(","):
        bounds = list(map(int, item.split("-")))
        first, last = bounds[0], bounds[-1]
        if last < first or last > 1048575:
            raise ValueError("invalid CPU range")
        cpus.update(range(first, last + 1))
    return sorted(cpus)


def cpu_affinity(profile, value=None):
    requested = parse_cpu_list(value if value is not None else PROFILES[profile]["cpu_list"])
    supported = hasattr(os, "sched_getaffinity") and hasattr(os, "sched_setaffinity")
    if requested is not None and not supported:
        raise ValueError("explicit CPU affinity requires Linux sched affinity support")
    allowed = sorted(os.sched_getaffinity(0)) if supported else None
    if requested is not None and not set(requested).issubset(allowed):
        raise ValueError("requested CPU list is outside parent allowed CPUs: " +
                         str(requested) + " vs " + str(allowed))
    return {"requested_cpu_list": requested, "parent_allowed_cpu_list": allowed,
            "child_effective_cpu_list": None,
            "policy": "set before joining memory cgroup" if requested is not None else "inherit without binding"}


def cgroup_exec(arguments):
    """Parse the complete private wrapper contract before any state change."""
    if not arguments:
        raise ValueError("missing cgroup tasks path")
    tasks, rest = arguments[0], arguments[1:]
    value = None
    if rest and rest[0] == "--cpu-list":
        if len(rest) < 2:
            raise ValueError("missing wrapper CPU list")
        value, rest = rest[1], rest[2:]
    if len(rest) < 2 or rest[0] != "--":
        raise ValueError("missing exec separator or command")
    affinity = cpu_affinity("legacy", value)
    if affinity["requested_cpu_list"] is not None:
        os.sched_setaffinity(0, set(affinity["requested_cpu_list"]))
    effective = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    if affinity["requested_cpu_list"] is not None and effective != affinity["requested_cpu_list"]:
        raise ValueError("effective CPU affinity differs from requested CPUs")
    affinity["child_effective_cpu_list"] = effective
    # Keep the helper PID/session unchanged; every later thread inherits affinity.
    print("MB_SST_JSON " + json.dumps({"protocol": "node_v1", "event": "cpu_affinity",
                                      "applied_before_cgroup": True, **affinity}), flush=True)
    with open(tasks, "w") as stream:
        stream.write(str(os.getpid()))
    os.execvp(rest[1], rest[1:])


def recovery_forecast(copy_s, writer_extra_s, restore_extra_s, cut_s, read_s=60):
    """Same complete-path cost used by forecasts and timeouts; no scale factor."""
    values = (copy_s, writer_extra_s, restore_extra_s, cut_s, read_s)
    if any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError("recovery forecast requires finite nonnegative costs")
    return {"total_s": sum(values) + 30, "copy_s": copy_s,
            "writer_timeout_s": writer_extra_s + cut_s + 30,
            "restore_timeout_s": restore_extra_s + read_s + 30,
            "writer_extra_s": writer_extra_s, "restore_extra_s": restore_extra_s,
            "fault_after_ready_s": cut_s, "read_s": read_s, "margin_s": 30}


def round_forecast(rows, calibration_phase_s=45, formal_phase_s=105):
    if not rows:
        raise ValueError("actual calibration round costs are required")
    costs = [row["preparation_s"] + row["elapsed_s"] for row in rows]
    if any(not math.isfinite(value) or value < calibration_phase_s for value in costs):
        raise ValueError("calibration round cost is incomplete or nonfinite")
    return max(costs) - calibration_phase_s + formal_phase_s + 30


def reject_json_constant(value):
    raise ValueError("nonfinite JSON number: " + value)


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def proc_identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        return {"pid": pid, "starttime_ticks": int(fields[19]),
                "pgrp": int(fields[2]), "cpu_ticks": int(fields[11]) + int(fields[12])}
    except (OSError, ValueError, IndexError):
        return None


def proc_resources(pid):
    """Read-only FD/thread/CPU evidence; exited or unreadable processes stay unknown."""
    root = Path(f"/proc/{pid}")
    result = {"fd_count": None, "thread_count": None, "allowed_cpu_list": None,
              "threads": [], "errors": []}
    try:
        result["fd_count"] = sum(1 for _ in (root / "fd").iterdir())
    except OSError as error:
        result["errors"].append({"scope": "fd", "error": str(error)})
    def allowed(path):
        for line in path.read_text().splitlines():
            if line.startswith("Cpus_allowed_list:"):
                return parse_cpu_list(line.split(":", 1)[1].strip())
        raise ValueError("missing Cpus_allowed_list")
    try:
        result["allowed_cpu_list"] = allowed(root / "status")
    except (OSError, ValueError) as error:
        result["errors"].append({"scope": "affinity", "error": str(error)})
    try:
        tasks = sorted((root / "task").iterdir(), key=lambda path: int(path.name))
        result["thread_count"] = len(tasks)
        for task in tasks:
            thread = {"tid": int(task.name), "last_processor_cpu": None, "allowed_cpu_list": None}
            try:
                fields = (task / "stat").read_text().rsplit(") ", 1)[1].split()
                thread["last_processor_cpu"] = int(fields[36])
                thread["allowed_cpu_list"] = allowed(task / "status")
            except (OSError, ValueError, IndexError) as error:
                result["errors"].append({"scope": "thread", "tid": thread["tid"], "error": str(error)})
            result["threads"].append(thread)
    except (OSError, ValueError) as error:
        result["errors"].append({"scope": "tasks", "error": str(error)})
    result["status"] = "partial" if result["errors"] and any(
        result[key] is not None for key in ("fd_count", "thread_count", "allowed_cpu_list")) else \
        "unknown" if result["errors"] else "known"
    return result


def kill_owned_group(identity):
    current = proc_identity(identity["pid"])
    if current and current["starttime_ticks"] == identity["starttime_ticks"] and \
            current["pgrp"] == identity["pid"]:
        try:
            os.killpg(identity["pid"], signal.SIGKILL)
            return True
        except ProcessLookupError:
            pass
    return False


def create_owned(parent, name, run_id):
    path = Path(parent) / name
    path.mkdir()  # Never reuse a directory, even if it appears empty.
    write_json(path / OWNER, {"run_uuid": run_id, "host": socket.gethostname(),
                             "uid": os.getuid(), **proc_identity(os.getpid())})
    return path


def check_owned(path, parent, run_id):
    path, parent = Path(path), Path(parent)
    if path.is_symlink() or path.resolve().parent != parent.resolve():
        raise ValueError("cleanup target is not a direct owned child")
    identity = json.loads((path / OWNER).read_text())
    if identity["run_uuid"] != run_id or identity["host"] != socket.gethostname() or \
            identity["uid"] != os.getuid() or path.stat().st_uid != os.getuid():
        raise ValueError("cleanup ownership mismatch")


def cleanup_owned(path, parent, run_id, success):
    if not success:
        return False
    check_owned(path, parent, run_id)
    shutil.rmtree(path)
    return True


class Deadline:
    def __init__(self, seconds=4 * 3600, now=None):
        self.start = time.monotonic() if now is None else now
        self.end = self.start + seconds

    def remaining(self, now=None):
        return max(0, self.end - (time.monotonic() if now is None else now))

    def require(self, seconds=0, now=None):
        if self.remaining(now) <= seconds:
            raise RuntimeError("hard deadline or insufficient remaining plan budget")


def clone_dataset(pairs):
    """Ordinary first copies; reconstruct only within-trial hardlinks."""
    copied = {}
    files = bytes_copied = links = 0
    for source, target in pairs:
        source, target = Path(source), Path(target)
        target.mkdir(exist_ok=True)
        for base, dirs, names in os.walk(source, followlinks=False):
            relative = Path(base).relative_to(source)
            destination = target / relative
            destination.mkdir(exist_ok=True)
            for name in dirs:
                if (Path(base) / name).is_symlink():
                    raise ValueError("seed contains symlink")
                (destination / name).mkdir(exist_ok=True)
            for name in names:
                if name == OWNER:
                    continue
                original, new = Path(base) / name, destination / name
                info = original.lstat()
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError("seed contains non-regular file")
                # Include destination device: no attempted cross-device links.
                key = (info.st_dev, info.st_ino, destination.stat().st_dev)
                if key in copied:
                    os.link(copied[key], new)
                    links += 1
                else:
                    shutil.copy2(original, new)
                    new.chmod(info.st_mode | stat.S_IWUSR)
                    copied[key] = new
                    bytes_copied += info.st_size
                files += 1
    return {"ordinary_copy_bytes": bytes_copied, "files": files,
            "reconstructed_internal_links": links, "shared_with_seed": False}


def space_snapshot(ssd, hdd, global_hdd=None, global_ssd=None):
    """Globally deduplicate allocated bytes; tolerate racing unlink/rename."""
    started_wall, started_cpu = time.monotonic(), time.thread_time()
    inodes, logical, paths, missing = {}, {}, {}, []
    roots = [(Path(ssd), "ssd"), (Path(hdd), "hdd")]
    def record(path, device):
        try:
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                return
            kind = ("sst" if path.suffix == ".sst" else "blob" if path.suffix == ".blob"
                    else "temp" if path.suffix in (".tmp", ".temp") else "metadata")
            category = device + "_" + kind
            logical[category] = logical.get(category, 0) + info.st_size
            paths[category] = paths.get(category, 0) + 1
            item = inodes.setdefault((info.st_dev, info.st_ino),
                                     {"bytes": info.st_blocks * 512, "categories": set(),
                                      "devices": set(), "paths": 0})
            item["categories"].add(category)
            item["devices"].add(device)
            item["paths"] += 1
        except OSError as error:
            missing.append({"path": str(path), "error": str(error)})
    for root, device in roots:
        record(root, device)
        for base, dirs, names in os.walk(root, onerror=lambda e: missing.append(
                {"path": e.filename, "error": str(e)}), followlinks=False):
            for name in dirs + names:
                record(Path(base) / name, device)
    categories, devices = {}, {}
    for item in inodes.values():
        for category in item["categories"]:
            categories[category] = categories.get(category, 0) + item["bytes"]
        for device in item["devices"]:
            devices[device] = devices.get(device, 0) + item["bytes"]
    run_scope = None
    if global_hdd is not None and (Path(global_hdd) != Path(hdd) or
                                   Path(global_ssd or ssd) != Path(ssd)):
        run_scope = space_snapshot(global_ssd or ssd, global_hdd)
    return {"complete": not missing, "scan_errors": missing[:20], "run_scope": run_scope,
            "scan_error_count": len(missing), "logical_bytes": logical,
            "physical_category_bytes": categories, "physical_device_bytes": devices,
            "physical_global_unique_bytes": sum(i["bytes"] for i in inodes.values()), "file_paths": paths,
            "hardlinked_extra_paths": sum(i["paths"] - 1 for i in inodes.values()),
            "scope": {"ssd": str(ssd), "hdd": str(hdd), "kind": "trial"},
            "scan_wall_s": time.monotonic() - started_wall,
            "scan_cpu_s": time.thread_time() - started_cpu}


def measurement_snapshot(ssd, hdd, global_hdd, global_ssd, mode, live_stats, cgroup=None):
    result = {"space": space_snapshot(ssd, hdd, global_hdd, global_ssd)}
    if cgroup is not None:
        result["cgroup_slabinfo"] = sample_slabinfo(cgroup)
    if mode is not None:
        result["sst_cache"] = sample_residency(Path(ssd) / "index", Path(hdd) / "backup", mode, live_stats)
    return result


def cache_gate(row, phase="saturated"):
    samples = [json.loads(line) for line in Path(row["samples_path"]).read_text().splitlines()]
    ends = {e["phase"]: e for e in row["events"] if e.get("event") == "phase_end"}
    begin = ends.get("warmup", {}).get("steady_time_us")
    end = ends.get(phase, {}).get("steady_time_us")
    if begin is None or end is None:
        return {"passed": False, "measurement_status": "missing_or_partial", "cache_dominated": None,
                "reason": "phase monotonic boundaries unavailable"}
    evidence = [s["sst_cache"] for s in samples if s.get("sst_cache") and
                begin <= s["sst_cache"]["started_monotonic_us"] <= end]
    covered = [e for e in evidence if e["complete"] and e["coverage_fraction"] >= 0.9]
    cold = [e for e in covered if e["resident_fraction"] <= 0.9]
    anonymous = [s.get("proc_status", {}).get("RssAnon", 0) * 1024 for s in samples if
                 begin <= s["monotonic_s"] * 1e6 <= end and s.get("proc_status", {}).get("RssAnon", 0) > 0]
    return {"passed": bool(cold), "phase": phase,
            "measurement_status": "non_full_cache_observed" if cold else "cache_dominated" if covered else "missing_or_partial",
            "cache_dominated": not bool(cold) if covered else None,
            "scope": "global routed live SST residency, not request cache-miss rate",
            "eligible_samples": len(evidence),
            "complete_high_coverage_samples": len(covered), "nonfully_resident_samples": len(cold),
            "resident_fraction_min": min((e["resident_fraction"] for e in covered), default=None),
            "resident_fraction_max": max((e["resident_fraction"] for e in covered), default=None),
            "minimum_saturated_anonymous_rss_bytes": min(anonymous) if anonymous else None,
            "evidence": covered, "method": "routed SST mincore; coverage >=90%, resident fraction <=90%"}


def read_numbers(path):
    try:
        result = {}
        for line in Path(path).read_text().splitlines():
            fields = line.replace(":", "").split()
            if len(fields) >= 2 and fields[1].isdigit():
                result[fields[0]] = int(fields[1])
        return result
    except OSError:
        return {}


def sample_slabinfo(path):
    try:
        return {"status": "known", "raw": (Path(path) / "memory.kmem.slabinfo").read_text(),
                "monotonic_s": time.monotonic()}
    except OSError as error:
        return {"status": "unknown", "raw": None, "error": str(error),
                "monotonic_s": time.monotonic()}


def memory_budget(snapshot):
    usage, limit = snapshot.get("memory.usage_in_bytes"), snapshot.get("memory.limit_in_bytes")
    return {"baseline_usage_bytes": usage,
            "baseline_kmem_usage_bytes": snapshot.get("memory.kmem.usage_in_bytes"),
            "remaining_headroom_bytes": limit - usage if limit is not None and usage is not None else None,
            "status": "unknown" if usage is None or limit is None else
                      "no_headroom" if usage >= limit else "headroom_available",
            "note": "All existing charges, including task-free residual kmem, consume the fixed limit; memory.stat cache/rss is not total usage."}


def sample_cgroup(path, include_slabinfo=False):
    path = Path(path)
    result = {"stats": read_numbers(path / "memory.stat")}
    for name in ("memory.usage_in_bytes", "memory.max_usage_in_bytes", "memory.failcnt",
                 "memory.limit_in_bytes", "memory.swappiness", "memory.kmem.usage_in_bytes",
                 "memory.kmem.max_usage_in_bytes", "memory.kmem.failcnt"):
        try:
            result[name] = int((path / name).read_text())
        except (OSError, ValueError):
            result[name] = None
    result["oom_control"] = read_numbers(path / "memory.oom_control")
    known = sum(result[key] is not None for key in ("memory.kmem.usage_in_bytes",
                "memory.kmem.max_usage_in_bytes", "memory.kmem.failcnt"))
    result["kmem_telemetry_status"] = "known" if known == 3 else "partial" if known else "unknown"
    if include_slabinfo:
        result["slabinfo"] = sample_slabinfo(path)
    return result


def diskstats(devices):
    try:
        selected = {f"{os.major(dev)}:{os.minor(dev)}" for dev in devices}
        result = {}
        for line in Path("/proc/diskstats").read_text().splitlines():
            fields = line.split()
            key = ":".join(fields[:2])
            if key in selected:
                result[key] = {"name": fields[2], "counters": list(map(int, fields[3:]))}
        return result
    except (OSError, ValueError):
        return {}


def choose_rate(saturated_rates):
    if len(saturated_rates) != 2 or any(not math.isfinite(x) or x <= 0
                                      for x in saturated_rates):
        raise ValueError("two positive disabled/adaptive saturation rates required")
    return max(1, math.floor(0.7 * min(saturated_rates)))


def plan_order():
    return [(repeat, workload, mode) for repeat in range(1, 4)
            for workload in ("uniform", "switch", "mixed")
            for mode in (("disabled", "adaptive") if repeat % 2 else
                         ("adaptive", "disabled"))]


def ready_clock(event, process_start_s, received_s):
    stamp = event.get("steady_time_us")
    if not isinstance(stamp, int) or isinstance(stamp, bool):
        raise ValueError("ready requires an integer CLOCK_MONOTONIC timestamp")
    ready = stamp / 1000000
    if ready < process_start_s - 0.000001 or ready > received_s + 0.000001:
        raise ValueError("ready timestamp outside process start/receive monotonic bounds")
    return ready


def rpo_result(acks, recovered_marker, fault_us):
    if not acks:
        raise ValueError("no externally persisted ACK evidence")
    by_seq = {int(a["seq"]): a for a in acks}
    seqs = sorted(by_seq)
    lost = [by_seq[seq] for seq in seqs if seq > recovered_marker]
    recovered = [by_seq[seq] for seq in seqs if seq <= recovered_marker]
    tail = recovered[-1] if recovered else None
    return {"last_acked_seq": seqs[-1], "recovered_marker": recovered_marker,
            "marker_beyond_last_ack": recovered_marker > seqs[-1],
            "lost_ack_batches": len(lost),
            "lost_business_bytes": sum(a["business_bytes"] for a in lost),
            "first_lost_ack": lost[0] if lost else None,
            "last_lost_ack": lost[-1] if lost else None,
            "last_recovered_ack": tail,
            "rpo_us": (max(0, fault_us - tail["steady_time_us"]) if tail else None)
                      if lost else 0,
            "rpo_lower_bound_us": max(0, fault_us - lost[0]["steady_time_us"])
                                  if lost and not tail else None}


def phase_validation(row, durations):
    errors = []
    schemas = [e for e in row["events"] if e.get("event") == "histogram_schema"]
    bounds = schemas[0].get("upper_bounds_us", []) if schemas else []
    if len(bounds) != 193 or any(not isinstance(v, int) or v < 1 for v in bounds) or any(a >= b for a, b in zip(bounds, bounds[1:])):
        errors.append("missing or invalid 193-bucket histogram schema")
    for name, seconds in durations.items():
        ends = [e for e in row["events"] if e.get("event") == "phase_end" and e.get("phase") == name]
        if len(ends) != 1:
            errors.append("missing/duplicate phase_end: " + name)
            continue
        phase = ends[0]
        if phase.get("ok") is not True or phase.get("elapsed_us", -1) < seconds * 1000000:
            errors.append("failed/truncated phase: " + name)
        if phase.get("ops") != sum(phase.get(op, {}).get("count", 0) for op in ("get", "put", "delete")):
            errors.append("operation counts disagree: " + name)
        for op in ("get", "put", "delete"):
            item = phase.get(op, {})
            for kind in ("service", "response"):
                hist = item.get(kind, {})
                buckets = hist.get("buckets", [])
                if len(buckets) != len(bounds) or sum(buckets) != hist.get("count") or \
                        hist.get("count") != item.get("count"):
                    errors.append("histogram counts/schema disagree: " + name + "/" + op + "/" + kind)
    return errors


def mechanism_coverage(row, phase="measured"):
    ends = {e["phase"]: e for e in row["events"] if e.get("event") == "phase_end"}
    warm = ends.get("warmup", {}).get("stats", {})
    measured = ends.get(phase, {}).get("stats", {})
    samples = measured.get("sampled_reads", 0) - warm.get("sampled_reads", 0)
    migration_keys = ("promotions", "demotions")
    migrations = sum(measured.get(k, 0) - warm.get(k, 0) for k in migration_keys)
    warm_migrations = sum(warm.get(k, 0) for k in migration_keys)
    cold = max(0, measured.get("protected_bytes", 0) + measured.get("unprotected_bytes", 0) -
               measured.get("ssd_bytes", 0))
    covered = samples > 0 and (migrations > 0 or warm_migrations > 0 and cold > 0)
    return {"covered": covered, "measured_sampled_reads": samples,
            "measured_migrations": migrations, "warmup_migrations": warm_migrations,
            "end_cold_live_lower_bound_bytes": cold,
            "basis": "measured_migration" if samples > 0 and migrations > 0 else
                     "warmup_settled_cold" if covered else "none"}


class Runner:
    def __init__(self, args):
        self.args = args
        self.run_id = str(uuid.uuid4())
        self.profile = getattr(args, "profile", "legacy")
        self.settings = dict(PROFILES[self.profile])
        self.deadline = Deadline(args.deadline_seconds)
        self.calibration_end = min(self.deadline.end, self.deadline.start + self.settings["calibration_s"])
        self.ssd = create_owned(args.ssd_root, "node-sst-" + self.run_id, self.run_id)
        self.hdd = create_owned(args.hdd_root, "node-sst-" + self.run_id, self.run_id)
        self.control = Path(args.output).resolve().parent / ("node-control-" + self.run_id)
        self.control.mkdir()
        self.devices = [self.ssd.stat().st_dev, self.hdd.stat().st_dev]
        self.report = {"protocol": PROTOCOL, "status": "calibrating", "run_uuid": self.run_id,
                       "profile": self.profile, "profile_parameters": self.settings,
                       "limits": {"hard_deadline_s": args.deadline_seconds,
                                  "calibration_s": self.settings["calibration_s"],
                                  "memory_bytes": self.settings["memory_bytes"],
                                  "queue_bytes": self.settings["queue_bytes"],
                                  "block_cache_bytes": 65536, "round_hard_limit_s": 480},
                       "started_monotonic_s": self.deadline.start,
                       "calibration_deadline_monotonic_s": self.calibration_end,
                       "host": socket.gethostname(), "controller": proc_identity(os.getpid()),
                       "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "deadline_monotonic_s": self.deadline.end, "arguments": vars(args),
                       "binary_sha256_start": digest(args.binary),
                       "source_identity": json.loads(Path(args.source_manifest).read_text()),
                       "preflight": {}, "commands": [], "calibration": [], "trials": [],
                       "rpo_samples": [], "data_roots": [str(self.ssd), str(self.hdd)],
                       "control_root": str(self.control)}
        self.active = None

    def save(self):
        write_json(self.args.output, self.report)

    def directories(self, name):
        return (create_owned(self.ssd, name, self.run_id),
                create_owned(self.hdd, name, self.run_id))

    def clean(self, dirs, success):
        if success and not self.args.keep_data:
            for path, parent in zip(dirs, (self.ssd, self.hdd)):
                cleanup_owned(path, parent, self.run_id, True)

    def command(self, action, dirs, mode="disabled", workload="uniform", budget=0,
                rate=10000, warmup=None, measured=None, saturated=None):
        settings = PROFILES[getattr(self.args, "profile", "legacy")]
        warmup = settings["warmup_s"] if warmup is None else warmup
        measured = settings["measured_s"] if measured is None else measured
        saturated = settings["saturated_s"] if saturated is None else saturated
        ssd, hdd = dirs
        flags = {"metabypass_mode": "sst_tiering", "metabypass_sst_action": action,
                 "db": str(ssd / "index"), "metabypass_data_dir": str(hdd / "data"),
                 "metabypass_backup_dir": str(hdd / "backup"), "num": self.keys,
                 "value_size": 1024, "key_size": 16, "compression_type": "none",
                 "metabypass_sst_mode": mode, "metabypass_sst_capacity_bytes": budget,
                 "metabypass_sst_workload": workload, "metabypass_sst_seed": 1,
                 "metabypass_sst_warmup_seconds": warmup,
                 "metabypass_sst_measure_seconds": measured,
                 "metabypass_sst_saturated_seconds": saturated,
                 "metabypass_sst_target_ops_per_sec": rate,
                 "metabypass_sst_flush_interval_ms": 10000,
                 "metabypass_sst_window_ms": 1000, "metabypass_sst_cache_bytes": 65536,
                 "metabypass_sst_flush_every": 65536, "metabypass_sst_verify_keys": 256,
                 "metabypass_queue_capacity": PROFILES[getattr(self.args, "profile", "legacy")]["queue_bytes"], "metabypass_batch_bytes": 262144,
                 "metabypass_interval_ms": 1000}
        if getattr(self.args, "profile", "legacy") == "small":
            flags.update(metabypass_sst_interval_ms=1000, metabypass_sst_half_life_ms=10000,
                         metabypass_sst_residency_ms=10000, metabypass_sst_promote_rounds=2,
                         metabypass_sst_demote_rounds=3, metabypass_sst_sample_one_in=64)
        return [str(Path(self.args.binary).resolve()), *[f"--{k}={v}" for k, v in flags.items()]]

    def execute(self, name, command, dirs, timeout=480, fault_seconds=None, calibration=False):
        self.deadline.require()
        affinity = cpu_affinity(getattr(self.args, "profile", "legacy"),
                                getattr(self.args, "cpu_list", None))
        started = time.monotonic()
        end = min(self.deadline.end, started + timeout,
                  self.calibration_end if calibration else self.deadline.end)
        cache_mode = next((item.split("=", 1)[1] for item in command
                           if item.startswith("--metabypass_sst_mode=")), None)
        wrapped = [sys.executable, str(Path(__file__).resolve()), "--cg-exec",
                   str(Path(self.args.cgroup) / "tasks")]
        if affinity["requested_cpu_list"] is not None:
            wrapped += ["--cpu-list", ",".join(map(str, affinity["requested_cpu_list"]))]
        wrapped += ["--", *command]
        row = {"name": name, "command": command, "wrapped_command": wrapped,
               "started_monotonic_s": started, "events": [], "parse_errors": [],
               "samples_path": str(self.control / (name + ".samples.jsonl")),
               "stdout_path": str(self.control / (name + ".stdout")),
               "stderr_path": str(self.control / (name + ".stderr")),
               "acks_path": str(self.control / (name + ".acks.jsonl")),
               "timed_out": False, "fault_injected": False, "phase": "startup",
               "cpu_affinity": affinity,
               "cgroup_start": sample_cgroup(self.args.cgroup, include_slabinfo=True)}
        row["memory_budget_start"] = memory_budget(row["cgroup_start"])
        self.report["commands"].append(row)
        self.save()
        with open(row["stdout_path"], "wb") as output, open(row["stderr_path"], "wb") as errors, \
                open(row["samples_path"], "w") as samples, open(row["acks_path"], "w") as journal:
            child = subprocess.Popen(wrapped, stdout=subprocess.PIPE, stderr=errors,
                                     start_new_session=True)
            identity = proc_identity(child.pid)
            if identity is None:
                child.wait()
                row["exit_code"] = child.returncode
                row["valid"] = False
                return row
            self.active = identity
            row["process_identity"] = {"run_uuid": self.run_id, "host": socket.gethostname(),
                                       **identity}
            write_json(self.control / (name + ".process.json"), row["process_identity"])
            selector = selectors.DefaultSelector()
            selector.register(child.stdout, selectors.EVENT_READ)
            os.set_blocking(child.stdout.fileno(), False)
            buffer = b""
            next_sample = next_space = started
            ready_time = None
            last_ack = None
            last_space = None
            scanner = ThreadPoolExecutor(max_workers=1)
            pending_space = None
            deadline_cancelled = threading.Event()
            def watchdog():
                if not deadline_cancelled.wait(max(0, end - time.monotonic())):
                    row["timed_out"] = True
                    kill_owned_group(identity)
            guard = threading.Thread(target=watchdog, daemon=True)
            guard.start()
            try:
                while True:
                    now = time.monotonic()
                    if now >= end and child.poll() is None:
                        row["timed_out"] = True
                        kill_owned_group(identity)
                    if fault_seconds is not None and ready_time is not None and \
                            now >= ready_time + fault_seconds and not row["fault_injected"]:
                        # Persist every externally received ACK before recording/triggering fault.
                        journal.flush()
                        os.fsync(journal.fileno())
                        row["ack_journal_pre_fault_sync"] = {"completed_monotonic_us": time.monotonic_ns() // 1000,
                                                              "last_externally_received_seq": last_ack}
                        row["fault_steady_time_us"] = time.monotonic_ns() // 1000
                        row["fault_injected"] = kill_owned_group(identity)
                        row["fault_elapsed_from_ready_us"] = (row["fault_steady_time_us"] -
                                                              row["ready_steady_time_us"])
                        row["fault_cut_jitter_us"] = row["fault_elapsed_from_ready_us"] - int(fault_seconds * 1000000)
                    if now >= next_sample:
                        status = read_numbers(f"/proc/{child.pid}/status")
                        sample = {"monotonic_s": now, "phase": row["phase"],
                                  "proc": proc_identity(child.pid), "proc_status": status,
                                  "proc_resources": proc_resources(child.pid),
                                  "proc_io": read_numbers(f"/proc/{child.pid}/io"),
                                  "cgroup": sample_cgroup(self.args.cgroup),
                                  "device_io": diskstats(self.devices)}
                        if pending_space is not None and pending_space.done():
                            last_space = pending_space.result()
                            sample.update(last_space)
                            sample["space_sample_started_monotonic_s"] = space_started
                            sample["space_sample_started_phase"] = space_phase
                            pending_space = None
                        if now >= next_space and pending_space is None:
                            space_started, space_phase = now, row["phase"]
                            live_stats = next((e["stats"] for e in reversed(row["events"]) if "stats" in e), None)
                            pending_space = scanner.submit(measurement_snapshot, *dirs, self.hdd, self.ssd,
                                                           cache_mode, live_stats, self.args.cgroup)
                            next_space = now + self.args.space_interval
                        samples.write(json.dumps(sample) + "\n")
                        samples.flush()
                        next_sample = time.monotonic() + 1
                    for key, _ in selector.select(timeout=0.1):
                        block = os.read(key.fileobj.fileno(), 65536)
                        if not block:
                            selector.unregister(key.fileobj)
                            continue
                        output.write(block)
                        output.flush()
                        buffer += block
                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            if not line.startswith(b"MB_SST_JSON "):
                                continue
                            try:
                                event = json.loads(line[12:], parse_constant=reject_json_constant)
                                event["controller_received_monotonic_us"] = time.monotonic_ns() // 1000
                                if event.get("protocol") != "node_v1":
                                    raise ValueError("missing node_v1 protocol")
                                if event["event"] == "cpu_affinity":
                                    actual = event.get("child_effective_cpu_list")
                                    if event.get("applied_before_cgroup") is not True or \
                                            affinity["requested_cpu_list"] is not None and actual != affinity["requested_cpu_list"]:
                                        raise ValueError("missing or inconsistent pre-cgroup CPU affinity")
                                    row["cpu_affinity"].update(child_effective_cpu_list=actual,
                                                              applied_before_cgroup=True)
                                    self.report.setdefault("source_identity", {})["cpu_affinity"] = dict(row["cpu_affinity"])
                                    self.report["cpu_affinity"] = dict(row["cpu_affinity"])
                                if event["event"] == "ack":
                                    event["received_before_fault"] = not row["fault_injected"]
                                    event["journal_write_monotonic_us"] = time.monotonic_ns() // 1000
                                    journal.write(json.dumps(event) + "\n")
                                    journal.flush()
                                    if event["received_before_fault"]:
                                        last_ack = event["seq"]
                                else:
                                    row["events"].append(event)
                                if event["event"] == "ready":
                                    received = event["controller_received_monotonic_us"] / 1000000
                                    ready_time = ready_clock(event, started, received)
                                    row["ready_received_monotonic_s"] = received
                                    row["ready_steady_time_us"] = event["steady_time_us"]
                                    row["ready_pipe_lag_us"] = event["controller_received_monotonic_us"] - event["steady_time_us"]
                                if "phase" in event:
                                    row["phase"] = event["phase"]
                                elif event["event"] == "phase":
                                    row["phase"] = event.get("name", "unknown")
                            except (ValueError, KeyError) as error:
                                row["parse_errors"].append({"line": line.decode(errors="replace"),
                                                            "error": str(error)})
                    if child.poll() is not None and not selector.get_map():
                        break
                child.wait()
            finally:
                if child.poll() is None:
                    kill_owned_group(identity)
                    child.wait()
                self.active = None
                deadline_cancelled.set()
                guard.join()
                scanner.shutdown(wait=True)
                if pending_space is not None:
                    samples.write(json.dumps({"monotonic_s": time.monotonic(),
                        "phase": space_phase, "space_sample_started_monotonic_s": space_started,
                        "space_sample_started_phase": space_phase, **pending_space.result()}) + "\n")
                selector.close()
                child.stdout.close()
                journal.flush()
                os.fsync(journal.fileno())
            if buffer.strip():
                partial = {"error": "incomplete final stdout line", "line": buffer.decode(errors="replace")}
                if row["fault_injected"]:
                    row["truncated_stdout_at_fault"] = partial
                else:
                    row["parse_errors"].append(partial)
        row["elapsed_s"] = time.monotonic() - started
        row["exit_code"] = child.returncode
        row["exit_space"] = space_snapshot(*dirs, global_hdd=self.hdd, global_ssd=self.ssd)
        row["cgroup_end"] = sample_cgroup(self.args.cgroup, include_slabinfo=True)
        row["memory_budget_end"] = memory_budget(row["cgroup_end"])
        for field, key in (("kmem_usage_delta_bytes", "memory.kmem.usage_in_bytes"),
                           ("kmem_failcnt_delta", "memory.kmem.failcnt")):
            before, after = row["cgroup_start"].get(key), row["cgroup_end"].get(key)
            row[field] = after - before if before is not None and after is not None else None
        row["memory_failcnt_delta"] = ((row["cgroup_end"].get("memory.failcnt") or 0) -
                                      (row["cgroup_start"].get("memory.failcnt") or 0))
        row["oom_kill_delta"] = (row["cgroup_end"].get("oom_control", {}).get("oom_kill", 0) -
                                  row["cgroup_start"].get("oom_control", {}).get("oom_kill", 0))
        summaries = [e for e in row["events"] if e.get("event") == "summary"]
        row["valid"] = child.returncode == 0 and not row["timed_out"] and \
            not row["parse_errors"] and row["cpu_affinity"].get("applied_before_cgroup") is True and \
            bool(summaries) and summaries[-1].get("ok") is True
        self.save()
        return row

    def copy(self, seed, dirs, name, calibration=False):
        command = [sys.executable, str(Path(__file__).resolve()), "--clone", *map(str, (*seed, *dirs))]
        row = self.execute(name, command, dirs, calibration=calibration)
        # Clone helper emits its own node summary, including copied inode/byte evidence.
        if not row["valid"]:
            raise RuntimeError("fresh copy failed; retained for diagnosis: " + name)
        return row

    def preflight(self):
        affinity = cpu_affinity(getattr(self.args, "profile", "legacy"),
                                getattr(self.args, "cpu_list", None))
        self.report.setdefault("source_identity", {})["cpu_affinity"] = affinity
        self.report["cpu_affinity"] = affinity
        cg = Path(self.args.cgroup)
        tasks = (cg / "tasks").read_text().split()
        if tasks:
            raise ValueError("private cgroup is occupied; refusing reuse")
        if not os.access(cg / "tasks", os.W_OK):
            raise ValueError("private cgroup tasks is not writable")
        cginfo = sample_cgroup(cg, include_slabinfo=True)
        expected_memory = PROFILES[getattr(self.args, "profile", "legacy")]["memory_bytes"]
        if cginfo["memory.limit_in_bytes"] != expected_memory or cginfo["memory.swappiness"] != 0:
            raise ValueError("expected preconfigured " + str(expected_memory // 1048576) +
                             " MiB/swappiness=0; no automatic changes")
        if self.devices[0] == self.devices[1]:
            raise ValueError("SSD/HDD must be distinct actual devices")
        mountinfo = Path("/proc/self/mountinfo").read_text()
        self.report["preflight"] = {"devices": [{"st_dev": dev, "major": os.major(dev),
                                    "minor": os.minor(dev)} for dev in self.devices],
                                    "cgroup": cginfo, "cgroup_initial_tasks": tasks,
                                    "cpu_affinity": affinity, "memory_budget": memory_budget(cginfo),
                                    "mountinfo": mountinfo, "diskstats": diskstats(self.devices),
                                    "free_bytes": [shutil.disk_usage(p).free for p in (self.ssd, self.hdd)],
                                    "baseline_global_hdd_space": space_snapshot(self.ssd, self.hdd,
                                                                               self.hdd)}
        self.save()

    def recovery_sample(self, seed, name, mode, cut, reads, writer_timeout,
                        restore_timeout, calibration=False, timing_only=False):
        dirs = self.directories(name)
        prep = self.copy(seed, dirs, name + "-copy", calibration=calibration)
        writer = self.execute(name + "-write", self.command("rpo_write", dirs,
            mode, "mixed", self.budget, self.rate, measured=max(120, cut + 30), saturated=0),
            dirs, timeout=writer_timeout, fault_seconds=cut, calibration=calibration)
        row = {"name": name, "mode": mode, "cut_seconds": cut, "read_seconds": reads,
               "timing_only": timing_only, "included_in_formal_rpo": not timing_only,
               "writer": writer, "preparation_s": prep["elapsed_s"], "valid": False}
        if not writer["fault_injected"] or writer["timed_out"] or writer["parse_errors"]:
            row["error"] = "writer did not reach the requested fault point"
            return row
        restore_ssd = create_owned(self.ssd, name + "-restore", self.run_id)
        restore = self.execute(name + "-restore", self.command("rpo_restore",
            (restore_ssd, dirs[1]), mode, "uniform", self.budget, self.rate,
            warmup=reads, measured=0, saturated=0), (restore_ssd, dirs[1]),
            timeout=restore_timeout, calibration=calibration)
        restore["phase_validation_errors"] = phase_validation(restore, {"first60s_read": reads})
        restore["valid"] = restore["valid"] and not restore["phase_validation_errors"]
        recovered = [e for e in restore["events"] if e.get("event") == "recovered"]
        row.update(restore=restore, valid=restore["valid"] and bool(recovered))
        if row["valid"]:
            row["recovered_marker"] = recovered[-1]["recovered_marker"]
            row["rto_us"] = recovered[-1]["rto_us"]
            if not timing_only:
                acks = [json.loads(line) for line in Path(writer["acks_path"]).read_text().splitlines()]
                acks = [ack for ack in acks if ack.get("received_before_fault", True)]
                if not acks:
                    row.update(valid=False, error="no externally received ACK before fault")
                else:
                    row.update(rpo_result(acks, row["recovered_marker"], writer["fault_steady_time_us"]))
        self.clean(dirs, row["valid"])
        cleanup_owned(restore_ssd, self.ssd, self.run_id, row["valid"] and not self.args.keep_data)
        return row

    def run_small(self):
        self.preflight()
        self.keys = 1000000
        self.report["calibration_attempts"] = []
        seed = self.directories("seed-1000000")
        seedrow = self.execute("seed-1000000", self.command("seed", seed), seed,
                               timeout=900, calibration=True)
        self.report["calibration"].append(seedrow)
        if not seedrow["valid"]:
            raise RuntimeError("small seed failed/OOM; no cgroup or dataset expansion")
        scan = seedrow["exit_space"]
        if not scan["complete"] or scan["file_paths"].get("ssd_sst", 0) < 4:
            raise RuntimeError("small seed space scan incomplete or fewer than four SSTs")
        sst = scan["logical_bytes"].get("ssd_sst", 0)
        self.budget = sst // 2
        self.report.update(keys=self.keys, baseline_sst_logical_bytes=sst, frozen_budget_bytes=self.budget)
        for root in seed:
            for path in root.rglob("*"):
                if path.is_file() and path.name != OWNER:
                    path.chmod(path.stat().st_mode & ~0o222)
        calibration_rows, cache_gates, physical_reads, rates = [], {}, {}, []
        for mode in ("disabled", "adaptive", "observe"):
            name = "cal-1000000-" + mode
            dirs = self.directories(name)
            prep = self.copy(seed, dirs, name + "-copy", calibration=True)
            row = self.execute(name, self.command("run", dirs, mode, "uniform", self.budget,
                10000, warmup=30, measured=0, saturated=15), dirs, calibration=True)
            row.update(mode=mode, workload="uniform", preparation_s=prep["elapsed_s"])
            row["phase_validation_errors"] = phase_validation(row, {"warmup": 30, "measured": 0, "saturated": 15})
            row["valid"] = row["valid"] and not row["phase_validation_errors"]
            self.report["calibration"].append(row)
            if not row["valid"]:
                raise RuntimeError("small calibration failed/OOM; sample retained")
            if mode != "observe":
                row["cache_gate"] = cache_gate(row)
                cache_gates[mode] = row["cache_gate"]
                saturation = next(e for e in row["events"] if e.get("event") == "phase_end" and
                                  e.get("phase") == "saturated")
                rates.append(saturation["ops"] * 1e6 / saturation["elapsed_us"])
                calibration_rows.append(row)
            if mode == "adaptive":
                row["mechanism_coverage"] = mechanism_coverage(row, "saturated")
                if not row["mechanism_coverage"]["covered"]:
                    raise RuntimeError("small adaptive calibration lacks sampling/migration/cold placement")
            samples = [json.loads(line) for line in Path(row["samples_path"]).read_text().splitlines()]
            io = [sample["device_io"] for sample in samples if sample.get("device_io")]
            if len(io) >= 2:
                for device in io[0]:
                    if device in io[-1]:
                        physical_reads[device] = physical_reads.get(device, 0) + max(0,
                            io[-1][device]["counters"][2] - io[0][device]["counters"][2]) * 512
            self.save()
            self.clean(dirs, row["valid"] and (mode == "observe" or row["cache_gate"]["passed"]))
        anonymous = [gate.get("minimum_saturated_anonymous_rss_bytes") for gate in cache_gates.values()]
        proxy_available = all(value is not None and value > 0 for value in anonymous)
        available_cache = max(1, 48 * 1024 * 1024 - min(anonymous) - 65536) if proxy_available else None
        pressure = {"keys": self.keys, "seed_sst_logical_bytes": sst, "cache_gates": cache_gates,
                    "available_file_cache_proxy_bytes": available_cache,
                    "sst_to_available_cache_ratio": sst / available_cache if available_cache else None,
                    "ratio_is_diagnostic_only": True,
                    "accepted": all(gate["passed"] for gate in cache_gates.values())}
        self.report["calibration_pressure"] = pressure
        self.report["calibration_attempts"].append(pressure)
        self.report["calibration_physical_read_bytes"] = physical_reads
        self.save()
        if not pressure["accepted"]:
            raise RuntimeError("small cache evidence missing or SSTs cache dominated; no scale/cgroup change")
        if len(physical_reads) != 2 or any(value <= 0 for value in physical_reads.values()):
            raise RuntimeError("small calibration lacks physical reads on both devices")
        self.rate = choose_rate(rates)
        self.report.update(frozen_target_ops_per_sec=self.rate, calibration_saturated_ops_per_sec=rates)
        predicted = round_forecast(calibration_rows)
        if predicted > 480:
            raise RuntimeError("small complete round forecast exceeds eight minutes")
        cal_extra = max(row["elapsed_s"] - 45 for row in calibration_rows)
        # One adaptive probe measures the full production Restore/Open path.
        # Its 10s read phase is only timing preheat, not one of the six samples.
        probe = self.recovery_sample(seed, "recovery-probe", "adaptive", 10, 10,
            writer_timeout=cal_extra + 10 + 30, restore_timeout=900,
            calibration=True, timing_only=True)
        self.report["recovery_probe"] = probe
        self.save()
        if not probe["valid"]:
            raise RuntimeError("small complete-path recovery probe failed; formal plan blocked")
        writer_extra = max(cal_extra, probe["writer"]["elapsed_s"] - 10)
        restore_extra = max(cal_extra, probe["restore"]["elapsed_s"] - 10)
        copy_s = max([row["preparation_s"] for row in calibration_rows] + [probe["preparation_s"]])
        rpo_plan = [(mode, cut) for mode in ("disabled", "adaptive") for cut in (60, 75, 90)]
        rpo_costs = [recovery_forecast(copy_s, writer_extra, restore_extra, cut) for _mode, cut in rpo_plan]
        self.report["budget_forecast"] = {"round_s": predicted,
            "formula": "max(actual_calibration_copy + execute) - 45 + 105 + 30",
            "writer_extra_s": writer_extra, "restore_extra_s": restore_extra,
            "copy_s": copy_s, "rpo_samples": rpo_costs,
            "cross_mode_recovery_cost_is_estimate": True,
            "remaining_required_s": 18 * predicted + sum(cost["total_s"] for cost in rpo_costs)}
        self.report.update(predicted_round_s=predicted, status="formal")
        self.save()
        formal = plan_order()
        recovery_reserve = sum(cost["total_s"] for cost in rpo_costs)
        self.deadline.require(len(formal) * predicted + recovery_reserve)
        for index, (repeat, workload, mode) in enumerate(formal):
            self.deadline.require((len(formal) - index) * predicted + recovery_reserve)
            name = f"trial-{repeat}-{workload}-{mode}"
            dirs = self.directories(name)
            started = time.monotonic()
            prep = self.copy(seed, dirs, name + "-copy")
            remaining_round = 480 - (time.monotonic() - started)
            if remaining_round <= 105:
                raise RuntimeError("small copy leaves insufficient fixed phase time")
            row = self.execute(name, self.command("run", dirs, mode, workload, self.budget,
                self.rate, warmup=30, measured=60, saturated=15), dirs, timeout=remaining_round)
            row.update(repeat=repeat, mode=mode, workload=workload, budget_bytes=self.budget,
                       preparation_s=prep["elapsed_s"], round_elapsed_s=time.monotonic() - started)
            row["phase_validation_errors"] = phase_validation(row, {"warmup": 30, "measured": 60, "saturated": 15})
            row["valid"] = row["valid"] and not row["phase_validation_errors"]
            row["mechanism_coverage"] = mechanism_coverage(row) if mode == "adaptive" else {"covered": True, "basis": "disabled"}
            row["cache_gate"] = cache_gate(row, phase="measured")
            row["performance_valid"] = row["valid"] and row["mechanism_coverage"]["covered"] and row["cache_gate"]["passed"]
            self.report["trials"].append(row)
            self.save()
            self.clean(dirs, row["performance_valid"])
            if not row["valid"]:
                raise RuntimeError("small formal correctness/timeout failure; sample retained")
            predicted = max(predicted, row["round_elapsed_s"] + 30)
            if predicted > 480:
                raise RuntimeError("observed small round plus safety margin exceeds eight minutes")
            self.report["budget_forecast"]["updated_round_s"] = predicted
        for index, ((mode, cut), cost) in enumerate(zip(rpo_plan, rpo_costs)):
            self.deadline.require(sum(remaining["total_s"] for remaining in rpo_costs[index:]))
            row = self.recovery_sample(seed, f"rpo-{mode}-{cut}", mode, cut, 60,
                cost["writer_timeout_s"], cost["restore_timeout_s"])
            self.report["rpo_samples"].append(row)
            self.save()
            if not row["valid"]:
                raise RuntimeError("small RPO sample failed; retained")
        self.report["status"] = "complete" if all(row["performance_valid"] for row in
            self.report["trials"]) else "complete_with_invalid_comparisons"
        self.clean(seed, True)

    def run(self):
        if getattr(self.args, "profile", "legacy") == "small":
            return self.run_small()
        self.preflight()
        self.keys = 4_000_000
        self.report["calibration_attempts"] = []
        for attempt in range(2):
            seed = self.directories("seed-" + str(self.keys))
            seedrow = self.execute("seed-" + str(self.keys), self.command("seed", seed), seed,
                                   timeout=1800, calibration=True)
            self.report["calibration"].append(seedrow)
            if not seedrow["valid"]:
                raise RuntimeError("seed failed/OOM: retain evidence; shrink benchmark RSS, not cgroup")
            if not seedrow["exit_space"]["complete"] or seedrow["exit_space"]["file_paths"].get("ssd_sst", 0) < 4:
                raise RuntimeError("seed space scan incomplete or fewer than four SST files")
            sst = seedrow["exit_space"]["logical_bytes"].get("ssd_sst", 0)
            self.budget = sst // 2
            # Freeze seed contents; copy helper restores owner-write permission.
            for root in seed:
                for path in root.rglob("*"):
                    if path.is_file() and path.name != OWNER:
                        path.chmod(path.stat().st_mode & ~0o222)
            calibration_rates, prep_times, calibration_io_rows, cache_gates = [], [], [], {}
            for mode in ("disabled", "adaptive", "observe"):
                name = f"cal-{self.keys}-{mode}"
                dirs = self.directories(name)
                prep = self.copy(seed, dirs, name + "-copy", calibration=True)
                prep_times.append(prep["elapsed_s"])
                row = self.execute(name, self.command("run", dirs, mode=mode,
                    budget=self.budget, rate=10000, warmup=60, measured=0, saturated=30),
                    dirs, calibration=True)
                row.update(mode=mode, workload="uniform", preparation_s=prep["elapsed_s"])
                row["phase_validation_errors"] = phase_validation(row,
                    {"warmup": 60, "measured": 0, "saturated": 30})
                row["valid"] = row["valid"] and not row["phase_validation_errors"]
                self.report["calibration"].append(row)
                if not row["valid"]:
                    raise RuntimeError("calibration failed/OOM; no limit change or formal launch")
                saturated = [e for e in row["events"] if e.get("event") == "phase_end" and
                             e.get("phase", e.get("name")) == "saturated"]
                if mode == "adaptive":
                    row["mechanism_coverage"] = mechanism_coverage(row, "saturated")
                    if not row["mechanism_coverage"]["covered"]:
                        raise RuntimeError("adaptive calibration lacks SST samples and migration/cold placement")
                io_samples = [json.loads(line) for line in Path(row["samples_path"]).read_text().splitlines()]
                calibration_io_rows.append([s for s in io_samples if s.get("device_io")])
                if mode != "observe":
                    row["cache_gate"] = cache_gate(row)
                    cache_gates[mode] = row["cache_gate"]
                    if not saturated or saturated[-1].get("elapsed_us", 0) <= 0:
                        raise RuntimeError("saturation phase evidence missing")
                    calibration_rates.append(saturated[-1]["ops"] * 1e6 / saturated[-1]["elapsed_us"])
                self.save()
                self.clean(dirs, row["valid"] and (mode == "observe" or row["cache_gate"]["passed"]))
            anonymous = [gate["minimum_saturated_anonymous_rss_bytes"] for gate in cache_gates.values()]
            if not all(value is not None and value > 0 for value in anonymous):
                raise RuntimeError("calibration saturated anonymous RSS telemetry missing")
            rss = min(anonymous)
            available_cache = max(1, 96 * 1024 * 1024 - rss - 65536)
            pressure = {"keys": self.keys, "seed_sst_logical_bytes": sst,
                "minimum_saturated_anonymous_rss_bytes": rss,
                "available_file_cache_proxy_bytes": available_cache,
                "sst_to_available_cache_ratio": sst / available_cache,
                "ratio_passed": sst >= 1.5 * available_cache,
                "formula": "cgroup_limit - minimum_calibrated_saturated_RssAnon - block_cache; supplementary proxy",
                "cache_gates": cache_gates}
            pressure["accepted"] = pressure["ratio_passed"] and all(g["passed"] for g in cache_gates.values())
            self.report["calibration_attempts"].append(pressure)
            self.report["calibration_pressure"] = pressure
            self.save()
            if pressure["accepted"]:
                break
            if attempt:
                raise RuntimeError("6M calibration still lacks routed SST non-full-cache/pressure evidence; plan blocked")
            self.clean(seed, True)
            self.keys = 6_000_000
        self.report["keys"] = self.keys
        self.report["baseline_sst_logical_bytes"] = sst
        self.report["frozen_budget_bytes"] = self.budget
        physical_reads = {}
        for rows in calibration_io_rows:
            if len(rows) < 2:
                continue
            for device in rows[0]["device_io"]:
                if device in rows[-1]["device_io"]:
                    first = rows[0]["device_io"][device]["counters"]
                    last = rows[-1]["device_io"][device]["counters"]
                    physical_reads[device] = physical_reads.get(device, 0) + max(0, last[2] - first[2]) * 512
        self.report["calibration_physical_read_bytes"] = physical_reads
        if len(physical_reads) != 2 or any(value <= 0 for value in physical_reads.values()):
            raise RuntimeError("calibration physical read I/O missing on SSD/HDD; formal plan blocked")
        self.rate = choose_rate(calibration_rates)
        predicted = max(prep_times) * 1.5 + 210 + 30
        if predicted > 480:
            raise RuntimeError("predicted round including fresh copy exceeds eight minutes")
        self.report.update(frozen_target_ops_per_sec=self.rate,
                           calibration_saturated_ops_per_sec=calibration_rates,
                           predicted_round_s=predicted, status="formal")
        self.save()
        formal = plan_order()
        recovery_reserve = 6 * (max(prep_times) * 1.5 + 90 + 120)
        self.deadline.require(len(formal) * predicted + recovery_reserve)
        for index, (repeat, workload, mode) in enumerate(formal):
            self.deadline.require((len(formal) - index) * predicted + recovery_reserve)
            name = f"trial-{repeat}-{workload}-{mode}"
            dirs = self.directories(name)
            round_started = time.monotonic()
            prep = self.copy(seed, dirs, name + "-copy")
            remaining_round = 480 - (time.monotonic() - round_started)
            if remaining_round <= 210:
                raise RuntimeError("copy leaves insufficient eight-minute round budget")
            row = self.execute(name, self.command("run", dirs, mode, workload, self.budget,
                                                  self.rate), dirs, timeout=remaining_round)
            row.update(repeat=repeat, mode=mode, workload=workload,
                       preparation_s=prep["elapsed_s"], budget_bytes=self.budget,
                       round_elapsed_s=time.monotonic() - round_started)
            row["phase_validation_errors"] = phase_validation(row, {"warmup": 60, "measured": 120, "saturated": 30})
            row["valid"] = row["valid"] and not row["phase_validation_errors"]
            row["mechanism_coverage"] = mechanism_coverage(row) if mode == "adaptive" else {"covered": True, "basis": "disabled"}
            self.report["trials"].append(row)
            self.save()
            self.clean(dirs, row["valid"])
            if not row["valid"]:
                raise RuntimeError("formal round failed; retained and plan stopped")
        for mode in ("disabled", "adaptive"):
            for cut in (60, 75, 90):
                self.deadline.require(max(prep_times) * 1.5 + cut + 120)
                name = f"rpo-{mode}-{cut}"
                dirs = self.directories(name)
                prep = self.copy(seed, dirs, name + "-copy")
                writer = self.execute(name + "-write", self.command("rpo_write", dirs,
                    mode, "mixed", self.budget, self.rate, measured=120, saturated=0),
                    dirs, timeout=cut + 45, fault_seconds=cut)
                restore_ssd = create_owned(self.ssd, name + "-restore", self.run_id)
                restore = self.execute(name + "-restore", self.command("rpo_restore",
                    (restore_ssd, dirs[1]), mode, "uniform", self.budget, self.rate,
                    warmup=60, measured=0, saturated=0), (restore_ssd, dirs[1]), timeout=120)
                restore["phase_validation_errors"] = phase_validation(restore, {"first60s_read": 60})
                restore["valid"] = restore["valid"] and not restore["phase_validation_errors"]
                recovered = [e for e in restore["events"] if e.get("event") == "recovered"]
                row = {"name": name, "mode": mode, "cut_seconds": cut,
                       "writer": writer, "restore": restore, "preparation_s": prep["elapsed_s"],
                       "valid": writer["fault_injected"] and not writer["timed_out"] and
                                not writer["parse_errors"] and restore["valid"] and bool(recovered)}
                if row["valid"]:
                    acks = [json.loads(line) for line in Path(writer["acks_path"]).read_text().splitlines()]
                    acks = [ack for ack in acks if ack.get("received_before_fault", True)]
                    row.update(rpo_result(acks, recovered[-1]["recovered_marker"],
                                          writer["fault_steady_time_us"]))
                    row["rto_us"] = recovered[-1]["rto_us"]
                self.report["rpo_samples"].append(row)
                self.save()
                self.clean(dirs, row["valid"])
                cleanup_owned(restore_ssd, self.ssd, self.run_id,
                              row["valid"] and not self.args.keep_data)
                if not row["valid"]:
                    raise RuntimeError("RPO sample failed; retained")
        self.report["status"] = "complete" if all(t["mechanism_coverage"]["covered"] for t in
            self.report["trials"]) else "complete_without_mechanism"
        self.clean(seed, True)


def helper():
    if len(sys.argv) > 1 and sys.argv[1] == "--cg-exec":
        cgroup_exec(sys.argv[2:])
        return True
    if len(sys.argv) == 6 and sys.argv[1] == "--clone":
        source_ssd, source_hdd, dest_ssd, dest_hdd = sys.argv[2:]
        details = clone_dataset(((source_ssd, dest_ssd), (source_hdd, dest_hdd)))
        print("MB_SST_JSON " + json.dumps({"protocol": "node_v1", "event": "summary",
                                            "ok": True, **details}), flush=True)
        return True
    return False


def main():
    if helper():
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--ssd-root", required=True)
    parser.add_argument("--hdd-root", required=True)
    parser.add_argument("--cgroup", default="/sys/fs/cgroup/memory/mbsst-20261001")
    parser.add_argument("--output", required=True)
    parser.add_argument("--profile", choices=tuple(PROFILES), default="legacy")
    parser.add_argument("--cpu-list", help="Linux helper CPU list/ranges; small default 0,1,2,3; legacy inherits")
    parser.add_argument("--deadline-seconds", type=int)
    parser.add_argument("--space-interval", type=float, default=10)
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    try:
        parse_cpu_list(args.cpu_list)
    except ValueError as error:
        parser.error(str(error))
    if args.deadline_seconds is None:
        args.deadline_seconds = PROFILES[args.profile]["deadline_s"]
    if not 0 < args.deadline_seconds <= PROFILES[args.profile]["deadline_s"] or not 5 <= args.space_interval <= 10:
        parser.error("deadline exceeds selected profile or space interval outside [5, 10] seconds")
    for root in (args.ssd_root, args.hdd_root):
        if not Path(root).is_dir():
            parser.error("experiment parents must already exist")
    ssd, hdd = Path(args.ssd_root).resolve(), Path(args.hdd_root).resolve()
    output = Path(args.output).resolve()
    if ssd == hdd or ssd in hdd.parents or hdd in ssd.parents:
        parser.error("parents must be disjoint")
    if output.exists() or not output.parent.is_dir() or any(root == output.parent or root in
            output.parents for root in (ssd, hdd)):
        parser.error("output must be new and outside both data parents")
    runner = Runner(args)
    def stop(signum, _frame):
        if runner.active:
            kill_owned_group(runner.active)
        raise RuntimeError("controller received signal " + str(signum))
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        runner.run()
    except Exception as error:
        runner.report.update(status="blocked_or_failed", error=str(error))
        return_code = 1
    else:
        return_code = 0 if runner.report["status"] == "complete" else 1
    finally:
        runner.report["binary_sha256_end"] = digest(args.binary)
        if runner.report["binary_sha256_end"] != runner.report["binary_sha256_start"]:
            runner.report.update(status="blocked_or_failed", error="benchmark binary changed during run")
            return_code = 1
        runner.report["elapsed_s"] = time.monotonic() - runner.deadline.start
        runner.save()
    print(json.dumps({"status": runner.report["status"], "output": args.output,
                      "error": runner.report.get("error")}))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
