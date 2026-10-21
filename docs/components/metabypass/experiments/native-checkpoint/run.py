#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Independent native-WAL experiment controller; never changes host settings."""
import argparse
import ctypes
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid

PROTOCOL = "metabypass-native-perf-v1"
PREFIX = "MB_NATIVE_JSON "
OWNER = ".native-owner.json"
GIB = 1024 ** 3
MIB = 1024 ** 2
GROUPS = ("U", "B", "D", "S")
STAGES = {"preflight": 1800, "calibration": 5400, "core": 9000,
          "specials": 5400, "recovery": 3600, "reserve": 3600}
CGROUP = Path("/sys/fs/cgroup/memory/mbgc-smallmem")
HERE = Path(__file__).resolve().parent


class RunFailure(RuntimeError):
    pass


class DriverFailure(RunFailure):
    pass


class CoverageFailure(RunFailure):
    pass


def strict_json(text):
    def bad(value):
        raise ValueError("nonfinite JSON constant: " + value)
    return json.loads(text, parse_constant=bad)


def validate_stage_budgets(value):
    if not isinstance(value, dict) or set(value) != set(STAGES):
        raise ValueError("stage budgets require exactly these keys: " + ", ".join(STAGES))
    for name, seconds in value.items():
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or \
                not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("stage budget must be finite positive seconds: " + name)
    if sum(value.values()) > 28800:
        raise ValueError("stage budgets must total at most 28800 seconds")
    return dict(value)


def stage_budget_configuration(path=None):
    if path is None:
        return dict(STAGES), None
    source = Path(path).resolve()
    raw = source.read_bytes()
    value = validate_stage_budgets(strict_json(raw.decode("utf-8")))
    return value, {"path": str(source), "sha256": hashlib.sha256(raw).hexdigest(),
                   "raw_json": raw.decode("utf-8"), "validated_seconds": value}


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(MIB), b""):
            result.update(block)
    return result.hexdigest()


def capture(command, timeout=10):
    try:
        result = subprocess.run(command, capture_output=True, text=True,
                                timeout=timeout, check=False)
        return {"command": command, "exit": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "error": str(error)}


class Deadline:
    def __init__(self, seconds, clock=time.monotonic):
        if seconds is not None and (not math.isfinite(seconds) or seconds <= 0):
            raise ValueError("deadline must be finite and positive")
        self.clock = clock
        self.end = None if seconds is None else clock() + seconds

    def remaining(self):
        if self.end is None:
            return math.inf
        return max(0, self.end - self.clock())

    def require(self, seconds=0):
        if self.end is None:
            return
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("forecast must be finite and nonnegative")
        if self.remaining() <= seconds:
            raise RunFailure("hard deadline or insufficient remaining budget")


def parse_proc_identity(pid, text):
    fields = text.rsplit(") ", 1)[1].split()
    return {"pid": pid, "starttime_ticks": int(fields[19]),
            "pgrp": int(fields[2]), "session": int(fields[3]),
            "state": fields[0], "flags": int(fields[6]),
            "cpu_ticks": int(fields[11]) + int(fields[12])}


def proc_identity(pid):
    try:
        return parse_proc_identity(pid, Path(f"/proc/{pid}/stat").read_text())
    except (OSError, ValueError, IndexError):
        return None


def kill_owned_group(identity):
    if not identity:
        return False
    current = proc_identity(identity["pid"])
    if current and current["starttime_ticks"] == identity["starttime_ticks"] and \
            current["pgrp"] == identity["pid"]:
        try:
            os.killpg(identity["pid"], signal.SIGKILL)
            return True
        except ProcessLookupError:
            pass
    return False


def no_symlinks(path):
    path = Path(os.path.abspath(path))
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise RunFailure("symlink path component: " + str(ancestor))
    return path


def create_owned(parent, name, run_id):
    parent = no_symlinks(parent)
    if not parent.is_dir() or parent.stat().st_uid != os.getuid():
        raise RunFailure("parent must exist and belong to current uid: " + str(parent))
    if Path(name).name != name or name in ("", ".", ".."):
        raise RunFailure("owned child requires a single name")
    path = parent / name
    path.mkdir()  # Existing directories, including empty ones, are never reused.
    write_json(path / OWNER, {"protocol": PROTOCOL, "run_uuid": run_id,
                             "host": socket.gethostname(), "uid": os.getuid(),
                             "controller": proc_identity(os.getpid())})
    return path


def check_owned(path, parent, run_id):
    path, parent = no_symlinks(path), no_symlinks(parent)
    if path.parent != parent or path.stat().st_uid != os.getuid():
        raise RunFailure("owned path boundary mismatch")
    owner = strict_json((path / OWNER).read_text())
    if owner.get("protocol") != PROTOCOL or owner.get("run_uuid") != run_id or \
            owner.get("host") != socket.gethostname() or owner.get("uid") != os.getuid():
        raise RunFailure("ownership marker mismatch")


def cleanup_owned(path, parent, run_id):
    check_owned(path, parent, run_id)
    # Never follow a later-added symlink while deleting a successful owned tree.
    if not shutil.rmtree.avoids_symlink_attacks:
        raise RunFailure("platform lacks symlink-safe rmtree")
    shutil.rmtree(path)


def space_snapshot(roots):
    """Logical and allocated bytes, counted once per inode across all roots."""
    seen = set()
    result = {"logical_bytes": 0, "allocated_bytes": 0, "files": 0, "errors": []}
    for root in roots:
        root = Path(root)
        if not root.exists():
            continue
        for base, dirs, files in os.walk(root, followlinks=False):
            for name in [".", *dirs, *files]:
                path = Path(base) if name == "." else Path(base) / name
                try:
                    info = path.lstat()
                    if stat.S_ISLNK(info.st_mode):
                        raise RunFailure("symlink in experiment data: " + str(path))
                    identity = info.st_dev, info.st_ino
                    if identity in seen:
                        continue
                    seen.add(identity)
                    result["allocated_bytes"] += info.st_blocks * 512
                    if stat.S_ISREG(info.st_mode):
                        result["logical_bytes"] += info.st_size
                        result["files"] += 1
                except FileNotFoundError:
                    pass  # Files may disappear during the engine's native GC.
                except OSError as error:
                    result["errors"].append(str(error))
    return result


def sample_residency(roots):
    """mincore reads cache residency without reading data or changing caching."""
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                         ctypes.c_int, ctypes.c_int, ctypes.c_longlong]
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    page = os.sysconf("SC_PAGE_SIZE")
    output = {"sst": {"bytes": 0, "resident_bytes": 0, "files": 0},
              "blob_extent": {"bytes": 0, "resident_bytes": 0, "files": 0},
              "errors": []}
    seen = set()
    for root in roots:
        if not Path(root).exists():
            continue
        for base, _, names in os.walk(root, followlinks=False):
            for name in names:
                category = "sst" if name.endswith(".sst") else \
                    "blob_extent" if name.endswith(".blob") or name.startswith("segment-") else None
                if category is None:
                    continue
                path = Path(base) / name
                fd, address = None, None
                try:
                    info = path.lstat()
                    if not stat.S_ISREG(info.st_mode):
                        raise RunFailure("non-regular residency file")
                    identity = info.st_dev, info.st_ino
                    if identity in seen or info.st_size == 0:
                        continue
                    seen.add(identity)
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                    address = libc.mmap(None, info.st_size, 1, 1, fd, 0)
                    if address == ctypes.c_void_p(-1).value:
                        address = None
                        raise OSError(ctypes.get_errno(), "mmap")
                    pages = (info.st_size + page - 1) // page
                    vector = (ctypes.c_ubyte * pages)()
                    if libc.mincore(address, info.st_size, vector):
                        raise OSError(ctypes.get_errno(), "mincore")
                    entry = output[category]
                    entry["bytes"] += info.st_size
                    entry["resident_bytes"] += sum(
                        min(page, info.st_size - index * page)
                        for index, value in enumerate(vector) if value & 1)
                    entry["files"] += 1
                except FileNotFoundError:
                    pass
                except OSError as error:
                    output["errors"].append({"path": str(path), "error": str(error)})
                finally:
                    if address is not None:
                        libc.munmap(address, info.st_size)
                    if fd is not None:
                        os.close(fd)
    return output


def telemetry(pid=None, cgroup=None, process=None, expected_identity=None):
    result = {"monotonic": time.monotonic(), "errors": []}
    for label, path in (("diskstats", "/proc/diskstats"),
                        ("meminfo", "/proc/meminfo"), ("vmstat", "/proc/vmstat"),
                        ("loadavg", "/proc/loadavg")):
        try:
            result[label] = Path(path).read_text()
        except OSError as error:
            result["errors"].append(label + ": " + str(error))
    if pid is not None:
        before = proc_identity(pid)
        exit_before = process.poll() if process is not None else None
        proc_errors = []
        for label in ("status", "io", "stat", "cgroup"):
            try:
                result["proc_" + label] = Path(f"/proc/{pid}/{label}").read_text()
            except OSError as error:
                proc_errors.append({"field": label, "errno": error.errno,
                                    "message": str(error)})
        after = proc_identity(pid)
        exit_after = process.poll() if process is not None else None
        sampled = None
        if "proc_stat" in result:
            try:
                sampled = parse_proc_identity(pid, result["proc_stat"])
            except (ValueError, IndexError) as error:
                result["errors"].append("stat identity: " + str(error))
        observations = [value for value in (before, sampled, after) if value is not None]
        identity_keys = ("pid", "starttime_ticks", "pgrp", "session")
        mismatched = expected_identity is not None and any(
            any(value.get(name) != expected_identity.get(name) for name in identity_keys)
            for value in observations)
        if mismatched:
            result["errors"].append("process identity changed during telemetry")
        owned_child = expected_identity is not None and expected_identity.get("pid") == pid and \
            process is not None and process.pid == pid and not mismatched
        zombie = any(value.get("state") in ("Z", "X", "x") for value in observations)
        # Linux sets PF_EXITING before exit_mm/cgroup_exit and before the
        # zombie state. The authenticated child can still report state R here.
        exiting = any(value.get("flags", 0) & 0x4 for value in observations)
        polled_exit = exit_before is not None or exit_after is not None
        expected_exit = owned_child and (zombie or exiting or polled_exit)
        result["process_observation"] = {"expected": expected_identity, "before": before,
                                         "sampled_stat": sampled, "after": after,
                                         "exit_code_before": exit_before, "exit_code_after": exit_after}
        if expected_exit:
            result["process_exited"] = True
        for error in proc_errors:
            # Only this sampling interval can justify an exit race. A later
            # successful wait must never excuse a live-process telemetry error.
            if expected_exit and error["errno"] in (errno.ENOENT, errno.ESRCH, errno.EACCES):
                result.setdefault("expected_exit_errors", []).append({
                    **error, "reason": "same_identity_zombie" if zombie else
                    "same_identity_pf_exiting" if exiting else "owned_child_polled_exit"})
            else:
                result["errors"].append(error["field"] + ": " + error["message"])
    if cgroup:
        result["cgroup"] = {}
        for name in ("memory.usage_in_bytes", "memory.max_usage_in_bytes",
                     "memory.failcnt", "memory.stat", "memory.oom_control", "tasks"):
            try:
                result["cgroup"][name] = (Path(cgroup) / name).read_text()
            except OSError as error:
                result["errors"].append(name + ": " + str(error))
    return result


def progress_signature(snapshot):
    """CPU/I/O advancement, rather than wall time, detects an inactive driver."""
    text = snapshot.get("proc_stat")
    cpu_ticks = None
    if text:
        fields = text.rsplit(") ", 1)[1].split()
        cpu_ticks = int(fields[11]) + int(fields[12])
    return cpu_ticks, snapshot.get("proc_io")


def disk_counters(text):
    counters = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 14:
            counters[fields[0] + ":" + fields[1]] = {
                "name": fields[2], "read_bytes": int(fields[5]) * 512,
                "write_bytes": int(fields[9]) * 512,
                "read_ops": int(fields[3]), "write_ops": int(fields[7])}
    return counters


def device_delta(before, after, paths):
    first, last = disk_counters(before), disk_counters(after)
    output = {}
    for label in ("index", "data", "backup", "staging"):
        device = Path(paths[label]).stat().st_dev
        key = str(os.major(device)) + ":" + str(os.minor(device))
        if key in output:
            output[key]["paths"].append(label)
            continue
        if key not in first or key not in last:
            output[key] = {"paths": [label], "available": False,
                           "reason": "filesystem device absent in /proc/diskstats"}
            continue
        output[key] = {"paths": [label], "available": True, "name": last[key]["name"],
                       **{field: last[key][field] - first[key][field]
                          for field in ("read_bytes", "write_bytes", "read_ops", "write_ops")}}
        if any(output[key][field] < 0 for field in ("read_bytes", "write_bytes", "read_ops", "write_ops")):
            raise RunFailure("device counter reset during sample")
    return {"devices": output, "attribution": "whole device; other host activity is not excluded, not engine write amplification"}


def cpu_list(value):
    cpus = set()
    for item in value.split(","):
        bounds = item.split("-")
        if len(bounds) > 2 or not all(bound.isdigit() for bound in bounds):
            raise ValueError("invalid CPU list")
        first, last = int(bounds[0]), int(bounds[-1])
        if last < first or last > 1048575:
            raise ValueError("invalid CPU range")
        cpus.update(range(first, last + 1))
    if not cpus or not cpus.issubset(os.sched_getaffinity(0)):
        raise ValueError("CPU list outside controller affinity")
    return sorted(cpus)


def child_exec(arguments):
    cgroup, cpus, *command = arguments
    selected = cpu_list(cpus)
    os.sched_setaffinity(0, selected)
    if sorted(os.sched_getaffinity(0)) != selected:
        raise RunFailure("child affinity did not apply")
    if cgroup != "none":
        with (Path(cgroup) / "tasks").open("w") as stream:
            stream.write(str(os.getpid()))
    print("MB_NATIVE_WRAPPER " + json.dumps({"pid": os.getpid(),
          "affinity": selected, "cgroup": cgroup}), flush=True)
    os.execv(command[0], command)


def validate_events(events, action, variant, workload, spec=None):
    if any(event.get("protocol") != PROTOCOL for event in events):
        raise RunFailure("driver protocol mismatch")
    done = [event for event in events if event.get("event") == "done"]
    if len(done) != 1 or done[0].get("status") != "pass":
        raise RunFailure("missing or non-pass unique done event")
    if any(event.get("event") == "error" for event in events):
        raise DriverFailure("driver emitted error")
    for key, expected in (("action", action), ("variant", variant), ("workload", workload)):
        if done[0].get(key) != expected:
            raise RunFailure("done event " + key + " mismatch")
    verification = [event for event in events if event.get("event") == "verification"]
    requested = (spec or {}).get("verify_samples", 0)
    verifying = action in ("verify", "restore", "append-reopen")
    if requested and verifying:
        if len(verification) != 1:
            raise RunFailure("sample verification requires one verification event")
        record = verification[0]
        population = spec["keys"]
        actual = min(requested, population)
        seed = spec.get("seed", 1729) ^ 0xd1b54a32d192ed03
        seed = seed or 0xd1b54a32d192ed03
        expected = {"verification_protocol": "metabypass-key-sample-v1", "mode": "sample",
                    "population": population, "requested": requested, "actual": actual,
                    "derived_seed": seed, "full_keycount_checked": False}
        if any(type(record.get(key)) is not type(value) or record[key] != value
               for key, value in expected.items()):
            raise RunFailure("sample verification policy identity mismatch")
        keys = record.get("keys")
        if not isinstance(keys, list) or len(keys) != actual or \
                any(type(key) is not int or not 0 <= key < population for key in keys) or \
                keys != sorted(set(keys)):
            raise RunFailure("sample verification key list invalid")
        mandatory = {0, population - 1}
        for worker in range(spec.get("threads", 1)):
            mandatory.update((population * worker // spec.get("threads", 1),
                              population * (worker + 1) // spec.get("threads", 1) - 1))
        if not mandatory.issubset(keys):
            raise RunFailure("sample verification omitted mandatory boundary")
        counts = [record.get(key) for key in ("live_checked", "deleted_checked", "unwritten_checked")]
        if any(type(count) is not int or count < 0 for count in counts) or sum(counts) != actual:
            raise RunFailure("sample verification category counts invalid")
        if type(done[0].get("verified_keys")) is not int or done[0]["verified_keys"] != actual:
            raise RunFailure("sample verification done count mismatch")
    elif verification:
        raise RunFailure("unexpected sample verification event for full or non-verifying action")
    for event in events:
        if event.get("event") == "sample":
            for key in ("ops", "elapsed_us", "ops_per_sec", "p50_us", "p95_us", "p99_us"):
                value = event.get(key)
                if not isinstance(value, (int, float)) or isinstance(value, bool) or \
                        not math.isfinite(value) or value < 0:
                    raise RunFailure("invalid sample metric " + key)
            if event["ops"] <= 0 or event["elapsed_us"] <= 0:
                raise RunFailure("empty sample")
    expected = 3 if workload == "gc" else 1
    if action == "run" and len([e for e in events if e.get("event") == "sample" and
                                 e.get("name", "foreground") == "foreground"]) != expected:
        raise RunFailure("run has an unexpected number of measured samples")
    return done[0]


def validate_crash_boundary(events, requested, injected, exit_code):
    ready = [event for event in events if event.get("event") == "ready"]
    if not injected or exit_code != -signal.SIGKILL or len(ready) != 1 or \
            ready[0].get("stage") != requested:
        raise CoverageFailure("requested crash stage did not reach authenticated ready/SIGKILL boundary")


def key_values(text):
    return {fields[0]: int(fields[1]) for line in text.splitlines()
            if len(fields := line.split()) == 2 and fields[1].isdigit()}


def driver_command(binary, paths, spec, action, fresh=False):
    options = {"variant": spec["group"], "action": action,
               "workload": spec["workload"], "keys": spec["keys"],
               "ops": spec.get("ops", spec["keys"]), "value-size": spec["value_size"],
               "threads": spec.get("threads", 1), "warmup-seconds": spec.get("warmup", 0),
               "measure-seconds": spec.get("seconds", 0), "sync": spec["workload"] == "sync",
               "flush": spec.get("flush", True), "fresh": fresh,
               "staging-capacity": spec.get("staging_capacity", 256 * MIB),
               "sst-mode": spec.get("sst_mode", "disabled"),
               "sst-budget": spec.get("sst_budget", 0), "gc": spec.get("gc", True),
               "cache-bytes": spec.get("cache_bytes", 256 * MIB),
               "flush-every": spec.get("flush_every", 0), "seed": spec.get("seed", 1729),
               "gc-wait-seconds": spec.get("gc_wait_seconds", 0)}
    # Omitted fields preserve the historical executable command protocol.
    for name in ("direct_write", "mirror_interval_ms", "checkpoint_interval_ms", "verify_samples"):
        if name in spec:
            options[name.replace("_", "-")] = spec[name]
    if spec.get("disable_auto_compactions"):
        options["disable-auto-compactions"] = True
    if spec.get("verify_acks"):
        options["verify-acks"] = spec["verify_acks"]
    if spec.get("crash_stage"):
        options["crash-stage"] = spec["crash_stage"]
    options.update({key: str(value) for key, value in paths.items()})
    return [str(binary)] + ["--" + key + "=" +
            (str(value).lower() if isinstance(value, bool) else str(value))
            for key, value in options.items()]


def read_tasks(cgroup):
    return set(map(int, (Path(cgroup) / "tasks").read_text().split()))


def last_stats(events):
    snapshots = [event for event in events if event.get("event") == "stats"]
    if not snapshots:
        raise RunFailure("driver did not emit native statistics")
    return snapshots[-1]


def mechanism_coverage(spec, events):
    stats = last_stats(events)
    missing = []
    mandatory = ["flushes", "compactions", "wal_bytes", "flush_data_bytes", "compaction_output_bytes"]
    if spec["group"] in ("D", "S"):
        mandatory += ["mirrored_wal_bytes", "recovery_points", "peak_queued_bytes",
                      "backpressure_micros", "candidate_copied_bytes", "hdd_extents"]
    if spec["group"] == "S":
        mandatory += ["peak_staging_bytes", "staging_spill_bytes", "staging_backpressure_micros"]
        capacity = spec.get("staging_capacity", 256 * MIB)
        if stats.get("peak_staging_bytes", 0) > capacity:
            raise DriverFailure("staging logical capacity exceeded")
    for key in mandatory:
        if key not in stats or not isinstance(stats[key], (int, float)) or \
                not math.isfinite(stats[key]) or stats[key] < 0:
            missing.append("missing/invalid counter " + key)
    if spec.get("stage") == "specials":
        if spec["workload"] == "gc":
            if stats.get("flushes", 0) < 3:
                missing.append("three GC generation flushes not observed")
            if spec.get("gc", True) and stats.get("reclaimed_blob_files", 0) <= 0:
                missing.append("no reclaimable whole Blob file was reclaimed")
        if spec["workload"] == "sst" and spec.get("sst_mode") != "disabled":
            if stats.get("sst_sampled_reads", 0) <= 0:
                missing.append("SST read sampling not observed")
            key = "sst_observed_demotions" if spec["sst_mode"] == "observe" else "sst_demotions"
            if stats.get(key, 0) <= 0:
                missing.append("SST policy/demotion mechanism not observed")
    return {"complete": not missing, "missing": missing,
            "native_flushes": stats.get("flushes"), "native_compactions": stats.get("compactions"),
            "note": "zero native counts mean that mechanism was not exercised in this sample"}


def run_driver(binary, paths, spec, action, output, deadline, cpus, cgroup=None,
               timeout=900, fresh=False, crash_after=None, progress_timeout=False):
    """One authenticated session group; controller remains outside memory cgroup."""
    deadline.require()
    if cgroup and read_tasks(cgroup):
        raise RunFailure("cgroup is occupied before database launch")
    output.mkdir()
    command = driver_command(binary, paths, spec, action, fresh)
    wrapper = [sys.executable, str(HERE / "run.py"), "_exec", str(cgroup or "none"),
               ",".join(map(str, cpus)), *command]
    record = {"command": command, "wrapper": wrapper, "events": [], "status": "running",
              "binary_sha256_start": digest(binary), "telemetry": [], "valid": False}
    record["timeout_policy"] = {"mode": "inactivity" if progress_timeout else "elapsed",
                                "seconds": timeout,
                                "progress_evidence": "stdout or process CPU/I/O counter advancement"}
    write_json(output / "command.json", record)
    record["telemetry"].append(telemetry(cgroup=cgroup))
    start = time.monotonic()
    last_progress = start
    last_signature = None
    process = None
    identity = None
    selector = selectors.DefaultSelector()
    buffer = b""
    ack_stream = (output / "ack.txt").open("w") if crash_after is not None else None
    try:
        with (output / "stdout.log").open("wb") as raw, \
                (output / "stderr.log").open("wb") as errors:
            process = subprocess.Popen(wrapper, stdout=subprocess.PIPE, stderr=errors,
                                       stdin=subprocess.PIPE if crash_after is not None else subprocess.DEVNULL,
                                       start_new_session=True)
            identity = proc_identity(process.pid)
            if identity is None or identity["pgrp"] != process.pid:
                raise RunFailure("cannot authenticate database session leader")
            record["process"] = identity
            selector.register(process.stdout, selectors.EVENT_READ)
            next_sample = start
            wrapper_seen = False
            while selector.get_map():
                now = time.monotonic()
                timeout_origin = last_progress if progress_timeout else start
                if now - timeout_origin >= timeout or deadline.remaining() <= 0:
                    raise RunFailure("driver inactivity timeout" if progress_timeout else "sample timeout or total deadline")
                if crash_after is not None and not spec.get("crash_stage") and now - start >= crash_after:
                    if not any(e.get("event") == "ack" for e in record["events"]):
                        raise RunFailure("crash target has no acknowledged writes")
                    if not kill_owned_group(identity):
                        raise RunFailure("crash target identity changed")
                    record["injected_sigkill"] = True
                    crash_after = None
                if now >= next_sample:
                    snapshot = telemetry(process.pid, cgroup, process=process,
                                         expected_identity=identity)
                    record["telemetry"].append(snapshot)
                    if progress_timeout:
                        signature = progress_signature(snapshot)
                        if signature != (None, None) and signature != last_signature:
                            last_progress = now
                            last_signature = signature
                    next_sample = now + 1
                    if cgroup:
                        for tid in read_tasks(cgroup):
                            member = proc_identity(tid)
                            if member and member["pgrp"] != process.pid:
                                raise RunFailure("foreign process entered experiment cgroup")
                for key, _ in selector.select(timeout=min(0.2, deadline.remaining())):
                    data = os.read(key.fileobj.fileno(), 65536)
                    if not data:
                        selector.unregister(key.fileobj)
                        continue
                    raw.write(data)
                    raw.flush()
                    if progress_timeout:
                        last_progress = time.monotonic()
                    buffer += data
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        line = line.decode("utf-8", errors="strict")
                        if line.startswith("MB_NATIVE_WRAPPER "):
                            value = strict_json(line[len("MB_NATIVE_WRAPPER "):])
                            if value != {"pid": process.pid, "affinity": cpus,
                                         "cgroup": str(cgroup or "none")}:
                                raise RunFailure("wrapper affinity/cgroup evidence mismatch")
                            wrapper_seen = True
                        elif line.startswith(PREFIX):
                            event = strict_json(line[len(PREFIX):])
                            if event.get("protocol") != PROTOCOL:
                                raise RunFailure("driver event protocol mismatch")
                            if event.get("event") == "ack" and ack_stream:
                                key, version = event.get("key"), event.get("version")
                                if not isinstance(key, int) or not isinstance(version, int) or key < 0 or version <= 0:
                                    raise RunFailure("invalid ACK key/version")
                                ack_stream.write(str(key) + " " + str(version) + "\n")
                                ack_stream.flush()
                                os.fsync(ack_stream.fileno())
                            record["events"].append(event)
                            if event.get("event") == "ready" and spec.get("crash_stage"):
                                if event.get("stage") != spec["crash_stage"]:
                                    raise RunFailure("crash stage evidence mismatch")
                                write_json(output / "ready.json", event)
                                if not kill_owned_group(identity):
                                    raise RunFailure("crash stage session identity changed")
                                record["injected_sigkill"] = True
                                record["crash_stage"] = event["stage"]
            process.wait(timeout=min(10, max(0.01, deadline.remaining())))
            record["exit"] = process.returncode
            if buffer:
                raise RunFailure("unterminated stdout record")
            if not wrapper_seen:
                raise RunFailure("missing child affinity/cgroup wrapper evidence")
            if spec.get("crash_stage"):
                validate_crash_boundary(record["events"], spec["crash_stage"],
                                        record.get("injected_sigkill", False), process.returncode)
            if record.get("injected_sigkill"):
                if process.returncode != -signal.SIGKILL:
                    raise RunFailure("crash target did not exit via SIGKILL")
                record["status"] = "fault_injected"
            else:
                if process.returncode != 0:
                    errors = [event.get("message", "") for event in record["events"] if event.get("event") == "error"]
                    message = "driver nonzero exit: " + str(process.returncode) + "; " + "; ".join(errors)
                    memory_error = any(marker in " ".join(errors).lower() for marker in (
                        "out of memory", "cannot allocate memory", "memory limit",
                        "memorylimit", "enomem"))
                    if process.returncode == -signal.SIGKILL or memory_error:
                        raise RunFailure(message)
                    raise DriverFailure(message)
                validate_events(record["events"], action, spec["group"], spec["workload"], spec)
                record["status"] = "pass"
        record["binary_sha256_end"] = digest(binary)
        if record["binary_sha256_end"] != record["binary_sha256_start"]:
            raise RunFailure("binary changed during sample")
        record["telemetry"].append(telemetry(cgroup=cgroup))
        if any(sample["errors"] for sample in record["telemetry"]):
            raise RunFailure("required telemetry could not be collected")
        record["device_io"] = device_delta(record["telemetry"][0]["diskstats"],
                                           record["telemetry"][-1]["diskstats"], paths)
        if cgroup:
            first = int(record["telemetry"][0]["cgroup"]["memory.failcnt"])
            last = int(record["telemetry"][-1]["cgroup"]["memory.failcnt"])
            record["cgroup_memory_failcnt_delta"] = last - first
            # On cgroup v1, failcnt can increase during successful page-cache
            # reclaim. It is evidence, not an OOM verdict for a >8 GiB dataset.
            initial_oom = key_values(record["telemetry"][0]["cgroup"]["memory.oom_control"])
            final_oom = key_values(record["telemetry"][-1]["cgroup"]["memory.oom_control"])
            record["cgroup_oom_kill_delta"] = final_oom.get("oom_kill", 0) - initial_oom.get("oom_kill", 0)
            record["cgroup_under_oom_observed"] = any(
                key_values(sample.get("cgroup", {}).get("memory.oom_control", "")).get("under_oom", 0) > 0
                for sample in record["telemetry"])
            if record["cgroup_oom_kill_delta"] > 0:
                raise RunFailure("cgroup oom_kill counter increased; sample incomplete")
            if any(not device["available"] for device in record["device_io"]["devices"].values()):
                raise RunFailure("required target device I/O telemetry missing")
        record["valid"] = record["status"] == "pass"
    except BaseException as error:
        record["status"] = "failed" if isinstance(error, DriverFailure) else "incomplete"
        record["error"] = str(error)
        kill_owned_group(identity)
        if process:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                record["cleanup_error"] = "owned process did not exit"
        raise
    finally:
        selector.close()
        if process and process.stdin:
            process.stdin.close()
        if ack_stream:
            ack_stream.close()
        record["elapsed_s"] = time.monotonic() - start
        write_json(output / "result.json", record)
    return record


def matrix(profile="formal", write_keys=512 * MIB // 1024, sync_ops=128):
    """A frozen, rotation-balanced matrix; never changes counts mid-measurement."""
    smoke = profile == "smoke"
    seed_keys = 2048 if smoke else GIB // 1024
    read_keys = 4096 if smoke else 12 * GIB // 1024
    rows = []
    definitions = [("insert-1k-1t", "insert", 1024, 1, GROUPS),
                   ("insert-1k-4t", "insert", 1024, 4, GROUPS),
                   ("insert-4k-1t", "insert", 4096, 1, GROUPS),
                   ("overwrite", "overwrite", 1024, 4, GROUPS),
                   ("randomread", "randomread", 1024, 1, GROUPS),
                   ("hotspot", "hotspot", 1024, 1, GROUPS),
                   ("scan100", "scan", 1024, 1, GROUPS),
                   ("randomread-4t", "randomread", 1024, 4, ("U", "B", "S")),
                   ("mixed", "mixed", 1024, 4, GROUPS),
                   ("sync", "sync", 1024, 1, GROUPS),
                   ("upstream-hdd", "insert", 1024, 1, ("U",))]
    for round_number in range(3):
        for scenario, workload, value_size, threads, groups in definitions:
            shift = round_number % len(groups)
            for group in groups[shift:] + groups[:shift]:
                is_read = workload in ("randomread", "hotspot", "scan")
                keys = read_keys if is_read else seed_keys if workload in ("mixed", "overwrite") \
                    else sync_ops if workload == "sync" else write_keys * 1024 // value_size
                seconds = (2 if smoke else 120 if workload == "mixed" else 60) \
                    if is_read or workload == "mixed" else 0
                rows.append({"stage": "core", "scenario": scenario, "round": round_number + 1,
                             "group": group, "workload": workload, "keys": keys,
                             "ops": sync_ops if workload == "sync" else keys,
                             "value_size": value_size, "threads": threads,
                             "warmup": (0 if smoke else 30 if workload == "mixed" else 15)
                             if is_read or workload == "mixed" else 0,
                             "seconds": seconds, "static": is_read,
                             "diagnostic": scenario == "upstream-hdd"})
    return rows


def special_matrix(profile="formal"):
    smoke = profile == "smoke"
    keys = 2048 if smoke else GIB // 1024
    rows = []
    for number in range(1, 4):
        for capacity in (256 * MIB, 65536):
            rows.append({"stage": "specials", "scenario": "staging-" + str(capacity),
                         "round": number, "group": "S", "workload": "insert",
                         "keys": keys, "ops": keys, "value_size": 1024, "threads": 4,
                         "staging_capacity": capacity})
        for enabled in (True, False):
            rows.append({"stage": "specials", "scenario": "gc-" + str(enabled).lower(),
                         "round": number, "group": "S", "workload": "gc",
                         "keys": keys, "ops": keys, "value_size": 1024, "threads": 4,
                         "gc": enabled, "flush_every": max(1, keys // 4),
                         "gc_wait_seconds": 0 if smoke else 60})
        for mode in ("disabled", "observe", "adaptive"):
            rows.append({"stage": "specials", "scenario": "sst-" + mode,
                         "round": number, "group": "S", "workload": "sst",
                         "keys": keys, "ops": keys, "value_size": 1024, "threads": 1,
                         "cache_bytes": 65536, "sst_mode": mode,
                         "disable_auto_compactions": True,
                         "flush_every": max(1, keys // 8),
                         "warmup": 0 if smoke else 30, "seconds": 2 if smoke else 120})
    return rows


def preflight(args):
    roots = {"ssd": no_symlinks(args.ssd_parent), "hdd": no_symlinks(args.hdd_parent)}
    output = {"protocol": PROTOCOL, "profile": args.profile,
              "stage_budget_seconds": getattr(args, "stage_budgets", STAGES),
              "stage_budget_override": getattr(args, "stage_budget_override", None),
              "budget_seconds": None if getattr(args, "no_time_limits", False) else min(args.budget_seconds, 28800),
              "time_limit_policy": getattr(args, "time_limit_policy", {"mode": "bounded"}),
              "host": socket.gethostname(), "uid": os.getuid(),
              "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "paths": {}, "checks": [], "ready": False,
              "host_info": {name: capture(command) for name, command in {
                  "mounts": ["findmnt", "-J"], "devices": ["lsblk", "-J", "-b", "-o",
                  "NAME,MAJ:MIN,SIZE,TYPE,MOUNTPOINT"], "processes": ["ps", "-eo",
                  "pid,ppid,user,pcpu,pmem,args"], "swap": ["swapon", "--show", "--bytes"],
                  "uname": ["uname", "-a"]}.items()}}
    failures = []
    for label, path in roots.items():
        if not path.is_dir() or path.stat().st_uid != os.getuid():
            failures.append(label + " parent missing or foreign-owned")
            continue
        usage = shutil.disk_usage(path)
        peak = (40 if label == "ssd" else 100) * GIB if args.profile == "formal" else 128 * MIB
        output["paths"][label] = {"path": str(path), "device": path.stat().st_dev,
                                  "total": usage.total, "free": usage.free,
                                  "projected_peak": peak, "reserved_fraction": .2,
                                  "peak_policy": "live peak with successful independent write data cleaned after archival; failed data retained"}
        if usage.free - peak < usage.total * .2:
            failures.append(label + " insufficient free space including 20% reserve")
    cpus = cpu_list(args.cpus) if args.cpus else sorted(os.sched_getaffinity(0))[:4]
    if len(cpus) < 4:
        failures.append("four allowed CPUs required for matching 1/4-thread scenarios")
    output["affinity"] = cpus
    if args.profile == "formal":
        import pwd
        if socket.gethostname() != "8001" or pwd.getpwuid(os.getuid()).pw_name != "lj":
            failures.append("formal execution requires lj@8001 (node-ssd)")
        if roots["ssd"] != Path("/home/lj/ssd/metabypass_test") or \
                roots["hdd"] != Path("/home/lj/hdd/metabypass_test"):
            failures.append("formal roots differ from approved SSD/HDD experiment parents")
        if roots["ssd"].exists() and roots["hdd"].exists() and \
                roots["ssd"].stat().st_dev == roots["hdd"].stat().st_dev:
            failures.append("formal SSD and HDD resolve to the same device")
        output["target_mounts"] = {}
        for label, expected in (("ssd", "/dev/sdb"), ("hdd", "/dev/sdc1")):
            mount = capture(["findmnt", "-n", "-o", "SOURCE,FSTYPE,TARGET", "-T", str(roots[label])])
            output["target_mounts"][label] = mount
            fields = mount.get("stdout", "").split()
            if mount.get("exit") != 0 or len(fields) != 3 or fields[0] != expected or fields[1] != "ext4":
                failures.append(label + " device/filesystem differs from approved mount")
        try:
            group = Path(args.cgroup)
            limit = int((group / "memory.limit_in_bytes").read_text())
            swappiness = int((group / "memory.swappiness").read_text())
            tasks = sorted(read_tasks(group))
            output["cgroup"] = {"path": str(group), "limit_bytes": limit,
                                "swappiness": swappiness, "tasks": tasks}
            if limit != 8 * GIB or swappiness != 0 or tasks or os.getpid() in tasks:
                failures.append("8 GiB swappiness=0 exclusive empty cgroup required")
            if not os.access(group / "tasks", os.W_OK):
                failures.append("cgroup tasks is not writable with existing permissions")
        except OSError as error:
            failures.append("cgroup unavailable: " + str(error))
    else:
        output["limitation"] = "local same-disk smoke; no device performance conclusion"
    output["telemetry"] = telemetry(cgroup=args.cgroup if args.profile == "formal" else None)
    if output["telemetry"]["errors"]:
        failures.extend(output["telemetry"]["errors"])
    output["failures"] = failures
    output["ready"] = not failures
    return output


class Experiment:
    def __init__(self, args):
        self.args = args
        self.stage_budgets = dict(args.stage_budgets)
        self.run_id = str(uuid.uuid4())
        self.budget_seconds = None if args.no_time_limits else min(args.budget_seconds, 28800)
        self.deadline = Deadline(self.budget_seconds)
        self.preflight = preflight(args)
        if not self.preflight["ready"]:
            raise RunFailure("preflight rejected: " + "; ".join(self.preflight["failures"]))
        self.cpus = self.preflight["affinity"]
        self.cgroup = Path(args.cgroup) if args.profile == "formal" else None
        output = no_symlinks(args.output)
        for root in (Path(args.ssd_parent), Path(args.hdd_parent)):
            if output == root or root in output.parents:
                raise RunFailure("output must be outside data parents")
        output.mkdir()  # Never reuse output, including empty directories.
        self.output = output
        self.ssd = create_owned(args.ssd_parent, "native-" + self.run_id, self.run_id)
        self.hdd = create_owned(args.hdd_parent, "native-" + self.run_id, self.run_id)
        self.rows = []
        self.counter = 0
        self.static = {}
        self.stage_deadlines = {}
        self.sst_budget = None
        self.static_verified = set()
        self.manifest = strict_json(Path(args.build_manifest).read_text())
        if self.manifest.get("protocol") != PROTOCOL:
            raise RunFailure("build manifest protocol mismatch")
        if "U" in getattr(args, "groups", GROUPS) and self.manifest.get("upstream_commit") != "abeebd9630f11bd08c28b7bd43c7bdfc62050654":
            raise RunFailure("upstream build is not pinned RocksDB v11.8.1")
        self.binaries = {}
        for group in getattr(args, "groups", GROUPS):
            name = "upstream" if group == "U" else "current"
            entry = self.manifest["binaries"][name]
            binary = Path(entry["path"]).resolve()
            if not binary.is_file() or digest(binary) != entry["sha256"]:
                raise RunFailure("build manifest binary/hash mismatch: " + name)
            self.binaries[group] = binary
        write_json(output / "preflight.json", self.preflight)
        write_json(output / "build.json", self.manifest)
        self.report = {"protocol": PROTOCOL, "profile": args.profile,
                       "run_uuid": self.run_id, "state": "running", "runs": self.rows,
                       "budget_seconds": self.budget_seconds,
                       "time_limit_policy": args.time_limit_policy,
                       "stage_budget_seconds": self.stage_budgets,
                       "stage_budget_override": args.stage_budget_override,
                       "controller_command": [sys.executable, *sys.argv],
                       "data_roots": {"ssd": str(self.ssd), "hdd": str(self.hdd)},
                       "limitations": [], "failure_ledger": [], "gates": [], "blocked_groups": {},
                       "cleanup_policy": "archive metrics/options, then remove successful independent sample data; preserve static datasets and all failures"}
        self.save()

    def save(self):
        remaining = self.deadline.remaining()
        self.report["remaining_seconds"] = remaining if math.isfinite(remaining) else None
        write_json(self.output / "results.json", self.report)

    def paths(self, name, diagnostic=False):
        index_root = self.hdd if diagnostic else self.ssd
        def database_dir(parent, suffix):
            container = create_owned(parent, name + suffix, self.run_id)
            database = container / "db"
            database.mkdir()
            return database
        # Ownership markers belong to containers, never engine-managed roots.
        index = database_dir(index_root, "-index")
        data = database_dir(self.hdd, "-data")
        backup = database_dir(self.hdd, "-backup")
        staging = database_dir(self.ssd, "-staging")
        return {"index": index, "data": data, "backup": backup, "staging": staging,
                "state": self.output / (name + ".state")}

    def check_space(self):
        for name, parent in (("ssd", self.args.ssd_parent), ("hdd", self.args.hdd_parent)):
            usage = shutil.disk_usage(parent)
            if usage.free <= usage.total * .2:
                raise RunFailure(name + " fell below required 20% free reserve; data preserved")

    def stage(self, name):
        if name not in self.stage_deadlines:
            self.stage_deadlines[name] = Deadline(self.stage_budgets[name])
            self.report.setdefault("stage_timing", {})[name] = {"started_monotonic": time.monotonic()}
        self.stage_deadlines[name].require()
        self.deadline.require()

    def remove_data(self, paths):
        for key in ("index", "data", "backup", "staging"):
            target = Path(paths[key]).parent
            parent = self.ssd if target.parent == self.ssd else self.hdd
            cleanup_owned(target, parent, self.run_id)

    def physical_space(self, paths):
        roots = {"ssd": [], "hdd": []}
        for key in ("index", "data", "backup", "staging"):
            path = paths[key]
            roots["ssd" if path.parent.parent == self.ssd else "hdd"].append(path)
        return {name: space_snapshot(items) for name, items in roots.items()}

    def invoke(self, spec, paths, action, fresh=False, timeout=1800, **extra):
        self.check_space()
        stage_name = spec.get("stage", "core")
        self.stage(stage_name)
        if not self.args.no_time_limits:
            timeout = min(timeout, self.stage_deadlines[stage_name].remaining())
        self.counter += 1
        directory = self.output / (f"{self.counter:04d}-" + action)
        return run_driver(self.binaries[spec["group"]], paths, spec, action,
                          directory, self.deadline, self.cpus, self.cgroup,
                          timeout=timeout, fresh=fresh,
                          progress_timeout=self.args.no_time_limits, **extra), directory

    def sample(self, spec, phase="measured"):
        self.deadline.require()
        row = {"spec": dict(spec), "phase": phase, "valid": False, "status": "running"}
        self.rows.append(row)
        self.save()
        if spec["group"] in self.report["blocked_groups"]:
            row.update(status="incomplete", error="configuration blocked by prior functional failure")
            self.save()
            return row
        paths = None
        try:
            if spec.get("static") and spec["group"] in self.static:
                paths = self.static[spec["group"]]
            else:
                name = f"sample-{len(self.rows):04d}-{spec['group']}"
                paths = self.paths(name, spec.get("diagnostic", False))
                if spec["workload"] != "insert" and spec["workload"] != "sync":
                    prepare_spec = {**spec, "warmup": 0, "seconds": 0, "ops": spec["keys"]}
                    prepared, path = self.invoke(prepare_spec, paths, "prepare", fresh=True)
                    row["prepare"] = {"result": str(path / "result.json"), "elapsed_s": prepared["elapsed_s"]}
                if spec.get("static"):
                    self.static[spec["group"]] = paths
            row["paths"] = {key: str(value) for key, value in paths.items()}
            row["space_before"] = self.physical_space(paths)
            row["residency_before"] = sample_residency([paths["index"], paths["data"],
                                                        paths["backup"], paths["staging"]])
            run, path = self.invoke(spec, paths, "run", fresh=spec["workload"] in ("insert", "sync"))
            row["result"] = str(path / "result.json")
            row["driver"] = run
            # Static datasets are fully checked once; every measured Get/scan
            # still validates its returned key/version/checksum in the driver.
            if not spec.get("static") or spec["group"] not in self.static_verified:
                verified, verify_path = self.invoke(spec, paths, "verify")
                row["verification"] = {"result": str(verify_path / "result.json"),
                                       "events": verified["events"]}
                if spec.get("static"):
                    self.static_verified.add(spec["group"])
            else:
                row["verification"] = {"method": "per-operation validation; static full verification already passed"}
            row["residency_after"] = sample_residency([paths["index"], paths["data"],
                                                       paths["backup"], paths["staging"]])
            row["space_after"] = self.physical_space(paths)
            for field in ("residency_before", "residency_after"):
                if row[field]["errors"]:
                    raise RunFailure("cache residency telemetry incomplete")
            for field in ("space_before", "space_after"):
                if any(value["errors"] for value in row[field].values()):
                    raise RunFailure("space telemetry incomplete")
            row["effective_options"] = []
            for original in paths["index"].glob("OPTIONS-*"):
                destination = path / original.name
                shutil.copyfile(original, destination)
                row["effective_options"].append({"path": str(destination), "sha256": digest(destination)})
            if not row["effective_options"]:
                raise RunFailure("effective RocksDB OPTIONS missing")
            isolated = []
            if spec.get("stage") == "specials" and spec["group"] in ("D", "S"):
                isolated = self.restore_gate(spec, paths, row)
            row["coverage"] = mechanism_coverage(spec, run["events"])
            if not row["coverage"]["complete"]:
                raise CoverageFailure("mechanism not covered: " + "; ".join(row["coverage"]["missing"]))
            if spec["workload"] == "sst" and spec.get("sst_mode") == "disabled" and self.sst_budget is None:
                # Disabled mode keeps the current live SST set in the physical
                # index. HDD checkpoint copies are space cost, not capacity input.
                live_index = isolated[0] / "db" if isolated else paths["index"]
                ssts = list(live_index.rglob("*.sst"))
                seen, logical = set(), 0
                for item in ssts:
                    info = item.stat()
                    identity = info.st_dev, info.st_ino
                    if identity not in seen:
                        logical += info.st_size
                        seen.add(identity)
                if len(seen) < 4:
                    raise CoverageFailure("SST special did not create at least four SSTs")
                self.sst_budget = int(logical * .65)
                self.report["sst_capacity_freeze"] = {"disabled_logical_sst_bytes": logical,
                                                       "sst_files": len(seen), "budget_bytes": self.sst_budget}
            row["status"] = "pass"
            row["valid"] = True
            if not spec.get("static"):
                self.remove_data(paths)
                for old in isolated:
                    cleanup_owned(old, self.ssd, self.run_id)
                row["data_cleaned_after_validation"] = True
        except BaseException as error:
            row["status"] = "failed" if isinstance(error, DriverFailure) else "incomplete"
            row["valid"] = False
            if isinstance(error, DriverFailure):
                self.report["blocked_groups"][spec["group"]] = str(error)
            row["error"] = str(error)
            self.report["failure_ledger"].append({"sample": len(self.rows), "error": str(error)})
            raise
        finally:
            self.save()
        return row

    def restore_gate(self, spec, paths, gate, verify_acks=None):
        """Preserve SSD originals by rename; restore using only HDD retained data."""
        isolated = []
        for key in ("index", "staging"):
            original = paths[key].parent
            check_owned(original, self.ssd, self.run_id)
            destination = original.with_name(original.name + ".isolated")
            if destination.exists() or destination.is_symlink():
                raise RunFailure("isolation destination already exists")
            original.rename(destination)
            isolated.append(destination)
            container = create_owned(self.ssd, original.name + ".restored", self.run_id)
            paths[key] = container / "db"
            paths[key].mkdir()
        restore_spec = dict(spec)
        if verify_acks:
            restore_spec["verify_acks"] = str(verify_acks)
        restored, directory = self.invoke(restore_spec, paths, "restore")
        gate["recovery"] = {"result": str(directory / "result.json"), "events": restored["events"]}
        # Restore adopted any newer unacknowledged valid writes into its sidecar.
        appended, directory = self.invoke(spec, paths, "append-reopen")
        gate["append_reopen"] = {"result": str(directory / "result.json"), "events": appended["events"]}
        gate["isolated_originals"] = list(map(str, isolated))
        gate["restore_source"] = "HDD backup/data only; original SSD index/staging renamed"
        return isolated

    def availability(self):
        self.stage("preflight")
        smoke = self.args.profile == "smoke"
        definitions = [("wal-only", 2048, 1024, False, 256 * MIB, 1),
                       ("checkpoint-blob", 8192, 1024, True, 256 * MIB, 4),
                       ("staging-prefix-2m", 8192, 1024, True, 2 * MIB, 4),
                       ("staging-direct-64k", 8192, 1024, True, 65536, 4),
                       ("staging-large-value", 1, 4 * MIB, True, 2 * MIB, 1)]
        for group in ("D", "S"):
            for scenario, keys, value_size, flush, capacity, threads in definitions:
                if group in self.report["blocked_groups"]:
                    continue
                if group == "D" and scenario.startswith("staging"):
                    continue
                spec = {"stage": "preflight", "scenario": scenario, "group": group,
                        "workload": "insert", "keys": keys, "ops": keys,
                        "value_size": value_size, "threads": threads,
                        "flush": flush, "staging_capacity": capacity}
                gate = {"scenario": scenario, "group": group, "status": "running"}
                self.report["gates"].append(gate)
                paths = self.paths("gate-" + str(len(self.report["gates"])) + "-" + group)
                try:
                    result, directory = self.invoke(spec, paths, "run", fresh=True, timeout=300)
                    gate["write"] = {"result": str(directory / "result.json"), "events": result["events"]}
                    stats = last_stats(result["events"])
                    if scenario == "wal-only" and stats.get("flushes") != 0:
                        raise RunFailure("WAL-only checkpoint caused flush or lacks flush counter")
                    if group == "S" and flush:
                        if capacity > 65536 and scenario != "staging-large-value" and \
                                stats.get("peak_staging_bytes", 0) <= 0:
                            raise RunFailure("staging residency was not observed")
                        if scenario in ("staging-prefix-2m", "staging-direct-64k", "staging-large-value") and \
                                stats.get("staging_spill_bytes", 0) <= 0:
                            raise RunFailure("staging spill was not observed")
                    isolated = self.restore_gate(spec, paths, gate)
                    gate["status"] = "pass"
                    self.remove_data(paths)
                    for old in isolated:
                        cleanup_owned(old, self.ssd, self.run_id)
                except BaseException as error:
                    gate["status"] = "failed" if isinstance(error, DriverFailure) else "incomplete"
                    gate["error"] = str(error)
                    if isinstance(error, DriverFailure):
                        self.report["blocked_groups"][group] = str(error)
                        self.save()
                        continue
                    self.save()
                    raise
                self.save()
        # Native routes must survive updates, tombstones, compaction, Close and
        # reopen before either baseline can be used in performance comparisons.
        for group in GROUPS:
            if group in self.report["blocked_groups"]:
                continue
            for workload in (("mixed", "sync", "gc") if group == "B" else ("mixed", "sync")):
                gate = {"scenario": "concurrent-" + workload, "group": group, "status": "running"}
                self.report["gates"].append(gate)
                spec = {"stage": "preflight", "scenario": gate["scenario"], "group": group,
                        "workload": workload, "keys": 128 if workload == "sync" else 2048,
                        "ops": 128 if workload == "sync" else 2048 if workload == "gc" else 8192,
                        "value_size": 1024, "threads": 4, "flush": workload != "sync"}
                if workload == "gc":
                    spec["flush_every"] = 512
                paths = self.paths("native-gate-" + group + "-" + workload)
                try:
                    if workload != "sync":
                        self.invoke(spec, paths, "prepare", fresh=True)
                    run, directory = self.invoke(spec, paths, "run", fresh=workload == "sync")
                    gate["write"] = {"result": str(directory / "result.json"), "events": run["events"]}
                    if group == "B" and workload == "gc" and last_stats(run["events"]).get("native_blob_deletes", 0) <= 0:
                        raise CoverageFailure("B route adapter native obsolete Blob Delete was not observed")
                    isolated = []
                    if group in ("D", "S"):
                        isolated = self.restore_gate(spec, paths, gate)
                    else:
                        result, directory = self.invoke(spec, paths, "verify")
                        gate["verify"] = {"result": str(directory / "result.json"), "events": result["events"]}
                        result, directory = self.invoke(spec, paths, "append-reopen")
                        gate["append_reopen"] = {"result": str(directory / "result.json"), "events": result["events"]}
                    gate["status"] = "pass"
                    self.remove_data(paths)
                    for old in isolated:
                        cleanup_owned(old, self.ssd, self.run_id)
                except (DriverFailure, CoverageFailure) as error:
                    gate.update(status="failed" if isinstance(error, DriverFailure) else "incomplete", error=str(error))
                    self.report["blocked_groups"][group] = str(error)
                    break
                finally:
                    self.save()

    def fault_recovery(self):
        self.stage("recovery")
        targets = [(group, stage) for group in ("D", "S") for stage in ("write", "publish")]
        targets += [("S", "migration")]
        for group, crash_stage in targets:
            if group in self.report["blocked_groups"]:
                continue
            spec = {"stage": "recovery", "scenario": "sigkill-" + crash_stage, "group": group,
                    "workload": "overwrite", "keys": 128, "ops": 128,
                    "value_size": 65536, "threads": 1, "seconds": 300,
                    "staging_capacity": 2 * MIB, "flush_every": 64}
            gate = {"scenario": "sigkill-" + crash_stage, "group": group, "status": "running"}
            self.report["gates"].append(gate)
            paths = self.paths("fault-" + group + "-" + crash_stage)
            try:
                self.invoke(spec, paths, "prepare", fresh=True)
                before = paths["state"].with_suffix(".precrash.state")
                shutil.copyfile(paths["state"], before)
                crash_spec = {**spec, "crash_stage": crash_stage}
                crashed, directory = self.invoke(crash_spec, paths, "crashwrite", crash_after=3, timeout=330)
                gate["crash"] = {"result": str(directory / "result.json"),
                                 "ack_log": str(directory / "ack.txt"), "events": crashed["events"]}
                # The controller's fsynced ACK file is authoritative. A killed
                # process may leave no sidecar or a partially rewritten one.
                shutil.copyfile(before, paths["state"])
                isolated = self.restore_gate(spec, paths, gate, directory / "ack.txt")
                gate["status"] = "pass"
                self.remove_data(paths)
                for old in isolated:
                    cleanup_owned(old, self.ssd, self.run_id)
            except BaseException as error:
                gate["status"] = "failed" if isinstance(error, DriverFailure) else "incomplete"
                gate["error"] = str(error)
                if isinstance(error, DriverFailure):
                    self.report["blocked_groups"][group] = str(error)
                    self.save()
                    continue
                if isinstance(error, CoverageFailure):
                    self.save()
                    continue
                self.save()
                raise
            self.save()
        self.report["limitations"].append(
            "SIGKILL gates prove the first successful write ACK, immediately before backup/LATEST rename, and before the first HDD segment creation after staging prefix. They are not physical power-loss certification.")

    def calibrate(self):
        smoke = self.args.profile == "smoke"
        counts = {"write_keys": 2048 if smoke else 512 * MIB // 1024,
                  "sync_ops": 8 if smoke else 128}
        measurements = []
        for workload in ("insert", "sync"):
            for group in GROUPS:
                if group in self.report["blocked_groups"]:
                    continue
                spec = {"stage": "calibration", "scenario": "calibrate-" + workload,
                        "round": 0, "group": group, "workload": workload,
                        "keys": counts["write_keys"] if workload == "insert" else counts["sync_ops"],
                        "value_size": 1024, "threads": 1}
                try:
                    row = self.sample(spec, "calibration")
                    if row["valid"]:
                        measurements.append(row)
                except DriverFailure:
                    continue
        for workload, field, maximum in (("insert", "write_keys", GIB // 1024),
                                        ("sync", "sync_ops", 512)):
            costs = [lifecycle_seconds(row) for row in measurements
                     if row["spec"]["workload"] == workload]
            if not costs:
                raise RunFailure("all configurations failed calibration")
            slowest = max(costs)
            if slowest <= 0:
                raise RunFailure("calibration missing complete lifecycle")
            if not smoke:
                counts[field] = min(maximum, max(counts[field],
                                                int(counts[field] * 30 / slowest)))
        self.report["calibration"] = {"counts": counts,
            "method": "common count, clamped to approved range; no historical counts",
            "sample_lifecycle_seconds": [lifecycle_seconds(row) for row in measurements]}
        return counts, measurements

    def prepare_static(self, core):
        for group in GROUPS:
            if group in self.report["blocked_groups"]:
                continue
            spec = next(row for row in core if row.get("static") and row["group"] == group)
            paths = self.paths("static-" + group)
            prepare_spec = {**spec, "stage": "calibration", "warmup": 0, "seconds": 0,
                            "ops": spec["keys"]}
            prepared, directory = self.invoke(prepare_spec, paths, "prepare", fresh=True, timeout=3600)
            verified, verify_dir = self.invoke(prepare_spec, paths, "verify", timeout=3600)
            self.static[group] = paths
            self.static_verified.add(group)
            self.report.setdefault("static_datasets", {})[group] = {
                "paths": {key: str(value) for key, value in paths.items()},
                "keys": spec["keys"], "payload_bytes": spec["keys"] * spec["value_size"],
                "prepare": str(directory / "result.json"), "verify": str(verify_dir / "result.json"),
                "elapsed_s": prepared["elapsed_s"] + verified["elapsed_s"],
                "repeated_windows_are_independent_builds": False}
            self.save()

    def execute(self, calibration_only=False):
        try:
            self.availability()
            counts, calibration = self.calibrate()
            core = matrix(self.args.profile, **counts)
            specials = special_matrix(self.args.profile)
            active_core = [row for row in core if row["group"] not in self.report["blocked_groups"]]
            active_specials = [row for row in specials if row["group"] not in self.report["blocked_groups"]]
            forecast = forecast_matrix(active_core, active_specials, calibration,
                                       self.args.forecast_stage_assumptions)
            forecast["informational_only"] = self.args.no_time_limits
            self.report["forecast"] = forecast
            write_json(self.output / "frozen-protocol.json", {
                "protocol": PROTOCOL, "profile": self.args.profile, "counts": counts,
                "core": core, "specials": specials, "stages": self.stage_budgets,
                "stage_budget_override": self.args.stage_budget_override,
                "budget_seconds": self.budget_seconds,
                "time_limit_policy": self.args.time_limit_policy,
                "controller_command": [sys.executable, *sys.argv],
                "forecast": forecast})
            self.save()
            if self.args.profile == "formal" and not self.args.no_time_limits:
                self.deadline.require(forecast["remaining_total_seconds"])
                for stage_name in ("calibration", "core", "specials"):
                    available = self.stage_deadlines[stage_name].remaining() if stage_name in self.stage_deadlines else self.stage_budgets[stage_name]
                    if forecast[stage_name] > available:
                        raise RunFailure("forecast exceeds " + stage_name + " stage budget; frozen matrix retained")
            if calibration_only:
                self.report["state"] = "calibrated"
                return
            self.prepare_static(core)
            for spec in core:
                try:
                    self.sample(spec)
                except DriverFailure:
                    continue
            self.fault_recovery()
            for spec in specials:
                if spec["group"] in self.report["blocked_groups"]:
                    self.sample(spec)
                    continue
                if spec["workload"] == "sst" and spec["sst_mode"] != "disabled":
                    if self.sst_budget is None:
                        self.rows.append({"spec": dict(spec), "phase": "measured", "valid": False,
                                          "status": "incomplete", "error": "disabled live SST budget not established"})
                        self.save()
                        continue
                    spec["sst_budget"] = self.sst_budget
                try:
                    self.sample(spec)
                except (CoverageFailure, DriverFailure):
                    # Retain uncovered data and the explicit incomplete row;
                    # continue independent mechanism evidence if space permits.
                    continue
            # Strict unit regression supplies detailed injected I/O failure and
            # reader-retention cases; require its audit rather than invent proof.
            self.audit_regression()
            from summary import summarize
            self.report["state"] = "complete"
            self.reserve_retests(summarize(self.report)["retest_triggers"])
            if any(not row["valid"] for row in self.rows) or self.report["blocked_groups"] or \
                    any(gate["status"] != "pass" for gate in self.report["gates"]):
                self.report["state"] = "incomplete"
                self.report["limitations"].append("One or more mechanisms or samples remain incomplete; inspect failure_ledger.")
        except BaseException as error:
            self.report["state"] = "incomplete"
            self.report["error"] = str(error)
            self.save()
            raise
        finally:
            self.save()
            from summary import summarize, report_markdown
            summary = summarize(self.report)
            write_json(self.output / "summary.json", summary)
            (self.output / "report.zh-CN.md").write_text(report_markdown(self.report, summary))

    def audit_regression(self):
        if not self.args.regression_audit:
            if self.args.profile == "formal":
                raise RunFailure("formal completion requires strict six-binary regression audit")
            self.report["limitations"].append("smoke omits external six-binary strict regression audit")
            return
        audit = strict_json(Path(self.args.regression_audit).read_text())
        if audit.get("status") != "pass" or audit.get("assert_status_checked") is not True or \
                audit.get("timeout_per_test_seconds") != 60 or audit.get("retry_workers") != [8, 2, 1, 1]:
            raise RunFailure("strict regression audit does not match approved policy")
        required = {"metabypass_test", "sst_tiering_test", "checkpoint_test", "db_blob_basic_test",
                    "db_blob_compaction_test", "db_blob_corruption_test"}
        if set(audit.get("binaries", [])) != required or audit.get("persistent_failures") or audit.get("incomplete_cases"):
            raise RunFailure("strict regression audit lacks complete six-binary passing case set")
        self.report["regression_audit"] = {"path": str(self.args.regression_audit),
                                            "sha256": digest(self.args.regression_audit), "audit": audit}

    def execute_gates(self):
        try:
            self.availability()
            self.fault_recovery()
            self.audit_regression()
            self.report["state"] = "failed" if self.report["blocked_groups"] else \
                "incomplete" if any(gate["status"] != "pass" for gate in self.report["gates"]) else "complete"
            self.report["gates_only"] = True
        except BaseException as error:
            self.report.update(state="incomplete", error=str(error))
            raise
        finally:
            self.save()
            from summary import summarize, report_markdown
            summary = summarize(self.report)
            write_json(self.output / "summary.json", summary)
            (self.output / "report.zh-CN.md").write_text(report_markdown(self.report, summary))

    def reserve_retests(self, triggers):
        self.report["retest_triggers"] = triggers
        if not triggers:
            return
        self.stage("reserve")
        seen = set()
        for trigger in triggers:
            scenario = trigger["scenario"]
            needed = (trigger["candidate"], trigger["reference"])
            for group in needed:
                key = scenario, group
                if key in seen:
                    continue
                originals = [row for row in self.rows if row["phase"] == "measured" and
                             row["spec"]["scenario"] == scenario and row["spec"]["group"] == group]
                if not originals:
                    continue
                spec = dict(originals[0]["spec"])
                spec["stage"], spec["round"] = "reserve", 1
                # Retests never replace or add a fourth successful original round.
                self.sample(spec, "retest")
                seen.add(key)


def capture_elapsed(path):
    return strict_json(Path(path).read_text())["elapsed_s"]


def forecast_matrix(core, specials, calibration, stage_budgets=None):
    """Byte-scaled insert/verification calibration; sync costs remain separate."""
    rates = {}
    stage_budgets = STAGES if stage_budgets is None else validate_stage_budgets(stage_budgets)
    sync = {}
    for group in sorted({row["group"] for row in core}):
        insert = next(row for row in calibration if row["spec"]["group"] == group and
                      row["spec"]["workload"] == "insert")
        payload = insert["spec"]["keys"] * insert["spec"]["value_size"]
        rates[group] = {"write_s_per_byte": insert["driver"]["elapsed_s"] / payload,
                        "verify_s_per_byte": capture_elapsed(insert["verification"]["result"]) / payload}
        sample = next(row for row in calibration if row["spec"]["group"] == group and
                      row["spec"]["workload"] == "sync")
        sync[group] = (sample["driver"]["elapsed_s"] + capture_elapsed(sample["verification"]["result"])) / sample["spec"]["keys"]
    stages = {"calibration": 0, "core": 0, "specials": 0,
              "recovery": stage_budgets["recovery"], "reserve": stage_budgets["reserve"]}
    for group in rates:
        spec = next(row for row in core if row["group"] == group and row.get("static"))
        rate = rates[group]
        stages["calibration"] += spec["keys"] * spec["value_size"] * sum(rate.values())
    for row in core + specials:
        rate = rates[row["group"]]
        payload = row["keys"] * row["value_size"]
        write = payload * rate["write_s_per_byte"]
        verify = payload * rate["verify_s_per_byte"]
        if row.get("static"):
            cost = row["warmup"] + row["seconds"] + 5
        elif row["workload"] == "sync":
            cost = row["ops"] * sync[row["group"]]
        elif row["workload"] == "insert":
            cost = write + verify
        elif row["workload"] == "overwrite":
            cost = write + 1.5 * write + verify
        elif row["workload"] in ("mixed", "sst"):
            cost = write + row.get("warmup", 0) + row.get("seconds", 0) + verify + 5
        elif row["workload"] == "gc":
            cost = write + 3 * 1.5 * write + verify + row.get("gc_wait_seconds", 0)
        else:
            raise RunFailure("unknown workload forecast")
        if row["stage"] == "specials" and row["group"] in ("D", "S"):
            cost += 2 * verify + 5  # Restore/full verify, append/reopen/full verify.
        stages[row["stage"]] += cost
    # Include process launch/state serialization and uncertainty in per-stage cost.
    for name in ("calibration", "core", "specials"):
        stages[name] = stages[name] * 1.2 + 30
    return {**stages, "remaining_total_seconds": sum(stages.values()),
            "calibration_rates": rates, "sync_seconds_per_operation": sync,
            "uncertainty_margin": .2,
            "method": "per-group complete insert and full verification per byte; sync separately per op; overwrite/GC 1.5x mutation estimate; static seed once per group; each D/S special adds 2*verify+5s for HDD Restore and append/reopen; 20% margin and fixed recovery/reserve"}


def lifecycle_seconds(row):
    phases = {event["name"]: event["elapsed_us"] for event in row["driver"]["events"]
              if event.get("event") == "phase"}
    total = phases.get("total", phases.get("lifecycle"))
    if total is None:
        required = ("foreground", "flush", "sync_backup", "close")
        if not all(key in phases for key in required):
            raise RunFailure("driver missing complete lifecycle phases")
        total = sum(phases[key] for key in required)
    return total / 1e6


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "_exec":
        child_exec(argv[1:])
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "plan", "gates", "calibrate", "run"))
    parser.add_argument("--profile", choices=("formal", "smoke"), default="formal")
    parser.add_argument("--ssd-parent", default="/home/lj/ssd/metabypass_test")
    parser.add_argument("--hdd-parent", default="/home/lj/hdd/metabypass_test")
    parser.add_argument("--cgroup", default=str(CGROUP))
    parser.add_argument("--cpus", default=None)
    parser.add_argument("--output")
    parser.add_argument("--build-manifest")
    parser.add_argument("--regression-audit")
    parser.add_argument("--budget-seconds", type=float, default=28800)
    parser.add_argument("--no-time-limits", action="store_true",
                        help="disable total/stage caps; retain per-driver inactivity detection")
    parser.add_argument("--stage-budgets-json", type=Path,
                        help="explicit six-stage budget file; default policy is unchanged")
    args = parser.parse_args(argv)
    try:
        args.stage_budgets, args.stage_budget_override = stage_budget_configuration(args.stage_budgets_json)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    args.forecast_stage_assumptions = dict(args.stage_budgets)
    args.time_limit_policy = {"mode": "unlimited" if args.no_time_limits else "bounded",
                              "explicit_no_time_limits": args.no_time_limits,
                              "per_driver_timeout_mode": "inactivity" if args.no_time_limits else "elapsed"}
    if args.no_time_limits:
        args.stage_budgets = {name: None for name in STAGES}
        if args.stage_budget_override is not None:
            args.stage_budget_override["applied_as_caps"] = False
    if args.command == "preflight":
        result = preflight(args)
        print(json.dumps(result, indent=2))
        return 0 if result["ready"] else 2
    if args.command == "plan":
        print(json.dumps({"protocol": PROTOCOL, "profile": args.profile,
                          "stage_budget_seconds": args.stage_budgets,
                          "stage_budget_override": args.stage_budget_override,
                          "budget_seconds": None if args.no_time_limits else min(args.budget_seconds, 28800),
                          "time_limit_policy": args.time_limit_policy,
                          "core": matrix(args.profile),
                          "specials": special_matrix(args.profile),
                          "status": "provisional until same-session calibration freezes counts"}, indent=2))
        return 0
    if not args.output or not args.build_manifest:
        parser.error("run requires --output and --build-manifest")
    try:
        experiment = Experiment(args)
        if args.command == "gates":
            experiment.execute_gates()
        else:
            experiment.execute(calibration_only=args.command == "calibrate")
    except (RunFailure, OSError, ValueError, KeyboardInterrupt) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
