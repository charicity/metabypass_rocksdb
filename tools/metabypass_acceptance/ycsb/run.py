#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

"""Run and pair one YCSB-cpp sample at a time."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import time


PROTOCOL = "metabypass-ycsb-cpp-v1"
GIB = 1 << 30
SST_ASYNC_DEFAULTS = {"sst_async_min_level": -1, "sst_async_delay_ms": 1000}
SST_ASYNC_ENV = {
    "sst_async_min_level": "MB_YCSB_SST_ASYNC_MIN_LEVEL",
    "sst_async_delay_ms": "MB_YCSB_SST_ASYNC_DELAY_MS",
}
SHARED_OPTIONS = (
    "threads", "warmup_seconds", "measure_seconds", "value_bytes", "cache_bytes",
    "write_buffer_bytes", "max_write_buffer_number", "max_background_jobs",
    "wal", "sync", "compression", "blob", "staging", "sst_tiering",
)
DRIVER_ID = ("ycsb_commit", "ycsb_patch_sha256", "adapter_sha256")
COMPARISON_KEYS = set(SHARED_OPTIONS + DRIVER_ID + ("workload", "memory_gib"))


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def one(events, name):
    matches = [event for event in events if event["event"] == name]
    if len(matches) != 1:
        raise ValueError(f"expected one {name} event, observed {len(matches)}")
    return matches[0]


def valid_initial(initial):
    hexdigits = set("0123456789abcdef")
    return (
        initial.get("initial_verified") is True
        and initial.get("initial_records", 0) > 0
        and initial.get("initial_sha256") == initial.get("dataset_logical_sha256")
        and all(
            isinstance(initial.get(key), str)
            and len(initial[key]) == 64
            and not (set(initial[key]) - hexdigits)
            for key in ("dataset_sha256", "initial_sha256")
        )
    )


def compare(left, right):
    result = dict(left=left.get("sample"), right=right.get("sample"),
                  throughput_ratio=None, throughput_change_pct=None)
    if any(row.get("protocol") != PROTOCOL or row.get("status") != "passed"
           for row in (left, right)):
        return dict(result, reason="missing or unsuccessful YCSB-cpp sample")
    a, b = left.get("initial", {}), right.get("initial", {})
    if not valid_initial(a) or not valid_initial(b):
        return dict(result, reason="initial database verification missing")
    if any(a[key] != b[key] for key in
           ("dataset_sha256", "initial_sha256", "initial_records")):
        return dict(result, reason="initial datasets differ")
    config = left.get("comparison", {})
    if not COMPARISON_KEYS <= config.keys() or config != right.get("comparison"):
        return dict(result, reason="workload or measurement configuration differs")
    if not left.get("ops_per_second") or not right.get("ops_per_second"):
        return dict(result, reason="missing successful throughput")
    ratio = left["ops_per_second"] / right["ops_per_second"]
    return dict(result, reason=None, throughput_ratio=ratio,
                throughput_change_pct=100 * (ratio - 1))


class Resources:
    """Observe the existing node-ssd v1 memory group when requested."""

    def __init__(self, args, front, back):
        self.group = args.cgroup.resolve(strict=True) if args.cgroup else None
        self.limit = args.memory_gib * GIB
        self.front, self.back = front, back
        self.observe()

    def pid_file(self):
        return self.group / "cgroup.procs"

    def enter(self):
        self.pid_file().write_text(str(os.getpid()))

    def observe(self, pid=None):
        if self.group is None:
            return None
        pids = set(map(int, self.pid_file().read_text().split()))
        if pids - ({pid} if pid else set()):
            raise RuntimeError("memory cgroup has a foreign process")
        def number(name):
            return int((self.group / name).read_text())
        if number("memory.limit_in_bytes") != self.limit or number("memory.swappiness"):
            raise RuntimeError("memory limit or swappiness differs")
        status = Path(f"/proc/{pid}/status") if pid else None
        swap_kib = None
        if status:
            try:
                for line in status.read_text().splitlines():
                    if line.startswith("VmSwap:"):
                        swap_kib = int(line.split()[1])
                        break
            except FileNotFoundError:
                pass
        if swap_kib:
            raise RuntimeError("database process used swap")
        if any(shutil.disk_usage(path).free < min_free * GIB for path, min_free
               in ((self.front, 20), (self.back, 100))):
            raise RuntimeError("SSD/HDD free space below 20/100 GiB")
        return dict(monotonic_ns=time.monotonic_ns(), pid=pid,
                    members=sorted(pids), swap_kib=swap_kib, memory_limit=self.limit,
                    memory_usage=number("memory.usage_in_bytes"),
                    memory_failcnt=number("memory.failcnt"))


def run_stage(command, logs, timeout, resources, crash=False, settings=None):
    logs.mkdir()
    save(logs / "command.json", command)
    overrides = {SST_ASYNC_ENV[key]: str(value) for key, value in (settings or {}).items()}
    if overrides:
        save(logs / "environment.json", overrides)
    environment = {**os.environ, **overrides}
    initial_resources = resources.observe()
    events, samples, pending = [], [], b""
    fault = None
    stop = time.monotonic() + timeout
    next_sample = time.monotonic()
    wait = None
    with (logs / "stdout.jsonl").open("wb") as out, (logs / "stderr.log").open("wb") as err:
        child = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=err, start_new_session=True,
            env=environment, preexec_fn=resources.enter if resources.group else None,
        )
        with child.stdout, selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            try:
                while selector.get_map() or wait is None:
                    if time.monotonic() >= stop:
                        raise TimeoutError(f"{logs.name} timed out")
                    for key, _ in selector.select(0.1):
                        block = os.read(key.fd, 65536)
                        if not block:
                            selector.unregister(child.stdout)
                            continue
                        pending += block
                        while b"\n" in pending:
                            line, pending = pending.split(b"\n", 1)
                            event = json.loads(line)
                            events.append(event)
                            if crash and event["event"] == "measurement_end" and fault is None:
                                fault = dict(monotonic_ns=time.monotonic_ns(),
                                             wall_us=time.time_ns() // 1000)
                                os.killpg(child.pid, signal.SIGKILL)
                        out.write(block)
                    if resources.group and wait is None and time.monotonic() >= next_sample:
                        samples.append(resources.observe(child.pid))
                        next_sample = time.monotonic() + 0.1
                    if wait is None:
                        pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                        if pid:
                            wait = (status, usage)
                # A final JSONL record can be torn by SIGKILL.
                if pending and fault is None:
                    raise ValueError("incomplete driver event")
            finally:
                if wait is None:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    _, status, usage = os.wait4(child.pid, 0)
                    wait = (status, usage)
                status, usage = wait
                child.returncode = (-os.WTERMSIG(status) if os.WIFSIGNALED(status)
                                    else os.WEXITSTATUS(status))
                save(logs / "process.json", dict(
                    returncode=child.returncode, fault=fault,
                    user_seconds=usage.ru_utime, system_seconds=usage.ru_stime,
                    max_rss_kib=usage.ru_maxrss, resources_before=initial_resources,
                    resource_samples=samples,
                ))
    resources.observe()
    code = -signal.SIGKILL if crash else 0
    if child.returncode != code or (crash and fault is None):
        raise RuntimeError(f"{logs.name} exited {child.returncode}, expected {code}")
    return events, fault


def sequence_age(events, sequence, fault):
    # The sample after the recovered sequence bounds how recently the
    # recovered prefix could have ended. Missing sides stay unknown.
    covered, uncovered = [], []
    for event in events:
        if event["event"] == "progress":
            if event["query_end_ns"] > fault["monotonic_ns"]:
                continue
            if event["sequence"] <= sequence:
                covered.append(event["query_begin_ns"])
            else:
                uncovered.append(event["query_end_ns"])
    now = fault["monotonic_ns"]
    return dict(lower_ns=now - min(uncovered) if uncovered else None,
                upper_ns=now - max(covered) if covered else None)


def linkage(driver, identity):
    expected = identity["core_library_sha256"]
    dependencies = subprocess.check_output(["ldd", driver], text=True)
    actual = next((line.split("=>", 1)[1].rsplit(" (", 1)[0].strip()
                   for line in dependencies.splitlines()
                   if line.lstrip().startswith("librocksdb") and "=>" in line), None)
    if actual is None:
        if expected:
            raise ValueError("expected a linked RocksDB shared library")
        return None
    library = Path(actual).resolve(strict=True)
    sha = hashlib.sha256(library.read_bytes()).hexdigest()
    if sha != expected:
        raise ValueError("loaded RocksDB shared library differs from the build")
    return dict(path=str(library), sha256=sha)


def sample(args):
    ssd, hdd = (p.resolve(strict=True) for p in (args.ssd_root, args.hdd_root))
    if ssd == hdd or ssd in hdd.parents or hdd in ssd.parents:
        raise ValueError("SSD and HDD roots must be disjoint")
    if Path(args.sample).name != args.sample or args.sample in ("", ".", ".."):
        raise ValueError("sample must name one new directory")
    if args.recover and args.engine == "native":
        raise ValueError("native samples have no surviving backup")
    settings = {key: getattr(args, key) for key in SST_ASYNC_DEFAULTS}
    if args.engine != "mb" and settings != SST_ASYNC_DEFAULTS:
        raise ValueError("nondefault SST async settings require --engine mb")
    front, back = ssd / args.sample, hdd / args.sample
    if front.exists() or back.exists():
        raise ValueError("sample already exists")
    driver = str(args.driver.resolve(strict=True))
    dataset = str(args.dataset.resolve(strict=True))
    workload = str(args.workload.resolve(strict=True))
    resources = Resources(args, ssd, hdd)
    front.mkdir()
    back.mkdir()
    db, original, target = front / "db", front / "isolated", front / "restored"
    backup = str(back / "backup") if args.engine != "native" else "-"
    result = dict(protocol=PROTOCOL, status="running", sample=args.sample,
                  engine=args.engine, ops_per_second=None, recovery=None,
                  dataset_path=dataset, properties_path=workload, fault=None)
    ledger = back / "result.json"
    save(ledger, result)
    try:
        identity, _ = run_stage([driver, "identity"], back / "identity", args.timeout, resources)
        result["identity"] = one(identity, "identity")
        result["runtime_rocksdb"] = linkage(driver, result["identity"])
        result["driver_sha256"] = hashlib.sha256(Path(driver).read_bytes()).hexdigest()
        result["properties_sha256"] = hashlib.sha256(Path(workload).read_bytes()).hexdigest()
        command = [driver, "run", dataset, workload, args.engine, str(db), backup,
                   str(args.threads), str(args.warmup_seconds), str(args.measure_seconds),
                   str(args.checkpoint_ms if args.engine != "native" else 0)]
        events, fault = run_stage(command, back / "writer", args.timeout, resources,
                                  crash=True, settings=settings)
        initial = one(events, "ready")
        result.update(initial=initial, fault=fault)
        if not valid_initial(initial):
            raise ValueError("driver did not verify initial contents")
        expected = dict(engine=args.engine, threads=args.threads,
                        warmup_seconds=args.warmup_seconds, measure_seconds=args.measure_seconds,
                        checkpoint_ms=args.checkpoint_ms if args.engine != "native" else 0)
        if any(initial.get(key) != value for key, value in expected.items()):
            raise ValueError("writer configuration differs from the requested sample")
        if args.engine == "mb" and any(
            initial.get(key, SST_ASYNC_DEFAULTS[key]) != value for key, value in settings.items()
        ):
            raise ValueError("writer SST async settings differ from the requested sample")
        start = one(events, "measurement_start")
        end = one(events, "measurement_end")
        if (end["elapsed_ns"] != args.measure_seconds * 10**9
                or end["monotonic_ns"] - start["monotonic_ns"] != end["elapsed_ns"]
                or end["monotonic_ns"] > fault["monotonic_ns"]
                or end["failed"] or not end["succeeded"] or end["attempted"] != end["succeeded"]
                or any(event["event"] == "error" for event in events)):
            raise ValueError("workload window failed or has no successful operations")
        result["measurement"] = end
        if "stats_sample_monotonic_ns" in start:
            result["measurement_start"] = start
        result["ops_per_second"] = end["succeeded"] * 1e9 / end["elapsed_ns"]
        result["comparison"] = dict(
            **{key: result["identity"][key] for key in DRIVER_ID},
            workload=initial["effective_workload"],
            memory_gib=args.memory_gib if args.cgroup else None,
            **{key: initial[key] for key in SHARED_OPTIONS},
        )
        db.rename(original)
        if (original / "LOG").exists():
            shutil.copyfile(original / "LOG", back / "writer" / "LOG")
        save(ledger, result)
        if args.recover:
            command = [driver, "recover", dataset, workload, args.engine, str(target),
                       backup, str(initial["initial_sequence"]), str(args.sample_count)]
            recovered, _ = run_stage(command, back / "recovery", args.timeout, resources,
                                     settings=SST_ASYNC_DEFAULTS)
            rec = one(recovered, "recovery")
            result["recovery"] = rec
            if (rec["recovered_sequence"] < initial["initial_sequence"]
                    or rec["sampled"] != min(initial["initial_records"], args.sample_count)
                    or rec["restore_start_ns"] < fault["monotonic_ns"]):
                raise ValueError("invalid recovery boundary or sample count")
            result["sequence_age_ns"] = sequence_age(events, rec["recovered_sequence"], fault)
            result["fault_to_sample_get_ns"] = (
                rec["restore_start_ns"] - fault["monotonic_ns"]
                + rec["restore_ns"] + rec["open_ns"] + rec["first_get_ns"]
            )
    except BaseException as error:
        result.update(status="failed", error=str(error))
        save(ledger, result)
        raise
    result["status"] = "passed"
    save(ledger, result)
    if not args.keep_data:
        shutil.rmtree(front)
        for name in ("backup", "data"):
            path = back / name
            if path.exists():
                shutil.rmtree(path)
    print(ledger)


def positive(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def sst_async_min_level(value):
    number = int(value)
    if number != -1 and not 1 <= number < 2**31:
        raise argparse.ArgumentTypeError("expected -1 or a positive int32 level")
    return number


def sst_async_delay_ms(value):
    number = int(value)
    if not 0 <= number < 2**64:
        raise argparse.ArgumentTypeError("expected a nonnegative uint64 delay")
    return number


def add_sst_async_arguments(parser):
    parser.add_argument("--sst-async-min-level", type=sst_async_min_level,
                        default=SST_ASYNC_DEFAULTS["sst_async_min_level"],
                        help="minimum compaction output level to stage; -1 disables staging")
    parser.add_argument("--sst-async-delay-ms", type=sst_async_delay_ms,
                        default=SST_ASYNC_DEFAULTS["sst_async_delay_ms"],
                        help="ordinary staging delay in milliseconds")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    for name in ("driver", "dataset", "workload", "ssd-root", "hdd-root"):
        run.add_argument("--" + name, type=Path, required=True)
    run.add_argument("--sample", required=True)
    run.add_argument("--engine", choices=("mb", "be", "native"), required=True)
    run.add_argument("--threads", type=positive, default=1)
    run.add_argument("--warmup-seconds", type=positive, default=65)
    run.add_argument("--measure-seconds", type=positive, default=180)
    run.add_argument("--checkpoint-ms", type=positive, default=40000)
    add_sst_async_arguments(run)
    run.add_argument("--sample-count", type=positive, default=4096)
    run.add_argument("--timeout", type=positive, default=1800)
    run.add_argument("--cgroup", type=Path)
    run.add_argument("--memory-gib", type=positive, default=32)
    run.add_argument("--recover", action="store_true")
    run.add_argument("--keep-data", action="store_true")
    pair = sub.add_parser("pair")
    pair.add_argument("left", type=Path)
    pair.add_argument("right", type=Path)
    args = parser.parse_args()
    if args.command == "pair":
        print(json.dumps(compare(json.loads(args.left.read_text()),
                                 json.loads(args.right.read_text())), indent=2))
    else:
        sample(args)


if __name__ == "__main__":
    main()
