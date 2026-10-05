#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Ten-run read-only-workload diagnostic using an existing immutable seed.

Clone and benchmark children join the private 48 MiB cgroup; the controller
and samplers remain outside. No seed rebuild, retry, cleanup or system tuning.
Optional syscall tracing belongs in a separate diagnostic run: the inherited
per-run .process.json files expose owned benchmark PID/starttime identities.
"""

import argparse
import hashlib
import json
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

import node_cache
import node_run


ARMS = {"disabled_ssd": "disabled", "observe_ssd": "observe",
        "adaptive_cold": "adaptive", "observe_cold": "observe"}
MIGRATION_COUNTERS = ("promotions", "demotions", "promoted_bytes", "demoted_bytes", "copied_bytes")
FORMAL_DURATIONS = {"warmup": 30, "measured": 30, "saturated": 15}
PRESET_DURATIONS = {"warmup": 30, "measured": 15, "saturated": 0}


def arm_order(repeat):
    if repeat not in (1, 2):
        raise ValueError("exactly two independent repeats are supported")
    names = list(ARMS)
    return names if repeat == 1 else list(reversed(names))


def placement_snapshot(dirs):
    """Closed-DB placement and allocation evidence; never read blob payloads."""
    ssd, hdd = map(Path, dirs)
    index = ssd / "index"
    local = node_cache.local_tables(index)
    path = index / "SST-PLACEMENT"
    payload = path.read_bytes() if path.exists() else None
    entries = node_cache.parse_placement(payload, (index / "IDENTITY").read_text()) \
        if payload is not None else {}
    for number, entry in entries.items():
        target = local.get(number) if entry["hot"] else hdd / "backup" / "sst-store" / entry["object"]
        if target is None or target.is_symlink():
            raise ValueError("missing or mismatched routed SST: " + str(number))
        try:
            info = target.stat()
        except OSError as error:
            raise ValueError("unreadable routed SST: " + str(number)) from error
        if not stat.S_ISREG(info.st_mode) or info.st_size != entry["size"]:
            raise ValueError("missing or mismatched routed SST: " + str(number))
    inodes = {}
    tables = {}
    for number, path in sorted(local.items()):
        info = path.stat()
        inodes[info.st_dev, info.st_ino] = info.st_blocks * 512
        tables[str(number)] = {"size": info.st_size, "allocated_bytes": info.st_blocks * 512}
    return {"placement_sha256": hashlib.sha256(payload).hexdigest() if payload is not None else None,
            "identity_sha256": node_run.digest(index / "IDENTITY"),
            "entries": {str(k): v for k, v in sorted(entries.items())},
            "hot_tables": sorted(n for n, e in entries.items() if e["hot"]),
            "cold_tables": sorted(n for n, e in entries.items() if not e["hot"]),
            "local_tables": tables, "ssd_sst_allocated_bytes": sum(inodes.values()),
            "local_sst_logical_bytes": sum(t["size"] for t in tables.values())}


def require_same_start(expected, actual):
    for key in ("placement_sha256", "identity_sha256", "entries", "hot_tables",
                "cold_tables", "local_tables", "ssd_sst_allocated_bytes"):
        if expected[key] != actual[key]:
            raise ValueError("initial dataset differs from common snapshot: " + key)


def phase_deltas(events):
    """Only cumulative phase_end snapshots define migration delta boundaries.

    ready carries no counters. Its first delta is intentionally unknown rather
    than subtracting the first window and losing startup/warmup migrations.
    """
    result, previous = {}, None
    for phase in FORMAL_DURATIONS:
        ends = [e for e in events if e.get("event") == "phase_end" and e.get("phase") == phase]
        if len(ends) != 1:
            raise ValueError("missing or duplicate phase_end: " + phase)
        event = ends[0]
        counters = event.get("stats", {})
        if any(type(counters.get(k)) is not int or counters[k] < 0 for k in MIGRATION_COUNTERS):
            raise ValueError("missing or invalid migration counters: " + phase)
        delta = {k: counters[k] - previous[k] for k in MIGRATION_COUNTERS} if previous else None
        if delta is not None and any(value < 0 for value in delta.values()):
            raise ValueError("migration counters decreased: " + phase)
        result[phase] = {"end_counters": {k: counters[k] for k in MIGRATION_COUNTERS},
                         "delta": delta, "boundary": "previous phase_end" if previous else "startup unknown",
                         "start_monotonic_us": event["steady_time_us"] - event["elapsed_us"],
                         "end_monotonic_us": event["steady_time_us"]}
        previous = counters
    return result


def require_static(events, before, after):
    deltas = phase_deltas(events)
    if any(value != 0 for phase in deltas.values() for value in phase["end_counters"].values()):
        raise ValueError("static observe arm performed a migration")
    require_same_start(before, after)
    return deltas


def dynamic_coverage(deltas):
    phases = {}
    for phase in ("measured", "saturated"):
        delta = deltas[phase]["delta"]
        migrations = delta["promotions"] + delta["demotions"]
        phases[phase] = {"covered": migrations > 0, "actual_migrations": migrations,
                         "actual_copied_bytes": delta["copied_bytes"]}
    return {"covered": all(p["covered"] for p in phases.values()), "phases": phases,
            "basis": "each measured phase needs actual promotions or demotions"}


def require_all_ssd_observe(events):
    deltas = phase_deltas(events)
    if any(phase["end_counters"][key] != 0 for phase in deltas.values()
           for key in ("promotions", "demotions")):
        raise ValueError("all-SSD observe arm performed a migration")
    # Initial protection copies may occur before measurement when an
    # unlayered seed is opened with observe enabled on separate devices.
    if any(deltas[phase]["delta"]["copied_bytes"] != 0 for phase in ("measured", "saturated")):
        raise ValueError("all-SSD observe arm copied SST bytes during measurement")
    return deltas


def phase_io(samples_path, boundaries):
    samples = [json.loads(line) for line in Path(samples_path).read_text().splitlines()]
    result = {}
    for phase, boundary in boundaries.items():
        first, last = boundary["start_monotonic_us"], boundary["end_monotonic_us"]
        selected = [s for s in samples if first <= s.get("monotonic_s", 0) * 1e6 <= last
                    and s.get("proc_io") and s.get("device_io")]
        result[phase] = {"sample_count": len(selected), "boundary": boundary,
                         "measurement": "interior one-second samples, excludes unsampled boundary tails"}
        if len(selected) < 2:
            result[phase]["status"] = "insufficient_samples"
            continue
        begin, end = selected[0], selected[-1]
        elapsed = end["monotonic_s"] - begin["monotonic_s"]
        proc = {k: end["proc_io"][k] - value for k, value in begin["proc_io"].items()
                if k in end["proc_io"]}
        devices = {}
        for device, initial in begin["device_io"].items():
            if device not in end["device_io"]:
                continue
            counters = [b - a for a, b in zip(initial["counters"], end["device_io"][device]["counters"])]
            devices[device] = {"counter_deltas": counters, "read_bytes": counters[2] * 512,
                               "write_bytes": counters[6] * 512, "busy_fraction": counters[9] / (elapsed * 1000)}
        result[phase].update(status="known", elapsed_s=elapsed, proc_io_delta=proc,
                             device_io_delta=devices, first_sample=begin, last_sample=end)
    return result


class DiagnosticRunner(node_run.Runner):
    def __init__(self, args):
        super().__init__(args)
        self.keys, self.rate = 1_000_000, 112
        self.report.update(protocol="metabypass-sst-diagnostic-v1", status="preflight",
                           presets=[], paired_start_checks=[], keep_data=True,
                           diagnostic_plan={"repeats": 2, "db_runs": 10, "arms": ARMS,
                                            "formal_durations": FORMAL_DURATIONS,
                                            "preset_durations": PRESET_DURATIONS,
                                            "seed_rebuild": False, "retries": False},
                           source_manifest_sha256_start=node_run.digest(args.source_manifest))
        self.report["limits"]["hard_deadline_s"] = args.deadline_seconds
        sources = [Path(__file__).resolve(), Path(node_run.__file__).resolve(),
                   Path(node_cache.__file__).resolve()]
        head = subprocess.run(["git", "-C", str(sources[0].parent), "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=10)
        self.report["driver_identity"] = {"source_sha256": {str(p): node_run.digest(p) for p in sources},
                                          "head": head.stdout.strip() if head.returncode == 0 else args.driver_source_head,
                                          "head_source": "checkout" if head.returncode == 0 else "provided",
                                          "head_probe_error": head.stderr.strip() if head.returncode else None}

    def check_idle(self):
        if (Path(self.args.cgroup) / "tasks").read_text().split():
            raise RuntimeError("private cgroup gained an unexpected task")

    def copy_round(self, source, name):
        self.check_idle()
        started = time.monotonic()
        dirs = self.directories(name)
        command = [sys.executable, str(Path(node_run.__file__).resolve()), "--clone",
                   *map(str, (*source, *dirs))]
        prep = self.execute(name + "-copy", command, dirs, timeout=480)
        if not prep["valid"] or prep.get("oom_kill_delta", 0) > 0:
            raise RuntimeError("clone failed/OOM; evidence retained: " + name)
        return dirs, prep, started

    def benchmark_round(self, name, dirs, started, mode, durations):
        self.check_idle()
        remaining = min(480 - (time.monotonic() - started), self.deadline.remaining())
        if remaining <= sum(durations.values()):
            raise RuntimeError("round/deadline leaves insufficient fixed phase time: " + name)
        row = self.execute(name, self.command("run", dirs, mode=mode, workload=self.args.workload,
            budget=self.budget, rate=self.rate, warmup=durations["warmup"],
            measured=durations["measured"], saturated=durations["saturated"]), dirs, timeout=remaining)
        row.update(mode=mode, workload=self.args.workload, budget_bytes=self.budget,
                   round_elapsed_s=time.monotonic() - started,
                   phase_validation_errors=node_run.phase_validation(row, durations))
        row["valid"] = row["valid"] and not row["phase_validation_errors"] and \
            row.get("oom_kill_delta", 0) == 0 and row["round_elapsed_s"] <= 480
        if not row["valid"]:
            self.save()
            raise RuntimeError("benchmark failed/OOM/timeout; evidence retained: " + name)
        return row

    def run(self):
        self.preflight()
        seed = (Path(self.args.seed_ssd).resolve(), Path(self.args.seed_hdd).resolve())
        baseline = placement_snapshot(seed)
        if baseline["placement_sha256"] is not None or len(baseline["local_tables"]) < 4:
            raise ValueError("existing seed must be unlayered, without SST-PLACEMENT, and have at least four SSTs")
        self.budget = baseline["local_sst_logical_bytes"] // 2
        self.report.update(seed={"paths": list(map(str, seed)), "initial": baseline},
                           keys=self.keys, frozen_budget_bytes=self.budget,
                           frozen_target_ops_per_sec=self.rate, status="running")
        self.save()
        for repeat in (1, 2):
            name = f"repeat-{repeat}-preset"
            preset, prep, started = self.copy_round(seed, name)
            require_same_start(baseline, placement_snapshot(preset))
            row = self.benchmark_round(name, preset, started, "adaptive", PRESET_DURATIONS)
            row.update(repeat=repeat, preparation_s=prep["elapsed_s"])
            self.report["presets"].append(row)
            settled = placement_snapshot(preset)
            row["closed_snapshot"] = settled
            counters = phase_deltas(row["events"])["saturated"]["end_counters"]
            phase_ends = [e for e in row["events"] if e.get("event") == "phase_end"]
            final_stats = phase_ends[-1].get("stats", {})
            if not settled["cold_tables"] or not settled["hot_tables"] or \
                    counters["promotions"] + counters["demotions"] <= 0 or \
                    final_stats.get("unprotected_bytes") != 0 or final_stats.get("migration_errors") != 0:
                self.save()
                raise RuntimeError("preset lacks migration, protected mixed placement, or has migration errors")
            hot_bytes = sum(e["size"] for e in settled["entries"].values() if e["hot"])
            row["preset_budget_check"] = {"hot_logical_bytes": hot_bytes, "budget_bytes": self.budget,
                                          "passed": 0 < hot_bytes <= self.budget}
            if not row["preset_budget_check"]["passed"]:
                self.save()
                raise RuntimeError("closed preset hot SST bytes exceed 50 percent budget")
            starts = {}
            for arm in arm_order(repeat):
                name = f"repeat-{repeat}-{arm}"
                source, expected = (preset, settled) if arm.endswith("cold") else (seed, baseline)
                dirs, prep, started = self.copy_round(source, name)
                before = placement_snapshot(dirs)
                require_same_start(expected, before)
                starts[arm] = before
                row = self.benchmark_round(name, dirs, started, ARMS[arm], FORMAL_DURATIONS)
                row.update(repeat=repeat, arm=arm, preparation_s=prep["elapsed_s"],
                           initial_snapshot=before, source_snapshot=expected,
                           phase_migration_deltas=phase_deltas(row["events"]))
                self.report["trials"].append(row)
                row["closed_snapshot"] = placement_snapshot(dirs)
                self.save()
                if arm == "observe_cold":
                    require_static(row["events"], before, row["closed_snapshot"])
                elif arm == "observe_ssd":
                    require_all_ssd_observe(row["events"])
                    if row["closed_snapshot"]["cold_tables"]:
                        raise RuntimeError("all-SSD observe arm became cold")
                elif arm == "adaptive_cold":
                    row["dynamic_coverage"] = dynamic_coverage(row["phase_migration_deltas"])
                row["phase_io"] = phase_io(row["samples_path"], row["phase_migration_deltas"])
                self.save()
            require_same_start(starts["adaptive_cold"], starts["observe_cold"])
            self.report["paired_start_checks"].append({"repeat": repeat, "passed": True,
                "placement_sha256": starts["adaptive_cold"]["placement_sha256"],
                "hot_tables": starts["adaptive_cold"]["hot_tables"],
                "ssd_sst_allocated_bytes": starts["adaptive_cold"]["ssd_sst_allocated_bytes"]})
            require_same_start(baseline, placement_snapshot(seed))
            self.save()
        self.deadline.require()
        uncovered = [row["name"] for row in self.report["trials"]
                     if row["arm"] == "adaptive_cold" and not row["dynamic_coverage"]["covered"]]
        self.report.update(status="complete_with_uncovered" if uncovered else "complete",
                           uncovered_dynamic_trials=uncovered)


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "source-manifest", "seed-ssd", "seed-hdd", "ssd-root", "hdd-root", "cgroup", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--driver-source-head", help="HEAD of driver checkout when deployed without .git")
    parser.add_argument("--workload", choices=("uniform",), default="uniform")
    parser.add_argument("--deadline-seconds", type=int, default=1800)
    parser.add_argument("--space-interval", type=float, default=10)
    args = parser.parse_args(arguments)
    args.profile, args.cpu_list, args.keep_data = "small", "0,1,2,3", True
    if not 0 < args.deadline_seconds <= 1800 or not 5 <= args.space_interval <= 10:
        parser.error("deadline must be (0,1800]; space interval must be [5,10]")
    for name in ("ssd_root", "hdd_root", "seed_ssd", "seed_hdd"):
        if not Path(getattr(args, name)).is_dir():
            parser.error(name + " must be an existing directory")
    roots = [Path(args.ssd_root).resolve(), Path(args.hdd_root).resolve()]
    if roots[0] == roots[1] or any(a in b.parents for a, b in (roots, roots[::-1])):
        parser.error("SSD/HDD parents must be disjoint")
    output = Path(args.output).resolve()
    if output.exists() or not output.parent.is_dir() or any(root == output.parent or root in output.parents for root in roots):
        parser.error("output must be new and outside data parents")
    return args


def main():
    args = parse_args()
    runner = DiagnosticRunner(args)
    def stop(signum, _frame):
        if runner.active:
            node_run.kill_owned_group(runner.active)
        raise RuntimeError("controller received signal " + str(signum))
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    code = 1
    try:
        runner.run()
        code = 0 if runner.report["status"] == "complete" else 1
    except Exception as error:
        runner.report.update(status="blocked_or_failed", error=str(error))
    finally:
        runner.report["binary_sha256_end"] = node_run.digest(args.binary)
        runner.report["source_manifest_sha256_end"] = node_run.digest(args.source_manifest)
        if runner.report["binary_sha256_end"] != runner.report["binary_sha256_start"] or \
                runner.report["source_manifest_sha256_end"] != runner.report["source_manifest_sha256_start"]:
            runner.report.update(status="blocked_or_failed", error="binary or frozen source manifest changed")
            code = 1
        runner.report["elapsed_s"] = time.monotonic() - runner.deadline.start
        runner.save()
    print(json.dumps({"status": runner.report["status"], "output": args.output,
                      "error": runner.report.get("error")}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
