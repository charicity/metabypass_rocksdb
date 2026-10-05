#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Frozen mini SSD/HDD policy tradeoff matrix from an existing immutable seed.

Every trial copies the same unlayered seed to independent inodes. Copy and DB
children join a preconfigured 48 MiB cgroup on CPUs 0-3; monitoring stays outside.
No seed DB open, preset, retry, cleanup, privileged operation or system tuning.
All planned trials, failures, timeouts, raw windows and data are retained.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

import node_cache
import node_diagnose
import node_run
import node_summary


POLICY_VERSION = "metabypass-sst-tradeoff-policies-v3"
PROTOCOL = "metabypass-sst-tradeoff-v3"
DURATIONS = {"uniform": {"warmup": 30, "measured": 45, "saturated": 15},
             "switch": {"warmup": 45, "measured": 120, "saturated": 15},
             "mixed": {"warmup": 30, "measured": 45, "saturated": 15}}
DEFAULT_WORKLOADS = ("uniform", "switch")
DEFAULT_STRATEGIES = ("disabled_ssd", "default50", "default65", "default80", "default90",
                      "conservative50", "conservative65")
AGGRESSIVE_STRATEGIES = ("fast_promote65", "fast_swap65", "aggressive65",
                        "fast_promote80", "fast_swap80", "aggressive80",
                        "fast_promote90", "fast_swap90", "aggressive90")
ROUND_LIMIT_S = 480
DEFAULT_DEADLINE_S = 7200
DEADLINE_LIMIT_S = 10800
TARGET_RATE = 112
KEYS = 1_000_000

# Explicit defaults freeze the experiment independently of future db_bench
# defaults. These names are the public SstTieringOptions names.
DEFAULT_PARAMETERS = {
    "reserve_percent": 10, "sample_one_in": 64,
    "sample_buffer_capacity": 4096, "interval_ms": 1000,
    "heat_half_life_ms": 10000, "promote_rounds": 2, "demote_rounds": 3,
    "min_residency_ms": 10000, "replacement_margin": 0.25,
    "migration_bytes_per_sec": 32 * 1024 * 1024, "migration_queue_capacity": 64,
}
POLICIES = {"disabled_ssd": {"mode": "disabled", "capacity_percent": 100, "overrides": {}}}
for _percent in (50, 65, 80, 90):
    POLICIES["default" + str(_percent)] = {
        "mode": "adaptive", "capacity_percent": _percent, "overrides": {}}
for _percent in (50, 65, 90):
    POLICIES["conservative" + str(_percent)] = {
        "mode": "adaptive", "capacity_percent": _percent,
        "overrides": {"demote_rounds": 8, "min_residency_ms": 30000, "replacement_margin": 1.0}}
for _percent in (65, 80, 90):
    for _prefix, _overrides in (
            ("fast_promote", {"promote_rounds": 1}),
            ("fast_swap", {"promote_rounds": 1, "demote_rounds": 1,
                           "min_residency_ms": 3000, "replacement_margin": 0.10}),
            ("aggressive", {"promote_rounds": 1, "demote_rounds": 1,
                            "min_residency_ms": 0, "replacement_margin": 0.0})):
        POLICIES[_prefix + str(_percent)] = {
            "mode": "adaptive", "capacity_percent": _percent, "overrides": dict(_overrides)}
PARAMETER_FLAGS = {name: "metabypass_sst_" + name for name in DEFAULT_PARAMETERS}
PARAMETER_FLAGS.update(heat_half_life_ms="metabypass_sst_half_life_ms",
                       min_residency_ms="metabypass_sst_residency_ms")


class SafetyStop(RuntimeError):
    """Identity, seed or isolation failures invalidate subsequent trials."""


def policy_configuration(name, baseline_bytes):
    policy = POLICIES[name]
    params = {**DEFAULT_PARAMETERS, **policy["overrides"]}
    capacity = baseline_bytes * policy["capacity_percent"] // 100 \
        if policy["mode"] == "adaptive" else 0
    # Match integer rounding in PlanSstPlacement, not capacity * 0.9 rounded
    # down. The reserve is rounded down and subtracted from capacity.
    effective = capacity - capacity * params["reserve_percent"] // 100
    return {"name": name, "version": POLICY_VERSION, "mode": policy["mode"],
            "capacity_percent": policy["capacity_percent"],
            "capacity_bytes": capacity, "effective_budget_bytes": effective,
            "effective_budget_percent": effective * 100 / baseline_bytes if baseline_bytes else None,
            "budget_applies": policy["mode"] == "adaptive", "parameters": params,
            "baseline_logical_sst_bytes": baseline_bytes}


def trial_plan(strategies, workloads, repeats):
    if repeats not in (1, 2):
        raise ValueError("repeats must be 1 or 2")
    if not strategies or len(set(strategies)) != len(strategies) or any(s not in POLICIES for s in strategies):
        raise ValueError("strategies must be a nonempty unique subset of frozen policies")
    if not workloads or len(set(workloads)) != len(workloads) or any(w not in DURATIONS for w in workloads):
        raise ValueError("workloads must be a nonempty unique subset of uniform/switch/mixed")
    pairs = [(workload, strategy) for workload in workloads for strategy in strategies]
    return [{"name": f"repeat-{repeat}-{workload}-{strategy}", "repeat": repeat,
             "workload": workload, "strategy": strategy, "status": "pending", "valid": False,
             "durations": dict(DURATIONS[workload])}
            for repeat in range(1, repeats + 1)
            for workload, strategy in (pairs if repeat == 1 else list(reversed(pairs)))]


def tree_metadata(dirs):
    """Read metadata only; do not read blob/SST payload or perturb seed pages.

    atime is deliberately excluded because copying may update it. Metadata
    checks detect ordinary writes/renames; they are not payload checksums.
    """
    result = []
    for device, root in enumerate(map(Path, dirs)):
        if root.is_symlink() or not root.is_dir():
            raise ValueError("dataset root is missing or a symlink: " + str(root))
        for base, directories, names in os.walk(root, followlinks=False):
            base = Path(base)
            for name in sorted([*directories, *names]):
                path = base / name
                info = path.lstat()
                if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                    raise ValueError("dataset contains symlink/nonregular file: " + str(path))
                if stat.S_ISREG(info.st_mode):
                    result.append({"root": device, "path": str(path.relative_to(root)),
                                   "dev": info.st_dev, "ino": info.st_ino, "size": info.st_size,
                                   "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
                                   "mode": info.st_mode, "nlink": info.st_nlink})
    return sorted(result, key=lambda item: (item["root"], item["path"]))


def metadata_digest(metadata):
    return hashlib.sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def require_independent_inodes(source_metadata, clone_metadata, previous_inodes=()):
    source = {(entry["dev"], entry["ino"]) for entry in source_metadata}
    actual = {(entry["dev"], entry["ino"]) for entry in clone_metadata}
    forbidden = source | set(previous_inodes)
    if actual & forbidden:
        raise SafetyStop("clone shares an inode with immutable seed or another trial")
    return actual


def placement_bytes(stats):
    keys = ("protected_bytes", "unprotected_bytes", "ssd_bytes", "reserved_bytes", "pending_delete_bytes")
    if any(type(stats.get(k)) is not int or stats[k] < 0 for k in keys):
        return None
    total = stats["protected_bytes"] + stats["unprotected_bytes"]
    hot = stats["ssd_bytes"] - stats["reserved_bytes"] - stats["pending_delete_bytes"]
    if not 0 <= hot <= total:
        return None
    return {"hot_bytes": hot, "cold_bytes": total - hot, "total_bytes": total,
            "unprotected_bytes": stats["unprotected_bytes"]}


def mixed_coverage(events, deltas):
    """Actual mixed-placement observations plus SST foreground-read sampling.

    hdd_bytes counts protected backup copies, including HOT copies; it is NOT
    cold placement. The conservative cold estimate subtracts reserved and
    pending-delete charges from ssd_bytes. Zero new migrations is allowed.
    """
    phases, previous = {}, None
    for phase in DURATIONS["uniform"]:
        ends = [e for e in events if e.get("event") == "phase_end" and e.get("phase") == phase]
        current = ends[0].get("stats", {}).get("sampled_reads") if len(ends) == 1 else None
        sampled = current - previous if type(current) is int and type(previous) is int else None
        if phase != "warmup":
            observations = []
            for event in events:
                if event.get("event") not in ("window", "phase_end") or event.get("phase") != phase:
                    continue
                stats = event.get("stats", {})
                placed = placement_bytes(stats)
                if placed is not None:
                    observations.append({"event": event["event"], "steady_time_us": event.get("steady_time_us"),
                                         **placed, "migration_errors": stats.get("migration_errors")})
            mixed = [p for p in observations if p["hot_bytes"] > 0 and p["cold_bytes"] > 0 and
                     p["unprotected_bytes"] == 0 and p["migration_errors"] == 0]
            delta = deltas.get(phase, {}).get("delta")
            phases[phase] = {"covered": sampled is not None and sampled > 0 and bool(mixed),
                             "sampled_reads_delta": sampled,
                             "mixed_observation_count": len(mixed), "observations": observations,
                             "actual_migrations": delta["promotions"] + delta["demotions"] if delta else None,
                             "actual_copied_bytes": delta["copied_bytes"] if delta else None}
        previous = current
    return {"covered": all(p["covered"] for p in phases.values()), "phases": phases,
            "basis": "each formal phase: sampled SST reads and observed protected mixed placement; migrations optional",
            "limitation": "placement and sampling do not attribute individual requests to hot/cold devices"}


class TradeoffRunner(node_run.Runner):
    def __init__(self, args):
        super().__init__(args)
        self.keys, self.rate = KEYS, TARGET_RATE
        self.seed = (Path(args.seed_ssd).resolve(), Path(args.seed_hdd).resolve())
        self.report.update(protocol=PROTOCOL, status="preflight", keep_data=True,
                           source_manifest_sha256_start=node_run.digest(args.source_manifest),
                           trials=trial_plan(args.strategies, args.workloads, args.repeats),
                           policy_version=POLICY_VERSION,
                           tradeoff_plan={"repeats": args.repeats, "strategies": args.strategies,
                              "workloads": args.workloads, "db_runs": len(args.strategies) * len(args.workloads) * args.repeats,
                              "durations": DURATIONS, "target_ops_per_sec": TARGET_RATE,
                              "presets": False, "seed_rebuild": False, "retries": False,
                              "repeat_two": "reverse entire workload/strategy order"})
        self.report["limits"].update(hard_deadline_s=args.deadline_seconds,
                                     round_hard_limit_s=ROUND_LIMIT_S)
        sources = [Path(module.__file__).resolve() for module in
                   (node_run, node_cache, node_diagnose, node_summary)] + [Path(__file__).resolve()]
        head = subprocess.run(["git", "-C", str(sources[0].parent), "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=10)
        self.report["driver_identity"] = {
            "source_sha256": {str(p): node_run.digest(p) for p in sources},
            "head": head.stdout.strip() if head.returncode == 0 else args.driver_source_head,
            "head_source": "checkout" if head.returncode == 0 else "provided",
            "head_probe_error": head.stderr.strip() if head.returncode else None}

    def check_idle(self):
        if (Path(self.args.cgroup) / "tasks").read_text().split():
            raise SafetyStop("private cgroup gained an unexpected task")

    def check_identity(self):
        for name, path in (("binary", self.args.binary), ("source_manifest", self.args.source_manifest)):
            try:
                if node_run.digest(path) != self.report[name + "_sha256_start"]:
                    raise SafetyStop(name + " changed during frozen experiment")
            except OSError as error:
                raise SafetyStop(name + " identity is unreadable: " + str(error)) from error
        for path, expected in self.report["driver_identity"]["source_sha256"].items():
            try:
                if node_run.digest(path) != expected:
                    raise SafetyStop("frozen Python source changed: " + path)
            except OSError as error:
                raise SafetyStop("frozen Python source is unreadable: " + path) from error

    def check_seed(self):
        actual = tree_metadata(self.seed)
        if actual != self.seed_metadata:
            raise SafetyStop("immutable seed metadata changed")
        node_diagnose.require_same_start(self.baseline, node_diagnose.placement_snapshot(self.seed))

    def trial_command(self, dirs, configuration, workload):
        durations = DURATIONS[workload]
        command = super().command("run", dirs, mode=configuration["mode"], workload=workload,
                                  budget=configuration["capacity_bytes"], rate=self.rate,
                                  warmup=durations["warmup"], measured=durations["measured"],
                                  saturated=durations["saturated"])
        # Replace inherited flags rather than appending duplicates. Every policy
        # parameter, including conservative overrides, reaches the actual argv.
        values = {PARAMETER_FLAGS[name]: value for name, value in configuration["parameters"].items()}
        values["metabypass_sst_read_delay_us"] = 0
        result = []
        for item in command:
            key = item[2:].split("=", 1)[0] if item.startswith("--") else None
            result.append(f"--{key}={values.pop(key)}" if key in values else item)
        result += [f"--{key}={value}" for key, value in values.items()]
        return result

    def prior_trial_inodes(self, current_dirs):
        # RocksDB may retire WALs or SSTs. Only still-existing files can share
        # data with a new clone; forbidding retired inode numbers rejects valid
        # filesystem inode reuse on later rounds.
        current_dirs = set(map(str, current_dirs))
        inodes = set()
        for trial in self.report["trials"]:
            for path in trial.get("directories", []):
                if path in current_dirs:
                    continue
                for entry in tree_metadata((path,)):
                    inodes.add((entry["dev"], entry["ino"]))
        return inodes

    def oom_kill_count(self):
        value = node_run.sample_cgroup(self.args.cgroup).get("oom_control", {}).get("oom_kill")
        if type(value) is not int or value < 0:
            raise SafetyStop("private cgroup OOM counter is missing or invalid")
        return value

    def execute_checked(self, row, command, dirs, timeout, preparation=False):
        """Catch OOM even when a child dies before Runner records its identity."""
        self.check_idle()
        name = row["name"] + "-copy" if preparation else row["name"]
        before = self.oom_kill_count()
        actual = None
        try:
            actual = self.execute(name, command, dirs, timeout=timeout)
        finally:
            after = self.oom_kill_count()
            delta = after - before
            check = {"command_name": name, "oom_kill_start": before,
                     "oom_kill_end": after, "oom_kill_delta": delta}
            row.setdefault("independent_oom_checks", []).append(check)
            if actual is not None:
                actual["independent_oom_check"] = check
                actual["oom_kill_delta"] = max(actual.get("oom_kill_delta") or 0, delta)
                if actual["oom_kill_delta"] > 0:
                    actual["valid"] = False
                if preparation:
                    row["preparation"] = actual
                else:
                    row.update(actual)
            self.save()
            if delta < 0:
                raise SafetyStop("private cgroup OOM counter decreased or was reset")
            if delta > 0 or actual is not None and actual["oom_kill_delta"] > 0:
                raise SafetyStop("clone OOM: " + name if preparation else "benchmark OOM: " + name)
        return actual

    def copy_round(self, row):
        self.check_idle()
        self.deadline.require(sum(row["durations"].values()) + 30)
        row["round_started_monotonic_s"] = time.monotonic()
        dirs = self.directories(row["name"])
        row["directories"] = list(map(str, dirs))
        row["status"] = "copying"
        self.save()
        command = [sys.executable, str(Path(node_run.__file__).resolve()), "--clone", *map(str, (*self.seed, *dirs))]
        prep = self.execute_checked(row, command, dirs, timeout=ROUND_LIMIT_S, preparation=True)
        row["preparation"] = prep
        row["preparation_s"] = prep.get("elapsed_s")
        if not prep.get("valid"):
            raise RuntimeError("clone failed/timeout: " + row["name"])
        self.check_idle()
        self.check_seed()
        before = node_diagnose.placement_snapshot(dirs)
        row["initial_snapshot"] = before
        node_diagnose.require_same_start(self.baseline, before)
        inodes = require_independent_inodes(self.seed_metadata, tree_metadata(dirs), self.prior_trial_inodes(dirs))
        row["independent_inode_check"] = {"passed": True, "unique_file_inodes": len(inodes),
                                          "shared_with_seed": False, "shared_with_prior_trial": False}
        return dirs

    def collect_evidence(self, row, dirs):
        """Preserve successful sibling evidence even for a partial/failed trial."""
        row.setdefault("evidence_errors", [])
        try:
            row["closed_snapshot"] = node_diagnose.placement_snapshot(dirs)
        except (OSError, ValueError) as error:
            row["evidence_errors"].append({"kind": "closed_snapshot", "error": str(error)})
        events = row.get("events", [])
        row["phases"] = {phase: node_summary.phase_result(events, phase) for phase in row["durations"]}
        try:
            row["phase_migration_deltas"] = node_diagnose.phase_deltas(events)
        except (ValueError, KeyError, TypeError) as error:
            row["evidence_errors"].append({"kind": "phase_migration_deltas", "error": str(error)})
        row["mixed_placement_coverage"] = mixed_coverage(events, row.get("phase_migration_deltas", {}))
        samples = row.get("samples_path")
        if samples:
            try:
                row["space"] = node_summary.sampled_space(row)
                row["cache_gate"] = {phase: node_run.cache_gate(row, phase=phase)
                                     for phase in ("measured", "saturated")}
                if "phase_migration_deltas" in row:
                    row["phase_io"] = node_diagnose.phase_io(samples, row["phase_migration_deltas"])
            except (OSError, ValueError, KeyError, TypeError) as error:
                row["evidence_errors"].append({"kind": "sample_evidence", "error": str(error)})
        self.save()

    def benchmark_round(self, row, dirs):
        self.check_idle()
        remaining = min(ROUND_LIMIT_S - (time.monotonic() - row["round_started_monotonic_s"]),
                        self.deadline.remaining())
        if remaining <= sum(row["durations"].values()):
            raise RuntimeError("round/deadline leaves insufficient fixed phase time")
        row["status"] = "running"
        self.save()
        actual = self.execute_checked(row, self.trial_command(dirs, row["effective_configuration"], row["workload"]),
                                      dirs, timeout=remaining)
        row.update(actual)
        row["round_elapsed_s"] = time.monotonic() - row["round_started_monotonic_s"]
        row["phase_validation_errors"] = node_run.phase_validation(row, row["durations"])
        expected = row["effective_configuration"]["capacity_bytes"]
        stats_events = [e for e in row["events"] if e.get("event") in ("window", "phase_end", "summary")]
        if not stats_events or any(e.get("stats", {}).get("ssd_capacity_bytes") != expected for e in stats_events):
            row["phase_validation_errors"].append("reported capacity does not match actual strategy command")
        row["valid"] = bool(row.get("valid")) and not row["phase_validation_errors"] and \
            row.get("oom_kill_delta", 0) == 0 and row["round_elapsed_s"] <= ROUND_LIMIT_S
        self.collect_evidence(row, dirs)
        if not row["valid"]:
            raise RuntimeError("benchmark failed/phase invalid/timeout: " + row["name"])
        if row["mode"] == "disabled":
            if row["workload"] == "mixed":
                if any(value != 0 for phase in row["phase_migration_deltas"].values()
                       for value in phase["end_counters"].values()):
                    raise RuntimeError("disabled mixed arm performed an SST migration")
                if row["closed_snapshot"]["placement_sha256"] is not None:
                    raise RuntimeError("disabled mixed arm created tiered SST placement")
            else:
                node_diagnose.require_static(row["events"], row["initial_snapshot"], row["closed_snapshot"])
        row["status"] = "complete"

    def run(self):
        self.preflight()
        self.check_identity()
        self.check_idle()
        self.seed_metadata = tree_metadata(self.seed)
        self.baseline = node_diagnose.placement_snapshot(self.seed)
        if self.baseline["placement_sha256"] is not None or len(self.baseline["local_tables"]) < 4:
            raise ValueError("seed must be unlayered without SST-PLACEMENT and have at least four SSTs")
        if any(source.stat().st_dev != destination.stat().st_dev
               for source, destination in zip(self.seed, (self.ssd, self.hdd))):
            raise ValueError("seed and trial roots must use the same corresponding SSD/HDD devices")
        self.configurations = {name: policy_configuration(name, self.baseline["local_sst_logical_bytes"])
                               for name in self.args.strategies}
        self.report.update(status="running", seed={"paths": list(map(str, self.seed)),
                             "initial": self.baseline, "initial_metadata": self.seed_metadata,
                             "metadata_sha256_start": metadata_digest(self.seed_metadata),
                             "payload_hashing": False, "opened_for_database_operations": False},
                           effective_configurations=self.configurations, keys=self.keys,
                           baseline_sst_logical_bytes=self.baseline["local_sst_logical_bytes"],
                           frozen_target_ops_per_sec=self.rate)
        for row in self.report["trials"]:
            configuration = self.configurations[row["strategy"]]
            row.update(effective_configuration=configuration, mode=configuration["mode"],
                       budget_bytes=configuration["capacity_bytes"],
                       effective_budget_bytes=configuration["effective_budget_bytes"])
        self.save()
        for row in self.report["trials"]:
            try:
                self.check_identity()
                self.check_seed()
                self.check_idle()
                dirs = self.copy_round(row)
                self.benchmark_round(row, dirs)
                self.check_identity()
                self.check_seed()
                self.check_idle()
            except SafetyStop as error:
                row.update(status="stopped", valid=False, error=str(error))
                self.save()
                raise
            except (RuntimeError, ValueError, OSError, KeyError, TypeError) as error:
                row.update(status="timed_out" if row.get("timed_out") or row.get("preparation", {}).get("timed_out")
                           else "failed", valid=False, error=str(error))
                if row.get("directories") and "closed_snapshot" not in row:
                    self.collect_evidence(row, tuple(map(Path, row["directories"])))
                self.save()
                # Recheck before proceeding after an ordinary round failure.
                # OOM, identity changes and unexpected cgroup tasks cannot be
                # treated as independent round failures.
                self.check_idle()
                self.check_identity()
                self.check_seed()
                if self.deadline.remaining() <= sum(row["durations"].values()) + 30:
                    raise RuntimeError("overall deadline leaves no further complete round")
            self.save()
        self.deadline.require()
        failed = [r["name"] for r in self.report["trials"] if not r["valid"]]
        uncovered = [r["name"] for r in self.report["trials"] if r["mode"] == "adaptive" and r["valid"] and
                     not r["mixed_placement_coverage"]["covered"]]
        self.report.update(status="complete_with_failures" if failed else
                           "complete_with_uncovered" if uncovered else "complete",
                           failed_trials=failed, uncovered_mixed_trials=uncovered)

    def finalize(self):
        """Attempt every final integrity check even when one check cannot read."""
        errors = []
        for name, path in (("binary", self.args.binary), ("source_manifest", self.args.source_manifest)):
            try:
                actual = node_run.digest(path)
                self.report[name + "_sha256_end"] = actual
                if actual != self.report[name + "_sha256_start"]:
                    errors.append(name + " changed during frozen experiment")
            except OSError as error:
                self.report[name + "_sha256_end"] = None
                errors.append(name + " final checksum failed: " + str(error))
        final_sources = {}
        for path, expected in self.report["driver_identity"]["source_sha256"].items():
            try:
                final_sources[path] = node_run.digest(path)
                if final_sources[path] != expected:
                    errors.append("frozen Python source changed: " + path)
            except OSError as error:
                final_sources[path] = None
                errors.append("frozen Python source final checksum failed: " + str(error))
        self.report["driver_identity"]["source_sha256_end"] = final_sources
        if hasattr(self, "seed_metadata"):
            try:
                self.report.setdefault("seed", {})["metadata_sha256_end"] = metadata_digest(tree_metadata(self.seed))
                self.check_seed()
                self.report["seed"]["final_immutable_check"] = True
            except (ValueError, OSError, RuntimeError, AttributeError) as error:
                self.report.setdefault("seed", {})["final_immutable_check"] = False
                errors.append("seed final check: " + str(error))
        for row in self.report["trials"]:
            if row["status"] in ("pending", "copying", "running"):
                row.update(status="not_run" if row["status"] == "pending" else "incomplete", valid=False,
                           error=row.get("error", self.report.get("error", "controller stopped before completion")))
        if errors:
            self.report.update(status="blocked_or_failed", final_integrity_errors=errors)
        self.report["elapsed_s"] = time.monotonic() - self.deadline.start
        self.save()


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "source-manifest", "seed-ssd", "seed-hdd", "ssd-root", "hdd-root", "cgroup", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--driver-source-head", help="HEAD of driver checkout when deployed without .git")
    parser.add_argument("--workloads", nargs="+", choices=tuple(DURATIONS), default=list(DEFAULT_WORKLOADS))
    parser.add_argument("--strategies", nargs="+", choices=tuple(POLICIES), default=list(DEFAULT_STRATEGIES))
    parser.add_argument("--repeats", type=int, choices=(1, 2), default=2)
    parser.add_argument("--deadline-seconds", type=int, default=DEFAULT_DEADLINE_S)
    parser.add_argument("--space-interval", type=float, default=10)
    args = parser.parse_args(arguments)
    args.profile, args.cpu_list, args.keep_data = "small", "0,1,2,3", True
    if not 0 < args.deadline_seconds <= DEADLINE_LIMIT_S or not 5 <= args.space_interval <= 10:
        parser.error("deadline must be (0,10800]; space interval must be [5,10]")
    if len(set(args.workloads)) != len(args.workloads) or len(set(args.strategies)) != len(args.strategies):
        parser.error("workloads and strategies must not contain duplicates")
    for name in ("ssd_root", "hdd_root", "seed_ssd", "seed_hdd"):
        if not Path(getattr(args, name)).is_dir():
            parser.error(name + " must be an existing directory")
    roots = [Path(args.ssd_root).resolve(), Path(args.hdd_root).resolve()]
    seed = [Path(args.seed_ssd).resolve(), Path(args.seed_hdd).resolve()]
    for paths, label in ((roots, "SSD/HDD parents"), (seed, "SSD/HDD seeds")):
        if paths[0] == paths[1] or any(a in b.parents for a, b in (paths, paths[::-1])):
            parser.error(label + " must be disjoint")
    output = Path(args.output).resolve()
    if output.exists() or not output.parent.is_dir() or any(root == output.parent or root in output.parents
                                                          for root in (*roots, *seed)):
        parser.error("output must be new and outside data parents and seeds")
    # Creating run-owned roots inside the immutable seed would itself mutate it.
    if any(s == root or s in root.parents for s in seed for root in roots):
        parser.error("data parents cannot be within immutable seeds")
    return args


def main():
    args = parse_args()
    runner = TradeoffRunner(args)

    def stop(signum, _frame):
        if runner.active:
            node_run.kill_owned_group(runner.active)
        raise SafetyStop("controller received signal " + str(signum))

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        runner.run()
    except Exception as error:
        runner.report.update(status="blocked_or_failed", error=str(error))
    finally:
        runner.finalize()
    print(json.dumps({"status": runner.report["status"], "output": args.output,
                      "error": runner.report.get("error"),
                      "final_integrity_errors": runner.report.get("final_integrity_errors")}))
    return 0 if runner.report["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
