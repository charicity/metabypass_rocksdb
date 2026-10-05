#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Fixed-arrival mixed policy matrix using the frozen tradeoff runner.

The binary implements 90% Get, 5% Put and 5% Delete, with 80% of access
directed to the first 25% of keys. Arrival rate is 80 ops/s, not a claim of
unsaturated service or production-like memory. The saturated phase is zero.
All seed, identity, cgroup, clone, timeout and retention safeguards are reused.
"""

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys

import node_cache
import node_diagnose
import node_run
import node_summary
import node_tradeoff as trade


PROTOCOL = "metabypass-sst-normal-v1"
POLICY_VERSION = "metabypass-sst-normal-policies-v1"
TARGET_RATE = 80
DURATIONS = {"warmup": 60, "measured": 180, "saturated": 0}
STRATEGIES = ("disabled_ssd", "aggressive65", "default65", "conservative65",
              "aggressive80", "default80", "conservative80",
              "aggressive90", "default90", "conservative90")
WORKLOAD = {"name": "mixed", "get_percent": 90, "put_percent": 5,
            "delete_percent": 5, "hot_access_percent": 80,
            "hot_key_percent": 25, "flush_interval_ms": 10000,
            "ratio_source": "frozen binary; no user override",
            "arrival_rate_ops_per_sec": TARGET_RATE,
            "normality": "diagnosed from completion and virtual backlog; not inferred from rate"}


def policy_configuration(name, baseline_bytes):
    if name not in STRATEGIES:
        raise ValueError("unknown normal policy")
    # Conservative80 is absent from the frozen tradeoff policy catalog.
    # Reuse its conservative65 parameter map with only the local fixed budget
    # changed; never mutate the imported catalog or its defaults.
    inherited = "conservative65" if name == "conservative80" else name
    result = trade.policy_configuration(inherited, baseline_bytes)
    result.update(name=name, version=POLICY_VERSION)
    if name == "conservative80":
        capacity = baseline_bytes * 80 // 100
        effective = capacity - capacity * result["parameters"]["reserve_percent"] // 100
        result.update(capacity_percent=80, capacity_bytes=capacity,
                      effective_budget_bytes=effective,
                      effective_budget_percent=effective * 100 / baseline_bytes if baseline_bytes else None)
    return result


def trial_plan(strategies, repeats):
    if repeats not in (1, 2) or not strategies or len(set(strategies)) != len(strategies) or \
            any(name not in STRATEGIES for name in strategies):
        raise ValueError("normal plan requires unique policies and one or two repeats")
    return [{"name": "repeat-%d-mixed-%s" % (repeat, strategy),
             "repeat": repeat, "workload": "mixed", "strategy": strategy,
             "status": "pending", "valid": False, "durations": dict(DURATIONS)}
            for repeat in range(1, repeats + 1)
            for strategy in (strategies if repeat == 1 else list(reversed(strategies)))]


def mixed_coverage(events, deltas):
    """Observe a conservative protected-cold lower bound during measured.

    Protected bytes include HOT backup copies; HDD bytes alone prove nothing
    about COLD reads. Subtracting all live HOT bytes from protected bytes gives
    a lower bound even when newly flushed, unprotected HOT SSTs are present.
    Stats are observations, not attribution of an individual foreground read.
    """
    ends = {phase: [e for e in events if e.get("event") == "phase_end" and e.get("phase") == phase]
            for phase in ("warmup", "measured")}
    counts = [ends[p][0].get("stats", {}).get("sampled_reads") if len(ends[p]) == 1 else None
              for p in ("warmup", "measured")]
    sampled = counts[1] - counts[0] if all(type(n) is int for n in counts) else None
    observations = []
    for event in events:
        if event.get("event") not in ("window", "phase_end") or event.get("phase") != "measured":
            continue
        stats = event.get("stats", {})
        placed = trade.placement_bytes(stats)
        if placed is None:
            continue
        observations.append({"event": event["event"], "steady_time_us": event.get("steady_time_us"),
                             **placed, "protected_bytes": stats["protected_bytes"],
                             "protected_cold_lower_bound_bytes": max(0, stats["protected_bytes"] - placed["hot_bytes"]),
                             "migration_errors": stats.get("migration_errors")})
    mixed = [o for o in observations if o["hot_bytes"] > 0 and
             o["protected_cold_lower_bound_bytes"] > 0 and
             type(o["migration_errors"]) is int and o["migration_errors"] == 0]
    delta = deltas.get("measured", {}).get("delta")
    measured = {"covered": sampled is not None and sampled > 0 and bool(mixed),
                "sampled_reads_delta": sampled, "mixed_observation_count": len(mixed),
                "observations": observations,
                "actual_migrations": delta["promotions"] + delta["demotions"] if delta else None,
                "actual_copied_bytes": delta["copied_bytes"] if delta else None}
    return {"covered": measured["covered"], "required_phases": ["measured"],
            "phases": {"measured": measured, "saturated": {"status": "not_applicable", "covered": None}},
            "basis": "measured sampled reads and observed HOT plus positive protected-COLD lower bound; migrations optional",
            "limitation": "stats observations do not attribute requests or establish exact cold bytes"}


class NormalRunner(trade.TradeoffRunner):
    def __init__(self, args):
        # The parent constructor freezes its own catalog and 112 ops/s plan.
        # Initialize the common runner directly and keep that catalog untouched.
        node_run.Runner.__init__(self, args)
        self.keys, self.rate = trade.KEYS, TARGET_RATE
        self.seed = (Path(args.seed_ssd).resolve(), Path(args.seed_hdd).resolve())
        self.report.update(protocol=PROTOCOL, status="preflight", keep_data=True,
                           source_manifest_sha256_start=node_run.digest(args.source_manifest),
                           trials=trial_plan(args.strategies, args.repeats), policy_version=POLICY_VERSION,
                           normal_workload=dict(WORKLOAD),
                           tradeoff_plan={"repeats": args.repeats, "strategies": args.strategies,
                              "workloads": ["mixed"], "db_runs": len(args.strategies) * args.repeats,
                              "durations": {"mixed": dict(DURATIONS)}, "target_ops_per_sec": TARGET_RATE,
                              "formal_phases": ["measured"], "presets": False,
                              "seed_rebuild": False, "retries": False,
                              "repeat_two": "reverse entire workload/strategy order"})
        sources = [Path(module.__file__).resolve() for module in
                   (node_run, node_cache, node_diagnose, node_summary, trade)] + [Path(__file__).resolve()]
        head = subprocess.run(["git", "-C", str(sources[0].parent), "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=10)
        self.report["driver_identity"] = {
            "source_sha256": {str(p): node_run.digest(p) for p in sources},
            "head": head.stdout.strip() if head.returncode == 0 else args.driver_source_head,
            "head_source": "checkout" if head.returncode == 0 else "provided",
            "head_probe_error": head.stderr.strip() if head.returncode else None}

    def trial_command(self, dirs, configuration, workload):
        if workload != "mixed":
            raise ValueError("normal runner only supports frozen mixed workload")
        command = super().trial_command(dirs, configuration, workload)
        values = {"metabypass_sst_" + flag + "_seconds": seconds
                  for flag, seconds in (("warmup", DURATIONS["warmup"]),
                                        ("measure", DURATIONS["measured"]),
                                        ("saturated", DURATIONS["saturated"]))}
        values["sync"] = "false"
        result = ["--%s=%s" % (item[2:].split("=", 1)[0], values.pop(item[2:].split("=", 1)[0]))
                if item.startswith("--") and item[2:].split("=", 1)[0] in values else item
                for item in command]
        result += ["--%s=%s" % (key, value) for key, value in values.items()]
        return result

    def collect_evidence(self, row, dirs):
        super().collect_evidence(row, dirs)
        row["mixed_placement_coverage"] = mixed_coverage(row.get("events", []), row.get("phase_migration_deltas", {}))
        row["formal_phases"] = ["measured"]
        row.setdefault("cache_gate", {})["saturated"] = {
            "status": "not_applicable", "passed": None, "cache_dominated": None,
            "reason": "configured saturated duration is zero; no capacity measurement"}
        if row["phases"].get("saturated") is not None:
            row["phases"]["saturated"].update(applicable=False, status="not_applicable")
        self.save()

    def run(self):
        # The parent orchestration references its module's frozen plan/config
        # helpers. Keep the same safety sequence with local normal helpers.
        self.preflight()
        self.check_identity()
        self.check_idle()
        self.seed_metadata = trade.tree_metadata(self.seed)
        self.baseline = node_diagnose.placement_snapshot(self.seed)
        if self.baseline["placement_sha256"] is not None or len(self.baseline["local_tables"]) < 4:
            raise ValueError("seed must be unlayered without SST-PLACEMENT and have at least four SSTs")
        if any(source.stat().st_dev != destination.stat().st_dev
               for source, destination in zip(self.seed, (self.ssd, self.hdd))):
            raise ValueError("seed and trial roots must use corresponding SSD/HDD devices")
        self.configurations = {name: policy_configuration(name, self.baseline["local_sst_logical_bytes"])
                               for name in self.args.strategies}
        self.report.update(status="running", seed={"paths": list(map(str, self.seed)),
                             "initial": self.baseline, "initial_metadata": self.seed_metadata,
                             "metadata_sha256_start": trade.metadata_digest(self.seed_metadata),
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
            except trade.SafetyStop as error:
                row.update(status="stopped", valid=False, error=str(error))
                self.save()
                raise
            except (RuntimeError, ValueError, OSError, KeyError, TypeError) as error:
                row.update(status="timed_out" if row.get("timed_out") or row.get("preparation", {}).get("timed_out")
                           else "failed", valid=False, error=str(error))
                if row.get("directories") and "closed_snapshot" not in row:
                    self.collect_evidence(row, tuple(map(Path, row["directories"])))
                self.save()
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


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    required = ("binary", "source-manifest", "seed-ssd", "seed-hdd", "ssd-root", "hdd-root", "cgroup", "output")
    for name in required:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--driver-source-head")
    parser.add_argument("--strategies", nargs="+", choices=STRATEGIES, default=list(STRATEGIES))
    parser.add_argument("--workloads", nargs="+", choices=("mixed",), default=["mixed"])
    parser.add_argument("--repeats", type=int, choices=(1, 2), default=2)
    parser.add_argument("--deadline-seconds", type=int, default=trade.DEADLINE_LIMIT_S)
    parser.add_argument("--space-interval", type=float, default=10)
    parsed = parser.parse_args(arguments)
    if len(set(parsed.strategies)) != len(parsed.strategies) or parsed.workloads != ["mixed"]:
        parser.error("strategies must be unique and workload must be mixed exactly once")
    # Reuse the existing path/seed/deadline validator with a supported catalog
    # entry, then install the independently validated local selection.
    shared = [item for name in required for item in ("--" + name, getattr(parsed, name.replace("-", "_")))]
    shared += ["--strategies", "disabled_ssd", "--workloads", "mixed", "--repeats", str(parsed.repeats),
               "--deadline-seconds", str(parsed.deadline_seconds), "--space-interval", str(parsed.space_interval)]
    if parsed.driver_source_head is not None:
        shared += ["--driver-source-head", parsed.driver_source_head]
    result = trade.parse_args(shared)
    result.strategies = parsed.strategies
    return result


def main():
    args = parse_args()
    runner = NormalRunner(args)

    def stop(signum, _frame):
        if runner.active:
            node_run.kill_owned_group(runner.active)
        raise trade.SafetyStop("controller received signal " + str(signum))

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        runner.run()
    except Exception as error:
        runner.report.update(status="blocked_or_failed", error=str(error))
    finally:
        runner.finalize()
    print(json.dumps({"status": runner.report["status"], "run_uuid": runner.run_id,
                                 "output": args.output, "error": runner.report.get("error"),
                                 "final_integrity_errors": runner.report.get("final_integrity_errors")}))
    return 0 if runner.report["status"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
