#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""DirectWrite comparison using the native checkpoint ownership/telemetry runner."""
import argparse
from collections import defaultdict
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / "native-checkpoint"
ORIGINAL = "23e242cba"


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


runner = module("native_checkpoint_runner", BASE / "run.py")
metrics = module("native_checkpoint_metrics", BASE / "summary.py")
ARMS = ("original1s", "newfalse5s", "newtrue5s")


def matrix_scope(write_bytes=4 * runner.GIB, seconds=30, verify_samples=0):
    scope = {"rounds": [1, 2, 3], "workloads": ["insert", "overwrite", "mixed"],
            "threads": [1, 4], "staging_cases": ["none", "ample", "saturated"],
            "samples": 162, "samples_per_arm": 54,
            "write_bytes": write_bytes, "seconds": seconds}
    if verify_samples:
        scope.update(verify_samples=verify_samples, verification_protocol="metabypass-key-sample-v1")
    return scope


def matrix(profile="formal", write_bytes=4 * runner.GIB, seconds=30, verify_samples=0):
    keys = 8192 if profile == "smoke" else write_bytes // 1024
    rows = []
    for number in range(1, 4):
        for workload in ("insert", "overwrite", "mixed"):
            for threads in (1, 4):
                for staging in ("none", "ample", "saturated"):
                    # Rotate execution order while preserving each paired seed.
                    arms = ARMS[number - 1:] + ARMS[:number - 1]
                    for arm in arms:
                        row = {"stage": "specials", "scenario": f"{workload}-{threads}t-{staging}",
                               "round": number, "arm": arm, "group": "D" if staging == "none" else "S",
                               "workload": workload, "keys": keys, "ops": keys,
                               "value_size": 1024, "threads": threads, "staging_case": staging,
                               "staging_capacity": (16 * runner.GIB if profile == "formal" else 16 * runner.MIB)
                               if staging == "ample" else 65536,
                               "seconds": seconds if workload != "insert" and profile == "formal" else 0,
                               "gc_wait_seconds": 5 if profile == "formal" else 0,
                               "seed": 20261020 + number, "flush": True}
                        if arm != "original1s":
                            row.update(direct_write=arm == "newtrue5s", mirror_interval_ms=1000,
                                       checkpoint_interval_ms=5000)
                        if verify_samples:
                            row["verify_samples"] = verify_samples
                        rows.append(row)
    return rows


def coverage(row, profile):
    events = row["driver"]["events"]
    last = runner.last_stats(events)
    spec = row["spec"]
    missing = []
    if spec["staging_case"] == "ample":
        if last.get("peak_staging_bytes", 0) <= 0:
            missing.append("ample staging was never occupied")
        if last.get("staging_spill_bytes", 0) != 0:
            missing.append("ample staging spilled; increase its capacity")
    elif spec["staging_case"] == "saturated" and last.get("staging_spill_bytes", 0) <= 0:
        missing.append("saturated staging never spilled")
    if profile == "formal":
        foreground = [e for e in events if e.get("event") == "sample" and e.get("name") == "foreground"]
        if sum(e["elapsed_us"] for e in foreground) < 15_000_000:
            missing.append("foreground shorter than 15 seconds; increase --write-bytes")
        if spec["arm"] != "original1s":
            points = {e.get("boundary"): e.get("recovery_points", 0)
                      for e in events if e.get("event") == "stats"}
            if points.get("foreground_end", 0) - points.get("foreground_begin", 0) < 2:
                missing.append("fewer than two points published during foreground")
    return {"complete": not missing, "missing": missing}


class Experiment(runner.Experiment):
    def __init__(self, args):
        original = runner.strict_json(Path(args.original_manifest).read_text())
        if not original.get("source_head", "").startswith(ORIGINAL):
            raise runner.RunFailure("original manifest must identify HEAD " + ORIGINAL)
        binary = Path(original["path"]).resolve()
        if not binary.is_file() or runner.digest(binary) != original["sha256"]:
            raise runner.RunFailure("original binary hash mismatch")
        self.original_binary = binary
        super().__init__(args)
        self.report.update(comparison_protocol="metabypass-direct-write-v1", original_binary=original,
                           arms=list(ARMS), matrix_scope=matrix_scope(args.write_bytes, args.seconds,
                                                                      getattr(args, "verify_samples", 0)),
                           background_tail_seconds=5 if args.profile == "formal" else 0)
        runner.write_json(self.output / "matrix.json", matrix(args.profile, args.write_bytes, args.seconds,
                                                              getattr(args, "verify_samples", 0)))
        self.save()

    def invoke(self, spec, paths, action, **kwargs):
        group = spec["group"]
        current = self.binaries[group]
        if spec["arm"] == "original1s":
            self.binaries[group] = self.original_binary
        try:
            return super().invoke(spec, paths, action, **kwargs)
        finally:
            self.binaries[group] = current

    def sample(self, spec, phase="measured"):
        # Validate mechanism coverage before the shared runner deletes sample data.
        original_coverage = runner.mechanism_coverage
        def checked(sample_spec, events):
            result = original_coverage(sample_spec, events)
            extra = coverage({"spec": sample_spec, "driver": {"events": events}}, self.args.profile)
            result["missing"] += extra["missing"]
            result["complete"] = not result["missing"]
            return result
        runner.mechanism_coverage = checked
        try:
            return super().sample(spec, phase)
        finally:
            runner.mechanism_coverage = original_coverage


def summarize(report):
    scope = report.get("matrix_scope", matrix_scope())
    verify_samples = scope.get("verify_samples", 0)
    expected_scope = matrix_scope(scope["write_bytes"], scope["seconds"], verify_samples)
    expected = {(spec["scenario"], spec["arm"], spec["round"]): spec
                for spec in matrix(report["profile"], scope["write_bytes"], scope["seconds"], verify_samples)}
    groups = defaultdict(dict)
    omitted = []
    seen = set()
    for index, row in enumerate(report["runs"]):
        spec = row.get("spec", {})
        identity = spec.get("scenario"), spec.get("arm"), spec.get("round")
        if identity in seen:
            raise ValueError("duplicate paired round")
        seen.add(identity)
        if not row.get("valid") or row.get("status") != "pass":
            omitted.append({"index": index, "error": row.get("error")})
            continue
        # Compare every field, including types, so a mislabeled seed, interval,
        # workload, or arm cannot supply an apparently complete paired round.
        if identity not in expected or json.dumps(spec, sort_keys=True) != \
                json.dumps(expected[identity], sort_keys=True):
            omitted.append({"index": index, "error": "sample does not match expected matrix"})
            continue
        key = spec["scenario"], spec["arm"]
        number = spec["round"]
        groups[key][number] = metrics.row_metrics(row)
    comparisons = []
    for scenario in sorted({key[0] for key in expected}):
        for candidate, reference, meaning in (("newtrue5s", "newfalse5s", "direct_write_same_interval"),
                                              ("newtrue5s", "original1s", "overall_revision_and_interval")):
            a, b = groups[scenario, candidate], groups[scenario, reference]
            paired = sorted(set(a) & set(b))
            common = set.intersection(*(set(a[n]) & set(b[n]) for n in paired)) if paired else set()
            ratios = {name: metrics.distribution([a[n][name] / b[n][name] for n in paired])
                      for name in sorted(common) if all(b[n][name] > 0 for n in paired)}
            comparisons.append({"scenario": scenario, "candidate": candidate, "reference": reference,
                                "meaning": meaning, "paired_rounds": paired, "complete": paired == [1, 2, 3],
                                "candidate_over_reference": ratios})
    return {"comparison_protocol": "metabypass-direct-write-v1", "profile": report["profile"],
            "arms": list(ARMS), "matrix_scope": expected_scope,
            "state": report["state"], "comparisons": comparisons, "omitted": omitted,
            "absolute": [{"scenario": scenario, "arm": arm, "rounds": sorted(rows),
                          "metrics_by_round": rows} for (scenario, arm), rows in sorted(groups.items())],
            "ranking_allowed": report["profile"] == "formal" and report["state"] == "completed"
            and report.get("arms", list(ARMS)) == list(ARMS) and scope == expected_scope
            and len(report["runs"]) == len(expected) and seen == set(expected) and not omitted
            and len(comparisons) == 36 and all(c["complete"] for c in comparisons)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "preflight", "run", "summarize"))
    parser.add_argument("--profile", choices=("formal", "smoke"), default="formal")
    parser.add_argument("--write-bytes", type=int, default=4 * runner.GIB)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--verify-samples", type=int, default=0)
    parser.add_argument("--ssd-parent", default="/home/lj/ssd/metabypass_test")
    parser.add_argument("--hdd-parent", default="/home/lj/hdd/metabypass_test")
    parser.add_argument("--cgroup", default=str(runner.CGROUP))
    parser.add_argument("--cpus")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--build-manifest", type=Path)
    parser.add_argument("--original-manifest", type=Path)
    parser.add_argument("--budget-seconds", type=float, default=28800)
    parser.add_argument("--no-time-limits", action="store_true")
    args = parser.parse_args(argv)
    if args.write_bytes < 4096 or args.write_bytes % 1024 or not 15 <= args.seconds <= 86400:
        parser.error("write-bytes must be a multiple of 1024 >=4096; seconds must be 15..86400")
    if not 0 <= args.verify_samples <= 100000000:
        parser.error("verify-samples must be 0..100000000 (0 preserves full verification)")
    args.stage_budgets = {name: None if args.no_time_limits else args.budget_seconds for name in runner.STAGES}
    args.stage_budget_override = None
    args.time_limit_policy = {"mode": "unlimited" if args.no_time_limits else "bounded"}
    args.groups = ("D", "S")
    if args.command == "plan":
        print(json.dumps(matrix(args.profile, args.write_bytes, args.seconds, args.verify_samples), indent=2))
        return 0
    if args.command == "preflight":
        result = runner.preflight(args)
        print(json.dumps(result, indent=2))
        return 0 if result["ready"] else 2
    if not args.output:
        parser.error("--output required")
    if args.command == "summarize":
        report = runner.strict_json((args.output / "results.json").read_text())
        runner.write_json(args.output / "direct-write-summary.json", summarize(report))
        return 0
    if not args.build_manifest or not args.original_manifest:
        parser.error("run requires --build-manifest and --original-manifest")
    experiment = None
    try:
        experiment = Experiment(args)
        for spec in matrix(args.profile, args.write_bytes, args.seconds, args.verify_samples):
            experiment.sample(spec)
        experiment.report["state"] = "completed"
    except (runner.RunFailure, OSError, ValueError, KeyboardInterrupt) as error:
        if experiment:
            experiment.report.update(state="incomplete", error=str(error))
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if experiment:
            experiment.save()
            runner.write_json(experiment.output / "direct-write-summary.json", summarize(experiment.report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
