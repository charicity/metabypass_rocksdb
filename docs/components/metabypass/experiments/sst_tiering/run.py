#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Run fixed-work SST tiering comparisons with complete trial provenance."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[5]
PROTOCOL = "metabypass-sst-tiering-v1"
WORKLOADS = ("uniform", "hotspot", "switch", "scan", "mixed")
CONFIGS = (("disabled", None), ("observe", 25), ("observe", 50),
           ("observe", 75), ("adaptive", 25), ("adaptive", 50),
           ("adaptive", 75))
PROFILES = {
    "smoke": dict(keys=8192, value_size=1024, warmup_ops=5000,
                  measure_ops=15000, target_ops_per_sec=5000,
                  window_ms=250, interval_ms=100, half_life_ms=1000,
                  residency_ms=1000, flush_every=1024,
                  cache_bytes=32 * 1024, timeout_s=90),
    "standard": dict(keys=100000, value_size=1024, warmup_ops=20000,
                     measure_ops=240000, target_ops_per_sec=10000,
                     window_ms=1000, interval_ms=1000, half_life_ms=10000,
                     residency_ms=10000, flush_every=4096,
                     cache_bytes=64 * 1024, timeout_s=600),
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git(*args):
    return subprocess.run(["git", "-C", str(ROOT), *args],
                          capture_output=True, check=True).stdout


def source_identity():
    names = set(git("ls-files", "-z", "--cached", "--others",
                    "--exclude-standard").decode().split("\0"))
    names = sorted(name for name in names if name and Path(name).suffix in
                   {".cc", ".c", ".h", ".py", ".md", ".mk"})
    files = {name: sha256(ROOT / name) if (ROOT / name).is_file() else None
             for name in names}
    return {"head": git("rev-parse", "HEAD").decode().strip(),
            "status": git("status", "--short").decode(),
            "source_sha256": hashlib.sha256(json.dumps(
                files, sort_keys=True).encode()).hexdigest(),
            "binary_source_files": len(files)}


def parse_events(stdout):
    events = []
    errors = []
    for line in stdout.splitlines():
        if line.startswith("MB_SST_JSON "):
            try:
                events.append(json.loads(line[len("MB_SST_JSON "):]))
            except json.JSONDecodeError as error:
                errors.append({"line": line, "error": str(error)})
    return events, errors


def process_usage(pid):
    """Linux procfs sampling; return empty values if the process already exited."""
    try:
        lines = (Path("/proc") / str(pid) / "status").read_text().splitlines()
        values = {line.split(":", 1)[0]: line.split(":", 1)[1].strip()
                  for line in lines if ":" in line}
        ticks = (Path("/proc") / str(pid) / "stat").read_text().rsplit(") ", 1)[1].split()
        return {"rss_kib": int(values.get("VmRSS", "0 kB").split()[0]),
                "cpu_ticks": int(ticks[11]) + int(ticks[12])}
    except (OSError, ValueError, IndexError):
        return {}


def execute(command, timeout_s):
    started = time.monotonic()
    with tempfile.TemporaryFile(mode="w+t") as stdout_file, \
            tempfile.TemporaryFile(mode="w+t") as stderr_file:
        try:
            child = subprocess.Popen(command, stdout=stdout_file,
                                     stderr=stderr_file, start_new_session=True)
        except OSError as error:
            return {"command": command, "exit_code": None,
                    "timed_out": False, "elapsed_s": time.monotonic() - started,
                    "peak_rss_kib": 0, "cpu_seconds_sampled": None,
                    "stdout": "", "stderr": str(error), "events": [],
                    "parse_errors": []}
        peak_rss_kib = 0
        first_ticks = None
        last_ticks = None
        timed_out = False
        while child.poll() is None:
            usage = process_usage(child.pid)
            if usage:
                peak_rss_kib = max(peak_rss_kib, usage["rss_kib"])
                if first_ticks is None:
                    first_ticks = usage["cpu_ticks"]
                last_ticks = usage["cpu_ticks"]
            if time.monotonic() - started > timeout_s:
                timed_out = True
                os.killpg(child.pid, signal.SIGKILL)
                break
            time.sleep(0.1)
        child.wait()
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read()
        stderr = stderr_file.read()
    events, parse_errors = parse_events(stdout)
    return {"command": command, "exit_code": child.returncode,
            "timed_out": timed_out, "elapsed_s": time.monotonic() - started,
            "peak_rss_kib": peak_rss_kib,
            "cpu_seconds_sampled": ((last_ticks - first_ticks) /
                                    os.sysconf("SC_CLK_TCK") if
                                    first_ticks is not None and
                                    last_ticks is not None else None),
            "stdout": stdout, "stderr": stderr,
            "events": events, "parse_errors": parse_errors}


def category(path, ssd, hdd):
    suffix = path.suffix.lower()
    if suffix == ".sst":
        return "ssd_sst" if path.is_relative_to(ssd) else "hdd_sst"
    if suffix == ".blob":
        return "blob"
    if "tmp" in path.name.lower() or suffix in {".temp", ".tmp"}:
        return "temp"
    return "metadata"


def space(ssd, hdd):
    """Report logical paths and allocated blocks globally deduped by dev+ino."""
    logical = {}
    inodes = {}
    file_counts = {}
    sst_file_sizes = []
    for base in (ssd, hdd):
        for path in [base, *base.rglob("*")]:
            if path.is_symlink():
                continue
            info = path.stat()
            kind = category(path, ssd, hdd) if path.is_file() else "metadata"
            logical[kind] = logical.get(kind, 0) + info.st_size
            file_counts[kind] = file_counts.get(kind, 0) + 1
            if kind == "ssd_sst":
                sst_file_sizes.append(info.st_size)
            key = (info.st_dev, info.st_ino)
            if key not in inodes:
                inodes[key] = {"allocated": info.st_blocks * 512,
                               "categories": set(), "paths": 0}
            inodes[key]["categories"].add(kind)
            inodes[key]["paths"] += 1
    physical_unique = sum(item["allocated"] for item in inodes.values())
    physical_by_category = {}
    physical_exclusive = {}
    hardlinked_paths = 0
    for item in inodes.values():
        for kind in item["categories"]:
            physical_by_category[kind] = (physical_by_category.get(kind, 0) +
                                          item["allocated"])
        if len(item["categories"]) == 1:
            kind = next(iter(item["categories"]))
            physical_exclusive[kind] = (physical_exclusive.get(kind, 0) +
                                        item["allocated"])
        hardlinked_paths += item["paths"] - 1
    return {"logical_bytes_by_path_category": logical,
            "file_paths_by_category": file_counts,
            "physical_allocated_unique_bytes": physical_unique,
            "physical_allocated_by_category_unique_inodes": physical_by_category,
            "physical_allocated_exclusive_category_bytes": physical_exclusive,
            "hardlinked_extra_paths": hardlinked_paths,
            "ssd_sst_file_sizes": sst_file_sizes,
            "unique_inodes": len(inodes),
            "note": "Category totals can overlap if an inode has links in "
                    "multiple categories; global total never double counts."}


def command_for(args, settings, ssd, hdd, mode, budget, workload):
    return [str(args.binary), "--metabypass_mode=sst_tiering",
            "--db=" + str(ssd / "index"),
            "--metabypass_data_dir=" + str(hdd / "data"),
            "--metabypass_backup_dir=" + str(hdd / "backup"),
            "--num=" + str(settings["keys"]),
            "--value_size=" + str(settings["value_size"]),
            "--metabypass_sst_mode=" + mode,
            "--metabypass_sst_capacity_bytes=" + str(budget or 0),
            "--metabypass_sst_workload=" + workload,
            "--metabypass_sst_seed=" + str(args.seed),
            "--metabypass_sst_warmup_ops=" + str(settings["warmup_ops"]),
            "--metabypass_sst_measure_ops=" + str(settings["measure_ops"]),
            "--metabypass_sst_target_ops_per_sec=" +
            str(settings["target_ops_per_sec"]),
            "--metabypass_sst_window_ms=" + str(settings["window_ms"]),
            "--metabypass_sst_interval_ms=" + str(settings["interval_ms"]),
            "--metabypass_sst_half_life_ms=" + str(settings["half_life_ms"]),
            "--metabypass_sst_residency_ms=" + str(settings["residency_ms"]),
            "--metabypass_sst_read_delay_us=" + str(args.read_delay_us),
            "--metabypass_sst_cache_bytes=" + str(
                args.cache_bytes or settings["cache_bytes"]),
            "--metabypass_sst_sample_one_in=" + str(args.sample_one_in),
            "--metabypass_sst_flush_every=" + str(
                args.flush_every or settings["flush_every"])]


def trial_result(args, settings, ssd, hdd, mode, budget, workload):
    (hdd / "data").mkdir(parents=True)
    (hdd / "backup").mkdir(parents=True)
    command = command_for(args, settings, ssd, hdd, mode, budget, workload)
    result = execute(command, settings["timeout_s"])
    result["space"] = space(ssd, hdd)
    result["ssd_dir"] = str(ssd)
    result["hdd_dir"] = str(hdd)
    result["mode"] = mode
    result["budget_bytes"] = budget
    result["workload"] = workload
    return analyze_result(result, settings, mode, workload)


def analyze_result(result, settings, mode, workload):
    """Reparse raw stdout and evaluate one completed trial without running it."""
    result["events"], result["parse_errors"] = parse_events(result["stdout"])
    summary = [e for e in result["events"] if e.get("event") == "summary"]
    verification = [e for e in result["events"] if e.get("event") == "verification"]
    measured = [e for e in result["events"] if e.get("event") == "phase_end"
                and e.get("name") == "measured"]
    windows = [e for e in result["events"] if e.get("event") == "window"]
    phase_stats = {name: [e for e in result["events"] if
                          e.get("event") == "phase_stats" and
                          e.get("name") == name]
                   for name in ("fill", "warmup", "measured")}
    counters = ("sampled_reads", "promotions", "demotions")
    gauges = ("ssd_sst_bytes", "hdd_sst_bytes", "protected_bytes",
              "unprotected_bytes", "reserved_bytes", "pending_delete_bytes")
    phase_stats_valid = all(len(rows) == 1 and all(
        isinstance(rows[0].get(field), int) and rows[0][field] >= 0
        for field in counters + gauges) for rows in phase_stats.values())
    phase_evidence = {"valid": phase_stats_valid}
    if phase_stats_valid:
        fill, warmup, measured_stats = (phase_stats[name][0] for name in
                                        ("fill", "warmup", "measured"))
        phase_stats_valid = all(
            fill[field] <= warmup[field] <= measured_stats[field]
            for field in counters)
        cold_lower_bound = max(0,
            measured_stats["protected_bytes"] +
            measured_stats["unprotected_bytes"] -
            measured_stats["ssd_sst_bytes"])
        phase_evidence = {
            "valid": phase_stats_valid,
            "fill_migrations": fill["promotions"] + fill["demotions"],
            "warmup_migrations": (
                warmup["promotions"] + warmup["demotions"] -
                fill["promotions"] - fill["demotions"]),
            "measured_migrations": (
                measured_stats["promotions"] + measured_stats["demotions"] -
                warmup["promotions"] - warmup["demotions"]),
            "warmup_sampled_reads": (warmup["sampled_reads"] -
                                     fill["sampled_reads"]),
            "measured_sampled_reads": (measured_stats["sampled_reads"] -
                                       warmup["sampled_reads"]),
            "cold_live_bytes_lower_bound": cold_lower_bound,
            "cold_lower_bound_formula":
                "max(0, protected_bytes + unprotected_bytes - ssd_sst_bytes)",
            "phase_stats": {name: phase_stats[name][0] for name in phase_stats},
        }
    result["phase_evidence"] = phase_evidence
    distribution = {"valid": settings["measure_ops"] == 0,
                    "reason": "calibration" if settings["measure_ops"] == 0
                    else "missing summary"}
    if len(summary) == 1 and settings["measure_ops"] > 0:
        hot_gets = summary[0].get("measured_hot_gets")
        cold_gets = summary[0].get("measured_cold_gets")
        cold_mask = summary[0].get("measured_cold_key_mod10_mask")
        if all(isinstance(item, int) for item in
               (hot_gets, cold_gets, cold_mask)):
            total_gets = hot_gets + cold_gets
            hot_fraction = hot_gets / total_gets if total_gets else None
            expected_hot = (hot_gets == 0 if workload == "uniform" else
                            hot_fraction is not None and
                            0.70 <= hot_fraction <= 0.90)
            distribution = {
                "valid": (total_gets >= 100 and expected_hot and
                          cold_mask == 0x3ff),
                "hot_gets": hot_gets, "cold_gets": cold_gets,
                "hot_fraction": hot_fraction,
                "cold_key_mod10_mask": cold_mask,
                "reason": "80/20 nonoverlapping key populations; "
                          "cold keys must cover all decimal endings",
            }
    result["distribution"] = distribution
    result["valid"] = (result["exit_code"] == 0 and not result["timed_out"]
                       and not result["parse_errors"] and len(summary) == 1
                       and phase_stats_valid
                       and distribution["valid"]
                       and summary[0].get("ok") is True and
                       len(verification) == 1 and
                       verification[0].get("keys") == settings["keys"] and
                       verification[0].get("ok") is True and
                       len(measured) == 1 and measured[0].get("ok") is True and
                       (settings["measure_ops"] == 0 or len(windows) > 0))
    sampled_in_measurement = phase_evidence.get("measured_sampled_reads", 0)
    migrations_in_measurement = phase_evidence.get("measured_migrations", 0)
    warmup_migrations = phase_evidence.get("warmup_migrations", 0)
    if not phase_stats_valid or sampled_in_measurement == 0:
        mechanism_basis = "none"
    elif mode == "observe":
        mechanism_basis = "measured_sampling"
    elif mode == "adaptive" and migrations_in_measurement > 0:
        mechanism_basis = "measured_migration"
    elif mode == "adaptive" and warmup_migrations > 0 and phase_evidence[
            "cold_live_bytes_lower_bound"] > 0:
        mechanism_basis = "warmup_settled_cold"
    else:
        mechanism_basis = "none"
    result["mechanism_basis"] = mechanism_basis
    result["mechanism_observed"] = mechanism_basis != "none"
    result["sampled_reads_measured"] = sampled_in_measurement
    result["migrations_measured"] = migrations_in_measurement
    result["migrations_warmup"] = warmup_migrations
    result["migrations_fill"] = phase_evidence.get("fill_migrations", 0)
    result["window_sampled_reads_measured"] = sum(
        e.get("sampled_reads_delta", 0) for e in windows)
    result["window_migrations_measured"] = sum(
        e.get("promotions_delta", 0) + e.get("demotions_delta", 0)
        for e in windows)
    switches = [e for e in result["events"] if e.get("event") ==
                "hotspot_switch"]
    post_switch = [e for e in windows if e.get("phase_elapsed_us", 0) >=
                   (switches[0]["elapsed_us"] if switches else float("inf"))
                   and e.get("promotions_delta", 0) > 0]
    result["hotspot_adaptation_us"] = (
        post_switch[0]["phase_elapsed_us"] - switches[0]["elapsed_us"]
        if switches and post_switch else None)
    return result


def save(path, report):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temp.replace(path)


def mechanism_coverage(trials):
    coverage = {
        "adaptive_trials": sum(row["mode"] == "adaptive" for row in trials),
        "adaptive_trials_with_evidence": sum(
            row["mode"] == "adaptive" and row["mechanism_observed"]
            for row in trials),
        "all_adaptive_observed": all(
            row["mechanism_observed"] for row in trials
            if row["mode"] == "adaptive"),
        "all_observe_sampled": all(
            row["sampled_reads_measured"] > 0 for row in trials
            if row["mode"] == "observe"),
        "disabled_without_sampling": all(
            row["sampled_reads_measured"] == 0 for row in trials
            if row["mode"] == "disabled"),
    }
    switch_trials = [row for row in trials if row["mode"] == "adaptive"
                     and row["workload"] == "switch"]
    coverage["switch_adaptive_trials"] = len(switch_trials)
    coverage["switch_trials_with_post_switch_promotion"] = sum(
        row["hotspot_adaptation_us"] is not None for row in switch_trials)
    return coverage


def final_status(trials, preflight, coverage):
    if not all(row["valid"] for row in trials):
        return "failed_trials"
    if not preflight["capacity_pressure_covered"] or not all(
            coverage[key] for key in ("all_adaptive_observed",
                                      "all_observe_sampled",
                                      "disabled_without_sampling")):
        return "coverage_incomplete"
    return "complete"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--ssd-root", type=Path, required=True)
    parser.add_argument("--hdd-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=PROFILES, default="smoke")
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--read-delay-us", type=int, default=0)
    parser.add_argument("--cache-bytes", type=int, default=None)
    parser.add_argument("--sample-one-in", type=int, default=64)
    parser.add_argument("--flush-every", type=int, default=None)
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.ssd_root = args.ssd_root.resolve()
    args.hdd_root = args.hdd_root.resolve()
    args.output = args.output.resolve()
    workloads = args.workloads.split(",")
    if (any(item not in WORKLOADS for item in workloads) or not workloads or
            len(workloads) != len(set(workloads))):
        parser.error("unknown or empty workloads")
    repeats = args.repeats if args.repeats is not None else (
        1 if args.profile == "smoke" else 3)
    if (repeats < 1 or args.read_delay_us < 0 or
            (args.cache_bytes is not None and args.cache_bytes < 1) or
            args.sample_one_in < 1 or
            (args.flush_every is not None and args.flush_every < 1)):
        parser.error("invalid repeats/delay/cache/sample/flush setting")
    if args.profile == "standard" and repeats < 3:
        parser.error("standard profile requires at least three repeats")
    if not args.binary.is_file():
        parser.error("binary does not exist")
    if args.ssd_root == args.hdd_root or args.ssd_root in args.hdd_root.parents or \
            args.hdd_root in args.ssd_root.parents:
        parser.error("SSD and HDD roots must be disjoint")
    if args.output.exists():
        parser.error("output exists; use a new file for each experiment")
    if any(path == ROOT or ROOT in path.parents for path in
           (args.ssd_root, args.hdd_root, args.output)):
        parser.error("trial roots and output must be outside the source checkout")
    for root in (args.ssd_root, args.hdd_root):
        if root.exists() and any(root.iterdir()):
            parser.error("roots must be empty; runner never clears existing data")
        root.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    settings = PROFILES[args.profile].copy()
    source = source_identity()
    report = {"protocol": PROTOCOL, "created_unix": time.time(),
              "profile": args.profile, "settings": settings,
              "repeats": repeats, "workloads": workloads,
              "configurations": [dict(mode=mode, budget_percent=percent)
                                 for mode, percent in CONFIGS],
              "seed": args.seed, "read_delay_us": args.read_delay_us,
              "benchmark_overrides": {
                  "cache_bytes": args.cache_bytes,
                  "sample_one_in": args.sample_one_in,
                  "flush_every": args.flush_every},
              "read_delay_contract": "RandomAccessFile Read/MultiRead of .sst "
                                     "under backup/sst-store only; not a "
                                     "physical HDD model",
              "host": {"uname": tuple(os.uname()),
                       "clock_ticks_per_second": os.sysconf("SC_CLK_TCK")},
              "source": source,
              "binary": {"path": str(args.binary),
                         "sha256": sha256(args.binary)},
              "roots": {"ssd": str(args.ssd_root),
                        "hdd": str(args.hdd_root)},
              "same_device": (args.ssd_root.stat().st_dev ==
                              args.hdd_root.stat().st_dev),
              "trials": []}
    save(args.output, report)
    # The first fresh disabled run fixes the SST budget for every subsequent
    # configuration, independent of compaction or workload growth later on.
    def run_at(label, mode, budget, workload):
        ssd_dir = args.ssd_root / label
        hdd_dir = args.hdd_root / label
        ssd_dir.mkdir()
        hdd_dir.mkdir()
        return trial_result(args, settings, ssd_dir, hdd_dir, mode,
                            budget, workload)
    calibration_settings = settings.copy()
    calibration_settings["warmup_ops"] = 0
    calibration_settings["measure_ops"] = 0
    original_settings = settings
    settings = calibration_settings
    baseline = run_at("baseline", "disabled", None, "uniform")
    settings = original_settings
    report["calibration"] = baseline
    baseline_bytes = baseline["space"]["logical_bytes_by_path_category"].get(
        "ssd_sst", 0)
    report["baseline_sst_logical_bytes"] = baseline_bytes
    if not args.keep_data and baseline["valid"]:
        shutil.rmtree(args.ssd_root / "baseline")
        shutil.rmtree(args.hdd_root / "baseline")
    if not baseline["valid"] or baseline_bytes <= 0:
        report["status"] = "calibration_failed"
        save(args.output, report)
        raise SystemExit("calibration failed; see output JSON")
    report["budgets"] = {str(p): max(1, baseline_bytes * p // 100)
                         for p in (25, 50, 75)}
    baseline_sizes = baseline["space"]["ssd_sst_file_sizes"]
    report["preflight"] = {
        "sst_count": len(baseline_sizes),
        "sst_logical_to_cache_ratio": baseline_bytes /
        (args.cache_bytes or settings["cache_bytes"]),
        "eligible_sst_files_by_budget": {
            str(p): sum(size <= report["budgets"][str(p)] * 0.9
                        for size in baseline_sizes) for p in (25, 50, 75)},
    }
    report["preflight"]["capacity_pressure_covered"] = (
        len(baseline_sizes) >= 4 and baseline_bytes >=
        (args.cache_bytes or settings["cache_bytes"]) * 4
        and all(count >= 1 for count in
                report["preflight"]["eligible_sst_files_by_budget"].values()))
    save(args.output, report)
    if not report["preflight"]["capacity_pressure_covered"]:
        report["status"] = "capacity_preflight_failed"
        save(args.output, report)
        raise SystemExit("insufficient SST capacity pressure; see output JSON")
    for repeat in range(repeats):
        for workload in workloads:
            for mode, percent in CONFIGS:
                label = f"r{repeat}-{workload}-{mode}-{percent or 0}"
                budget = report["budgets"][str(percent)] if percent else None
                result = run_at(label, mode, budget, workload)
                result["repeat"] = repeat
                result["budget_percent"] = percent
                report["trials"].append(result)
                save(args.output, report)
                if not args.keep_data and result["valid"]:
                    shutil.rmtree(args.ssd_root / label)
                    shutil.rmtree(args.hdd_root / label)
                print(label, "valid=" + str(result["valid"]),
                      "mechanism=" + str(result["mechanism_observed"]),
                      flush=True)
    report["mechanism_coverage"] = mechanism_coverage(report["trials"])
    report["status"] = final_status(report["trials"], report["preflight"],
                                    report["mechanism_coverage"])
    disabled = {(row["repeat"], row["workload"]): row for row in
                report["trials"] if row["mode"] == "disabled"}
    report["space_comparisons"] = []
    for row in report["trials"]:
        if row["mode"] == "disabled":
            continue
        reference = disabled[(row["repeat"], row["workload"])]
        def ssd_sst_physical(trial):
            return trial["space"][
                "physical_allocated_by_category_unique_inodes"].get(
                    "ssd_sst", 0)
        report["space_comparisons"].append({
            "repeat": row["repeat"], "workload": row["workload"],
            "mode": row["mode"], "budget_percent": row["budget_percent"],
            "both_valid": row["valid"] and reference["valid"],
            "ssd_sst_category_allocated_delta_bytes": (
                ssd_sst_physical(reference) - ssd_sst_physical(row)),
            "ssd_physical_saved_bytes": (
                ssd_sst_physical(reference) - ssd_sst_physical(row)
                if not report["same_device"] else None),
            "total_physical_delta_bytes": (
                row["space"]["physical_allocated_unique_bytes"] -
                reference["space"]["physical_allocated_unique_bytes"]),
        })
    report["binary"]["sha256_at_end"] = sha256(args.binary)
    if report["binary"]["sha256_at_end"] != report["binary"]["sha256"]:
        report["status"] = "binary_changed"
    save(args.output, report)
    if report["status"] != "complete":
        raise SystemExit(report["status"] + "; see output JSON")


if __name__ == "__main__":
    main()
