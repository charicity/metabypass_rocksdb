#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Run bounded comparison or current-only mechanism experiments."""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import tempfile
import time

from build import identity, sha256

HERE = Path(__file__).resolve().parent
PROTOCOL = "metabypass-quick-v1"
VARIANTS = ("current", "baseline", "upstream")
SCENARIOS = ("control", "slow", "pressure")
SETTINGS = {
    "value_size": 1024, "writers": 1, "sync": False, "wal": True,
    "compression": "none", "staging": False, "interval_ms": 1000,
    "queue_bytes": 64 * 1024**2, "batch_bytes": 256 * 1024,
    "pressure": {"key_size": 128, "write_buffer_size": 256 * 1024,
                 "target_file_size_base": 256 * 1024,
                 "max_bytes_for_level_base": 1024 * 1024,
                 "level0_trigger": 2, "max_background_jobs": 4,
                 "queue_bytes": 2 * 1024**2, "batch_bytes": 64 * 1024},
    "delay_us": {"control": 0, "slow": 1000, "pressure": 1000},
    "delay_contract": "selected writable Append/Sync/Fsync and named SyncFile; "
                      "no read, bandwidth, directory-sync or power-loss model",
    "layout": {"current": "fast index; slow data+backup; no staging",
               "baseline": "fast index; slow data; no backup",
               "upstream": "all database files slow; ordinary inline values"},
}
BACKPRESSURE_COUNT = 16384
BACKPRESSURE_SETTINGS = {
    "value_size": 1024, "key_size": 128, "writers": 1, "sync": False,
    "wal": True, "compression": "none", "staging": False,
    "interval_ms": 1000, "queue_bytes": 2 * 1024**2,
    "batch_bytes": 64 * 1024, "delay_us": 1000,
    "delay_paths": ["backup"],
    "delay_contract": "backup writable Append/Sync/Fsync and named SyncFile; "
                      "no index/data/read/directory-sync delay",
    "layout": "current only; fast index and data; delayed backup",
    "write_count": BACKPRESSURE_COUNT, "trials": 1, "warmup": False,
    "calibration": False,
}


class RunFailure(RuntimeError):
    pass


def capture(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5)
        return {"command": command, "exit": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "error": str(error)}


def allocated_bytes(root):
    """Count allocated blocks once per inode, including directory blocks."""
    seen = set()
    total = 0
    for path in [root, *root.rglob("*")]:
        info = path.lstat()
        key = info.st_dev, info.st_ino
        if key not in seen:
            seen.add(key)
            total += info.st_blocks * 512
    return total


def history():
    specs = [
        ("version-results.json", "version-comparison.md",
         "original=e7663cf90; incremental=296800123; fixed=bbd60f2ca; "
         "1 KiB, 50k/200k, five trials, no injected delay; shared ext4 VM"),
        ("no-backup-results.json", "version-comparison.md",
         "separated no-backup baseline, 50k/200k, five trials; measured "
         "after historical backup versions, not interleaved"),
        ("channel-no-backup-results.json", "channel-baseline-comparison.md",
         "100k, five trials; current means historical notification version, "
         "channels means dual-channel version; no injected delay"),
    ]
    items = []
    for filename, document, context in specs:
        path = HERE.parent / filename
        doc = HERE.parents[1] / document
        item = {"path": str(path), "document": str(doc), "context": context}
        if path.exists():
            data = json.loads(path.read_text())
            item.update(sha256=sha256(path), summary=data.get("summary", {}),
                        provenance={key: data[key] for key in
                                    ("binaries", "binary", "sha256", "build",
                                     "source_sha256", "measurement_order")
                                    if key in data})
            item["document_text"] = doc.read_text() if doc.exists() else None
        else:
            item["missing"] = True
        items.append(item)
    return items


def summarize(rows):
    summary = {}
    for scenario in SCENARIOS:
        summary[scenario] = {}
        for variant in VARIANTS:
            samples = [r for r in rows if r["phase"] == "measured" and
                       r["scenario"] == scenario and r["variant"] == variant and
                       r.get("valid")]
            merged = []
            for row in samples:
                metrics = dict(row["write"]["metrics"])
                metrics["ops_per_second"] = (metrics["successful_ops"] * 1e6 /
                                             max(1, metrics["foreground_us"]))
                metrics["allocated_bytes"] = row["allocated_bytes"]
                if "restore_us" in row["verify"]["metrics"]:
                    metrics["restore_us"] = row["verify"]["metrics"]["restore_us"]
                merged.append(metrics)
            common = set.intersection(*(set(m) for m in merged)) if merged else set()
            summary[scenario][variant] = {
                "samples": len(samples),
                "metrics": {key: {"median": statistics.median(m[key] for m in merged),
                                  "min": min(m[key] for m in merged),
                                  "max": max(m[key] for m in merged)}
                            for key in sorted(common)},
            }
    return summary


def summarize_backpressure(rows):
    samples = [r for r in rows if r["phase"] == "measured" and
               r["scenario"] == "backpressure" and r["variant"] == "current" and
               r.get("valid")]
    if not samples:
        return {"samples": 0, "metrics": {}}
    row = samples[0]
    metrics = dict(row["write"]["metrics"])
    metrics["verified_ops"] = row["verify"]["metrics"]["successful_ops"]
    metrics["restore_us"] = row["verify"]["metrics"]["restore_us"]
    return {"samples": len(samples), "metrics": metrics}


def backpressure_report_markdown(report):
    metrics = report["summary"]["metrics"]
    lines = ["# Metabypass backpressure mechanism validation", "",
             "State: **" + report["state"] + "**; profile: backpressure; "
             "type: mechanism_validation; current-only.", "",
             "Injection: backup writable Append/Sync/Fsync and named SyncFile "
             "at 1000 us per call. Index and data paths have no injected delay.",
             "", "| Metric | Value |", "|---|---:|"]
    fields = (("successful_ops", "Writes"), ("verified_ops", "Verified values"),
              ("recovery_points", "Published recovery points"),
              ("foreground_backpressure_us", "Foreground backpressure us"),
              ("backpressure_us", "Total backpressure us"),
              ("queue_peak_bytes", "Queue peak bytes"),
              ("queue_capacity_bytes", "Queue capacity bytes"),
              ("injected_wait_us", "Injected wait us"),
              ("foreground_us", "Put loop us"),
              ("sync_us", "Backup barrier us"),
              ("close_us", "Close us"),
              ("total_us", "Write lifecycle us"),
              ("restore_us", "Restore + Open us"))
    for key, label in fields:
        lines.append(f"| {label} | {metrics.get(key, 'N/A')} |")
    lines += ["", "Validation includes deleting this run's index, Restore + Open, "
              "all-value verification, an appended write, and reopen verification."]
    if report.get("error"):
        lines += ["", "Failure/incompletion: " + report["error"]]
    return "\n".join(lines) + "\n"


def report_markdown(report):
    lines = ["# Metabypass quick comparison", "",
             "State: **" + report["state"] + "**; profile: " + report["profile"] +
             "; protocol: " + PROTOCOL + ".",
             "Smoke results validate feasibility only, not performance.", "",
             "Medians [min, max]; only writes with successful content/reopen "
             "verification are included. Raw failed/incomplete runs remain in JSON.",
             "", "| Scenario | Variant | n | ops/s | Put P99 us | Total ms | "
             "Restore ms | CPU ms | RSS MiB | Disk MiB |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    def cell(metrics, key, scale=1):
        if key not in metrics:
            return "N/A"
        m = metrics[key]
        return (f"{m['median']/scale:.2f} "
                f"[{m['min']/scale:.2f}, {m['max']/scale:.2f}]")
    for scenario in SCENARIOS:
        for variant in VARIANTS:
            group = report["summary"][scenario][variant]
            m = group["metrics"]
            lines.append(f"| {scenario} | {variant} | {group['samples']} | " +
                         " | ".join((cell(m, "ops_per_second"),
                                     cell(m, "p99_ns", 1000),
                                     cell(m, "total_us", 1000),
                                     cell(m, "restore_us", 1000),
                                     cell(m, "cpu_total_us", 1000),
                                     cell(m, "peak_rss_kib", 1024),
                                     cell(m, "allocated_bytes", 1024**2))) + " |")
    lines += ["", "Total = foreground + backup barrier + Close/destruction; "
              "excludes Open and verification. Baselines do not publish recovery "
              "points. CPU/RSS cover the complete write process.", "",
              "## Same-session comparisons", "",
              "Positive time/P99 change means higher cost; positive throughput "
              "change means faster. Layout differences are included versus upstream.",
              "", "| Scenario | Current relative to | Throughput | Total | P99 |",
              "|---|---|---:|---:|---:|"]
    for scenario in SCENARIOS:
        groups = report["summary"][scenario]
        for reference in ("baseline", "upstream"):
            a, b = groups["current"], groups[reference]
            changes = []
            for key in ("ops_per_second", "total_us", "p99_ns"):
                if a["samples"] != report["trials"] or b["samples"] != report["trials"]:
                    changes.append("incomplete")
                elif b["metrics"][key]["median"] == 0:
                    changes.append("N/A")
                else:
                    changes.append(f"{100*(a['metrics'][key]['median']/b['metrics'][key]['median']-1):+.1f}%")
            lines.append(f"| {scenario} | {reference} | " + " | ".join(changes) + " |")
    lines += ["", "## Coverage", ""]
    for scenario in SCENARIOS:
        samples = [r for r in report["runs"] if r["phase"] == "measured" and
                   r["variant"] == "current" and r["scenario"] == scenario and r.get("valid")]
        covered = {key: sum(r["write"]["metrics"].get(key, 0) > 0 for r in samples)
                   for key in ("foreground_flushes", "foreground_compactions", "backpressure_us")}
        lines.append(f"- {scenario}: count={report['counts'].get(scenario, 'not calibrated')}; "
                     f"runs with foreground flush/compaction/backpressure="
                     f"{covered['foreground_flushes']}/{covered['foreground_compactions']}/"
                     f"{covered['backpressure_us']} of {len(samples)}. "
                     "Zero means that mechanism was not exercised.")
    lines += ["", "## Historical records (independent experiments)", "",
              "Do not compute speedups between these records and this session.", "",
              "| Dataset / original workload | Original label | Foreground ms | Total ms |",
              "|---|---|---:|---:|"]
    for item in report["history"]:
        if item.get("missing"):
            lines.append(f"| {Path(item['path']).name} | missing | N/A | N/A |")
            continue
        for scenario, groups in item["summary"].items():
            if "foreground_us" in groups:
                groups = {"no-backup": groups}
            for variant, metrics in groups.items():
                total = "total_us" if "total_us" in metrics else "write_sync_close_us"
                lines.append(f"| {Path(item['path']).stem} / {scenario} | {variant} | "
                             f"{cell(metrics, 'foreground_us', 1000)} | "
                             f"{cell(metrics, total, 1000)} |")
    for item in report["history"]:
        lines += ["", "- " + item["context"] + "; source: " + item["path"]]
    if report.get("error"):
        lines += ["", "Failure/incompletion: " + report["error"]]
    return "\n".join(lines) + "\n"


def validate_result(result, action, count, variant, scenario=None):
    if result.get("status") != "OK" or result["exit"] != 0:
        raise RunFailure("non-OK driver result")
    m = result.get("metrics", {})
    if m.get("successful_ops") != count:
        raise RunFailure("incorrect successful operation count")
    if scenario == "backpressure" and (variant != "current" or count != BACKPRESSURE_COUNT):
        raise RunFailure("backpressure requires 16384 current writes")
    if action == "write":
        required = ("foreground_us", "sync_us", "close_us", "total_us", "p99_ns",
                    "cpu_user_us", "cpu_system_us", "peak_rss_kib")
        if any(key not in m for key in required):
            raise RunFailure("missing required write metrics")
        if m["total_us"] != sum(m[k] for k in ("foreground_us", "sync_us", "close_us")):
            raise RunFailure("inconsistent total time")
        if variant == "current" and m.get("recovery_points", 0) == 0:
            raise RunFailure("no published recovery point")
        if scenario == "backpressure":
            required = ("foreground_backpressure_us", "backpressure_us",
                        "queue_peak_bytes", "queue_capacity_bytes", "recovery_points",
                        "injected_wait_us", "delayed_append_calls",
                        "delayed_sync_calls", "delayed_fsync_calls",
                        "delayed_named_sync_calls")
            if any(type(m.get(key)) is not int for key in required):
                raise RunFailure("missing or invalid backpressure metric")
            if m["foreground_backpressure_us"] <= 0 or m["backpressure_us"] <= 0:
                raise RunFailure("backpressure was not observed")
            capacity = m["queue_capacity_bytes"]
            if (capacity != BACKPRESSURE_SETTINGS["queue_bytes"] or
                    not 0 < m["queue_peak_bytes"] <= capacity):
                raise RunFailure("invalid queue peak or capacity")
            delay_calls = sum(m[key] for key in required if key.startswith("delayed_"))
            if m["injected_wait_us"] <= 0 or delay_calls <= 0:
                raise RunFailure("backup delay was not observed")
    elif variant == "current" and "restore_us" not in m:
        raise RunFailure("missing restore metric")


def execute(command, timeout):
    start = time.monotonic()
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, start_new_session=True) as process:
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
            if isinstance(error, KeyboardInterrupt):
                raise
            timed_out = True
    result = {"command": command, "exit": process.returncode,
              "elapsed_seconds": time.monotonic() - start,
              "stdout": stdout, "stderr": stderr, "timed_out": timed_out}
    for line in reversed(stdout.splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "status" in payload:
            result.update(status=payload["status"], metrics=payload.get("metrics", {}))
            break
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--build", type=Path, required=True, help="build.json from build.py")
    p.add_argument("--output", type=Path, required=True, help="new result directory")
    p.add_argument("--data-parent", type=Path, required=True, help="existing local disk directory")
    p.add_argument("--profile", choices=("smoke", "quick", "standard", "backpressure"),
                   default="standard")
    p.add_argument("--cpus", help="comma-separated CPU IDs; default first eight available")
    p.add_argument("--counts-from", type=Path,
                   help="reuse frozen counts from a complete quick/standard results.json")
    p.add_argument("--budget-minutes", type=float,
                   help="default smoke/backpressure=5, quick=30, standard=60; maximum 60")
    p.add_argument("--timeout-seconds", type=float, default=180)
    p.add_argument("--keep-data", action="store_true")
    a = p.parse_args()
    if a.timeout_seconds <= 0 or not math.isfinite(a.timeout_seconds):
        p.error("timeout must be finite and positive")
    budget = a.budget_minutes if a.budget_minutes is not None else {
        "smoke": 5, "quick": 30, "standard": 60, "backpressure": 5}[a.profile]
    if not 0 < budget <= 60:
        p.error("budget must be in (0, 60] minutes")
    if a.profile in ("smoke", "backpressure") and a.counts_from:
        p.error("smoke and backpressure use fixed counts")
    output = a.output.resolve()
    if output.exists():
        p.error("--output must be a new directory; reports are never overwritten")
    parent = a.data_parent.resolve(strict=True)
    if not parent.is_dir():
        p.error("data parent must be an existing directory")
    if shutil.disk_usage(parent).free < 2 * 1024**3:
        p.error("at least 2 GiB free space required")
    filesystem = capture(["findmnt", "-T", str(parent), "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"])
    if filesystem.get("exit") != 0:
        p.error("findmnt is required to identify the benchmark filesystem")
    fstype = json.loads(filesystem["stdout"])["filesystems"][0]["fstype"]
    if fstype in ("tmpfs", "ramfs", "nfs", "nfs4", "cifs", "smb3"):
        p.error("use a local disk filesystem, not " + fstype)
    allowed = sorted(os.sched_getaffinity(0))
    cpus = [int(cpu) for cpu in a.cpus.split(",")] if a.cpus else allowed[:8]
    if not cpus or not set(cpus) <= set(allowed):
        p.error("CPU IDs must be inside the current affinity mask")
    os.sched_setaffinity(0, cpus)
    build = json.loads(a.build.read_text())
    if build.get("protocol") != PROTOCOL or build.get("build_type") != "Release":
        p.error("a release build manifest for this protocol is required")
    if build["driver_sha256"] != sha256(HERE / "driver.cc"):
        p.error("driver changed since build; rebuild before running")
    if build["source"] != identity():
        p.error("source changed since build; rebuild before running")
    for binary in build["binaries"].values():
        if sha256(binary["path"]) != binary["sha256"]:
            p.error("binary hash does not match build manifest")
    mechanism = a.profile == "backpressure"
    trials = {"smoke": 1, "quick": 3, "standard": 5, "backpressure": 1}[a.profile]
    counts = ({"backpressure": BACKPRESSURE_COUNT} if mechanism else
              {scenario: 64 for scenario in SCENARIOS} if a.profile == "smoke" else {})
    if a.counts_from:
        previous = json.loads(a.counts_from.read_text())
        if (previous.get("protocol") != PROTOCOL or previous.get("settings") != SETTINGS or
                previous.get("profile") not in ("quick", "standard") or
                previous.get("report_type", "comparison") != "comparison" or
                previous.get("state") != "complete"):
            p.error("counts source must be a complete compatible quick/standard report")
        counts = previous.get("counts")
        if not isinstance(counts, dict):
            p.error("counts source is missing frozen counts")
        for scenario in SCENARIOS:
            count = counts.get(scenario)
            if type(count) is not int or not 2000 <= count <= (200000 if scenario == "control" else 50000):
                p.error("invalid frozen counts")
    output.mkdir(parents=True)
    root = Path(tempfile.mkdtemp(prefix="mb-quick-", dir=parent))
    started = time.monotonic()
    report = {"protocol": PROTOCOL, "profile": a.profile, "trials": trials,
              "state": "running", "build": build,
              "report_type": "mechanism_validation" if mechanism else "comparison",
              "settings": BACKPRESSURE_SETTINGS if mechanism else SETTINGS,
              "scenarios": ["backpressure"] if mechanism else list(SCENARIOS),
              "variants": ["current"] if mechanism else list(VARIANTS),
              "counts": counts, "counts_from": str(a.counts_from) if a.counts_from else None,
              "budget_seconds": budget * 60, "data_root": str(root),
              "started_unix": time.time(), "environment": {
                  "uname": platform.uname()._asdict(), "affinity": cpus,
                  "cpu": capture(["lscpu"]), "memory": Path("/proc/meminfo").read_text(),
                  "filesystem": filesystem,
                  "loadavg": os.getloadavg(), "cache_policy": "fresh DBs; no global drop_caches"},
              "history": [] if mechanism else history(), "runs": [], "summary": {},
              "runner_sha256": sha256(Path(__file__))}

    def save():
        report["elapsed_seconds"] = time.monotonic() - started
        report["summary"] = (summarize_backpressure(report["runs"]) if mechanism
                             else summarize(report["runs"]))
        temp = output / "results.json.tmp"
        temp.write_text(json.dumps(report, indent=2) + "\n")
        temp.replace(output / "results.json")
        temp = output / "report.md.tmp"
        temp.write_text(backpressure_report_markdown(report) if mechanism else
                        report_markdown(report))
        temp.replace(output / "report.md")

    def run(variant, scenario, phase, trial, count):
        remaining = budget * 60 - (time.monotonic() - started)
        if remaining <= 0:
            raise RunFailure("global time budget exhausted")
        folder = root / f"{len(report['runs']):03d}-{phase}-{scenario}-{variant}"
        folder.mkdir()
        row = {"variant": variant, "scenario": scenario, "phase": phase,
               "trial": trial, "count": count, "folder": str(folder), "valid": False}
        report["runs"].append(row)
        save()
        binary = build["binaries"]["upstream" if variant == "upstream" else "current"]["path"]
        for action in ("write", "verify"):
            if action == "verify":
                row["allocated_bytes"] = allocated_bytes(folder)
                option_files = sorted((folder / "index").glob("OPTIONS-*"))
                row["effective_options"] = {
                    path.name: path.read_text() for path in option_files}
                if variant == "current":
                    if list((folder / "backup").rglob("*.blob")):
                        raise RunFailure("backup unexpectedly contains blob payloads")
                    # Only this newly created run's index is removed.
                    shutil.rmtree(folder / "index")
            remaining = budget * 60 - (time.monotonic() - started)
            if remaining <= 0:
                raise RunFailure("global time budget exhausted before " + action)
            delay_us = (BACKPRESSURE_SETTINGS["delay_us"] if mechanism else
                        SETTINGS["delay_us"][scenario])
            command = [binary, variant, action, str(folder), str(count), scenario,
                       str(delay_us)]
            result = execute(command, min(a.timeout_seconds, remaining))
            row[action] = result
            save()
            if result["timed_out"]:
                raise RunFailure("timeout: " + " ".join(command))
            validate_result(result, action, count, variant, scenario)
            if action == "write":
                m = result["metrics"]
                m["cpu_total_us"] = m["cpu_user_us"] + m["cpu_system_us"]
                if delay_us and m.get("injected_wait_us", 0) == 0:
                    raise RunFailure("slow scenario did not inject latency")
        row["valid"] = True
        save()
        print(phase, scenario, variant, trial, "count=", count,
              "total_ms=", row["write"]["metrics"]["total_us"] / 1000, flush=True)
        if not a.keep_data:
            shutil.rmtree(folder)
        return row

    try:
        save()
        if mechanism:
            run("current", "backpressure", "measured", 0, BACKPRESSURE_COUNT)
        else:
            for scenario_index, scenario in enumerate(SCENARIOS):
                pilots = [run(v, scenario, "warmup", -1,
                              32 if a.profile == "smoke" else 2000) for v in VARIANTS]
                if scenario not in counts:
                    slowest = max(row["write"]["metrics"]["total_us"] for row in pilots)
                    cap = 200000 if scenario == "control" else 50000
                    counts[scenario] = max(2000, min(cap, int(2000 * 30e6 / max(1, slowest))))
                    save()
                for trial in range(trials):
                    shift = (trial + scenario_index) % len(VARIANTS)
                    order = VARIANTS[shift:] + VARIANTS[:shift]
                    for variant in order:
                        run(variant, scenario, "measured", trial, counts[scenario])
        report["state"] = "complete"
    except (RunFailure, OSError, ValueError, KeyboardInterrupt) as error:
        report["state"] = "incomplete"
        report["error"] = type(error).__name__ + ": " + str(error)
        print("Stopped; logs and failed data retained:", report["error"], flush=True)
    finally:
        report["environment"]["final_loadavg"] = os.getloadavg()
        save()
    if report["state"] == "complete" and not a.keep_data:
        root.rmdir()
    print("Report:", output / "report.md", flush=True)
    return 0 if report["state"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
