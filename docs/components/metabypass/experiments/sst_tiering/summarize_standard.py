#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Preserve a compact, all-trial summary of a completed standard experiment."""

import argparse
import hashlib
import json
from pathlib import Path
from statistics import median

import run


WINDOW_FIELDS = (
    "window", "elapsed_us", "phase_elapsed_us", "ops", "gets", "scans",
    "writes", "get_ops_per_sec", "get_p50_us", "get_p95_us", "get_p99_us",
    "hot_gets", "hot_p50_us", "hot_p95_us", "hot_p99_us", "cold_gets",
    "cold_p50_us", "cold_p95_us", "cold_p99_us", "ssd_sst_bytes",
    "hdd_sst_bytes", "sampled_reads_delta", "promotions_delta",
    "demotions_delta", "promoted_bytes_delta", "demoted_bytes_delta",
    "ssd_over_budget_us_delta", "publish_lag_us", "cutoff_score")


def stats(values):
    values = [value for value in values if value is not None]
    if not values:
        return {"median": None, "min": None, "max": None, "count": 0}
    return {"median": median(values), "min": min(values),
            "max": max(values), "count": len(values)}


def identity(row):
    return (row["repeat"], row["workload"], row["mode"],
            row["budget_percent"])


def summarize_trial(row, reference, same_device):
    events, errors = run.parse_events(row["stdout"])
    if errors or events != row["events"]:
        raise ValueError("saved events differ from raw stdout: " +
                         str(identity(row)))
    summaries = [event for event in events if event.get("event") == "summary"]
    measured_ends = [event for event in events if event.get("event") ==
                     "phase_end" and event.get("name") == "measured"]
    windows = [event for event in events if event.get("event") == "window" and
               event.get("phase") == "measured"]
    if len(summaries) != 1 or len(measured_ends) != 1 or not windows:
        raise ValueError("missing measurement events: " + str(identity(row)))
    summary = summaries[0]
    duration_us = measured_ends[0]["elapsed_us"]
    if duration_us <= 0:
        raise ValueError("nonpositive measurement duration: " +
                         str(identity(row)))
    gets = sum(window["gets"] for window in windows)
    hot = [window for window in windows if window["hot_gets"] > 0]
    cold = [window for window in windows if window["cold_gets"] > 0]
    space = row["space"]
    logical = space["logical_bytes_by_path_category"]
    physical = space["physical_allocated_by_category_unique_inodes"]
    ref_logical = reference["space"]["logical_bytes_by_path_category"]
    ref_physical = reference["space"][
        "physical_allocated_by_category_unique_inodes"]
    return {
        "repeat": row["repeat"], "workload": row["workload"],
        "mode": row["mode"], "budget_percent": row["budget_percent"],
        "budget_bytes": row["budget_bytes"], "command": row["command"],
        "valid": row["valid"], "exit_code": row["exit_code"],
        "timed_out": row["timed_out"],
        "mechanism_observed": row["mechanism_observed"],
        "mechanism_basis": row["mechanism_basis"],
        "distribution": row["distribution"],
        "measured_duration_us": duration_us,
        "whole_process_elapsed_s": row["elapsed_s"],
        "measured_operations": sum(window["ops"] for window in windows),
        "measured_gets": gets,
        "measured_writes": sum(window["writes"] for window in windows),
        "measured_scans": sum(window["scans"] for window in windows),
        "measured_gets_per_second": gets * 1000000 / duration_us,
        "window_get_p50_us": stats(window["get_p50_us"] for window in windows
                                   if window["gets"] > 0),
        "window_get_p95_us": stats(window["get_p95_us"] for window in windows
                                   if window["gets"] > 0),
        "window_get_p99_us": stats(window["get_p99_us"] for window in windows
                                   if window["gets"] > 0),
        "window_hot_p99_us": stats(window["hot_p99_us"] for window in hot),
        "window_cold_p99_us": stats(window["cold_p99_us"] for window in cold),
        "sampled_reads_measured": row["sampled_reads_measured"],
        "migrations_fill": row["migrations_fill"],
        "migrations_warmup": row["migrations_warmup"],
        "migrations_measured": row["migrations_measured"],
        "promotions_measured_windows": sum(window["promotions_delta"]
                                           for window in windows),
        "demotions_measured_windows": sum(window["demotions_delta"]
                                          for window in windows),
        "promoted_bytes_measured_windows": sum(window["promoted_bytes_delta"]
                                               for window in windows),
        "demoted_bytes_measured_windows": sum(window["demoted_bytes_delta"]
                                              for window in windows),
        "ssd_over_budget_us_measured_windows": sum(
            window["ssd_over_budget_us_delta"] for window in windows),
        "peak_publish_lag_us_measured_windows": max(
            window["publish_lag_us"] for window in windows),
        "publish_lag_us_at_end": summary["publish_lag_us"],
        "hotspot_adaptation_us": row["hotspot_adaptation_us"],
        "cpu_seconds_sampled": row["cpu_seconds_sampled"],
        "peak_rss_kib": row["peak_rss_kib"],
        "summary_counters": {key: value for key, value in summary.items()
                             if key != "event"},
        "space": {
            "logical_bytes_by_path_category": logical,
            "physical_allocated_unique_bytes": space[
                "physical_allocated_unique_bytes"],
            "physical_allocated_by_category_unique_inodes": physical,
            "physical_allocated_exclusive_category_bytes": space[
                "physical_allocated_exclusive_category_bytes"],
            "hardlinked_extra_paths": space["hardlinked_extra_paths"],
            "unique_inodes": space["unique_inodes"],
            "ssd_sst_logical_saved_vs_disabled_bytes": (
                ref_logical.get("ssd_sst", 0) - logical.get("ssd_sst", 0)),
            "ssd_sst_category_allocated_saved_vs_disabled_bytes": (
                ref_physical.get("ssd_sst", 0) -
                physical.get("ssd_sst", 0)),
            "ssd_physical_saved_vs_disabled_bytes": (
                None if same_device else ref_physical.get("ssd_sst", 0) -
                physical.get("ssd_sst", 0)),
            "total_unique_physical_delta_vs_disabled_bytes": (
                space["physical_allocated_unique_bytes"] -
                reference["space"]["physical_allocated_unique_bytes"]),
        },
        "windows": [{key: window[key] for key in WINDOW_FIELDS}
                    for window in windows],
    }


def summarize_report(report, raw_sha):
    if report.get("profile") != "standard" or report.get("repeats") != 3:
        raise ValueError("requires completed three-repeat standard profile")
    required_configurations = [
        {"mode": mode, "budget_percent": percent}
        for mode, percent in run.CONFIGS]
    if (report.get("workloads") != list(run.WORKLOADS) or
            report.get("configurations") != required_configurations):
        raise ValueError("requires the full default five-workload, seven-"
                         "configuration matrix")
    if report.get("status") != "complete":
        raise ValueError("source report status is not complete")
    binary = report["binary"]
    if not binary.get("sha256_at_end") or binary["sha256"] != binary[
            "sha256_at_end"]:
        raise ValueError("benchmark binary changed or end hash is absent")
    if not report["preflight"]["capacity_pressure_covered"] or not all(
            report["mechanism_coverage"][key] for key in
            ("all_adaptive_observed", "all_observe_sampled",
             "disabled_without_sampling")):
        raise ValueError("mechanism preflight or coverage incomplete")
    configurations = required_configurations
    expected = {(repeat, workload, config["mode"], config["budget_percent"])
                for repeat in range(3) for workload in report["workloads"]
                for config in configurations}
    if len(expected) != 105:
        raise ValueError("expected exactly 105 distinct trial slots")
    rows = report["trials"]
    keys = [identity(row) for row in rows]
    if len(keys) != len(expected) or set(keys) != expected or not all(
            row["valid"] for row in rows):
        raise ValueError("incomplete, duplicate or failed trial matrix")
    disabled = {(row["repeat"], row["workload"]): row for row in rows
                if row["mode"] == "disabled"}
    trials = [summarize_trial(
        row, disabled[(row["repeat"], row["workload"])],
        report["same_device"])
              for row in rows]
    grouped = {}
    for trial in trials:
        key = (trial["workload"], trial["mode"], trial["budget_percent"])
        grouped.setdefault(key, []).append(trial)
    groups = []
    for key, members in grouped.items():
        if len(members) != 3 or {row["repeat"] for row in members} != {0, 1, 2}:
            raise ValueError("group lacks three independent repeats: " +
                             str(key))
        groups.append({
            "workload": key[0], "mode": key[1], "budget_percent": key[2],
            "repeats": [row["repeat"] for row in members],
            "all_valid": all(row["valid"] for row in members),
            "all_mechanism_observed": all(row["mechanism_observed"]
                                          for row in members),
            "measured_gets_per_second": stats(
                row["measured_gets_per_second"] for row in members),
            "median_window_get_p50_us": stats(
                row["window_get_p50_us"]["median"] for row in members),
            "median_window_get_p95_us": stats(
                row["window_get_p95_us"]["median"] for row in members),
            "median_window_get_p99_us": stats(
                row["window_get_p99_us"]["median"] for row in members),
            "max_window_get_p99_us": stats(
                row["window_get_p99_us"]["max"] for row in members),
            "median_window_hot_p99_us": stats(
                row["window_hot_p99_us"]["median"] for row in members),
            "median_window_cold_p99_us": stats(
                row["window_cold_p99_us"]["median"] for row in members),
            "ssd_sst_logical_bytes": stats(
                row["space"]["logical_bytes_by_path_category"].get(
                    "ssd_sst", 0) for row in members),
            "ssd_sst_category_allocated_bytes": stats(
                row["space"]["physical_allocated_by_category_unique_inodes"]
                .get("ssd_sst", 0) for row in members),
            "total_unique_physical_bytes": stats(
                row["space"]["physical_allocated_unique_bytes"]
                for row in members),
            "ssd_sst_logical_saved_vs_disabled_bytes": stats(
                row["space"]["ssd_sst_logical_saved_vs_disabled_bytes"]
                for row in members),
            "ssd_sst_category_allocated_saved_vs_disabled_bytes": stats(
                row["space"][
                    "ssd_sst_category_allocated_saved_vs_disabled_bytes"]
                for row in members),
            "total_unique_physical_delta_vs_disabled_bytes": stats(
                row["space"]["total_unique_physical_delta_vs_disabled_bytes"]
                for row in members),
            "measured_migrations": stats(
                row["migrations_measured"] for row in members),
            "promotions_measured_windows": stats(
                row["promotions_measured_windows"] for row in members),
            "demotions_measured_windows": stats(
                row["demotions_measured_windows"] for row in members),
            "promoted_bytes_measured_windows": stats(
                row["promoted_bytes_measured_windows"] for row in members),
            "demoted_bytes_measured_windows": stats(
                row["demoted_bytes_measured_windows"] for row in members),
            "ssd_over_budget_us_measured_windows": stats(
                row["ssd_over_budget_us_measured_windows"]
                for row in members),
            "peak_publish_lag_us_measured_windows": stats(
                row["peak_publish_lag_us_measured_windows"]
                for row in members),
            "cpu_seconds_sampled": stats(
                row["cpu_seconds_sampled"] for row in members),
            "peak_rss_kib": stats(row["peak_rss_kib"] for row in members),
            "hotspot_adaptation_us": stats(
                row["hotspot_adaptation_us"] for row in members),
            "hotspot_adaptation_observed_count": sum(
                row["hotspot_adaptation_us"] is not None for row in members),
        })
    return {
        "source_json_sha256": raw_sha,
        "source_binary": binary,
        "source_identity": report["source"],
        "source_report_status": report["status"],
        "protocol": report["protocol"],
        "profile": report["profile"], "repeats": report["repeats"],
        "workloads": report["workloads"],
        "configurations": configurations,
        "settings": report["settings"],
        "benchmark_overrides": report["benchmark_overrides"],
        "seed": report["seed"], "read_delay_us": report["read_delay_us"],
        "roots": report["roots"], "same_device": report["same_device"],
        "host": report["host"],
        "calibration": {
            "baseline_sst_logical_bytes": report["baseline_sst_logical_bytes"],
            "budgets": report["budgets"],
            "preflight": report["preflight"],
        },
        "mechanism_coverage": report["mechanism_coverage"],
        "metrics_definition": {
            "trial_get_rate": "total measured Gets / measured phase wall time",
            "trial_latency": "median and range of measured-window percentiles; "
                             "not a pooled whole-trial percentile",
            "group_latency": "median and range across three trial-level "
                             "median window percentiles",
            "hot_cold": "workload key group, not actual SSD/HDD read path",
            "space": "logical path bytes versus allocated blocks deduplicated "
                     "globally by device+inode",
            "ssd_physical_saved": "null on same-device roots",
        },
        "trial_count": len(trials), "group_count": len(groups),
        "trials": trials, "groups": groups,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve previous summaries")
    raw = args.input.read_bytes()
    report = json.loads(raw)
    try:
        result = summarize_report(report, hashlib.sha256(raw).hexdigest())
    except (KeyError, ValueError) as error:
        parser.error(str(error))
    result["source_json"] = str(args.input.resolve())
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(result["trial_count"], "trials in", result["group_count"],
          "three-repeat groups")


if __name__ == "__main__":
    main()
