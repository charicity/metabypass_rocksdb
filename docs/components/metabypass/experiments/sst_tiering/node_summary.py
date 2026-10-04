#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Summarize node_v1 evidence without averaging window percentiles."""

import argparse
import hashlib
import html
import json
import os
from pathlib import Path
import statistics


def merge_histograms(histograms, upper_bounds):
    buckets = [0] * len(upper_bounds)
    for histogram in histograms:
        incoming = histogram["buckets"]
        if len(incoming) != len(buckets) or any(v < 0 for v in incoming):
            raise ValueError("histogram schema mismatch")
        if sum(incoming) != histogram["count"]:
            raise ValueError("histogram count differs from buckets")
        for i, value in enumerate(incoming):
            buckets[i] += value
    count = sum(buckets)
    result = {"count": count, "buckets": buckets, "quantiles": "bucket_upper_bound"}
    for quantile in (50, 95, 99):
        wanted = (count * quantile + 99) // 100
        cumulative = 0
        value = None
        for bound, bucket in zip(upper_bounds, buckets):
            cumulative += bucket
            if count and cumulative >= wanted:
                value = bound
                break
        result[f"p{quantile}_us"] = value
    result["max_us"] = max((h.get("max_us", 0) for h in histograms), default=0) if count else None
    return result


def _phase_result(events, name):
    phases = [e for e in events if e.get("event") == "phase_end" and
              e.get("phase", e.get("name")) == name]
    if not phases:
        return None
    if len(phases) != 1:
        raise ValueError("multiple phase_end events for " + name)
    phase = phases[0]
    result = {k: phase.get(k) for k in ("elapsed_us", "ops", "target_ops_per_sec",
                                       "scheduled_ops", "unfinished_ops", "late_ops", "late_fraction", "stats", "ok")}
    result["throughput_ops_s"] = phase["ops"] * 1e6 / phase["elapsed_us"] if phase["elapsed_us"] else None
    schema = [e for e in events if e.get("event") == "histogram_schema"]
    if not schema:
        raise ValueError("histogram schema missing")
    bounds = schema[0]["upper_bounds_us"]
    for operation in ("get", "put", "delete"):
        item = phase[operation]
        result[operation] = {"count": item["count"]}
        for kind in ("service", "response"):
            result[operation][kind] = merge_histograms([item[kind]], bounds)
    return result


def phase_result(events, name):
    try:
        result = _phase_result(events, name)
        return {"valid": True, **result} if result is not None else None
    except (ValueError, KeyError, TypeError, IndexError) as error:
        return {"valid": False, "phase": name, "error": str(error),
                "throughput_ops_s": None, "latency_unknown": True}


def median_range(values):
    values = [v for v in values if v is not None]
    return {"n": len(values), "median": statistics.median(values) if values else None,
            "min": min(values) if values else None, "max": max(values) if values else None}


def load_samples(path):
    samples, errors = [], []
    try:
        with Path(path).open() as stream:
            for number, line in enumerate(stream, 1):
                try:
                    sample = json.loads(line)
                    if not isinstance(sample, dict):
                        raise ValueError("sample is not an object")
                    samples.append(sample)
                except ValueError as error:
                    errors.append({"line": number, "error": str(error)})
    except OSError as error:
        errors.append({"path": str(path), "error": str(error)})
    return samples, errors


def sampled_memory(command, samples):
    baseline, end = command.get("cgroup_start", {}), command.get("cgroup_end", {})
    snapshots = [baseline, *[s["cgroup"] for s in samples if "cgroup" in s], end]
    def peak(key):
        values = [s.get(key) for s in snapshots if s.get(key) is not None]
        return max(values) if values else None
    kmem_peak = peak("memory.kmem.usage_in_bytes")
    kmem_start = baseline.get("memory.kmem.usage_in_bytes")
    counter_start, counter_end = baseline.get("memory.kmem.failcnt"), end.get("memory.kmem.failcnt")
    resources = [s["proc_resources"] for s in samples if "proc_resources" in s]
    return {"kmem_measurement_status": "known" if kmem_peak is not None else "unknown",
            "baseline_usage_bytes": baseline.get("memory.usage_in_bytes"),
            "baseline_kmem_usage_bytes": kmem_start,
            "baseline_headroom_bytes": command.get("memory_budget_start", {}).get("remaining_headroom_bytes"),
            "sampled_peak_kmem_usage_bytes": kmem_peak,
            "sampled_peak_kmem_increase_bytes": kmem_peak - kmem_start if
                kmem_peak is not None and kmem_start is not None else None,
            "cgroup_lifetime_max_kmem_usage_bytes": peak("memory.kmem.max_usage_in_bytes"),
            "exit_kmem_usage_bytes": end.get("memory.kmem.usage_in_bytes"),
            "kmem_usage_delta_bytes": command.get("kmem_usage_delta_bytes"),
            "kmem_failcnt_delta": counter_end - counter_start if
                counter_start is not None and counter_end is not None else None,
            "sampled_peak_fd_count": max((r["fd_count"] for r in resources if r.get("fd_count") is not None), default=None),
            "sampled_peak_thread_count": max((r["thread_count"] for r in resources if r.get("thread_count") is not None), default=None),
            "observed_thread_processor_cpus": sorted({t["last_processor_cpu"] for r in resources
                for t in r.get("threads", []) if t.get("last_processor_cpu") is not None}),
            "proc_resource_unknown_samples": sum(r.get("status") != "known" for r in resources),
            "note": "Usage peaks include exec endpoints; max_usage is cgroup lifetime, not trial peak. Residual kmem counts against the limit; cache/rss are incomplete memory accounting. Thread processor is the last scheduled CPU, not continuous migration tracing."}


def sampled_space(command):
    samples, sample_errors = load_samples(command["samples_path"])
    good = [s for s in samples if s.get("space", {}).get("complete")]
    phase_end = [e for e in command.get("events", []) if e.get("event") == "phase_end"]
    def phase_of(sample):
        # Match scan START, not completion, to benchmark monotonic boundaries.
        stamp = sample.get("space_sample_started_monotonic_s", sample["monotonic_s"]) * 1e6
        for event in phase_end:
            end = event.get("steady_time_us")
            if end is not None and end - event["elapsed_us"] <= stamp <= end:
                return event["phase"]
        return sample.get("space_sample_started_phase", sample.get("phase"))
    measured = [s for s in good if phase_of(s) == "measured"]
    def category(sample, key):
        return sample["space"]["physical_category_bytes"].get(key, 0)
    exit_space = command.get("exit_space", {})
    complete_exit = exit_space.get("complete", False)
    cache_samples = [s["sst_cache"] for s in samples if s.get("sst_cache")]
    cache_complete = [c for c in cache_samples if c["complete"]]
    cpu_ticks = [s["proc"]["cpu_ticks"] for s in samples if s.get("proc")]
    return {"sample_log_errors": sample_errors, "memory": sampled_memory(command, samples),
            "cpu_affinity": command.get("cpu_affinity"), "routed_sst_cache": {
                "complete_samples": len(cache_complete), "partial_samples": len(cache_samples) - len(cache_complete),
                "resident_fraction": median_range([c["resident_fraction"] for c in cache_complete]),
                "coverage_fraction": median_range([c["coverage_fraction"] for c in cache_complete]),
                "sample_cpu_s": sum(c["sample_cpu_s"] for c in cache_samples),
                "method": "mincore of active routed SST files, no data reads or cache eviction"},
            "sampled_process_cpu_s": (max(cpu_ticks) - min(cpu_ticks)) / os.sysconf("SC_CLK_TCK")
                                    if cpu_ticks else None,
            "complete_runtime_space_samples": len(good), "measured_runtime_space_samples": len(measured),
            "incomplete_runtime_space_samples": sum(not s["space"].get("complete") for s in
                                                       samples if "space" in s),
            "sampled_peak_ssd_sst_allocated_bytes": max((category(s, "ssd_sst") for s in good), default=None),
            "sampled_measured_median_ssd_sst_allocated_bytes": statistics.median(
                [category(s, "ssd_sst") for s in measured]) if measured else None,
            "sampled_peak_all_ssd_allocated_bytes": max((s["space"]["physical_device_bytes"].get("ssd", 0)
                                                        for s in good), default=None),
            "sampled_peak_global_hdd_allocated_bytes": max(((s["space"].get("run_scope") or s["space"])["physical_device_bytes"].get("hdd", 0)
                                                           for s in good if (s["space"].get("run_scope") or s["space"])["complete"]), default=None),
            "exit_ssd_sst_allocated_bytes": exit_space.get("physical_category_bytes", {}).get("ssd_sst", 0)
                                           if complete_exit else None,
            "exit_all_ssd_allocated_bytes": exit_space.get("physical_device_bytes", {}).get("ssd", 0)
                                           if complete_exit else None,
            "exit_global_hdd_allocated_bytes": (exit_space.get("run_scope") or exit_space).get("physical_device_bytes", {}).get("hdd", 0)
                                             if (exit_space.get("run_scope") or exit_space).get("complete") else None,
            "exit_trial_hdd_allocated_bytes": exit_space.get("physical_device_bytes", {}).get("hdd", 0)
                                             if complete_exit else None,
            "exit_trial_global_unique_bytes": exit_space.get("physical_global_unique_bytes")
                                             if complete_exit else None,
            "space_scan_cpu_seconds": sum(s["space"].get("scan_cpu_s", 0) for s in samples if "space" in s),
            "space_scan_wall_seconds": sum(s["space"].get("scan_wall_s", 0) for s in samples if "space" in s),
            "sampled_peak_rss_kib": max((s.get("proc_status", {}).get("VmRSS", 0) for s in samples), default=None),
            "sampled_cgroup_peak_usage_bytes": max((s["cgroup"]["memory.usage_in_bytes"]
                for s in samples if s.get("cgroup", {}).get("memory.usage_in_bytes") is not None), default=None),
            "sampled_cgroup_cache_bytes": median_range([s["cgroup"]["stats"].get("cache") for s in samples
                                                       if "cgroup" in s]),
            "device_io_first": next((s["device_io"] for s in samples if s.get("device_io")), None),
            "device_io_last": next((s["device_io"] for s in reversed(samples) if s.get("device_io")), None),
            "peak_method": "sampled, not exact peak; 1s process/cgroup and 5-10s space polling"}


def summarize(raw):
    trials = []
    for command in raw.get("trials", []):
        row = {k: command.get(k) for k in ("name", "repeat", "mode", "workload", "valid", "exit_code",
                                          "timed_out", "budget_bytes", "preparation_s", "round_elapsed_s",
                                          "mechanism_coverage", "phase_validation_errors", "cache_gate", "performance_valid", "cpu_affinity")}
        row["phases"] = {name: phase_result(command["events"], name)
                         for name in ("warmup", "measured", "saturated")}
        row["space"] = sampled_space(command)
        row["memory"] = row["space"]["memory"]
        trials.append(row)
    groups = []
    for workload in ("uniform", "switch", "mixed"):
        for mode in ("disabled", "adaptive"):
            selected = [t for t in trials if t["workload"] == workload and t["mode"] == mode]
            correct = [t for t in selected if t["valid"] and t["phases"]["measured"] and t["phases"]["measured"].get("valid")]
            mechanism_valid = [t for t in correct if t.get("mechanism_coverage", {}).get("covered")]
            valid = [t for t in mechanism_valid if t.get("performance_valid", True)]
            group_coverage = len(mechanism_valid)
            group = {"workload": workload, "mode": mode, "samples": len(selected),
                     "valid_samples": len(valid), "correctness_valid_samples": len(correct),
                     "mechanism_valid_samples": group_coverage,
                     "cache_valid_samples": sum(t.get("cache_gate", {}).get("passed", True) for t in correct),
                     "cache_dominated_samples": sum(t.get("cache_gate", {}).get("cache_dominated") is True for t in correct),
                     "cache_evidence_missing_samples": sum(t.get("cache_gate", {}).get("measurement_status") == "missing_or_partial" for t in correct),
                     "complete_three_repeats": len(valid) == 3}
            for phase in ("measured", "saturated"):
                group[phase + "_throughput_ops_s"] = median_range([
                    t["phases"][phase]["throughput_ops_s"] for t in valid if t["phases"][phase] and t["phases"][phase].get("valid")])
            for operation in ("get", "put", "delete"):
                for kind in ("service", "response"):
                    group[f"{operation}_{kind}_p99_us"] = median_range([
                        t["phases"]["measured"][operation][kind]["p99_us"] for t in valid])
            for field in ("sampled_measured_median_ssd_sst_allocated_bytes",
                          "sampled_peak_ssd_sst_allocated_bytes", "exit_ssd_sst_allocated_bytes",
                          "sampled_peak_all_ssd_allocated_bytes", "exit_global_hdd_allocated_bytes"):
                group[field] = median_range([t["space"][field] for t in valid])
            groups.append(group)
    comparisons = []
    for adaptive in (t for t in trials if t["mode"] == "adaptive"):
        disabled = next((t for t in trials if t["repeat"] == adaptive["repeat"] and
                         t["workload"] == adaptive["workload"] and t["mode"] == "disabled"), None)
        row = {"repeat": adaptive["repeat"], "workload": adaptive["workload"]}
        valid = disabled is not None and disabled["valid"] and adaptive["valid"] and adaptive.get("mechanism_coverage", {}).get("covered") and adaptive.get("performance_valid", True) and disabled.get("performance_valid", True)
        for field in ("sampled_measured_median_ssd_sst_allocated_bytes",
                      "sampled_peak_ssd_sst_allocated_bytes", "exit_ssd_sst_allocated_bytes",
                      "exit_all_ssd_allocated_bytes", "exit_trial_hdd_allocated_bytes",
                      "exit_trial_global_unique_bytes"):
            baseline = disabled["space"][field] if disabled else None
            candidate = adaptive["space"][field]
            row[field + "_disabled_minus_adaptive"] = baseline - candidate if valid and \
                baseline is not None and candidate is not None else None
        comparisons.append(row)
    rpo = []
    for sample in raw.get("rpo_samples", []):
        row = {k: sample.get(k) for k in ("name", "mode", "cut_seconds", "valid", "last_acked_seq",
            "recovered_marker", "marker_beyond_last_ack", "lost_ack_batches", "lost_business_bytes",
            "first_lost_ack", "last_lost_ack", "last_recovered_ack", "rpo_us", "rpo_lower_bound_us", "rto_us")}
        row["first60s_read"] = phase_result(sample.get("restore", {}).get("events", []), "first60s_read")
        rpo.append(row)
    metadata = {k: raw.get(k) for k in ("protocol", "status", "error", "run_uuid", "host", "arguments",
        "profile", "profile_parameters", "limits", "budget_forecast", "recovery_probe", "calibration_deadline_monotonic_s", "deadline_monotonic_s",
        "source_identity", "cpu_affinity", "binary_sha256_start", "binary_sha256_end", "preflight", "calibration_pressure", "calibration_attempts",
        "baseline_sst_logical_bytes", "frozen_budget_bytes", "frozen_target_ops_per_sec", "elapsed_s")}
    return {**metadata, "trials": trials, "groups": groups, "space_comparisons": comparisons,
            "rpo_samples": rpo, "rpo_range_us": median_range([s["rpo_us"] for s in rpo if s["valid"]]),
            "rto_range_us": median_range([s["rto_us"] for s in rpo if s["valid"]]),
            "command_records": [{k: c.get(k) for k in ("name", "command", "wrapped_command",
                "process_identity", "exit_code", "timed_out", "valid", "memory_failcnt_delta", "oom_kill_delta",
                "cpu_affinity", "cgroup_start", "cgroup_end", "memory_budget_start", "memory_budget_end",
                "kmem_usage_delta_bytes", "kmem_failcnt_delta",
                "stdout_path", "stderr_path", "samples_path", "acks_path", "parse_errors", "phase_validation_errors",
                "ready_steady_time_us", "ready_received_monotonic_s", "ready_pipe_lag_us",
                "fault_elapsed_from_ready_us", "fault_cut_jitter_us")}
                for c in raw.get("commands", [])],
            "notes": ["Phase P99 uses cumulative histogram bucket upper bounds; no percentile averaging.",
                      "Response latency includes queue delay for completed requests; unfinished_ops must be read alongside latency.",
                      "Hot/cold groups and placement counters are proxies, not per-request device attribution.",
                      "Device I/O includes blob and SST traffic and cannot separate them.",
                      "Kmem/cache/rss are separate evidence: memory.stat cache/rss does not cover total charges. Missing kmem is unknown; an empty tasks file does not reset residual charges.",
                      "Small helper affinity defaults to CPUs 0,1,2,3 before cgroup entry; legacy inherits. Per-thread last CPU and FD counts are read-only sampled diagnostics.",
                      "Cache gate uses globally routed live SST residency, not request miss rate; cache dominated and missing/partial evidence are distinct.",
                      "Small uses measured-phase per-trial cache gates; a single 10s adaptive recovery timing probe is separate from six formal fault samples with 60s first reads.",
                      "Calibration cache gate requires both modes' post-warmup routed SST mincore samples with >=90% live-byte coverage and <=90% resident fraction; anonymous RSS/cache capacity is supplementary only.",
                      "Primary physical space covers each trial SSD+HDD with global inode deduplication; run_scope separately includes seed/active/retained failures for experiment peak and free-space monitoring.",
                      "Space saving is null for failed or incomplete samples; physical totals deduplicate dev+inode.",
                      "RPO has six fault samples only; report all samples and ranges, not P99."]}


def format_value(value):
    return "n/a" if value is None else f"{value:,.1f}"


def markdown(summary):
    lines = ["# Node SSD/HDD SST tiering results", "", f"Status: `{summary['status']}`; host: `{summary['host']}`.", "",
             "| Workload | Mode | Valid repeats | Fixed ops/s median [range] | Saturated ops/s median [range] | Get response P99 us median [range] | Steady SSD SST MiB median |",
             "|---|---|---:|---:|---:|---:|---:|"]
    def range_text(value):
        return f"{format_value(value['median'])} [{format_value(value['min'])}, {format_value(value['max'])}]"
    for group in summary["groups"]:
        space = group["sampled_measured_median_ssd_sst_allocated_bytes"]["median"]
        lines.append(f"| {group['workload']} | {group['mode']} | {group['valid_samples']}/3 | " +
                     range_text(group["measured_throughput_ops_s"]) + " | " +
                     range_text(group["saturated_throughput_ops_s"]) + " | " +
                     range_text(group["get_response_p99_us"]) + " | " +
                     format_value(space / 1048576 if space is not None else None) + " |")
    lines += ["", "## Trial kernel-memory and CPU evidence", "",
              "| Trial | Effective helper CPUs | Baseline kmem MiB | Sampled peak kmem MiB | Increase MiB | Peak FDs | Peak threads |",
              "|---|---|---:|---:|---:|---:|---:|"]
    for trial in summary["trials"]:
        memory = trial.get("memory", {})
        cpus = (trial.get("cpu_affinity") or {}).get("child_effective_cpu_list")
        def mib(key):
            value = memory.get(key)
            return format_value(value / 1048576 if value is not None else None)
        lines.append("| " + str(trial["name"]) + " | " +
                     (",".join(map(str, cpus)) if cpus is not None else "unknown") + " | " +
                     mib("baseline_kmem_usage_bytes") + " | " + mib("sampled_peak_kmem_usage_bytes") + " | " +
                     mib("sampled_peak_kmem_increase_bytes") + " | " +
                     format_value(memory.get("sampled_peak_fd_count")) + " | " +
                     format_value(memory.get("sampled_peak_thread_count")) + " |")
    lines += ["", "## Fault recovery samples", "", "| Mode | Cut s | Valid | ACK seq | Recovered marker | Lost batches | Lost bytes | RPO ms | RTO ms |",
              "|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for row in summary["rpo_samples"]:
        lines.append("| " + " | ".join(str(row.get(k)) for k in
            ("mode", "cut_seconds", "valid", "last_acked_seq", "recovered_marker", "lost_ack_batches", "lost_business_bytes")) +
            " | " + format_value(row["rpo_us"] / 1000 if row["rpo_us"] is not None else None) +
            " | " + format_value(row["rto_us"] / 1000 if row["rto_us"] is not None else None) + " |")
    failures = [c for c in summary["command_records"] if not c.get("valid")]
    lines += ["", "## Retained failed or faulted commands", "", "| Name | Exit | Timeout | Parse/phase errors |",
              "|---|---:|---|---|"]
    for command in failures:
        details = (command.get("parse_errors") or []) + (command.get("phase_validation_errors") or [])
        lines.append(f"| {command['name']} | {command.get('exit_code')} | {command.get('timed_out')} | " +
                     html.escape(str(details)).replace("|", "\\|") + " |")
    if summary.get("error"):
        lines += ["", "Run error: " + str(summary["error"])]
    lines += ["", "## Measurement limits", "", *["- " + note for note in summary["notes"]], "",
              "Commands, source identity, configuration, every failure/timeout and detailed trial histograms are preserved in the JSON summary and raw report.", ""]
    return "\n".join(lines)


def charts(summary, output):
    fields = [("saturated_throughput_ops_s", "Saturated throughput (ops/s)", 1),
              ("sampled_measured_median_ssd_sst_allocated_bytes", "Steady SSD SST allocated (MiB)", 1048576)]
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        # A standalone technical SVG avoids installing dependencies on the node.
        for field, title, scale in fields:
            rows = [(g["workload"] + " " + g["mode"], g[field]["median"] / scale)
                    for g in summary["groups"] if g[field]["median"] is not None]
            maximum = max((value for _, value in rows), default=1) or 1
            parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="350" viewBox="0 0 900 350">',
                     '<rect width="900" height="350" fill="white"/>',
                     f'<text x="20" y="28" font-family="sans-serif" font-size="18">{html.escape(title)}</text>']
            for index, (label, value) in enumerate(rows):
                y = 55 + 45 * index
                parts += [f'<text x="20" y="{y+17}" font-family="sans-serif">{html.escape(label)}</text>',
                          f'<rect x="200" y="{y}" width="{value/maximum*580:.2f}" height="26" fill="#377eb8"/>',
                          f'<text x="{205+value/maximum*580:.2f}" y="{y+18}" font-family="sans-serif">{value:.1f}</text>']
            parts.append("</svg>")
            output.with_name(output.stem + "-" + field + ".svg").write_text("\n".join(parts))
        return
    for field, title, scale in fields:
        fig, ax = plt.subplots(figsize=(9, 4))
        for offset, mode in ((-0.18, "disabled"), (0.18, "adaptive")):
            groups = [g for g in summary["groups"] if g["mode"] == mode]
            valid = [(i, g[field]) for i, g in enumerate(groups) if g[field]["median"] is not None]
            if valid:
                x = [i + offset for i, _ in valid]
                y = [v["median"] / scale for _, v in valid]
                lower = [(v["median"] - v["min"]) / scale for _, v in valid]
                upper = [(v["max"] - v["median"]) / scale for _, v in valid]
                ax.bar(x, y, 0.34, label=mode, yerr=[lower, upper], capsize=4)
        ax.set_xticks(range(3))
        ax.set_xticklabels(["uniform", "switch", "mixed"])
        ax.set_ylabel(title)
        ax.set_title("Median and observed range; valid repeat counts in report")
        if ax.has_data():
            ax.legend()
        fig.tight_layout()
        for extension in ("svg", "png"):
            fig.savefig(output.with_name(output.stem + "-" + field + "." + extension))
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw_bytes = args.input.read_bytes()
    result = summarize(json.loads(raw_bytes))
    result["raw_report_sha256"] = hashlib.sha256(raw_bytes).hexdigest()
    result["raw_report_path"] = str(args.input.resolve())
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    args.output.with_suffix(".md").write_text(markdown(result))
    charts(result, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
