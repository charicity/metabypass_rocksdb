#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Paired screening statistics. Failed and incomplete runs never enter ranking."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics

PROTOCOL = "metabypass-native-perf-v1"


def distribution(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def histogram_percentile(histogram, percentile):
    """Merge frequency counts before finding quantiles; never average quantiles."""
    buckets = defaultdict(int)
    for upper, count in histogram:
        if not isinstance(upper, (int, float)) or isinstance(upper, bool) or \
                not math.isfinite(upper) or upper < 0 or \
                not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("invalid histogram bucket")
        buckets[upper] += count
    total = sum(buckets.values())
    if not total or not 0 < percentile <= 1:
        raise ValueError("empty histogram or invalid percentile")
    threshold = math.ceil(total * percentile)
    cumulative = 0
    for upper in sorted(buckets):
        cumulative += buckets[upper]
        if cumulative >= threshold:
            return upper
    raise AssertionError("unreachable histogram percentile")


def row_metrics(row):
    events = row["driver"]["events"]
    samples = [event for event in events if event.get("event") == "sample" and
               event.get("name", "foreground") == "foreground"]
    if not samples:
        raise ValueError("no measured sample")
    ops = sum(sample["ops"] for sample in samples)
    elapsed = sum(sample["elapsed_us"] for sample in samples)
    metrics = {"ops_per_sec": ops * 1e6 / elapsed, "ops": ops,
               "foreground_us": elapsed}
    hist = [bucket for sample in samples for bucket in sample["hist_us"]]
    if sum(bucket[1] for bucket in hist) != ops:
        raise ValueError("histogram count differs from measured operation count")
    for label, quantile in (("p50_us", .5), ("p95_us", .95), ("p99_us", .99)):
        metrics[label] = histogram_percentile(hist, quantile)
    for event in events:
        if event.get("event") == "phase":
            key = event["name"] + "_us"
            metrics[key] = metrics.get(key, 0) + event["elapsed_us"]
        elif event.get("event") in ("stats", "resources"):
            # Stats are cumulative snapshots; the last snapshot is authoritative.
            metrics.update({key: value for key, value in event.items()
                            if isinstance(value, (int, float)) and not isinstance(value, bool)})
    for location, value in row.get("space_after", {}).items():
        for key in ("logical_bytes", "allocated_bytes"):
            if key in value:
                metrics[location + "_" + key] = value[key]
    for event in row.get("recovery", {}).get("events", []):
        if event.get("event") == "phase":
            metrics["recovery_" + event["name"] + "_us"] = event["elapsed_us"]
    for key, value in metrics.items():
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid metric " + key)
    return metrics


def summarize(report):
    if report.get("protocol") != PROTOCOL:
        raise ValueError("refusing historical or unknown protocol")
    groups = defaultdict(list)
    omitted = []
    for index, row in enumerate(report.get("runs", [])):
        if row.get("phase") != "measured":
            continue
        if not row.get("valid") or row.get("status") != "pass":
            omitted.append({"index": index, "status": row.get("status"), "error": row.get("error")})
            continue
        metrics = row_metrics(row)
        key = row["spec"]["scenario"], row["spec"]["group"]
        groups[key].append((row["spec"]["round"], metrics))
    absolute = []
    for (scenario, group), rows in sorted(groups.items()):
        common = set.intersection(*(set(metrics) for _, metrics in rows))
        absolute.append({"scenario": scenario, "group": group, "samples": len(rows),
                         "rounds": [number for number, _ in rows],
                         "metrics": {key: distribution([metrics[key] for _, metrics in rows])
                                     for key in sorted(common)}})
    comparisons = []
    triggers = []
    for scenario in sorted({scenario for scenario, _ in groups}):
        if scenario == "upstream-hdd":
            continue
        for candidate, reference, meaning in (("D", "B", "core_backup_cost"),
                                              ("S", "D", "staging_cost"),
                                              ("D", "U", "overall_layout_and_service"),
                                              ("S", "U", "overall_layout_and_service")):
            a = dict(groups.get((scenario, candidate), []))
            b = dict(groups.get((scenario, reference), []))
            paired = sorted(set(a) & set(b))
            if len(a) != len(groups.get((scenario, candidate), [])) or \
                    len(b) != len(groups.get((scenario, reference), [])):
                raise ValueError("duplicate round identity")
            complete = paired == [1, 2, 3]
            blocked = candidate in report.get("blocked_groups", {}) or reference in report.get("blocked_groups", {})
            complete = complete and not blocked
            ratios = {}
            common = set.intersection(*(set(a[r]) & set(b[r]) for r in paired)) if paired else set()
            for key in sorted(common):
                values = [{"round": number,
                           "value": 1 - a[number][key] / b[number][key]
                           if key == "ops_per_sec" else a[number][key] / b[number][key] - 1}
                          for number in paired if b[number][key] > 0]
                if len(values) == len(paired) and values:
                    ratios[key] = {"pairs": values, **distribution([value["value"] for value in values])}
            comparison = {"scenario": scenario, "candidate": candidate, "reference": reference,
                          "meaning": meaning, "complete": complete, "paired_rounds": paired,
                          "degradation": ratios,
                          "sync_semantics": "additional_backup_service_cost" if scenario == "sync"
                          and candidate in ("D", "S") and reference in ("U", "B") else "same_group_contract"}
            comparisons.append(comparison)
            exceeded = [key for key, threshold in (("ops_per_sec", .1), ("lifecycle_us", .1),
                                                   ("p99_us", .2))
                        if complete and key in ratios and ratios[key]["median"] > threshold]
            if exceeded:
                triggers.append({"scenario": scenario, "candidate": candidate,
                                 "reference": reference, "metrics": exceeded,
                                 "action": "independent_retest_within_reserve_budget"})
    return {"protocol": PROTOCOL, "profile": report.get("profile"), "state": report.get("state"),
            "absolute": absolute, "comparisons": comparisons, "retest_triggers": triggers,
            "omitted": omitted,
            "independent_attempts": [{"index": index, "spec": row.get("spec"),
                                      "status": row.get("status"), "valid": row.get("valid"),
                                      "error": row.get("error"), "result": row.get("result"),
                                      "metrics": row_metrics(row) if row.get("valid") and
                                      row.get("status") == "pass" else None}
                                     for index, row in enumerate(report.get("runs", []))
                                     if row.get("phase") == "attempt"],
            "ranking_allowed": report.get("profile") == "formal" and any(row["complete"] for row in comparisons),
            "blocked_groups": report.get("blocked_groups", {})}


def report_markdown(report, summary=None):
    summary = summary or summarize(report)
    # Unicode is constructed at runtime to keep Python sources ASCII, as required.
    title = "\u65b0\u7248 Metabypass \u53ef\u7528\u6027\u4e0e\u6027\u80fd\u7b5b\u67e5"
    lines = ["# " + title, "", "State: **" + report["state"] + "**; profile: " +
             report["profile"] + "; protocol: " + PROTOCOL + ".", "",
             "\u5386\u53f2\u5b9e\u9a8c\uff1a[\u603b\u89c8](../history.zh-CN.md)\u3002", "",
             "\u7ed3\u8bba\uff1a" + ("\u7ed3\u679c\u5b8c\u6574\uff0c\u53ef\u67e5\u770b\u4e0b\u8868\u3002"
             if summary["ranking_allowed"] else
             "\u5c1a\u672a\u6ee1\u8db3\u6b63\u5f0f\u5b8c\u6574\u9a8c\u6536\u6761\u4ef6\uff0c\u4e0d\u53d1\u5e03\u6027\u80fd\u6392\u540d\u3002"), "",
             "U: upstream v11.8.1 inline SSD; B: BlobDB SSD/HDD; D: backup; S: staging.", "",
             "\u6574\u4f53 incomplete \u65f6\uff0c\u4ec5\u5217\u51fa\u901a\u8fc7\u529f\u80fd\u95e8\u7981\u4e14\u5177\u5907\u5b8c\u6574\u4e09\u8f6e\u914d\u5bf9\u7684\u914d\u7f6e\u3002", "",
             "\u4e0b\u8868\u4e3a\u6709\u6548\u6837\u672c\u7684\u7edd\u5bf9\u503c\uff0c\u4e2d\u4f4d\u6570 [min, max]\u3002"
             "\u4e09\u8f6e\u4ec5\u7528\u4e8e\u7b5b\u67e5\uff0c\u4e0d\u8868\u793a\u7edf\u8ba1\u663e\u8457\u6027\u3002", "",
             "| Scenario | Group | n | ops/s | P99 us | Lifecycle us | RSS KiB |",
             "|---|---|---:|---:|---:|---:|---:|"]
    def cell(metrics, key):
        value = metrics.get(key)
        return "N/A" if value is None else f"{value['median']:.2f} [{value['min']:.2f}, {value['max']:.2f}]"
    for row in summary["absolute"]:
        lines.append(f"| {row['scenario']} | {row['group']} | {row['samples']} | " +
                     " | ".join(cell(row["metrics"], key) for key in
                                ("ops_per_sec", "p99_us", "lifecycle_us", "peak_rss_kib")) + " |")
    lines += ["", "\u9000\u5316\u53e3\u5f84\uff1a\u541e\u5410\u4e3a 1 - candidate/reference\uff1b"
              "\u5ef6\u8fdf\u548c\u8d44\u6e90\u4e3a candidate/reference - 1\u3002"
              "\u540c\u8f6e\u914d\u5bf9\u5dee\u5f02\u89c1 summary.json\u3002", "",
              "\u540c\u6b65\u5199\uff1aU/B \u4fdd\u8bc1\u539f\u751f WAL \u6301\u4e45\u5316\uff1b"
              "D/S \u8fd8\u4fdd\u8bc1 HDD \u5907\u4efd\u8986\u76d6\u3002\u6bd4\u503c\u4e3a\u989d\u5916\u670d\u52a1\u6210\u672c\u3002", "",
              "\u53ea\u8bfb\u4e09\u8f6e\u590d\u7528\u5404\u7ec4\u9759\u6001\u6570\u636e\u96c6\uff0c"
              "\u662f\u72ec\u7acb\u8ba1\u65f6\u7a97\u53e3\u3002\u5fc5\u987b\u7ed3\u5408 diskstats \u4e0e mincore \u5224\u65ad\u5b9e\u9645\u8bfb\u53d6\u3002", "",
              "\u8bbe\u5907\u603b\u8bfb\u5199\u91cf\u5305\u542b\u5176\u4ed6\u8fdb\u7a0b\uff0c\u4e0d\u76f4\u63a5\u7b49\u540c SST \u6d41\u91cf\u6216\u5f15\u64ce\u5199\u653e\u5927\u3002", ""]
    if summary["ranking_allowed"]:
        lines += ["| Scenario | Candidate / reference | Throughput loss | Lifecycle increase | P99 increase |",
                  "|---|---|---:|---:|---:|"]
        for row in summary["comparisons"]:
            if not row["complete"]:
                continue
            values = [f"{row['degradation'][key]['median'] * 100:+.2f}%"
                      if key in row["degradation"] else "N/A"
                      for key in ("ops_per_sec", "lifecycle_us", "p99_us")]
            lines.append(f"| {row['scenario']} | {row['candidate']} / {row['reference']} | " +
                         " | ".join(values) + " |")
    for entry in report.get("limitations", []):
        lines.append("- " + entry)
    if summary["independent_attempts"]:
        lines += ["", "Independent attempts (excluded from original paired ranking):", "",
                  "| Scenario / round | Status | Raw |", "|---|---|---|"]
        for attempt in summary["independent_attempts"]:
            spec = attempt["spec"]
            lines.append(f"| {spec['scenario']} / {spec['round']} | {attempt['status']} | " +
                         str(attempt.get("result")) + " |")
    if report.get("error"):
        lines += ["", "Failure/incomplete: " + report["error"]]
    lines += ["", "\u5931\u8d25\u8d26\u672c\u4e0e\u539f\u59cb\u6570\u636e\uff1a[results.json](results.json)\uff1b"
              "\u914d\u5bf9\u7edf\u8ba1\uff1a[summary.json](summary.json)\u3002", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    def reject(value):
        raise ValueError("invalid JSON constant " + value)
    report = json.loads(args.results.read_text(), parse_constant=reject)
    summary = summarize(report)
    args.results.with_name("summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    args.results.with_name("report.zh-CN.md").write_text(report_markdown(report, summary))


if __name__ == "__main__":
    main()
