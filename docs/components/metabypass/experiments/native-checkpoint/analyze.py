#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Detailed native experiment analysis; never starts an experiment or edits raw data."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
import os
from pathlib import Path

import summary

PROTOCOL = summary.PROTOCOL
CORE = {"insert-1k-1t": "UBDS", "insert-1k-4t": "UBDS", "insert-4k-1t": "UBDS",
        "overwrite": "UBDS", "randomread": "UBDS", "hotspot": "UBDS", "scan100": "UBDS",
        "randomread-4t": "UBS", "mixed": "UBDS", "sync": "UBDS", "upstream-hdd": "U"}
SPECIALS = ("staging-268435456", "staging-65536", "gc-true", "gc-false",
            "sst-disabled", "sst-observe", "sst-adaptive")
MIB = 1024 ** 2


def read_json(path):
    def reject(value):
        raise ValueError("nonfinite JSON constant: " + value)
    return json.loads(Path(path).read_text(), parse_constant=reject)


def key(row):
    spec = row["spec"]
    return spec.get("stage", "core"), spec["scenario"], spec["group"], spec["round"]


def identity(value):
    stage, scenario, group, number = value
    return {"stage": stage, "scenario": scenario, "group": group, "round": number}


def foreground(events):
    return [event for event in events if event.get("event") == "sample" and
            event.get("name", "foreground") == "foreground"]


def operation_metrics(events):
    output = {}
    for operation in ("get", "put", "delete"):
        name = operation + "_hist_us"
        samples = foreground(events)
        known = all(name in sample for sample in samples)
        histogram = [bucket for sample in samples for bucket in sample.get(name, [])]
        count = sum(bucket[1] for bucket in histogram)
        entry = {"histogram_available": known, "operations": count if known else None}
        if count:
            for label, quantile in (("p50_us", .5), ("p95_us", .95), ("p99_us", .99)):
                entry[label] = summary.histogram_percentile(histogram, quantile)
        else:
            entry.update(p50_us=None, p95_us=None, p99_us=None)
        output[operation] = entry
    return output


def timings(events):
    result = {}
    for event in events:
        if event.get("event") == "phase":
            value = event["elapsed_us"]
            if not isinstance(value, (int, float)) or isinstance(value, bool) or \
                    not math.isfinite(value) or value < 0:
                raise ValueError("invalid phase timing")
            result[event["name"] + "_us"] = result.get(event["name"] + "_us", 0) + value
    return result


def detailed_row(row, index):
    metrics = summary.row_metrics(row)
    if "user_cpu_us" in metrics and "system_cpu_us" in metrics:
        metrics["cpu_total_us"] = metrics["user_cpu_us"] + metrics["system_cpu_us"]
    operations = operation_metrics(row["driver"]["events"])
    for name, values in operations.items():
        for metric, value in values.items():
            if metric != "histogram_available" and value is not None:
                metrics[name + "_" + metric] = value
    if row["spec"]["workload"] == "mixed":
        if not all(entry["histogram_available"] for entry in operations.values()):
            raise ValueError("mixed workload lacks operation-specific histogram")
        if sum(entry["operations"] for entry in operations.values()) != metrics["ops"]:
            raise ValueError("mixed operation histogram counts differ from total operations")
    devices = row["driver"].get("device_io", {}).get("devices", {})
    for number, device in devices.items():
        if not device.get("available"):
            continue
        prefix = "device_" + number.replace(":", "_") + "_"
        for metric in ("read_bytes", "write_bytes", "read_ops", "write_ops"):
            if metric in device:
                metrics[prefix + metric] = device[metric]
    for when in ("before", "after"):
        for category, values in row.get("residency_" + when, {}).items():
            if category not in ("sst", "blob_extent"):
                continue
            for metric in ("bytes", "resident_bytes", "files"):
                if metric in values:
                    metrics["residency_" + when + "_" + category + "_" + metric] = values[metric]
            if values.get("bytes", 0) > 0 and "resident_bytes" in values:
                metrics["residency_" + when + "_" + category + "_resident_fraction"] = values["resident_bytes"] / values["bytes"]
    for prefix, field in (("restore_", "recovery"), ("append_reopen_", "append_reopen")):
        for name, value in timings(row.get(field, {}).get("events", [])).items():
            metrics[prefix + name] = value
    for name, value in metrics.items():
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid detailed metric " + name)
    return {"index": index, "identity": identity(key(row)), "spec": row["spec"],
            "metrics": metrics, "operations": operations, "result": row.get("result"),
            "verification": row.get("verification"), "recovery": row.get("recovery"),
            "append_reopen": row.get("append_reopen"), "coverage": row.get("coverage"),
            "devices": devices, "options": row.get("effective_options", [])}


def aggregate(rows):
    metrics = {}
    names = set().union(*(row["metrics"].keys() for row in rows)) if rows else set()
    for name in sorted(names):
        present = [row for row in rows if name in row["metrics"]]
        metrics[name] = {**summary.distribution([row["metrics"][name] for row in present]),
                         "n": len(present), "rounds": [row["identity"]["round"] for row in present]}
    return metrics


def paired(candidate, reference, profile):
    a = {row["identity"]["round"]: row for row in candidate}
    b = {row["identity"]["round"]: row for row in reference}
    rounds = sorted(set(a) & set(b))
    defaults = {"ops": None, "threads": 1, "warmup": 0, "seconds": 0,
                "cache_bytes": 256 * MIB, "flush_every": 0, "gc_wait_seconds": 0,
                "flush": True, "disable_auto_compactions": False}
    incompatible = []
    for number in rounds:
        mismatches = []
        for name in ("workload", "keys", "value_size", *defaults):
            av = a[number]["spec"].get(name, defaults.get(name))
            bv = b[number]["spec"].get(name, defaults.get(name))
            if name == "ops":
                av = a[number]["spec"]["keys"] if av is None else av
                bv = b[number]["spec"]["keys"] if bv is None else bv
            if av != bv:
                mismatches.append(name)
        if mismatches:
            incompatible.append({"round": number, "fields": mismatches})
    names = set.intersection(*(set(a[n]["metrics"]) & set(b[n]["metrics"]) for n in rounds)) if rounds else set()
    ratios = {}
    for name in sorted(names):
        pairs = []
        undefined = []
        for number in rounds:
            numerator, denominator = a[number]["metrics"][name], b[number]["metrics"][name]
            if denominator == 0:
                undefined.append(number)
                continue
            value = 1 - numerator / denominator if name == "ops_per_sec" else numerator / denominator - 1
            pairs.append({"round": number, "candidate": numerator, "reference": denominator, "degradation": value})
        entry = {"pairs": pairs, "undefined_reference_zero_rounds": undefined}
        if pairs:
            entry.update(summary.distribution([pair["degradation"] for pair in pairs]))
        entry["three_defined_pairs"] = len(pairs) == 3 and rounds == [1, 2, 3]
        ratios[name] = entry
    return {"paired_rounds": rounds, "missing_candidate_rounds": sorted({1, 2, 3} - set(a)),
            "missing_reference_rounds": sorted({1, 2, 3} - set(b)),
            "rankable": profile == "formal" and rounds == [1, 2, 3] and not incompatible,
            "incompatible_load_rounds": incompatible,
            "candidate_absolute": aggregate(candidate), "reference_absolute": aggregate(reference),
            "degradation": ratios}


def expected_keys(source):
    protocol = source.parent / "frozen-protocol.json"
    if protocol.exists():
        frozen = read_json(protocol)
        if frozen.get("protocol") != PROTOCOL:
            raise ValueError("frozen protocol mismatch")
        return {key({"spec": spec}) for spec in frozen["core"] + frozen["specials"]}, str(protocol)
    expected = {("core", scenario, group, number) for scenario, groups in CORE.items()
                for group in groups for number in (1, 2, 3)}
    expected |= {("specials", scenario, "S", number) for scenario in SPECIALS for number in (1, 2, 3)}
    return expected, "default approved matrix: 120 core + 21 specials"


def recovery_record(label, group, status, owner):
    return {"scenario": label, "group": group, "status": status,
            "round": owner.get("spec", {}).get("round"),
            "restore_source": owner.get("restore_source"),
            "restore_timings": timings(owner.get("recovery", {}).get("events", [])),
            "append_reopen_timings": timings(owner.get("append_reopen", {}).get("events", [])),
            "result": owner.get("recovery", {}).get("result") or
                      owner.get("verify", {}).get("result") or owner.get("write", {}).get("result"),
            "append_result": owner.get("append_reopen", {}).get("result"),
            "error": owner.get("error")}


def analyze(report, source, history_link):
    if report.get("protocol") != PROTOCOL:
        raise ValueError("refusing historical or unknown protocol")
    blocked = report.get("blocked_groups", {})
    rows, calibrations, retests, attempts, excluded = [], [], [], [], []
    identities = Counter(key(row) for row in report.get("runs", []) if row.get("phase") == "measured")
    accepted_raw = []
    for index, row in enumerate(report.get("runs", [])):
        label = {"index": index, "phase": row.get("phase"), "identity": identity(key(row)),
                 "status": row.get("status"), "error": row.get("error"), "result": row.get("result")}
        reason = None
        if not row.get("valid") or row.get("status") != "pass":
            reason = "raw_failed_or_incomplete"
        elif row["spec"]["group"] in blocked:
            reason = "configuration_blocked: " + str(blocked[row["spec"]["group"]])
        elif row.get("phase") == "measured" and identities[key(row)] != 1:
            reason = "duplicate original round identity"
        if reason:
            excluded.append({**label, "reason": reason})
            continue
        try:
            detail = detailed_row(row, index)
        except (ValueError, KeyError, TypeError, ZeroDivisionError) as error:
            excluded.append({**label, "reason": "invalid evidence: " + str(error)})
            continue
        if row.get("phase") == "calibration":
            calibrations.append(detail)
        elif row.get("phase") == "retest":
            retests.append(detail)
        elif row.get("phase") == "attempt":
            attempts.append(detail)
        elif row.get("phase") == "measured":
            rows.append(detail)
            accepted_raw.append(row)
        else:
            excluded.append({**label, "reason": "unknown phase"})
    base = summary.summarize({**report, "runs": [row for row in accepted_raw if row["spec"].get("stage") == "core"]})
    grouped = defaultdict(list)
    for row in rows:
        item = row["identity"]
        grouped[item["stage"], item["scenario"], item["group"]].append(row)
    absolute = [{"stage": stage, "scenario": scenario, "group": group,
                 "samples": len(values), "rounds": sorted(row["identity"]["round"] for row in values),
                 "metrics": aggregate(values)}
                for (stage, scenario, group), values in sorted(grouped.items())]
    core_pairs = []
    for scene in CORE:
        if scene == "upstream-hdd":
            continue
        for candidate, reference, meaning in (("D", "B", "core_backup_cost"), ("S", "D", "staging_cost"),
                                              ("D", "U", "overall_layout_and_service"), ("S", "U", "overall_layout_and_service")):
            comparison = paired(grouped["core", scene, candidate], grouped["core", scene, reference], report.get("profile"))
            core_pairs.append({"scenario": scene, "candidate": candidate, "reference": reference,
                               "meaning": meaning,
                               "sync_semantics": "additional_backup_service_cost" if scene == "sync" and
                               reference in ("B", "U") else "same_service_contract",
                               **comparison})
    mechanism_pairs = []
    for candidate, reference, meaning in (("staging-65536", "staging-268435456", "small_vs_default_staging"),
                                          ("gc-true", "gc-false", "whole_file_gc_cost_and_reclamation"),
                                          ("sst-observe", "sst-disabled", "sst_observation_cost"),
                                          ("sst-adaptive", "sst-disabled", "sst_adaptation_cost"),
                                          ("sst-adaptive", "sst-observe", "sst_migration_vs_observation")):
        mechanism_pairs.append({"candidate": candidate, "reference": reference, "group": "S", "meaning": meaning,
                                **paired(grouped["specials", candidate, "S"], grouped["specials", reference, "S"], report.get("profile"))})
    expected, expected_source = expected_keys(source)
    completed = {tuple(row["identity"][name] for name in ("stage", "scenario", "group", "round")) for row in rows}
    gates = [recovery_record(gate["scenario"], gate["group"], gate["status"], gate) for gate in report.get("gates", [])]
    gate_expected = {(group, scene) for group, scenes in {
        "U": ("concurrent-mixed", "concurrent-sync"),
        "B": ("concurrent-mixed", "concurrent-sync", "concurrent-gc"),
        "D": ("wal-only", "checkpoint-blob", "concurrent-mixed", "concurrent-sync", "sigkill-write", "sigkill-publish"),
        "S": ("wal-only", "checkpoint-blob", "staging-prefix-2m", "staging-direct-64k", "staging-large-value",
              "concurrent-mixed", "concurrent-sync", "sigkill-write", "sigkill-publish", "sigkill-migration")}.items() for scene in scenes}
    gate_passed = {(gate["group"], gate["scenario"]) for gate in gates if gate["status"] == "pass"}
    special_recoveries = [recovery_record(row["spec"]["scenario"], row["spec"]["group"], row.get("status"), row)
                          for row in report.get("runs", []) if row.get("phase") == "measured" and
                          row["spec"].get("stage") == "specials"]
    return {"protocol": PROTOCOL, "source": str(source),
            "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "history_link": history_link,
            "state": report.get("state"), "profile": report.get("profile"),
            "time_limit_policy": report.get("time_limit_policy"), "blocked_groups": blocked,
            "resume_provenance": report.get("resume_provenance"),
            "raw_result_roots": report.get("resume_provenance", {}).get("raw_result_roots", []),
            "availability": {"gates": gates, "expected_gates": len(gate_expected), "passed_gates": len(gate_passed & gate_expected),
                             "missing_or_nonpass_gates": [{"group": group, "scenario": scene} for group, scene in sorted(gate_expected - gate_passed)],
                             "strict_regression_audit": report.get("regression_audit"), "special_hdd_recoveries": special_recoveries},
            "expected_matrix_source": expected_source, "expected_samples": len(expected), "valid_measured_samples": len(rows),
            "missing_or_invalid_original_samples": [identity(value) for value in sorted(expected - completed)],
            "absolute": absolute, "core_comparisons": core_pairs, "mechanism_comparisons": mechanism_pairs,
            "rankable_core_comparisons": sum(value["rankable"] for value in core_pairs),
            "rankable_mechanism_comparisons": sum(value["rankable"] for value in mechanism_pairs),
            "rows": rows, "calibration_only_rows": calibrations, "independent_retests": retests,
            "independent_attempts": attempts,
            "excluded": excluded, "failure_ledger": report.get("failure_ledger", []),
            "raw_error": report.get("error"), "raw_limitations": report.get("limitations", []),
            "base_summary": base}


def cell(metrics, name, scale=1):
    value = metrics.get(name)
    if value is None:
        return "N/A"
    suffix = "" if value.get("n", 3) == 3 else " (n=" + str(value["n"]) + ")"
    return f"{value['median'] / scale:.3g} [{value['min'] / scale:.3g}, {value['max'] / scale:.3g}]" + suffix


def ratio(comparison, metric):
    value = comparison["degradation"].get(metric)
    if not comparison["rankable"] or value is None or not value["three_defined_pairs"]:
        return "incomplete/N/A"
    return f"{100 * value['median']:+.1f}% [{100 * value['min']:+.1f}%, {100 * value['max']:+.1f}%]"


def raw_link(path, root, result_roots=()):
    if not path:
        return "N/A"
    path = Path(path)
    if path.is_absolute():
        local = root / path.parent.name / path.name
        if root in path.parents:
            local = path
        else:
            # Resume archives contain distinct source and destination result
            # roots; identical process folder numbers must never alias.
            for original in result_roots:
                original = Path(original)
                if original in path.parents:
                    destination = root if original == Path(result_roots[-1]) else root.parent / original.name
                    local = destination / path.relative_to(original)
                    break
    else:
        local = root / path
    return "[raw](" + os.path.relpath(local, root).replace(" ", "%20") + ")"


def markdown(analysis, root):
    a = analysis
    def link(path):
        return raw_link(path, root, a.get("raw_result_roots", []))
    availability = a["availability"]
    lines = ["# \u65b0\u7248 Metabypass \u53ef\u7528\u6027\u4e0e\u6027\u80fd\u9000\u5316\u5206\u6790", "",
             f"\u72b6\u6001\uff1a**{a['state']}**\uff1bprofile\uff1a`{a['profile']}`\uff1b\u534f\u8bae\uff1a`{PROTOCOL}`\u3002",
             "\u539f\u59cb\u8bc1\u636e\uff1a[results.json](results.json)\uff1b\u5b8c\u6574\u914d\u5bf9\u4e0e\u6307\u6807\uff1a[analysis.json](analysis.json)\uff1b"
             f"\u5386\u53f2\u5b9e\u9a8c\uff1a[\u603b\u89c8]({a['history_link']})\u3002", "",
             f"\u53ef\u7528\u6027\u95e8\u7981 {availability['passed_gates']}/{availability['expected_gates']} \u901a\u8fc7\uff1b"
             f"\u6b63\u5f0f\u6709\u6548\u539f\u59cb\u6837\u672c {a['valid_measured_samples']}/{a['expected_samples']}\uff1b"
             f"\u5df2\u5b8c\u6210\u4e09\u8f6e\u7684\u6838\u5fc3\u914d\u5bf9 {a['rankable_core_comparisons']}\uff0c\u673a\u5236\u914d\u5bf9 {a['rankable_mechanism_comparisons']}\u3002"]
    audit = availability.get("strict_regression_audit")
    provenance = a.get("resume_provenance")
    if provenance:
        lines += ["", "\u7ee7\u627f\u539f\u59cb\u8bc1\u636e\uff1a" + link(provenance["source_results"]) +
                  "\uff1bSHA256: `" + provenance["source_sha256"] +
                  "`\u3002\u539f\u5931\u8d25\u8d26\u672c\u4e0e incomplete \u4fdd\u7559\uff0cattempt \u4e0e retest \u4e0d\u53c2\u4e0e\u539f\u59cb\u6392\u540d\u3002"]
    audit_status = audit.get("audit", {}).get("status", "unknown") if audit else "not recorded in this results snapshot"
    lines += ["", "\u4e25\u683c\u56de\u5f52\u5ba1\u8ba1\uff1a`" + audit_status + "`\u3002"]
    if availability["missing_or_nonpass_gates"] or a["blocked_groups"]:
        lines += ["", "\u53ef\u7528\u6027\u7ed3\u8bba\uff1a\u5f53\u524d\u8bc1\u636e\u5c1a\u4e0d\u8db3\u4ee5\u786e\u8ba4\u8ba1\u5212\u7684\u5168\u90e8\u9a8c\u6536\u9879\u901a\u8fc7\uff1b\u5df2\u901a\u8fc7\u4e0e\u7f3a\u5931\u95e8\u7981\u9010\u9879\u5217\u5728\u4e0b\u6587\u3002"]
    else:
        lines += ["", "\u53ef\u7528\u6027\u7ed3\u8bba\uff1a21 \u9879\u57fa\u7840\u4e0e\u6545\u969c\u95e8\u7981\u5df2\u901a\u8fc7\uff1bGC/SST \u7b49\u4e13\u9879\u6062\u590d\u7684\u8986\u76d6\u4e0e\u8ba1\u65f6\u53e6\u5217\uff0c\u6574\u4f53\u5b8c\u6210\u72b6\u6001\u4ecd\u4ee5\u5168\u90e8\u539f\u59cb\u6837\u672c\u4e3a\u51c6\u3002"]
    if not a["valid_measured_samples"]:
        lines += ["", "\u76ee\u524d\u4ec5\u6709\u95e8\u7981/\u6821\u51c6\u8bc1\u636e\uff0c\u5c1a\u65e0\u6b63\u5f0f\u6027\u80fd\u6392\u540d\u3002\u6821\u51c6\u6570\u636e\u5355\u5217\uff0c\u4e0d\u80fd\u56de\u7b54\u65b0\u7248\u76f8\u5bf9\u57fa\u7ebf\u7684\u6b63\u5f0f\u9000\u5316\u7a0b\u5ea6\u3002"]
    if a["blocked_groups"]:
        lines += ["", "\u5df2\u5c01\u7981\u914d\u7f6e\uff1a`" + json.dumps(a["blocked_groups"], ensure_ascii=False) + "`\u3002\u5176\u539f\u59cb\u8bb0\u5f55\u4fdd\u7559\uff0c\u5168\u90e8\u9000\u51fa\u6027\u80fd\u6bd4\u8f83\u3002"]
    lines += ["", "U\uff1a\u56fa\u5b9a upstream v11.8.1\u3001inline\u3001\u5168 SSD\uff1bB\uff1a\u5f53\u524d\u666e\u901a BlobDB\u3001\u7d22\u5f15/WAL \u5728 SSD\u3001Blob \u5728 HDD\uff1b"
              "D\uff1a\u589e\u52a0 HDD \u5907\u4efd\u3001\u65e0 staging\uff1bS\uff1a\u589e\u52a0 256 MiB SSD staging\u3002", "",
              "\u541e\u5410\u9000\u5316\u4e3a `1-candidate/reference`\uff1b\u65f6\u95f4\u3001\u5ef6\u8fdf\u4e0e\u8d44\u6e90\u589e\u52a0\u4e3a `candidate/reference-1`\u3002"
              "\u4ee5\u4e0b\u5747\u4e3a\u540c\u8f6e\u914d\u5bf9\u7684\u4e2d\u4f4d\u6570 [\u6700\u5c0f, \u6700\u5927]\uff0c\u4e0d\u662f\u4e2d\u4f4d\u6570\u76f8\u9664\u3002\u8d1f\u503c\u8868\u793a\u8be5\u6307\u6807\u6539\u5584\u3002"
              "\u5206\u6bcd\u4e3a\u96f6\u65f6\u4e0d\u8ba1\u7b97\u767e\u5206\u6bd4\uff1b\u7f3a\u5931\u4e0d\u586b\u96f6\u3002\u4e09\u8f6e\u662f\u5feb\u901f\u7b5b\u67e5\uff0c\u4e0d\u6784\u6210\u7edf\u8ba1\u663e\u8457\u6027\u8bc1\u636e\u3002", "",
              "**\u540c\u6b65\u5199\u8bed\u4e49\u4e0d\u540c\uff1aU/B \u4ec5\u627f\u8bfa\u539f\u751f WAL \u6301\u4e45\u5316\uff1bD/S \u8fd8\u627f\u8bfa\u5199\u5165\u88ab HDD \u5907\u4efd\u8986\u76d6\u3002"
              "\u56e0\u6b64 D/B\u3001D/U\u3001S/U \u7684\u540c\u6b65\u5ef6\u8fdf\u6bd4\u8868\u793a\u989d\u5916\u670d\u52a1\u6210\u672c\uff0c\u4e0d\u80fd\u79f0\u4e3a\u540c\u7b49\u6062\u590d\u4fdd\u8bc1\u4e0b\u7684\u6548\u7387\u5dee\u3002**", "",
              "## D/B \u6838\u5fc3\u5907\u4efd\u6210\u672c\u3001D/S \u76f8\u5bf9 U \u7684\u6574\u4f53\u5dee\u5f02\u53ca S/D staging \u53d6\u820d", "",
              "| \u573a\u666f | candidate/reference | \u541e\u5410\u4e0b\u964d | lifecycle \u589e\u52a0 | P99 \u589e\u52a0 | CPU \u589e\u52a0 | RSS \u589e\u52a0 |",
              "|---|---|---:|---:|---:|---:|---:|"]
    for item in a["core_comparisons"]:
        if not item["rankable"]:
            continue
        lines.append(f"| {item['scenario']} | {item['candidate']}/{item['reference']} | " +
                     " | ".join(ratio(item, metric) for metric in
                                ("ops_per_sec", "lifecycle_us", "p99_us", "cpu_total_us", "peak_rss_kib")) + " |")
    lines += ["", "D/B \u9694\u79bb\u6838\u5fc3\u5907\u4efd\u9644\u52a0\u6210\u672c\uff1bD\u3001S/U \u5305\u542b SSD/HDD \u5e03\u5c40\u3001Blob \u5206\u79bb\u53ca\u6062\u590d\u627f\u8bfa\u5dee\u5f02\uff1bS/D \u8868\u793a staging \u7684\u5b9e\u9645\u53d6\u820d\u3002"
              "\u5404\u573a\u666f\u72ec\u7acb\u89e3\u91ca\uff0c\u4e0d\u8de8\u8d1f\u8f7d\u5e73\u5747\u6210\u5355\u4e00\u9000\u5316\u6570\u5b57\u3002\u672a\u5b8c\u6210\u4e09\u8f6e\u7684\u914d\u5bf9\u89c1\u4e0b\u65b9\u7f3a\u5931\u6e05\u5355\u3002", "",
              "## \u6709\u6548\u6837\u672c\u7edd\u5bf9\u503c", "",
              "\u6240\u6709\u8868\u4e2d\u6570\u503c\u4e3a\u4e09\u8f6e\u4e2d\u4f4d\u6570 [\u6700\u5c0f, \u6700\u5927]\uff1b\u4e0d\u8db3\u4e09\u8f6e\u660e\u786e\u663e\u793a n\u3002", "",
              "| \u573a\u666f | \u7ec4 | \u8f6e\u6b21 | ops/s | P50/P95/P99 \u03bcs | lifecycle ms | CPU ms | RSS MiB |",
              "|---|---|---|---:|---|---:|---:|---:|"]
    for item in a["absolute"]:
        m = item["metrics"]
        quantiles = "; ".join(cell(m, metric) for metric in ("p50_us", "p95_us", "p99_us"))
        lines.append(f"| {item['scenario']} | {item['group']} | {item['rounds']} | {cell(m, 'ops_per_sec')} | {quantiles} | " +
                     " | ".join((cell(m, "lifecycle_us", 1000), cell(m, "cpu_total_us", 1000), cell(m, "peak_rss_kib", 1024))) + " |")
    lines += ["", "CPU \u4e3a user+system \u5b8c\u6574\u8fdb\u7a0b\u65f6\u95f4\u3002\u541e\u5410\u5305\u542b\u5171\u540c\u9a71\u52a8\u7684\u503c\u751f\u6210/\u6821\u9a8c\u5de5\u4f5c\uff1b\u5355\u6b21\u64cd\u4f5c\u5ef6\u8fdf\u8fb9\u754c\u4ee5\u9a71\u52a8\u5951\u7ea6\u4e3a\u51c6\u3002", "",
              "### \u9636\u6bb5\u8017\u65f6", "",
              "| \u573a\u666f/\u7ec4 | \u524d\u53f0 ms | generation Flush \u5408\u8ba1 ms | generation Compact \u5408\u8ba1 ms | final Flush ms | final SyncBackup ms | Close ms | Open ms | lifecycle ms |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for item in a["absolute"]:
        lines.append(f"| {item['scenario']}/{item['group']} | " + " | ".join(
            cell(item["metrics"], metric, 1000) for metric in
            ("foreground_us", "generation_flush_us", "generation_compact_us", "flush_us",
             "sync_backup_us", "close_us", "open_us", "lifecycle_us")) + " |")
    lines += ["", "Open \u4e0d\u8ba1\u5165 lifecycle\uff1b\u9884\u70ed\u548c GC \u6536\u655b\u7b49\u5f85\u53ef\u80fd\u8ba1\u5165\u751f\u547d\u5468\u671f\uff0c\u524d\u53f0 throughput/P99 \u53ea\u4f7f\u7528 foreground histogram\u3002"
              "GC \u4e09\u4ee3\u524d\u53f0\u548c generation Flush/Compact \u8ba1\u65f6\u5206\u522b\u6309\u4e8b\u4ef6\u6c42\u548c\uff0c\u4e0e final Flush/SyncBackup \u5206\u5217\u3002"
              "\u6bcf\u4ee3 SyncBackup \u6ca1\u6709\u72ec\u7acb phase \u8ba1\u65f6\uff0c\u65e0\u6cd5\u5355\u72ec\u5f52\u56e0\uff1bstats \u4e3a\u7d2f\u8ba1\u5feb\u7167\uff0c\u53ea\u53d6\u6700\u540e\u4e00\u6b21\uff0c\u4e0d\u76f8\u52a0\u3002", "",
              "### \u6df7\u5408\u8d1f\u8f7d\u9010\u64cd\u4f5c\u5206\u4f4d", "",
              "| \u573a\u666f/\u7ec4 | \u64cd\u4f5c | \u64cd\u4f5c\u6570 | P50 \u03bcs | P95 \u03bcs | P99 \u03bcs |",
              "|---|---|---:|---:|---:|---:|"]
    for item in a["absolute"]:
        if item["scenario"] != "mixed":
            continue
        for operation in ("get", "put", "delete"):
            lines.append(f"| mixed/{item['group']} | {operation} | " + " | ".join(
                cell(item["metrics"], operation + "_" + metric)
                for metric in ("operations", "p50_us", "p95_us", "p99_us")) + " |")
    lines += ["", "Get/Put/Delete \u7684 histogram \u5206\u522b\u5728\u540c\u4e00\u8f6e\u5185\u5408\u5e76\u64cd\u4f5c\u9891\u6b21\uff0c\u518d\u8ba1\u7b97\u5206\u4f4d\uff1b\u8de8\u8f6e\u62a5\u544a\u4e0a\u8ff0\u5206\u4f4d\u7684\u8303\u56f4\uff0c"
              "\u4e0d\u5e73\u5747\u7ebf\u7a0b\u3001\u4ee3\u9645\u6216\u7a97\u53e3\u7684 P99\u3002", "",
              "### \u5b57\u8282\u3001\u7a7a\u95f4\u4e0e\u5907\u4efd", "",
              "| \u573a\u666f/\u7ec4 | WAL MiB | \u955c\u50cf WAL MiB | \u955c\u50cf\u603b MiB | \u5019\u9009\u590d\u5236 MiB | SSD logical/allocated MiB | HDD logical/allocated MiB |",
              "|---|---:|---:|---:|---:|---|---|"]
    for item in a["absolute"]:
        m = item["metrics"]
        spaces = [cell(m, place + "_logical_bytes", MIB) + " / " + cell(m, place + "_allocated_bytes", MIB) for place in ("ssd", "hdd")]
        lines.append(f"| {item['scenario']}/{item['group']} | " + " | ".join(
            [cell(m, metric, MIB) for metric in ("wal_bytes", "mirrored_wal_bytes", "mirrored_bytes", "candidate_copied_bytes")] + spaces) + " |")
    lines += ["", "logical/allocated \u6309 inode \u53bb\u91cd\uff1b\u672a\u4ea7\u751f\u6216\u4e0d\u9002\u7528\u4e8e U/B \u7684\u5907\u4efd\u6307\u6807\u663e\u793a N/A\uff0c\u4e0d\u80fd\u89e3\u91ca\u4e3a\u96f6\u5f00\u9500\u3002", "",
              "### \u5b9e\u9645\u8bbe\u5907 I/O \u4e0e\u7f13\u5b58\u9a7b\u7559", "",
              "\u8bbe\u5907\u8ba1\u6570\u662f\u6574\u4e2a\u8bbe\u5907\u7684 diskstats \u5dee\u503c\uff0c\u53ef\u80fd\u5305\u542b\u5176\u4ed6\u8fdb\u7a0b\uff1b\u4e0d\u80fd\u5f52\u56e0\u4e3a\u5f15\u64ce\u5199\u653e\u5927\u6216 SST \u5355\u72ec\u6d41\u91cf\u3002"
              "Blob \u8bfb\u53ef\u80fd\u5360\u4e3b\u8981\u90e8\u5206\uff1b12 GiB \u5927\u4e8e 8 GiB \u4e0d\u80fd\u5355\u72ec\u8bc1\u660e\u51b7\u8bfb\u3002mincore \u4ec5\u89c2\u5bdf\uff0c\u4e0d\u8bfb\u53d6\u6587\u4ef6\u4ee5\u5236\u9020\u9884\u70ed\u3002", "",
              "| \u573a\u666f/\u7ec4 | \u8bbe\u5907 | \u8bfb MiB | \u5199 MiB |",
              "|---|---|---:|---:|"]
    for item in a["absolute"]:
        m = item["metrics"]
        prefixes = sorted({name[:-len("read_bytes")] for name in m if name.startswith("device_") and name.endswith("read_bytes")})
        for prefix in prefixes:
            lines.append(f"| {item['scenario']}/{item['group']} | {prefix[len('device_'):].rstrip('_')} | " +
                         cell(m, prefix + "read_bytes", MIB) + " | " + cell(m, prefix + "write_bytes", MIB) + " |")
    lines += ["", "| \u573a\u666f/\u7ec4 | SST \u6d4b\u524d/\u6d4b\u540e\u9a7b\u7559\u6bd4\u4f8b | Blob/segment \u6d4b\u524d/\u6d4b\u540e\u9a7b\u7559\u6bd4\u4f8b |",
              "|---|---|---|"]
    for item in a["absolute"]:
        m = item["metrics"]
        cells = [" / ".join(cell(m, "residency_" + when + "_" + category + "_resident_fraction") for when in ("before", "after"))
                 for category in ("sst", "blob_extent")]
        lines.append(f"| {item['scenario']}/{item['group']} | " + " | ".join(cells) + " |")
    lines += ["", "\u9759\u6001\u53ea\u8bfb\u4e09\u8f6e\u590d\u7528\u5404\u7ec4\u81ea\u5df1\u7684\u540c\u4e00\u4efd\u6570\u636e\uff0c\u662f\u72ec\u7acb\u8ba1\u65f6\u7a97\u53e3\uff0c\u5e76\u975e\u4e09\u6b21\u72ec\u7acb\u5efa\u5e93\u3002"
              "\u9a7b\u7559\u7edf\u8ba1\u5305\u62ec\u89c2\u6d4b\u8def\u5f84\u4e2d\u7684\u72ec\u7acb\u7269\u7406\u526f\u672c\uff1b\u4ec5\u51ed SST \u9a7b\u7559\u6bd4\u4f8b\u4e0d\u80fd\u8bc1\u660e\u7d22\u5f15\u5c42\u7684\u8bbe\u5907\u8bfb\u5f52\u56e0\u3002", "",
              "## staging\u3001\u6574\u6587\u4ef6 GC \u4e0e SST \u673a\u5236", "",
              "| S \u7ec4 candidate/reference | \u541e\u5410\u4e0b\u964d | lifecycle \u589e\u52a0 | P99 \u589e\u52a0 | \u4e09\u8f6e\u72b6\u6001 |",
              "|---|---:|---:|---:|---|"]
    for item in a["mechanism_comparisons"]:
        lines.append(f"| {item['candidate']}/{item['reference']} | " + " | ".join(
            [ratio(item, metric) for metric in ("ops_per_sec", "lifecycle_us", "p99_us")] +
            ["\u5b8c\u6574" if item["rankable"] else "\u7f3a\u5c11 candidate " + str(item["missing_candidate_rounds"]) + "; reference " + str(item["missing_reference_rounds"])]) + " |")
    lines += ["", "64 KiB/256 MiB \u8868\u793a\u7f29\u5c0f staging \u7684\u5f71\u54cd\uff1bgc-true/gc-false \u8868\u793a\u5f00\u542f\u6574\u6587\u4ef6 GC \u7684\u6210\u672c\uff1b"
              "observe/adaptive \u5206\u522b\u76f8\u5bf9 disabled\uff0c\u5e76\u989d\u5916\u6bd4\u8f83 adaptive/observe\u3002\u53ea\u5728\u540c\u8f6e\u3001\u76f8\u540c\u6570\u636e\u91cf\u548c\u8d1f\u8f7d\u4e0b\u914d\u5bf9\u3002", "",
              "| \u573a\u666f/\u7ec4 | staging \u5cf0 MiB | spill MiB | \u8fc1\u79fb Blob MiB | extent \u6570 | staging \u7b49\u5f85 ms | \u5907\u4efd\u7b49\u5f85 ms | \u53d1\u5e03\u70b9\u6570 |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for item in a["absolute"]:
        if item["group"] not in ("D", "S"):
            continue
        m = item["metrics"]
        fields = (("peak_staging_bytes", MIB), ("staging_spill_bytes", MIB), ("migrated_blob_bytes", MIB),
                  ("hdd_extents", 1), ("staging_backpressure_micros", 1000), ("checkpoint_wait_micros", 1000), ("recovery_points", 1))
        lines.append(f"| {item['scenario']}/{item['group']} | " + " | ".join(cell(m, name, scale) for name, scale in fields) + " |")
    lines += ["", "| \u4e13\u9879 | \u56de\u6536 Blob \u6570/MiB | \u5f85 GC MiB | GC ms | SST sampled | promotions/demotions | observed promotions/demotions | \u8fc1\u79fb MiB (up/down) | SSD \u5cf0 MiB | \u961f\u5217\u5cf0 MiB |",
              "|---|---|---:|---:|---:|---|---|---|---:|---:|"]
    for item in a["absolute"]:
        if item["stage"] != "specials":
            continue
        m = item["metrics"]
        cells = [cell(m, "reclaimed_blob_files") + " / " + cell(m, "reclaimed_blob_bytes", MIB),
                 cell(m, "pending_blob_gc_bytes", MIB), cell(m, "blob_gc_micros", 1000), cell(m, "sst_sampled_reads"),
                 cell(m, "sst_promotions") + " / " + cell(m, "sst_demotions"),
                 cell(m, "sst_observed_promotions") + " / " + cell(m, "sst_observed_demotions"),
                 cell(m, "sst_promoted_bytes", MIB) + " / " + cell(m, "sst_demoted_bytes", MIB),
                 cell(m, "sst_peak_ssd_bytes", MIB), cell(m, "peak_queued_bytes", MIB)]
        lines.append(f"| {item['scenario']} | " + " | ".join(cells) + " |")
    lines += ["", "\u672a\u547d\u4e2d\u56de\u6536\u3001\u91c7\u6837\u3001\u8fc1\u79fb\u6216\u5bb9\u91cf\u9a8c\u6536\u7684\u6837\u672c\u4e0d\u8fdb\u5165\u4e0a\u8ff0\u6392\u540d\uff0c\u5176\u539f\u59cb coverage/error \u89c1\u6392\u9664\u6e05\u5355\u4e0e results.json\u3002"
              "\u5168\u90e8\u673a\u5236\u8ba1\u6570\u53ca\u9010\u8f6e\u53d6\u820d\u4fdd\u7559\u5728 analysis.json\u3002", "",
              "## HDD-only \u6062\u590d\u4e0e\u95e8\u7981", "",
              "| \u573a\u666f/\u7ec4 | \u72b6\u6001 | Restore ms | Open/WAL \u56de\u653e ms | \u9996\u6b21\u6b63\u786e\u8bfb\u53d6 ms | \u5168\u91cf verify ms | append/reopen verify ms | \u539f\u59cb\u8bc1\u636e |",
              "|---|---|---:|---:|---:|---:|---:|---|"]
    for item in availability["gates"] + availability["special_hdd_recoveries"]:
        def time_cell(values, name):
            return "N/A" if name not in values else f"{values[name] / 1000:.3g}"
        restore, appended = item["restore_timings"], item["append_reopen_timings"]
        round_label = "" if item["round"] is None else "/r" + str(item["round"])
        lines.append(f"| {item['scenario']}/{item['group']}{round_label} | {item['status']} | " + " | ".join(
            [time_cell(restore, name) for name in ("restore_us", "open_us", "first_verified_read_us", "verify_us")] +
            [time_cell(appended, "verify_us"), link(item["result"])]) + " |")
    lines += ["", "Restore \u4f7f\u7528\u4fdd\u7559\u5728 HDD \u7684\u5185\u5bb9\uff0c\u539f SSD index/staging \u9694\u79bb\u540e\u6062\u590d\u5230\u65b0\u7a7a\u76ee\u6807\u3002"
              "\u8868\u4e2d Open \u5305\u542b\u539f\u751f WAL \u56de\u653e\uff1b\u9996\u6b21\u6b63\u786e\u8bfb\u53d6\u5305\u542b\u503c\u6821\u9a8c\u3002\u6062\u590d\u3001\u8ffd\u52a0\u548c\u91cd\u5f00\u8017\u65f6\u4e0d\u8ba1\u5165\u539f\u6027\u80fd\u7a97\u53e3\u3002"
              "SIGKILL \u5728\u6307\u5b9a write/publish/migration \u6587\u4ef6\u64cd\u4f5c\u8fb9\u754c\u9a8c\u8bc1\uff0c\u4e0d\u662f\u7269\u7406\u65ad\u7535\u8ba4\u8bc1\u3002", "",
              "## \u672a\u5b8c\u6210\u9879\u3001\u5931\u8d25\u4e0e\u72ec\u7acb\u590d\u6d4b", "",
              f"\u7f3a\u5c11\u6216\u65e0\u6548\u539f\u59cb\u6837\u672c\uff1a{len(a['missing_or_invalid_original_samples'])}\uff1b\u6392\u9664\u8bb0\u5f55\uff1a{len(a['excluded'])}\uff1b"
              f"\u72ec\u7acb\u590d\u6d4b\uff1a{len(a['independent_retests'])}\uff1b\u72ec\u7acb attempt\uff1a{len(a['independent_attempts'])}\u3002\u590d\u6d4b\u548c attempt \u4e0d\u8865\u9f50\u3001\u8986\u76d6\u6216\u66ff\u4ee3\u5931\u8d25\u7684\u539f\u8f6e\u6b21\u3002"]
    if availability["missing_or_nonpass_gates"]:
        lines += ["", "\u672a\u901a\u8fc7/\u5c1a\u672a\u6267\u884c\u7684\u95e8\u7981\uff1a" + "; ".join(item["group"] + "/" + item["scenario"] for item in availability["missing_or_nonpass_gates"]) + "\u3002"]
    missing_groups = defaultdict(list)
    for item in a["missing_or_invalid_original_samples"]:
        missing_groups[item["stage"], item["scenario"], item["group"]].append(item["round"])
    if missing_groups:
        lines += ["", "| \u9636\u6bb5/\u573a\u666f/\u7ec4 | \u7f3a\u5931\u6216\u65e0\u6548\u8f6e\u6b21 |", "|---|---|"]
        for (stage, scenario, group), rounds in sorted(missing_groups.items()):
            lines.append(f"| {stage}/{scenario}/{group} | {rounds} |")
    for item in a["excluded"]:
        lines.append("- " + str(item.get("phase")) + ": " + str(item["identity"]) + ": " + item["reason"] + "; " + str(item.get("error")))
    if a["independent_retests"] or a["independent_attempts"]:
        lines += ["", "| \u72ec\u7acb\u590d\u6d4b | ops/s | P99 \u03bcs | lifecycle ms | raw |", "|---|---:|---:|---:|---|"]
        for item in a["independent_retests"] + a["independent_attempts"]:
            m = item["metrics"]
            kind = "attempt" if item in a["independent_attempts"] else "retest"
            lines.append(f"| {kind}: {item['identity']} | {m.get('ops_per_sec', 'N/A')} | {m.get('p99_us', 'N/A')} | " +
                         str(m["lifecycle_us"] / 1000 if "lifecycle_us" in m else "N/A") + " | " + link(item["result"]) + " |")
    if a["calibration_only_rows"]:
        lines += ["", "### \u6821\u51c6\u8bb0\u5f55\uff08\u4e0d\u7528\u4e8e\u6b63\u5f0f\u9000\u5316\u6392\u540d\uff09", "",
                  "| \u573a\u666f/\u7ec4 | payload MiB | ops/s | P99 \u03bcs | lifecycle ms | raw |", "|---|---:|---:|---:|---:|---|"]
        for item in a["calibration_only_rows"]:
            m, spec = item["metrics"], item["spec"]
            lines.append(f"| {spec['scenario']}/{spec['group']} | {spec['keys'] * spec['value_size'] / MIB:.3g} | " +
                         str(m["ops_per_sec"]) + " | " + str(m["p99_us"]) + " | " +
                         str(m["lifecycle_us"] / 1000 if "lifecycle_us" in m else "N/A") + " | " + link(item["result"]) + " |")
    if a["raw_error"]:
        lines += ["", "\u63a7\u5236\u5668\u72b6\u6001\u8bf4\u660e\uff1a" + a["raw_error"]]
    lines += ["", "\u673a\u5668/\u6e90\u7801/\u5b9e\u9645 OPTIONS\u3001\u539f\u59cb stdout/stderr\u3001\u5931\u8d25\u8d26\u672c\u548c\u9065\u6d4b\u89c1\u540c\u76ee\u5f55 results.json \u53ca\u9010\u8fdb\u7a0b raw \u94fe\u63a5\u3002"
              "analysis.json \u4fdd\u5b58\u5168\u90e8\u6570\u503c\u3001\u9010\u8f6e\u7edd\u5bf9\u503c\u4e0e\u914d\u5bf9\u6bd4\u503c\uff1b\u672c\u62a5\u544a\u4e0d\u9690\u85cf\u672a\u5b8c\u6210\u9879\u3002", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--history-link")
    args = parser.parse_args()
    source = args.results.resolve()
    history = Path(__file__).resolve().parent.parent / "history.zh-CN.md"
    history_link = args.history_link if args.history_link is not None else os.path.relpath(history, source.parent)
    analysis = analyze(read_json(source), source, history_link)
    source.with_name("analysis.json").write_text(json.dumps(analysis, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    source.with_name("analysis.zh-CN.md").write_text(markdown(analysis, source.parent))
    print(json.dumps({"state": analysis["state"], "valid_measured_samples": analysis["valid_measured_samples"],
                      "rankable_core_comparisons": analysis["rankable_core_comparisons"],
                      "rankable_mechanism_comparisons": analysis["rankable_mechanism_comparisons"],
                      "report": str(source.with_name("analysis.zh-CN.md"))}))


if __name__ == "__main__":
    main()
