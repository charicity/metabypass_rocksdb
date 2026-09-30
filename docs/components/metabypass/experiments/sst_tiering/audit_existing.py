#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Re-evaluate preserved runner stdout under the current coverage contract."""

import argparse
import json
from pathlib import Path

import run


def audit(original, source_path):
    settings = original["settings"]
    rows = []
    analyzed = []
    for prior in original["trials"]:
        # All derived fields are discarded. In particular, the historical
        # mechanism verdict and parsed event cache cannot influence this audit.
        raw = {key: prior[key] for key in ("stdout", "exit_code", "timed_out")}
        current = run.analyze_result(raw, settings, prior["mode"],
                                     prior["workload"])
        current.update(mode=prior["mode"], workload=prior["workload"])
        analyzed.append(current)
        phase = current["phase_evidence"]
        rows.append({
            "repeat": prior["repeat"], "workload": prior["workload"],
            "mode": prior["mode"], "budget_percent": prior["budget_percent"],
            "original_valid": prior["valid"],
            "reparsed_valid": current["valid"],
            "original_mechanism_observed": prior["mechanism_observed"],
            "reparsed_mechanism_observed": current["mechanism_observed"],
            "mechanism_basis": current["mechanism_basis"],
            "fill_migrations": phase.get("fill_migrations"),
            "warmup_migrations": phase.get("warmup_migrations"),
            "measured_migrations": phase.get("measured_migrations"),
            "measured_sampled_reads": phase.get("measured_sampled_reads"),
            "cold_live_bytes_lower_bound_at_end": phase.get(
                "cold_live_bytes_lower_bound"),
            "hotspot_adaptation_us": current["hotspot_adaptation_us"],
        })
    coverage = run.mechanism_coverage(analyzed)
    configs = original.get("configurations") or [
        {"mode": mode, "budget_percent": percent}
        for mode, percent in run.CONFIGS]
    expected_keys = {
        (repeat, workload, config["mode"], config["budget_percent"])
        for repeat in range(original["repeats"])
        for workload in original["workloads"] for config in configs}
    actual_keys = [(row["repeat"], row["workload"], row["mode"],
                    row["budget_percent"]) for row in rows]
    matrix_complete = (len(actual_keys) == len(expected_keys) and
                       set(actual_keys) == expected_keys)
    status = (run.final_status(analyzed, original["preflight"], coverage)
              if matrix_complete else "incomplete_trial_matrix")
    binary_at_start = original["binary"].get("sha256")
    binary_at_end = original["binary"].get("sha256_at_end")
    binary_hashes_match = bool(binary_at_start and binary_at_end and
                               binary_at_start == binary_at_end)
    if status == "complete" and not binary_hashes_match:
        status = "binary_changed"
    return {
        "audit_kind": "raw_stdout_reparse; no benchmark rerun",
        "source_json": str(source_path),
        "source_json_sha256": run.sha256(source_path),
        "source_report_status": original["status"],
        "reparsed_status": status,
        "source_binary_sha256": binary_at_start,
        "source_binary_sha256_at_end": binary_at_end,
        "binary_hashes_match": binary_hashes_match,
        "source_identity": original["source"],
        "profile": original["profile"],
        "trial_count": len(rows),
        "expected_trial_count": len(expected_keys),
        "trial_matrix_complete": matrix_complete,
        "preflight": original["preflight"],
        "mechanism_coverage": coverage,
        "changed_mechanism_verdicts": sum(
            row["original_mechanism_observed"] !=
            row["reparsed_mechanism_observed"] for row in rows),
        "changed_validity_verdicts": sum(
            row["original_valid"] != row["reparsed_valid"] for row in rows),
        "trials": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve earlier audits")
    source_path = args.input.resolve()
    original = json.loads(source_path.read_text())
    result = audit(original, source_path)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(result["reparsed_status"], result["trial_count"],
          "trials; source unchanged")


if __name__ == "__main__":
    main()
