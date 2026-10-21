#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Resume an authenticated stopped formal run without replacing original rows."""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import socket
import struct
import sys

import run
import summary


def identity(spec):
    return tuple(spec[name] for name in ("stage", "scenario", "group", "round"))


def command_identity(command):
    """Compare exact binary/flag values independently of JSON object ordering."""
    if not isinstance(command, list) or not command or \
            any(not isinstance(item, str) or not item or "\0" in item for item in command) or \
            command[0].startswith("--"):
        raise run.RunFailure("malformed source driver command")
    options = {}
    for item in command[1:]:
        if not item.startswith("--") or "=" not in item:
            raise run.RunFailure("malformed source driver flag")
        name, value = item[2:].split("=", 1)
        if not re.fullmatch(r"[a-z][a-z0-9-]*", name) or not value or name in options:
            raise run.RunFailure("duplicate or malformed source driver flag")
        options[name] = value
    return command[0], options


def checked_json(path, expected=None):
    path = run.no_symlinks(path)
    if not path.is_file() or path.stat().st_uid != os.getuid():
        raise run.RunFailure("missing or foreign-owned evidence: " + str(path))
    before = run.digest(path)
    if expected is not None and before != expected:
        raise run.RunFailure("evidence SHA256 mismatch: " + str(path))
    value = run.strict_json(path.read_text())
    if run.digest(path) != before:
        raise run.RunFailure("evidence changed while reading: " + str(path))
    return value, before


def normalize_spec(spec, budget):
    value = dict(spec)
    if value.get("workload") == "sst" and value.get("sst_mode") != "disabled":
        if value.get("sst_budget", 0) not in (0, budget):
            raise run.RunFailure("SST capacity differs from frozen disabled result")
        value["sst_budget"] = budget
    return value


def remaining_plan(report, frozen, expected_budget):
    """Original identities remain unique; invalid originals are never replaced."""
    if report.get("protocol") != run.PROTOCOL or frozen.get("protocol") != run.PROTOCOL or \
            report.get("profile") != "formal" or frozen.get("profile") != "formal":
        raise run.RunFailure("resume requires the native formal protocol")
    if report.get("state") != "incomplete" or report.get("blocked_groups"):
        raise run.RunFailure("resume requires an incomplete run without functional blocks")
    if report.get("time_limit_policy", {}).get("explicit_no_time_limits") is not True:
        raise run.RunFailure("resume must preserve the explicit unlimited policy")
    counts = frozen.get("counts", {})
    if counts != report.get("calibration", {}).get("counts"):
        raise run.RunFailure("frozen calibration identity mismatch")
    generated = run.matrix("formal", counts["write_keys"], counts["sync_ops"])
    if frozen.get("core") != generated or frozen.get("specials") != run.special_matrix("formal"):
        raise run.RunFailure("frozen matrix differs from the approved native workload")
    freeze = report.get("sst_capacity_freeze", {})
    if freeze.get("budget_bytes") != expected_budget or freeze.get("sst_files", 0) < 4 or \
            int(freeze.get("disabled_logical_sst_bytes", 0) * .65) != expected_budget:
        raise run.RunFailure("missing or different SST capacity freeze")
    planned = {identity(s): normalize_spec(s, expected_budget)
               for s in frozen["core"] + frozen["specials"]}
    seen, successful, failed = {}, [], []
    for index, row in enumerate(report.get("runs", [])):
        if row.get("phase") != "measured":
            if row.get("phase") != "calibration":
                raise run.RunFailure("source already contains attempts/retests; refusing ambiguous restart")
            continue
        key = identity(row["spec"])
        if key in seen or key not in planned:
            raise run.RunFailure("duplicate or unplanned original identity")
        if normalize_spec(row["spec"], expected_budget) != planned[key]:
            raise run.RunFailure("source row differs from its full frozen spec")
        seen[key] = row
        if row.get("valid") is True and row.get("status") == "pass":
            successful.append(index)
        else:
            failed.append(index)
    if any(identity(s) not in seen or not seen[identity(s)].get("valid")
           for s in frozen["core"]):
        raise run.RunFailure("all 120 core originals must already have passed")
    # This entry point deliberately targets the audited v7 instrumentation stop.
    originals = [r for r in report["runs"] if r.get("phase") == "measured"]
    special_rows = [r for r in originals if r["spec"]["stage"] == "specials"]
    if len(report["runs"]) != 135 or len(successful) != 126 or len(failed) != 1 or \
            len(special_rows) != 7 or identity(special_rows[-1]["spec"]) != \
            ("specials", "sst-adaptive", "S", 1) or \
            special_rows[-1].get("status") != "incomplete" or \
            special_rows[-1].get("error") != "required telemetry could not be collected":
        raise run.RunFailure("source stop differs from the audited v7 incident")
    if [identity(r["spec"]) for r in special_rows] != [identity(s) for s in frozen["specials"][:7]] or \
            len([r for r in report["runs"] if r.get("phase") == "calibration" and
                 r.get("valid") is True and r.get("status") == "pass"]) != 8:
        raise run.RunFailure("source prefix/calibration evidence differs from the audited incident")
    todo = [planned[identity(s)] for s in frozen["specials"] if identity(s) not in seen]
    if len(todo) != 14:
        raise run.RunFailure("remaining original special count is not fourteen")
    return {"successful_original_indexes": successful, "original_incomplete_indexes": failed,
            "pending_original_specs": todo,
            "independent_attempt_specs": [planned[identity(report["runs"][failed[0]]["spec"])]]}


class SourceEvidence:
    def __init__(self, args):
        self.path = run.no_symlinks(args.source).absolute()
        self.root = self.path.parent
        self.artifacts = {}
        self.report, self.report_sha = checked_json(self.path, args.source_sha256)
        self.frozen, self.frozen_sha = checked_json(self.root / "frozen-protocol.json", args.protocol_sha256)
        self.plan = remaining_plan(self.report, self.frozen, args.expected_sst_budget)
        self.build, _ = self.load(self.root / "build.json")
        current, _ = checked_json(args.build_manifest)
        if current != self.build:
            raise run.RunFailure("current build/source manifest differs from source run")
        for name, entry in current["binaries"].items():
            if run.digest(run.no_symlinks(entry["path"])) != entry["sha256"]:
                raise run.RunFailure("current binary SHA256 mismatch: " + name)
        self.preflight, _ = self.load(self.root / "preflight.json")
        if self.preflight.get("host") != socket.gethostname() or self.preflight.get("uid") != os.getuid():
            raise run.RunFailure("source host/uid mismatch")
        tools, tools_sha = checked_json(args.source_tools_manifest, args.source_tools_sha256)
        command = self.report.get("controller_command", [])
        if len(command) < 2 or Path(command[1]).name != "run.py":
            raise run.RunFailure("source controller command is missing")
        tools_root = run.no_symlinks(command[1]).parent
        for name, sha in tools.items():
            if Path(name).name != name or run.digest(run.no_symlinks(tools_root / name)) != sha:
                raise run.RunFailure("frozen source controller tool SHA256 mismatch: " + name)
            self.artifacts[str(tools_root / name)] = sha
        self.artifacts[str(run.no_symlinks(args.source_tools_manifest))] = tools_sha
        self.tools = {"path": str(args.source_tools_manifest), "sha256": tools_sha, "files": tools}
        self.source_roots = {name: run.no_symlinks(path) for name, path in self.report["data_roots"].items()}
        self.check_source_roots(args)
        self.audit_rows()
        self.audit_gates()
        self.audit_static_records()
        self.plan["retest_triggers"] = summary.summarize(self.report)["retest_triggers"]

    def load(self, path):
        path = run.no_symlinks(path)
        if self.root not in path.parents:
            raise run.RunFailure("source evidence reference escapes result root: " + str(path))
        value, sha = checked_json(path)
        self.artifacts[str(path)] = sha
        return value, sha

    def check_source_roots(self, args):
        parents = {"ssd": run.no_symlinks(args.ssd_parent), "hdd": run.no_symlinks(args.hdd_parent)}
        for name, root in self.source_roots.items():
            if root != parents[name] / ("native-" + self.report["run_uuid"]):
                raise run.RunFailure("source data root differs from UUID/approved parent")
            run.check_owned(root, parents[name], self.report["run_uuid"])
            owner, owner_sha = checked_json(root / run.OWNER)
            self.artifacts[str(root / run.OWNER)] = owner_sha
            controller = owner.get("controller")
            if not controller:
                raise run.RunFailure("source controller ownership identity missing")
            observed = run.proc_identity(controller["pid"])
            if observed and observed["starttime_ticks"] == controller["starttime_ticks"] and \
                    observed["state"] not in ("Z", "X", "x"):
                raise run.RunFailure("source controller is still alive")

    def artifact(self, ref, group, action=None, workload=None):
        if not isinstance(ref, dict) or not ref.get("result"):
            raise run.RunFailure("required source process evidence missing")
        raw, _ = self.load(ref["result"])
        if ref.get("events") is not None and ref["events"] != raw.get("events"):
            raise run.RunFailure("embedded process events differ from raw evidence")
        binary, options = command_identity(raw.get("command"))
        entry = self.build["binaries"]["upstream" if group == "U" else "current"]
        if binary != entry["path"] or options.get("variant") != group:
            raise run.RunFailure("source process group identity mismatch")
        action = action or options.get("action")
        workload = workload or options.get("workload")
        if options.get("action") != action or options.get("workload") != workload:
            raise run.RunFailure("source process action/workload identity mismatch")
        sha = entry["sha256"]
        expected_status = "fault_injected" if action == "crashwrite" else "pass"
        if raw.get("binary_sha256_start") != sha or raw.get("binary_sha256_end") != sha or \
                raw.get("status") != expected_status or raw.get("valid") is not (action != "crashwrite"):
            raise run.RunFailure("source process result/binary identity invalid")
        directory = Path(ref["result"]).parent
        command, _ = self.load(directory / "command.json")
        if command_identity(command.get("command")) != (binary, options) or command.get("binary_sha256_start") != sha:
            raise run.RunFailure("source raw command identity mismatch")
        log = run.no_symlinks(directory / "stdout.log")
        events = [run.strict_json(line[len(run.PREFIX):]) for line in log.read_text().splitlines()
                  if line.startswith(run.PREFIX)]
        if events != raw["events"]:
            raise run.RunFailure("source raw stdout differs from recorded events")
        for name in ("stdout.log", "stderr.log"):
            path = run.no_symlinks(directory / name)
            if not path.is_file() or path.stat().st_uid != os.getuid():
                raise run.RunFailure("source raw log missing/foreign-owned")
            self.artifacts[str(path)] = run.digest(path)
        if any(snapshot.get("errors") for snapshot in raw.get("telemetry", [])):
            raise run.RunFailure("passing source process contains telemetry errors")
        if action == "crashwrite":
            run.validate_crash_boundary(raw["events"], options.get("crash-stage"),
                                        raw.get("injected_sigkill"), raw.get("exit"))
        else:
            if raw.get("exit") != 0:
                raise run.RunFailure("passing source process has nonzero exit")
            run.validate_events(raw["events"], action, group, workload)
        return raw, options

    def audit_rows(self):
        for row in self.report["runs"]:
            if row.get("valid") is not True or row.get("status") != "pass":
                continue  # Incomplete evidence is retained verbatim, never certified.
            spec, group = row["spec"], row["spec"]["group"]
            raw, _ = self.artifact(row, group, "run", spec["workload"])
            if row.get("driver") != raw:
                raise run.RunFailure("embedded measured driver differs from raw result")
            binary = self.build["binaries"]["upstream" if group == "U" else "current"]["path"]
            expected_command = run.driver_command(binary, row["paths"], spec, "run",
                                                 fresh=spec["workload"] in ("insert", "sync"))
            if command_identity(raw["command"]) != command_identity(expected_command):
                raise run.RunFailure("source driver command differs from full frozen spec/paths")
            if row.get("coverage", {}).get("complete") is not True:
                raise run.RunFailure("passing original lacks mechanism coverage")
            verified = row.get("verification", {})
            if verified.get("result"):
                self.artifact(verified, group, "verify", spec["workload"])
            elif not spec.get("static") or verified.get("method") != \
                    "per-operation validation; static full verification already passed":
                raise run.RunFailure("passing original lacks full/per-operation verification")
            if not row.get("effective_options"):
                raise run.RunFailure("passing original lacks effective OPTIONS")
            for option in row["effective_options"]:
                path = run.no_symlinks(option["path"])
                if self.root not in path.parents or run.digest(path) != option["sha256"]:
                    raise run.RunFailure("source effective OPTIONS SHA256 mismatch")
                self.artifacts[str(path)] = option["sha256"]
            if spec.get("stage") == "specials":
                self.artifact(row.get("recovery"), group, "restore", spec["workload"])
                self.artifact(row.get("append_reopen"), group, "append-reopen", spec["workload"])

    def audit_gates(self):
        gates = self.report.get("gates", [])
        keys = [(gate["group"], gate["scenario"]) for gate in gates]
        expected = {(g, s) for g, scenes in {
            "U": ("concurrent-mixed", "concurrent-sync"),
            "B": ("concurrent-mixed", "concurrent-sync", "concurrent-gc"),
            "D": ("wal-only", "checkpoint-blob", "concurrent-mixed", "concurrent-sync", "sigkill-write", "sigkill-publish"),
            "S": ("wal-only", "checkpoint-blob", "staging-prefix-2m", "staging-direct-64k", "staging-large-value",
                  "concurrent-mixed", "concurrent-sync", "sigkill-write", "sigkill-publish", "sigkill-migration")}.items()
                    for s in scenes}
        if len(keys) != 21 or set(keys) != expected or any(g["status"] != "pass" for g in gates):
            raise run.RunFailure("source does not contain exactly twenty-one passing gates")
        for gate in gates:
            for name in ("write", "verify", "verification", "recovery", "append_reopen", "crash"):
                if name in gate:
                    self.artifact(gate[name], gate["group"])

    def audit_static_records(self):
        if set(self.report.get("static_datasets", {})) != set(run.GROUPS):
            raise run.RunFailure("source must retain all four static datasets")
        for group, record in self.report["static_datasets"].items():
            if record.get("keys") != 12 * run.GIB // 1024 or record.get("payload_bytes") != 12 * run.GIB:
                raise run.RunFailure("source static payload identity mismatch")
            for name in ("index", "data", "backup", "staging"):
                label = "ssd" if name in ("index", "staging") else "hdd"
                root = self.source_roots[label]
                expected = root / ("static-" + group + "-" + name) / "db"
                path = run.no_symlinks(record["paths"][name])
                if path != expected or not path.is_dir():
                    raise run.RunFailure("source static role/group boundary mismatch")
                run.check_owned(path.parent, root, self.report["run_uuid"])
                self.artifacts[str(path.parent / run.OWNER)] = run.digest(path.parent / run.OWNER)
            if run.no_symlinks(record["paths"]["state"]) != self.root / ("static-" + group + ".state"):
                raise run.RunFailure("source static sidecar boundary mismatch")
            self.artifact({"result": record["prepare"]}, group, "prepare")
            verified, _ = self.artifact({"result": record["verify"]}, group, "verify")
            done = next(e for e in verified["events"] if e.get("event") == "done")
            if done.get("verified_keys") != record["keys"]:
                raise run.RunFailure("source static original full verification count mismatch")

    def confirm_unchanged(self):
        if run.digest(self.path) != self.report_sha or \
                run.digest(self.root / "frozen-protocol.json") != self.frozen_sha:
            raise run.RunFailure("source report/protocol changed during resume")
        for path, sha in self.artifacts.items():
            if run.digest(run.no_symlinks(path)) != sha:
                raise run.RunFailure("archived source artifact changed during resume: " + path)


class ResumeExperiment(run.Experiment):
    def __init__(self, args, evidence):
        self.evidence = evidence
        super().__init__(args)
        old = evidence.report
        if self.preflight["affinity"] != evidence.preflight["affinity"] or \
                self.preflight.get("target_mounts") != evidence.preflight.get("target_mounts"):
            raise run.RunFailure("resume CPU affinity or target mounts changed")
        self.rows.extend(copy.deepcopy(old["runs"]))
        for name in ("gates", "blocked_groups", "failure_ledger", "calibration", "static_datasets", "sst_capacity_freeze"):
            self.report[name] = copy.deepcopy(old[name])
        self.sst_budget = old["sst_capacity_freeze"]["budget_bytes"]
        self.report["error"] = old.get("error")
        self.report["limitations"].extend(old.get("limitations", []))
        self.report["limitations"].append("The original instrumentation incomplete remains invalid; independent attempts never replace it.")
        self.report["resume_provenance"] = {
            "source_results": str(evidence.path), "source_sha256": evidence.report_sha,
            "source_run_uuid": old["run_uuid"], "source_protocol_sha256": evidence.frozen_sha,
            "source_tools": evidence.tools, "source_artifacts_sha256": evidence.artifacts,
            "raw_result_roots": [str(evidence.root), str(self.output)],
            "source_error": old.get("error"), "preserved_source_rows": len(old["runs"]),
            "source_failure_ledger": copy.deepcopy(old["failure_ledger"])}
        self.report["static_resume_validation"] = {}
        run.write_json(self.output / "frozen-protocol.json", evidence.frozen)
        run.write_json(self.output / "resume-plan.json", evidence.plan)
        self.save()

    def physical_space(self, paths):
        roots = {"ssd": [], "hdd": []}
        for name in ("index", "data", "backup", "staging"):
            path = run.no_symlinks(paths[name])
            parent = path.parent.parent
            if parent in (self.ssd, self.hdd):
                label = "ssd" if parent == self.ssd else "hdd"
            else:
                label = next((k for k, v in self.evidence.source_roots.items() if parent == v), None)
                if label is None or path not in getattr(self, "adopted_static_paths", set()):
                    raise run.RunFailure("unvalidated source path cannot enter space accounting")
            roots[label].append(path)
        return {name: run.space_snapshot(values) for name, values in roots.items()}

    def validate_static(self, group):
        if group in self.static_verified:
            return
        record = self.report["static_datasets"][group]
        if record.get("keys") != 12 * run.GIB // 1024 or record.get("payload_bytes") != 12 * run.GIB:
            raise run.RunFailure("source static dataset size differs from frozen workload")
        paths = {k: run.no_symlinks(v) for k, v in record["paths"].items()}
        for name in ("index", "data", "backup", "staging"):
            label = "ssd" if name in ("index", "staging") else "hdd"
            root = self.evidence.source_roots[label]
            expected = root / ("static-" + group + "-" + name) / "db"
            if paths[name] != expected or not paths[name].is_dir():
                raise run.RunFailure("source static path does not match group/role")
            run.check_owned(paths[name].parent, root, self.evidence.report["run_uuid"])
            snapshot = run.space_snapshot([paths[name]])
            if snapshot["errors"]:
                raise run.RunFailure("source static inventory unavailable")
        if paths["state"] != self.evidence.root / ("static-" + group + ".state") or \
                paths["state"].stat().st_uid != os.getuid() or \
                paths["state"].stat().st_size != 24 + record["keys"] * 8:
            raise run.RunFailure("source static state identity/size mismatch")
        with paths["state"].open("rb") as state:
            if state.read(24) != b"MBNPST01" + struct.pack("<QQ", record["keys"], 1024):
                raise run.RunFailure("source static sidecar header mismatch")
            # Static preparation wrote version one to every key; no original
            # read window or resumed read-only retest may change this state.
            block = struct.pack("<Q", 1) * 131072
            while True:
                data = state.read(len(block))
                if not data:
                    break
                if data != block[:len(data)]:
                    raise run.RunFailure("source static sidecar no longer represents the original seed")
        self.evidence.artifact({"result": record["prepare"]}, group, "prepare")
        self.evidence.artifact({"result": record["verify"]}, group, "verify")
        before = run.digest(paths["state"])
        spec = next(s for s in self.evidence.frozen["core"] if s.get("static") and s["group"] == group)
        spec = {**spec, "stage": "reserve", "warmup": 0, "seconds": 0}
        result, output = self.invoke(spec, paths, "verify", timeout=3600)
        after = run.digest(paths["state"])
        if before != after:
            raise run.RunFailure("read-only static validation changed logical version sidecar")
        done = next(e for e in result["events"] if e.get("event") == "done")
        if done.get("verified_keys") != record["keys"]:
            raise run.RunFailure("source static full verification count mismatch")
        self.report["static_resume_validation"][group] = {
            "result": str(output / "result.json"), "state_sha256_before": before,
            "state_sha256_after": after, "verified_keys": done["verified_keys"],
            "note": "Logical contents/sidecar unchanged; native Open may create WAL/OPTIONS. Source containers are never cleaned."}
        self.adopted_static_paths = getattr(self, "adopted_static_paths", set()) | set(paths.values())
        self.static[group] = paths
        self.static_verified.add(group)
        self.save()

    def sample(self, spec, phase="measured"):
        if spec["group"] in self.report["blocked_groups"]:
            return super().sample(spec, phase)
        if spec.get("static"):
            if phase != "retest" or spec["workload"] not in ("randomread", "hotspot", "scan"):
                raise run.RunFailure("source static reuse is restricted to independent read-only retests")
            self.validate_static(spec["group"])
            before = run.digest(self.static[spec["group"]]["state"])
            row = super().sample(spec, phase)
            if run.digest(self.static[spec["group"]]["state"]) != before:
                row.update(valid=False, status="incomplete", error="read-only retest changed source logical state")
                self.save()
                raise run.RunFailure(row["error"])
            return row
        return super().sample(spec, phase)

    def execute_resume(self):
        try:
            for spec in self.evidence.plan["pending_original_specs"]:
                try:
                    self.sample(spec)
                except (run.CoverageFailure, run.DriverFailure):
                    continue
            for spec in self.evidence.plan["independent_attempt_specs"]:
                try:
                    self.sample(spec, "attempt")
                except (run.CoverageFailure, run.DriverFailure):
                    continue
            self.audit_regression()
            self.reserve_retests(self.evidence.plan["retest_triggers"])
            self.evidence.confirm_unchanged()
            self.report["resume_execution"] = "finished"
        except BaseException as error:
            self.report["resume_error"] = str(error)
            raise
        finally:
            self.report["state"] = "incomplete"  # The original invalid sample remains invalid.
            self.save()
            value = summary.summarize(self.report)
            run.write_json(self.output / "summary.json", value)
            (self.output / "report.zh-CN.md").write_text(summary.report_markdown(self.report, value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "run"))
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--protocol-sha256", required=True)
    parser.add_argument("--source-tools-manifest", type=Path, required=True)
    parser.add_argument("--source-tools-sha256", required=True)
    parser.add_argument("--expected-sst-budget", type=int, default=15337964)
    parser.add_argument("--build-manifest", type=Path, required=True)
    parser.add_argument("--regression-audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ssd-parent", default="/home/lj/ssd/metabypass_test")
    parser.add_argument("--hdd-parent", default="/home/lj/hdd/metabypass_test")
    parser.add_argument("--cgroup", default=str(run.CGROUP))
    parser.add_argument("--cpus", default=None)
    args = parser.parse_args()
    args.profile, args.no_time_limits, args.budget_seconds = "formal", True, 28800
    args.stage_budgets = {name: None for name in run.STAGES}
    args.stage_budget_override = None
    args.time_limit_policy = {"mode": "unlimited", "explicit_no_time_limits": True,
                              "per_driver_timeout_mode": "inactivity"}
    try:
        evidence = SourceEvidence(args)
        evidence.confirm_unchanged()
        if args.command == "plan":
            result = run.preflight(args)
            if not result["ready"]:
                raise run.RunFailure("resume preflight rejected: " + "; ".join(result["failures"]))
            print(json.dumps({"source_sha256": evidence.report_sha, "plan": evidence.plan,
                              "preflight": result}, allow_nan=False, indent=2))
            return 0
        experiment = ResumeExperiment(args, evidence)
        experiment.execute_resume()
        return 0
    except (run.RunFailure, OSError, ValueError, KeyError) as error:
        print("Resume rejected/stopped: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
