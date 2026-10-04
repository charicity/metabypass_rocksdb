#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

"""Build derived A/B summaries without changing the raw report or samples."""

import collections
import hashlib
import json
from pathlib import Path


BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "raw-node" / "ab-report.json"
RAW_DIR = BASE / "raw-node"
POST_CGROUPS = BASE / "metadata" / "ab-cgroups-postrun.json"
LOCAL_CONTROL = RAW_DIR / "node-control-846a5774-e232-4dce-b28c-74fea19f8571"


def read_json(path):
    return json.loads(Path(path).read_text())


def sample_rows(row):
    path = LOCAL_CONTROL / Path(row["samples_path"]).name
    for line in path.read_text().splitlines():
        yield json.loads(line)


def parse_slabinfo(snapshot):
    """Parse raw cgroup slabinfo; derived byte figures are nominal estimates."""
    snapshot = snapshot or {}
    raw = snapshot.get("raw") or ""
    classes = {}
    for line in raw.splitlines():
        if not line or line.startswith(("slabinfo", "#")):
            continue
        left, sep, right = line.partition(": slabdata ")
        if not sep:
            continue
        fields = left.split()
        tail = right.split()
        if len(fields) < 6 or len(tail) < 2:
            continue
        name = fields[0]
        active_objs, num_objs, objsize, objperslab, pagesperslab = map(int, fields[1:6])
        active_slabs, num_slabs = map(int, tail[:2])
        classes[name] = {
            "active_objects": active_objs,
            "total_objects": num_objs,
            "object_size_bytes": objsize,
            "objects_per_slab": objperslab,
            "pages_per_slab": pagesperslab,
            "active_slabs": active_slabs,
            "total_slabs": num_slabs,
            "active_object_payload_estimate_bytes": active_objs * objsize,
            "active_slab_page_estimate_bytes": active_slabs * pagesperslab * 4096,
        }
    return {
        "status": snapshot.get("status", "unknown"),
        "raw_bytes": len(raw.encode()),
        "class_count": len(classes),
        "active_object_payload_estimate_bytes": sum(
            row["active_object_payload_estimate_bytes"] for row in classes.values()),
        "active_slab_page_estimate_bytes": sum(
            row["active_slab_page_estimate_bytes"] for row in classes.values()),
        "classes": classes,
    }


def command_slabinfo(row, point):
    return parse_slabinfo((row.get(point) or {}).get("slabinfo"))


def main():
    report = read_json(RAW)
    post = read_json(POST_CGROUPS)
    summary = {
        "schema": "sst_sourcefix_ab_summary_v1",
        "status": report["status"],
        "started_utc": report["started_utc"],
        "finished_utc": report["finished_utc"],
        "elapsed_seconds": report["elapsed_s"],
        "hard_deadline_utc": report["experiment_deadline_utc"],
        "test_parameters": report["ab_protocol"],
        "source_and_binary": report["source_and_binary"],
        "seed": {key: value for key, value in report["seed"].items()
                 if key != "space_snapshot"},
        "seed_space_snapshot": report["seed"]["space_snapshot"],
        "remote_report_path": "/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/control/ab-report.json",
        "remote_control_log_directory": "/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/control/node-control-846a5774-e232-4dce-b28c-74fea19f8571",
        "local_raw_report_sha256": hashlib.sha256(RAW.read_bytes()).hexdigest(),
        "paired_results": [],
        "groups": {},
        "command_end_slabinfo": {},
        "interpretation_limits": [
            "Short diagnostic only; not a performance claim.",
            "Populated per-command cgroup-end slabinfo snapshots are retained in the raw report; independent initial and final idle snapshots can be empty.",
            "Slab object-payload and active-slab-page figures are nominal estimates, not exact memory-controller charges; active object counts alone do not establish a file-descriptor leak.",
            "Raw report and raw samples are retained unchanged.",
            "The report Runner object used its legacy profile as a controller template to allow unbound A. Its profile_parameters/limits fields are template defaults; actual A/B values are recorded in ab_protocol, exact commands, and cgroup snapshots.",
            "Each B command has one initial sampler tick before the helper affinity event; that tick shows the inherited 0-95 set and occurs before helper cgroup join. The helper event records applied_before_cgroup=true; all later B samples show allowed CPUs 0-3 and last processors within 0-3."
        ]
    }

    for pair in report["ab_pairs"]:
        pair_item = {"pair": pair["pair"], "groups": {}}
        for trial in pair["order"]:
            label = trial.get("group")
            name = trial.get("name")
            if not label or not name:
                continue
            copy = next(row for row in report["commands"] if row["name"] == name + "-copy")
            run = next(row for row in report["commands"] if row["name"] == name + "-run")
            copy_summary = next(event for event in copy["events"]
                                if event.get("event") == "summary")
            measured = next(event for event in run["events"]
                            if event.get("event") == "phase_end" and event.get("phase") == "measured")
            pair_item["groups"][label] = {
                "name": name,
                "copy_valid": copy["valid"],
                "run_valid": run["valid"],
                "copy_elapsed_s": copy.get("elapsed_s"),
                "run_elapsed_s": run.get("elapsed_s"),
                "copy_files": copy_summary.get("files"),
                "copy_bytes": copy_summary.get("ordinary_copy_bytes"),
                "internal_links_recreated": copy_summary.get("reconstructed_internal_links"),
                "shared_with_seed": copy_summary.get("shared_with_seed"),
                "measured_ops": measured.get("ops"),
                "phase_validation_errors": run.get("phase_validation_errors"),
                "run_cgroup_current_kmem_bytes": run["cgroup_end"].get("memory.kmem.usage_in_bytes"),
                "run_cgroup_kmem_peak_bytes": run["cgroup_end"].get("memory.kmem.max_usage_in_bytes"),
                "run_cgroup_usage_peak_bytes": run["cgroup_end"].get("memory.max_usage_in_bytes"),
                "oom_kill_delta": run.get("oom_kill_delta"),
                "failcnt_delta_copy": copy.get("memory_failcnt_delta"),
                "failcnt_delta_run": run.get("memory_failcnt_delta")
            }
        summary["paired_results"].append(pair_item)

    for label in ("A", "B"):
        cgroup_path = "/sys/fs/cgroup/memory/mbsst-source-%s-20261004" % label.lower()
        command_rows = [row for row in report["commands"]
                        if (cgroup_path + "/tasks") in " ".join(row.get("wrapped_command", []))]
        all_cpus = set()
        effective_cpus = set()
        startup_samples = []
        allowed_counts = collections.Counter()
        sample_count = 0
        effective_sample_count = 0
        for row in command_rows:
            affinity = next((event for event in row["events"]
                             if event.get("event") == "cpu_affinity"), None)
            event_received_us = affinity.get("controller_received_monotonic_us") if affinity else None
            for sample in sample_rows(row):
                resources = sample.get("proc_resources") or {}
                threads = resources.get("threads") or []
                allowed = resources.get("allowed_cpu_list") or []
                allowed_counts[tuple(allowed)] += 1
                cpus = {thread.get("last_processor_cpu") for thread in threads
                        if thread.get("last_processor_cpu") is not None}
                all_cpus.update(cpus)
                sample_count += 1
                sample_us = int(sample["monotonic_s"] * 1000000)
                if label == "B" and (event_received_us is None or sample_us < event_received_us):
                    startup_samples.append({"command": row["name"],
                                            "sample_before_affinity_event_us": event_received_us - sample_us
                                            if event_received_us is not None else None,
                                            "allowed_cpu_list": allowed,
                                            "last_processor_cpus": sorted(cpus)})
                    continue
                if label == "A" or (allowed == [0, 1, 2, 3] and
                                     all(set(thread.get("allowed_cpu_list") or []) <= {0, 1, 2, 3}
                                         for thread in threads)):
                    effective_cpus.update(cpus)
                    effective_sample_count += 1
        initial = report["cgroup_initial"][label]["snapshot"]
        idle = post["mbsst-source-%s-20261004" % label.lower()]
        aggregate = report["group_metrics"][label]
        summary["groups"][label] = {
            "cgroup_path": cgroup_path,
            "initial_usage_bytes": initial.get("memory.usage_in_bytes"),
            "initial_kmem_usage_bytes": initial.get("memory.kmem.usage_in_bytes"),
            "postrun_idle_state": idle,
            "aggregate": {key: aggregate[key] for key in (
                "commands", "sample_count", "peak_cgroup_usage_bytes", "peak_cgroup_kmem_bytes",
                "peak_cgroup_rss_bytes", "peak_cgroup_cache_bytes", "peak_proc_vmrss_bytes",
                "peak_proc_rssanon_bytes", "peak_proc_rssfile_bytes", "peak_fd_count",
                "peak_thread_count", "oom_kill_delta", "failcnt_delta")},
            "sampled_processor_cpus_raw": sorted(all_cpus),
            "sampled_processor_cpus_after_expected_affinity": sorted(effective_cpus),
            "sample_count": sample_count,
            "samples_after_expected_affinity": effective_sample_count,
            "allowed_cpu_list_sample_counts": {
                ",".join(map(str, key)): value for key, value in allowed_counts.items()},
            "samples_before_expected_affinity": startup_samples
        }

    for row in report["commands"]:
        summary["command_end_slabinfo"][row["name"]] = command_slabinfo(row, "cgroup_end")

    out = BASE / "summary.json"
    out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    a, b = summary["groups"]["A"], summary["groups"]["B"]
    ak = [item["groups"]["A"]["run_cgroup_kmem_peak_bytes"]
          for item in summary["paired_results"]]
    bk = [item["groups"]["B"]["run_cgroup_kmem_peak_bytes"]
          for item in summary["paired_results"]]
    def class_value(command, class_name, field):
        return summary["command_end_slabinfo"][command]["classes"].get(class_name, {}).get(field, 0)

    slab_rows = "\n".join(
        "| {label}{idx} | {ro} / {rp} / {rs} B | {bo} / {bp} / {bs} B | {ko} / {kp} / {ks} B |".format(
            label=label, idx=idx,
            ro=class_value(f"{label}{idx}-run", "radix_tree_node", "active_objects"),
            rp=class_value(f"{label}{idx}-run", "radix_tree_node", "active_object_payload_estimate_bytes"),
            rs=class_value(f"{label}{idx}-run", "radix_tree_node", "active_slab_page_estimate_bytes"),
            bo=class_value(f"{label}{idx}-run", "buffer_head", "active_objects"),
            bp=class_value(f"{label}{idx}-run", "buffer_head", "active_object_payload_estimate_bytes"),
            bs=class_value(f"{label}{idx}-run", "buffer_head", "active_slab_page_estimate_bytes"),
            ko=class_value(f"{label}{idx}-run", "kmalloc-4k", "active_objects"),
            kp=class_value(f"{label}{idx}-run", "kmalloc-4k", "active_object_payload_estimate_bytes"),
            ks=class_value(f"{label}{idx}-run", "kmalloc-4k", "active_slab_page_estimate_bytes"))
        for idx in range(1, 5) for label in ("A", "B"))
    readme = """# Small SST CPU-affinity / kmem A/B

This bounded diagnostic compared four paired trials on `node-ssd` with the same validated 1M-key seed, binary, adaptive SST capacity, offered rate, cache and queue settings. A inherited the host's allowed CPU set without binding. B used the frozen `node_run.py` helper to apply CPUs 0-3 before joining the cgroup and execing the copy or benchmark command. Order was A1/B1 through A4/B4.

The run completed at {finish} after {elapsed:.3f} seconds (start {start}). All eight independent copies and all eight benchmark commands exited successfully. Each copy reported 170 files, {copy_bytes} bytes copied, 44 within-destination hardlinks recreated, and `shared_with_seed=false`. Each measured phase completed 1,070 operations in 10 seconds. There were zero OOM kills and no command timeouts.

| Metric | A: inherited CPUs | B: CPUs 0-3 |
| --- | ---: | ---: |
| Initial cgroup usage / kmem | {a0} / {ak0} B | {b0} / {bk0} B |
| End idle usage / kmem | {au} / {akm} B | {bu} / {bkm} B |
| Cgroup peak usage | {ap} B | {bp} B |
| Cgroup kmem high-water after runs 1-4 | {ak} B | {bk} B |
| Highest sampled process VmRSS | {av} B | {bv} B |
| Highest sampled process RssAnon | {aa} B | {ba} B |
| Peak threads / FDs | {at} / {afd} | {bt} / {bfd} |
| OOM kill counter delta | 0 | 0 |
| `memory.failcnt` delta | {af} | {bf} |
| `memory.kmem.slabinfo` at command end | Populated; see class estimates below | Populated; see class estimates below |

A's kmem high-water rose over the four runs to {aklast} B; B reached {bklast} B and ended with {bkm} B residual kmem. Process RSS/RssAnon peaks were similar. This is evidence that unrestricted CPU affinity is associated with materially larger residual kernel memory in this workload and may explain part of the 48 MiB headroom gap. The probe does not identify why the allocation differed. Treat the result as a strong diagnostic signal, not proof of a specific kernel mechanism or the exact cause of an earlier OOM.

## Per-command slabinfo evidence

The runner captured populated cgroup-end slab tables for all 16 copy/run commands (2,451 bytes after copy; 2,665 bytes after benchmark runs). The independent initial group and final idle snapshots are empty; those do not describe command-end state. The table reports four selected classes after each run. `active payload` is active object count x object size; `active slab pages` is active slabs x pages per slab x 4,096. Both are nominal slabinfo estimates, not exact memcg charges.

| Run | radix_tree_node active objects / payload / slab pages | buffer_head active objects / payload / slab pages | kmalloc-4k active objects / payload / slab pages |
| --- | ---: | ---: | ---: |
{slab_rows}

At A4-run, `radix_tree_node` was {a4radixobjs} active objects ({a4radixpayload} B nominal object payload; {a4radixpages} B active slab pages), versus {b4radixobjs} ({b4radixpayload} B; {b4radixpages} B) at B4-run. `buffer_head` was {a4bufferobjs} active objects at A4-run and {b4bufferobjs} at B4-run. A4-run also listed `ext4_inode_cache`, `proc_inode_cache`, `pid`, `signal_cache`, `sighand_cache`, `files_cache`, and `task_delay_info`; see the raw report for all class rows. These end snapshots support a class-level association with larger kernel metadata under A, especially radix-tree nodes, while not identifying why that allocation differed. Slabinfo object/slab estimates do not equal `memory.kmem.usage_in_bytes`; active object counts alone do not establish a file-descriptor leak.

Both groups ran close to the fixed 48 MiB limit and accumulated `memory.failcnt`, but neither recorded an OOM kill. At the final idle read, A and B tasks/cgroup.procs were empty. A/B commands never targeted the third fixed-test cgroup; it was subsequently used by the separately authorized formal small run.

## CPU sampling detail

The B helper itself initially started with the inherited 0-95 set. The one-second sampler recorded one initial tick for each B command before the helper's `cpu_affinity` event: each shows an unrestricted allowed set and a last CPU outside 0-3. The event records `applied_before_cgroup=true`; the helper then joined the cgroup and execed the child. All {bpost} later B samples show allowed CPUs 0-3 and last processors within 0-3. These initial samples remain in the raw report. The raw all-sample CPU union includes CPUs {braw}; the post-affinity workload CPU set is 0-3. A inherited CPUs 0-95 and observed processors across {acount} CPUs.

The controller reused the frozen `Runner` API with `profile=legacy` as a template so A could remain unbound. Therefore JSON `profile_parameters` and `limits` fields show controller-template defaults, including 96 MiB and an 8 MiB queue; they are not the A/B configuration. Actual values are in `ab_protocol`, exact command argv, and the 48 MiB/swappiness=0 cgroup snapshots. Raw command and sample data have not been edited to hide these fields or startup samples.

## Identity and artifacts

- Binary SHA-256 at start and end: `{binsha}`.
- Frozen controller SHA-256: `{runsha}`; source manifest SHA-256: `{manifestsha}`.
- Validated seed: `{seedssd}` and matching HDD path `{seedhdd}`; its prior run recorded success and 22 SSD SSTs.
- New trial data remains on node-ssd under `{ssdroot}` and `{hddroot}`. The remote roots held 39,140,860 SSD bytes and 8,758,174,497 HDD bytes after the probe. The large data and benchmark binary were not downloaded.
- Remote raw report: `/home/lj/ssd/metabypass_test/sst-sourcefix-ab-20261004/control/ab-report.json`.
- Local unchanged raw report: `raw-node/ab-report.json` (SHA-256 `{rawsha}`). Full argv, stdout, stderr, ACK, process and one-second sample logs are in `raw-node/node-control-846a5774-e232-4dce-b28c-74fea19f8571/`.
- Idle A/B cgroup `memory.stat` and kmem state: `metadata/ab-cgroups-postrun.json`.
- Derived metrics: `summary.json`.
""".format(
        finish=report["finished_utc"], elapsed=report["elapsed_s"], start=report["started_utc"],
        copy_bytes=summary["paired_results"][0]["groups"]["A"]["copy_bytes"],
        a0=a["initial_usage_bytes"], ak0=a["initial_kmem_usage_bytes"],
        b0=b["initial_usage_bytes"], bk0=b["initial_kmem_usage_bytes"],
        au=a["postrun_idle_state"]["memory.usage_in_bytes"],
        akm=a["postrun_idle_state"]["memory.kmem.usage_in_bytes"],
        bu=b["postrun_idle_state"]["memory.usage_in_bytes"],
        bkm=b["postrun_idle_state"]["memory.kmem.usage_in_bytes"],
        ap=a["aggregate"]["peak_cgroup_usage_bytes"], bp=b["aggregate"]["peak_cgroup_usage_bytes"],
        ak=", ".join(map(str, ak)), bk=", ".join(map(str, bk)),
        av=a["aggregate"]["peak_proc_vmrss_bytes"], bv=b["aggregate"]["peak_proc_vmrss_bytes"],
        aa=a["aggregate"]["peak_proc_rssanon_bytes"], ba=b["aggregate"]["peak_proc_rssanon_bytes"],
        at=a["aggregate"]["peak_thread_count"], afd=a["aggregate"]["peak_fd_count"],
        bt=b["aggregate"]["peak_thread_count"], bfd=b["aggregate"]["peak_fd_count"],
        af=a["aggregate"]["failcnt_delta"], bf=b["aggregate"]["failcnt_delta"],
        aklast=ak[-1], bklast=bk[-1], bpost=b["samples_after_expected_affinity"],
        slab_rows=slab_rows,
        a4radixobjs=class_value("A4-run", "radix_tree_node", "active_objects"),
        a4radixpayload=class_value("A4-run", "radix_tree_node", "active_object_payload_estimate_bytes"),
        a4radixpages=class_value("A4-run", "radix_tree_node", "active_slab_page_estimate_bytes"),
        b4radixobjs=class_value("B4-run", "radix_tree_node", "active_objects"),
        b4radixpayload=class_value("B4-run", "radix_tree_node", "active_object_payload_estimate_bytes"),
        b4radixpages=class_value("B4-run", "radix_tree_node", "active_slab_page_estimate_bytes"),
        a4bufferobjs=class_value("A4-run", "buffer_head", "active_objects"),
        b4bufferobjs=class_value("B4-run", "buffer_head", "active_objects"),
        braw=", ".join(map(str, b["sampled_processor_cpus_raw"])),
        acount=len(a["sampled_processor_cpus_after_expected_affinity"]),
        binsha=report["source_and_binary"]["binary_sha256_start"],
        runsha=report["source_and_binary"]["node_run_sha256"],
        manifestsha=report["source_and_binary"]["source_manifest_sha256"],
        seedssd=report["seed"]["ssd"], seedhdd=report["seed"]["hdd"],
        ssdroot=report["data_roots"][0], hddroot=report["data_roots"][1], rawsha=summary["local_raw_report_sha256"])
    (BASE / "README.md").write_text(readme)
    print("wrote", out, out.stat().st_size)
    print("wrote", BASE / "README.md", (BASE / "README.md").stat().st_size)


if __name__ == "__main__":
    main()
