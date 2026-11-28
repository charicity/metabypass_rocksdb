#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

"""Archive and analyze published MetaBypass checkpoints in YCSB-A or YCSB-D."""

import argparse
import csv
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid

from run import SST_ASYNC_DEFAULTS, add_sst_async_arguments, one, positive, save


WORKLOAD_SAMPLES = {"workloada": "ycsb-a", "workloadd": "ycsb-d"}
REPORTED_LEVELS = (1, 2, 5)
CHECKPOINT_METRICS = (
    "last_build_micros", "last_checkpoint_sst_copy_bytes", "last_checkpoint_sst_ready_bytes",
    "last_checkpoint_sst_wait_micros", "last_checkpoint_sst_micros",
)
CUMULATIVE_COUNTERS = (
    "sst_async_copy_bytes", "sst_async_discarded_bytes", "sst_async_cancelled_bytes",
    "wal_backpressure_micros", "io_backup_write_bytes",
)
GAUGES = ("sst_async_staged_bytes", "wal_queued_bytes", "peak_queued_bytes")
POINT = re.compile(r"point-([1-9][0-9]*)")
MANIFEST = re.compile(r"MANIFEST-[0-9]+")
OPTIONS = re.compile(r"OPTIONS-[0-9]+")
SST = re.compile(r"([0-9]+)\.(?:sst|ldb)")
WAL = re.compile(r"[0-9]+\.log")
CF = re.compile(r'-+ Column family "(.*)" +\(ID ([0-9]+)\) -+')
LEVEL = re.compile(r"--- level ([0-9]+) --- version# ")
TABLE = re.compile(r" +([0-9]+):([0-9]+)\[")
DUMP_END = re.compile(
    r"next_file_number [0-9]+ last_sequence [0-9]+ +prev_log_number [0-9]+ "
    r"max_column_family [0-9]+ min_log_number_to_keep [0-9]+"
)
MAX_SEQUENCE_NUMBER = (1 << 56) - 1
MAX_WAL_FILE_NUMBER = (1 << 64) - 1


def read_name(contents, pattern):
    name = contents[:-1] if contents.endswith("\n") else contents
    if pattern.fullmatch(name) is None:
        raise ValueError(f"invalid name: {contents!r}")
    return name


def read_inventory(contents):
    header, *rows = contents.splitlines()
    if header != "MBI7" or not rows:
        raise ValueError("expected nonempty MBI7 inventory")
    inventory = {}
    for row in rows:
        name, length = row.split()
        if name in inventory or not re.fullmatch(r"[0-9]+", length) or not (
            name in ("CURRENT", "IDENTITY")
            or any(pattern.fullmatch(name) for pattern in (MANIFEST, OPTIONS, SST, WAL))
        ):
            raise ValueError(f"invalid inventory row: {row!r}")
        inventory[name] = int(length)
    return inventory


def current_manifest(point):
    return read_name((point / "CURRENT").read_text(), MANIFEST)


def archive_latest(backup, archives):
    try:
        latest = (backup / "LATEST").read_text()
    except FileNotFoundError:
        return None
    observed = time.monotonic_ns()
    name = read_name(latest, POINT)
    target = archives / name
    if target.exists():
        return name
    source = backup / name
    temp = archives / ".capture"
    temp.mkdir()
    try:
        manifest = current_manifest(source)
        inventory = read_inventory((source / "INVENTORY").read_text())
        metadata = ("CURRENT", manifest, *(key for key in inventory if OPTIONS.fullmatch(key)))
        for key in (*metadata, "INVENTORY", "RECOVERY"):
            shutil.copyfile(source / key, temp / key)
            if key in metadata and (temp / key).stat().st_size != inventory[key]:
                raise ValueError(f"{name}: archived length differs from INVENTORY for {key}")
        (temp / "LATEST").write_text(latest)
        save(temp / "observation.json", {"observed_monotonic_ns": observed})
        temp.rename(target)
    except FileNotFoundError:
        # GC can win if a different point replaced LATEST while copying.
        if (backup / "LATEST").read_text() == latest:
            raise
    finally:
        if temp.exists():
            shutil.rmtree(temp)
    return name


def run_study(args):
    driver, ldb, dataset, workload = (
        path.resolve(strict=True) for path in (args.driver, args.ldb, args.dataset, args.workload)
    )
    archive_sample = WORKLOAD_SAMPLES.get(workload.name)
    if archive_sample is None:
        raise ValueError("--workload must point to the YCSB-cpp workloada or workloadd profile")
    output = args.output.resolve()
    if (args.front_root is None) != (args.back_root is None):
        raise ValueError("--front-root and --back-root must be supplied together")
    external = args.front_root is not None
    if external:
        front, back = (p.resolve(strict=True) for p in (args.front_root, args.back_root))
        if not front.is_dir() or not back.is_dir():
            raise ValueError("both data roots must be directories")
        if front == back or front in back.parents or back in front.parents:
            raise ValueError("front and back roots must be disjoint")
        sample = f"{archive_sample}-{uuid.uuid4().hex}"
    else:
        front, back = output / "front", output / "back"
        sample = archive_sample
    output.mkdir()
    archives = output / "points"
    archives.mkdir()
    if not external:
        front.mkdir()
        back.mkdir()
    backup = back / sample / "backup"
    command = [
        sys.executable, str(Path(__file__).with_name("run.py")), "run",
        "--driver", str(driver), "--dataset", str(dataset), "--workload", str(workload),
        "--ssd-root", str(front), "--hdd-root", str(back), "--sample", sample,
        "--engine", "mb", "--keep-data",
    ]
    for key in ("threads", "warmup_seconds", "measure_seconds", "checkpoint_ms", "timeout",
                *SST_ASYNC_DEFAULTS):
        command.extend(("--" + key.replace("_", "-"), str(getattr(args, key))))
    if args.cgroup is not None:
        command.extend(("--cgroup", str(args.cgroup)))
    if args.memory_gib is not None:
        command.extend(("--memory-gib", str(args.memory_gib)))
    save(output / "command.json", command)
    archive_latest(backup, archives)
    with (output / "runner.log").open("wb") as logs:
        child = subprocess.Popen(command, stdout=logs, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        try:
            while child.poll() is None:
                archive_latest(backup, archives)
                time.sleep(0.1)
        finally:
            if child.poll() is None:
                # The runner handles SIGINT and reaps its writer in run_stage.
                try:
                    child.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    pass
                child.wait()
        final_latest = archive_latest(backup, archives)
    capture = {
        "workload": workload.name,
        "final_latest": final_latest,
        "runner_returncode": child.returncode,
        "front_sample": str(front / sample),
        "back_sample": str(back / sample),
        "front_device": front.stat().st_dev,
        "back_device": back.stat().st_dev,
    }
    save(output / "capture.json", capture)
    back_sample = back / sample
    if external and back_sample.is_dir():
        shutil.copytree(back_sample, output / "back" / archive_sample,
                        ignore=shutil.ignore_patterns("backup", "data"))
    if child.returncode:
        raise ValueError(f"run.py exited {child.returncode}; see {output / 'runner.log'}")
    report = summarize(output, ldb)
    if args.cleanup_data:
        shutil.rmtree(front / sample)
        shutil.rmtree(backup)
        if (back_sample / "data").exists():
            shutil.rmtree(back_sample / "data")
        save(output / "capture.json", dict(capture, data_cleaned=True))
    return report


def parse_dump(contents):
    edits, live, families, levels = [], {}, [], []
    level = None
    lines = contents.rstrip("\n").split("\n")
    if not DUMP_END.fullmatch(lines[-1]):
        raise ValueError("missing final ldb MANIFEST summary")
    for line in lines[:-1]:
        if not families and line.startswith("{"):
            edit = json.loads(line)
            if edit["EditNumber"] != len(edits) or edit["ColumnFamily"] != 0:
                raise ValueError("expected sequential edits for the default column family")
            edits.append(edit)
        elif family := CF.fullmatch(line):
            families.append(family.groups())
        elif header := LEVEL.match(line):
            level = int(header[1])
            levels.append(level)
        elif table := TABLE.match(line):
            number, size = map(int, table.groups())
            if level is None or number in live:
                raise ValueError(f"invalid table in final MANIFEST state: {line}")
            live[number] = (level, size)
    if families != [("default", "0")] or not levels or not edits:
        raise ValueError("incomplete ldb output or unexpected column families")
    if levels != list(range(len(levels))):
        raise ValueError("incomplete ldb level headers")
    return live, edits


def validate_ssts(inventory, live):
    sizes = {}
    for name, length in inventory.items():
        if match := SST.fullmatch(name):
            number = int(match[1])
            if number in sizes:
                raise ValueError(f"multiple INVENTORY entries for SST {number}")
            sizes[number] = length
    manifest_sizes = {number: size for number, (_, size) in live.items()}
    if sizes != manifest_sizes:
        different = sorted(number for number in sizes.keys() | manifest_sizes.keys()
                           if sizes.get(number) != manifest_sizes.get(number))
        raise ValueError(f"MANIFEST and INVENTORY SST numbers/lengths differ: {different}")


def read_recovery_boundary(path):
    fields = re.findall(r"\S+", path.read_text(), flags=re.ASCII)
    exact = fields[:1] != ["MBR2"]
    if not exact:
        fields = fields[1:]
    # Strip leading zeroes before checking the maximum uint64 decimal width.
    digits = [field.lstrip("0") or "0" for field in fields]
    if len(digits) not in ((2,) if exact else (2, 3)) or any(
        not re.fullmatch(r"[0-9]{1,20}", field) for field in digits
    ):
        raise ValueError(f"{path}: invalid RECOVERY boundary")
    sequence, wal_floor, *manifest = map(int, digits)
    manifest_sequence = manifest[0] if manifest else None
    if (sequence > MAX_SEQUENCE_NUMBER or wal_floor > MAX_WAL_FILE_NUMBER
            or (manifest_sequence is not None
                and not sequence <= manifest_sequence <= MAX_SEQUENCE_NUMBER)):
        raise ValueError(f"{path}: invalid RECOVERY boundary")
    return dict(sequence=sequence, wal_floor=wal_floor,
                sequence_boundary_type="exact" if exact else "lower_bound",
                manifest_sequence=manifest_sequence)


def read_point(path, ldb):
    if read_name((path / "LATEST").read_text(), POINT) != path.name:
        raise ValueError(f"{path}: archived LATEST does not match point")
    manifest = current_manifest(path)
    inventory = read_inventory((path / "INVENTORY").read_text())
    options = [name for name in inventory if OPTIONS.fullmatch(name)]
    if len(options) != 1:
        raise ValueError(f"{path}: expected one checkpoint OPTIONS file")
    for name in ("CURRENT", manifest, *options):
        if (path / name).stat().st_size != inventory[name]:
            raise ValueError(f"{path}: archived length differs from INVENTORY for {name}")
    configured_levels = re.findall(r"^\s*num_levels\s*=\s*([1-9][0-9]*)\s*$",
                                   (path / options[0]).read_text(), re.MULTILINE)
    if len(configured_levels) != 1:
        raise ValueError(f"{path}: expected one OPTIONS num_levels value")
    levels = int(configured_levels[0])
    dump = subprocess.run([ldb, "manifest_dump", f"--path={path / manifest}", "--hex", "--json"],
                          check=True, capture_output=True, text=True)
    if dump.stderr:
        raise ValueError(f"{path}: {dump.stderr.strip()}")
    live, edits = parse_dump(dump.stdout)
    validate_ssts(inventory, live)
    if any(level >= levels for level, _ in live.values()):
        raise ValueError(f"{path}: MANIFEST level exceeds OPTIONS num_levels")
    boundary = read_recovery_boundary(path / "RECOVERY")
    wal_bytes = sum(size for name, size in inventory.items() if WAL.fullmatch(name))
    metadata_bytes = sum(size for name, size in inventory.items()
                         if not (SST.fullmatch(name) or WAL.fullmatch(name)))
    metadata_bytes += sum((path / name).stat().st_size for name in ("RECOVERY", "INVENTORY"))
    observed = json.loads((path / "observation.json").read_text())["observed_monotonic_ns"]
    return dict(live=live, levels=levels, edits=edits, manifest=manifest,
                observed_monotonic_ns=observed, wal_bytes=wal_bytes,
                metadata_bytes=metadata_bytes, **boundary)


def level_rows(current, previous, levels):
    rows = [dict(level=level, sst_files=0, sst_bytes=0,
                 added_files=0 if previous is not None else None,
                 added_bytes=0 if previous is not None else None,
                 moved_in_files=0 if previous is not None else None,
                 moved_in_bytes=0 if previous is not None else None)
            for level in range(levels)]
    for number, (level, size) in current.items():
        row = rows[level]
        row["sst_files"] += 1
        row["sst_bytes"] += size
        if previous is None:
            continue
        if number not in previous:
            row["added_files"] += 1
            row["added_bytes"] += size
        else:
            before, old_size = previous[number]
            if old_size != size:
                raise ValueError(f"SST {number} changed length between points")
            if before != level:
                row["moved_in_files"] += 1
                row["moved_in_bytes"] += size
    return rows


def transient_files(previous, current, edits):
    live, born, highest = dict(previous), {}, {}
    for edit in edits:
        for deleted in edit.get("DeletedFiles", ()):
            number = deleted["FileNumber"]
            if number not in live or live[number][0] != deleted["Level"]:
                raise ValueError(f"MANIFEST deleted SST {number} at an unexpected level")
            del live[number]
        for added in edit.get("AddedFiles", ()):
            number, level, size = (added[key] for key in ("FileNumber", "Level", "FileSize"))
            if number not in previous:
                if born.setdefault(number, size) != size:
                    raise ValueError(f"SST {number} changed length within MANIFEST")
                highest[number] = max(highest.get(number, level), level)
            elif previous[number][1] != size:
                raise ValueError(f"SST {number} changed length within MANIFEST")
            live[number] = (level, size)
    if live != current:
        raise ValueError("MANIFEST edits do not replay to the next point's SST set")
    dead = born.keys() - current.keys()
    result = dict(transient_files=len(dead), transient_bytes=sum(born[n] for n in dead))
    for level in REPORTED_LEVELS:
        touched = [n for n in dead if highest[n] >= level]
        result[f"transient_touch_l{level}plus_files"] = len(touched)
        result[f"transient_touch_l{level}plus_bytes"] = sum(born[n] for n in touched)
    return result


def short_lived(archives, previous, current):
    if previous["manifest"] != current["manifest"]:
        return {"manifest_coverage": "different_manifest"}
    name = current["manifest"]
    first = archives / previous["point"] / name
    second = archives / current["point"] / name
    with first.open("rb") as old, second.open("rb") as new:
        while block := old.read(1 << 20):
            if new.read(len(block)) != block:
                return {"manifest_coverage": "not_a_prefix"}
    prefix = len(previous["edits"])
    if current["edits"][:prefix] != previous["edits"]:
        return {"manifest_coverage": "edit_prefix_mismatch"}
    return dict(manifest_coverage="covered",
                **transient_files(previous["live"], current["live"], current["edits"][prefix:]))


def writer_publications(events, fault, warmup_start):
    initial = one([event for event in events if event.get("initial") is True and
                   event.get("completed") is True], "backup")
    initial_count = initial["recovery_points"]
    if initial_count < 1 or initial["end_ns"] > warmup_start:
        raise ValueError("invalid initial backup confirmation")
    publications = {}
    for event in events:
        if event.get("last_publish_micros") is None:
            continue
        if event["event"] not in ("backup", "progress", "measurement_start", "measurement_end"):
            continue
        timestamp = event["last_publish_micros"] * 1000
        sampled = event.get("stats_sample_monotonic_ns", event.get("query_end_ns", fault - 1))
        if timestamp >= fault or sampled >= fault:
            continue
        count = event["recovery_points"]
        if count < initial_count or timestamp <= 0:
            raise ValueError("invalid writer publication counters")
        old = publications.get(count)
        if old is not None and old["last_publish_micros"] != event["last_publish_micros"]:
            raise ValueError(f"point-{count}: inconsistent publication timestamps")
        if old is None or sampled < old.get("stats_sample_monotonic_ns", fault):
            publications[count] = event
    timestamps = [publications[count]["last_publish_micros"] for count in sorted(publications)]
    if timestamps != sorted(timestamps):
        raise ValueError("writer publication timestamps moved backwards")
    return initial_count, max(publications, default=initial_count), publications


def measurement_backup(start, end, events, points):
    snapshots = (start, end)
    counters = {}
    for key in CUMULATIVE_COUNTERS:
        if all(key in sample for sample in snapshots):
            delta = end[key] - start[key]
            if delta < 0:
                raise ValueError(f"measurement counter moved backwards: {key}")
            counters[key] = delta
    in_window = [start, end] + [
        event for event in events
        if event["event"] == "progress" and
        start["monotonic_ns"] <= event.get("stats_sample_monotonic_ns", -1) < end["monotonic_ns"]
    ]
    sampled = [point for point in points if "last_checkpoint_sst_copy_bytes" in point]
    return {
        "counter_deltas": counters,
        "measurement_start_stats_monotonic_ns": start.get("stats_sample_monotonic_ns"),
        "measurement_end_stats_monotonic_ns": end.get("stats_sample_monotonic_ns"),
        "max_sampled_wal_queued_bytes": max(
            (event["wal_queued_bytes"] for event in in_window if "wal_queued_bytes" in event),
            default=None,
        ),
        "process_queue_peak_upper_bound_bytes_at_measurement_end": end.get("peak_queued_bytes"),
        "checkpoint_metric_points": len(sampled),
        "published_checkpoint_sst_copy_bytes": sum(
            point["last_checkpoint_sst_copy_bytes"] for point in sampled) if sampled else None,
        "published_checkpoint_sst_ready_bytes": sum(
            point["last_checkpoint_sst_ready_bytes"] for point in sampled) if sampled else None,
    }


def phase(generation, initial, timestamp, warmup, start, end):
    if generation == initial:
        return "initial"
    if generation < initial or (timestamp is not None and timestamp < warmup):
        return "load"
    if timestamp is None:
        return "unknown"
    if timestamp < start:
        return "warmup"
    if timestamp < end:
        return "measurement"
    return "after_measurement"


def write_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def summarize(output, ldb):
    capture = json.loads((output / "capture.json").read_text())
    # Older captures identified Workload A by its fixed archive directory.
    workload = capture.get("workload", "workloada")
    sample = output / "back" / WORKLOAD_SAMPLES[workload]
    writer = sample / "writer"
    result = json.loads((sample / "result.json").read_text())
    if capture["runner_returncode"] or result["status"] != "passed":
        raise ValueError("a successful run.py sample is required")
    # SIGKILL may leave a partial final stdout record.
    records = (writer / "stdout.jsonl").read_bytes().split(b"\n")[:-1]
    events = [json.loads(record) for record in records]
    start_event = one(events, "measurement_start")
    end_event = one(events, "measurement_end")
    start, end = (event["monotonic_ns"] for event in (start_event, end_event))
    warmup = start - result["initial"]["warmup_seconds"] * 10**9
    fault = json.loads((writer / "process.json").read_text())["fault"]["monotonic_ns"]
    initial, confirmed, publication = writer_publications(events, fault, warmup)

    archives = output / "points"
    observed = sorted(int(match[1]) for p in archives.iterdir() if (match := POINT.fullmatch(p.name)))
    final = capture["final_latest"]
    if not observed or observed != list(range(1, len(observed) + 1)) or not (
        confirmed <= len(observed) <= confirmed + 1
    ):
        raise ValueError(f"nonconsecutive publication archive: writer confirms through "
                         f"point-{confirmed}, final LATEST is {final}, observer captured {observed}")
    if f"point-{observed[-1]}" != final:
        raise ValueError("final LATEST was not archived")
    rows, points, previous = [], [], None
    for generation in observed[:confirmed]:
        name = f"point-{generation}"
        info = read_point(archives / name, ldb)
        stats = publication.get(generation, {})
        timestamp = stats["last_publish_micros"] * 1000 if stats else None
        if previous and info["sequence"] < previous["sequence"]:
            raise ValueError(f"{name}: RECOVERY sequence moved backwards")
        info.update(point=name, phase=phase(generation, initial, timestamp, warmup, start, end))
        labels = dict(point=name, phase=info["phase"],
                      previous_point=previous["point"] if previous else None,
                      previous_phase=previous["phase"] if previous else None)
        rows.extend(dict(labels, **row) for row in
                    level_rows(info["live"], previous["live"] if previous else None, info["levels"]))
        points.append(dict(labels, published_monotonic_ns=timestamp,
                           **{key: info[key] for key in ("observed_monotonic_ns", "sequence",
                                                        "wal_floor", "wal_bytes", "metadata_bytes",
                                                        "manifest", "levels",
                                                        "sequence_boundary_type",
                                                        "manifest_sequence")},
                           **(short_lived(archives, previous, info) if previous else
                              {"manifest_coverage": "no_previous_point"}),
                           **{key: stats[key] for key in (*CHECKPOINT_METRICS, *CUMULATIVE_COUNTERS,
                                                         *GAUGES, "stats_sample_monotonic_ns")
                              if key in stats}))
        previous = info

    measurement = [row for row in rows if row["phase"] == "measurement" and
                   row["added_bytes"] is not None]
    measured_points = [point for point in points if point["phase"] == "measurement"]
    totals = {key: sum(row[key] for row in measurement)
              for key in ("added_files", "added_bytes", "moved_in_files", "moved_in_bytes")}
    for level in REPORTED_LEVELS:
        for quantity in ("bytes", "files"):
            key = f"added_{quantity}"
            totals[f"l{level}plus_added_{quantity}"] = sum(
                row[key] for row in measurement if row["level"] >= level)
        totals[f"l{level}plus_added_byte_share"] = (
            totals[f"l{level}plus_added_bytes"] / totals["added_bytes"] if totals["added_bytes"] else None)

    covered = [point for point in measured_points if point["manifest_coverage"] == "covered"]
    transient_keys = ["transient_files", "transient_bytes"]
    transient_keys.extend(f"transient_touch_l{level}plus_{quantity}"
                          for level in REPORTED_LEVELS for quantity in ("files", "bytes"))
    transient = {key: sum(point[key] for point in covered) if covered else None
                 for key in transient_keys}
    summary = dict(
        workload=workload, confirmed_points=confirmed, initial_point=f"point-{initial}",
        excluded_unconfirmed_points=[f"point-{n}" for n in observed[confirmed:]],
        unknown_phase_points=[point["point"] for point in points if point["phase"] == "unknown"],
        warmup_start_monotonic_ns=warmup, measurement_start_monotonic_ns=start,
        measurement_end_monotonic_ns=end, sigkill_monotonic_ns=fault,
        ldb=str(ldb), ops_per_second=result["ops_per_second"], p99_ns=end_event.get("p99_ns"),
        sst_async={key: result["initial"].get(key, default)
                   for key, default in SST_ASYNC_DEFAULTS.items()},
        measurement_backup=measurement_backup(start_event, end_event, events, measured_points),
        measurement_points=len(measured_points), measurement_intervals=len({r["point"] for r in measurement}),
        measurement_manifest_covered_intervals=len(covered),
        measurement_wal_bytes_per_point={p["point"]: p["wal_bytes"] for p in measured_points},
        measurement_metadata_bytes_per_point={p["point"]: p["metadata_bytes"] for p in measured_points},
        measurement_sst=totals, measurement_transient_sst=transient,
    )
    write_csv(output / "levels.csv", rows)
    write_csv(output / "points.csv", points)
    save(output / "summary.json", summary)
    return output / "summary.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    collect = sub.add_parser("run")
    offline = sub.add_parser("summarize")
    for command in (collect, offline):
        for key in ("output", "ldb"):
            command.add_argument("--" + key, type=Path, required=True)
    for key in ("driver", "dataset", "workload"):
        collect.add_argument("--" + key, type=Path, required=True)
    collect.add_argument("--front-root", type=Path, help="existing foreground data directory")
    collect.add_argument("--back-root", type=Path, help="existing backup data directory")
    collect.add_argument("--cgroup", type=Path, help="existing cgroup v1 memory group for the driver")
    collect.add_argument("--memory-gib", type=positive,
                         help="expected cgroup memory limit in GiB (runner default: 32)")
    collect.add_argument("--cleanup-data", action="store_true",
                         help="remove this run's database data after archiving and summarizing")
    add_sst_async_arguments(collect)
    for key, default in (("threads", 16), ("warmup-seconds", 65), ("measure-seconds", 180),
                         ("checkpoint-ms", 40000), ("timeout", 1800)):
        collect.add_argument("--" + key, type=positive, default=default)
    args = parser.parse_args()
    if args.command == "run":
        print(run_study(args))
    else:
        print(summarize(args.output.resolve(strict=True), args.ldb.resolve(strict=True)))


if __name__ == "__main__":
    main()
