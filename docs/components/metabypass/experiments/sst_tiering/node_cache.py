#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Read-only Linux mincore sampling of current routed SSTs, outside the cgroup."""

import ctypes
import os
from pathlib import Path
import re
import sys
import time


def crc32c(data):
    result = 0xffffffff
    for byte in data:
        result ^= byte
        for _ in range(8):
            result = (result >> 1) ^ (0x82f63b78 if result & 1 else 0)
    return result ^ 0xffffffff


def parse_placement(payload, identity):
    data, checksum = payload.rsplit(b"CRC ", 1)
    if crc32c(data) != int(checksum.strip()):
        raise ValueError("placement CRC32C mismatch")
    lines = data.decode("ascii").splitlines()
    if not lines or lines[0].split() != ["MBS1", identity.strip()]:
        raise ValueError("placement identity/version mismatch")
    entries = {}
    for line in lines[1:]:
        fields = line.split()
        if len(fields) != 5:
            raise ValueError("invalid placement field count")
        number, size, checksum, hot = map(int, fields[:4])
        name = fields[4]
        if number <= 0 or size <= 0 or hot not in (0, 1) or number in entries or not \
                re.fullmatch(r"[1-9][0-9]*-" + str(number) + "-" + str(checksum) + r"\.sst", name):
            raise ValueError("invalid placement object or duplicate table")
        entries[number] = {"size": size, "hot": bool(hot), "object": name}
    return entries


def local_tables(index):
    result = {}
    for path in Path(index).iterdir():
        if not re.fullmatch(r"[0-9]+\.sst", path.name):
            continue
        number = int(path.stem)
        if number in result or path.is_symlink():
            raise ValueError("duplicate table number or symlink")
        result[number] = path
    return result


def file_signature(path):
    info = Path(path).lstat()
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def routed_tables(index, backup, mode, stats=None):
    index, backup = Path(index), Path(backup)
    local = local_tables(index)
    placement = None
    entries = {}
    if mode != "disabled":
        placement = (index / "SST-PLACEMENT").read_bytes()
        entries = parse_placement(placement, (index / "IDENTITY").read_text())
    routed = {}
    for number, entry in entries.items():
        if entry["hot"]:
            if number not in local:
                raise ValueError("hot table missing from SSD")
            path = local[number]
        else:
            path = backup / "sst-store" / entry["object"]
        routed[number] = {"path": str(path), "size": entry["size"],
                          "source": "ssd_hot" if entry["hot"] else "hdd_cold"}
    unprotected = 0
    for number, path in local.items():
        if number in routed:
            continue  # A cold route's retired/local copy is not another live SST.
        size = path.stat().st_size
        routed[number] = {"path": str(path), "size": size, "source": "ssd_unprotected"}
        unprotected += size
    expected = sum(item["size"] for item in routed.values())
    if mode != "disabled" and stats is not None:
        # Unmapped local files may also be retired or in-progress compaction output.
        # Refuse to call them live unless telemetry agrees with this snapshot.
        if unprotected != stats.get("unprotected_bytes") or expected != \
                stats.get("protected_bytes", 0) + stats.get("unprotected_bytes", 0):
            raise ValueError("placement/local SST set differs from live-byte telemetry")
    elif mode != "disabled":
        raise ValueError("live SST telemetry unavailable for tiered sample")
    signatures = {number: file_signature(item["path"]) for number, item in routed.items()}
    for number, item in routed.items():
        if signatures[number][2] != item["size"]:
            raise ValueError("routed SST size changed")
    return routed, signatures, placement, expected


def mincore_file(path, expected_signature=None):
    if sys.platform != "linux":
        raise OSError("mincore requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                          ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.mmap.restype = ctypes.c_void_p
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    libc.mincore.restype = ctypes.c_int
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    libc.munmap.restype = ctypes.c_int
    page_size = os.sysconf("SC_PAGE_SIZE")
    descriptor = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        signature = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        if expected_signature is not None and signature != expected_signature:
            raise ValueError("SST replaced between selection and open")
        size = info.st_size
        if size <= 0:
            raise ValueError("empty SST cannot establish cache residency")
        pages = (size + page_size - 1) // page_size
        vector = (ctypes.c_ubyte * pages)()
        # PROT_READ + MAP_PRIVATE reserves a lazy mapping; never read/touch it.
        address = libc.mmap(None, size, 1, 2, descriptor, 0)
        if address == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_errno(), "mmap failed")
        try:
            if libc.mincore(address, size, vector) != 0:
                raise OSError(ctypes.get_errno(), "mincore failed")
            resident = sum(min(page_size, size - page * page_size)
                           for page in range(pages) if vector[page] & 1)
        finally:
            if libc.munmap(address, size) != 0:
                raise OSError(ctypes.get_errno(), "munmap failed")
        if file_signature(path) != signature:
            raise ValueError("SST changed or unlinked during mincore")
        return {"logical_bytes": size, "resident_bytes": resident, "pages": pages,
                "resident_fraction": resident / size}
    finally:
        os.close(descriptor)


def sample_residency(index, backup, mode, stats=None):
    wall, cpu = time.monotonic(), time.thread_time()
    result = {"complete": False, "mode": mode, "started_monotonic_us": time.monotonic_ns() // 1000, "expected_live_logical_bytes": None,
              "covered_logical_bytes": 0, "resident_bytes": 0, "coverage_fraction": None,
              "resident_fraction": None, "files": [], "errors": [],
              "method": "lazy read-only mmap + mincore, no data access/cache eviction"}
    try:
        routed, signatures, placement, expected = routed_tables(index, backup, mode, stats)
        result["expected_live_logical_bytes"] = expected
        for number, item in sorted(routed.items()):
            try:
                measured = mincore_file(item["path"], signatures[number])
                result["files"].append({"number": number, **item, **measured})
                result["covered_logical_bytes"] += measured["logical_bytes"]
                result["resident_bytes"] += measured["resident_bytes"]
            except (OSError, ValueError) as error:
                result["errors"].append({"path": item["path"], "error": str(error)})
        after, after_signatures, after_placement, after_expected = routed_tables(index, backup, mode, stats)
        if after != routed or after_signatures != signatures or after_placement != placement or \
                after_expected != expected:
            result["errors"].append({"error": "live route changed during cache sample"})
        result["complete"] = not result["errors"] and expected > 0
        if expected > 0:
            result["coverage_fraction"] = result["covered_logical_bytes"] / expected
        if result["complete"] and result["covered_logical_bytes"] > 0:
            result["resident_fraction"] = result["resident_bytes"] / result["covered_logical_bytes"]
    except (OSError, ValueError) as error:
        result["errors"].append({"error": str(error)})
    result["sample_wall_s"] = time.monotonic() - wall
    result["sample_cpu_s"] = time.thread_time() - cpu
    return result
