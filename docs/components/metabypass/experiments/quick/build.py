#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Build one release binary for the inline Metabypass comparison."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def identity():
    # Include uncommitted/untracked source files; omit generated ignored files.
    names = set(git("ls-files", "-z", "--cached", "--others",
                    "--exclude-standard").decode().split("\0"))
    relevant = [name for name in names if name and
                (Path(name).suffix in (".cc", ".c", ".h", ".cmake", ".in") or
                 Path(name).name in ("CMakeLists.txt", "src.mk"))]
    hashes = {name: sha256(ROOT / name) if (ROOT / name).is_file() else None
              for name in sorted(relevant)}
    return {"head": git("rev-parse", "HEAD").decode().strip(),
            "source_sha256": hashlib.sha256(
                json.dumps(hashes, sort_keys=True).encode()).hexdigest(),
            "files": hashes}


def default_jobs():
    cpus = len(os.sched_getaffinity(0))
    available = next(int(line.split()[1]) * 1024
                     for line in Path("/proc/meminfo").read_text().splitlines()
                     if line.startswith("MemAvailable:"))
    # Leave memory for the host and conservatively cap compiler concurrency.
    return max(1, min(cpus, 16, available // (2 * 1024**3)))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--jobs", type=int, default=default_jobs())
    a = p.parse_args()
    if a.jobs < 1:
        p.error("--jobs must be positive")
    out = a.out.resolve()
    if out == ROOT or ROOT in out.parents:
        p.error("use a build directory outside the source checkout")
    out.mkdir(parents=True, exist_ok=True)
    marker = out / ".mb-quick-build"
    if not marker.exists() and any(out.iterdir()):
        p.error("build directory must be empty or owned by this builder")
    if marker.exists() and marker.read_text().strip() != "metabypass-quick-v2":
        p.error("use a new build directory for this protocol")
    marker.write_text("metabypass-quick-v2\n")
    # An interrupted rebuild must never leave an apparently valid manifest.
    (out / "build.json").unlink(missing_ok=True)
    before = identity()
    metadata = {"protocol": "metabypass-quick-v2", "build_type": "Release",
                "created_unix": time.time(), "source": before,
                "jobs": a.jobs,
                "git_status": git("status", "--short").decode(),
                "driver_sha256": sha256(HERE / "driver.cc"),
                "compiler": subprocess.check_output(
                    [os.environ.get("CXX", "c++"), "--version"], text=True),
                "commands": [], "binaries": {}}
    directory = out / "current"
    commands = [
        ["cmake", "-S", str(HERE), "-B", str(directory),
         "-DCMAKE_BUILD_TYPE=Release", "-DPORTABLE=ON", "-DUSE_RTTI=OFF",
         "-DROCKSDB_SOURCE=" + str(ROOT)],
        ["cmake", "--build", str(directory), "--target", "mb_quick",
         "--parallel", str(a.jobs)],
    ]
    log = out / "current-build.log"
    with log.open("w") as stream:
        for command in commands:
            print("Running:", " ".join(command), "log:", log, flush=True)
            metadata["commands"].append(command)
            subprocess.run(command, check=True, stdout=stream,
                           stderr=subprocess.STDOUT)
    binary = directory / "mb_quick"
    metadata["binaries"]["current"] = {
        "path": str(binary), "sha256": sha256(binary),
        "cmake_cache_sha256": sha256(directory / "CMakeCache.txt"),
        "compile_commands_sha256": sha256(directory / "compile_commands.json")}
    if identity() != before:
        raise RuntimeError("source changed during build; rerun before measuring")
    (out / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print("Ready:", out / "build.json", flush=True)


if __name__ == "__main__":
    main()
