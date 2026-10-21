#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Build two release drivers in an external, reusable CMake build directory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]
UPSTREAM = "abeebd9630f11bd08c28b7bd43c7bdfc62050654"


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


def verify_upstream(source):
    """Check the reusable source export against Git objects, not its name."""
    verified = {}
    for entry in git("ls-tree", "-r", "-z", UPSTREAM).split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        if kind != "blob":
            continue
        path = source / name.decode()
        if not path.exists() and not path.is_symlink():
            raise RuntimeError("upstream export missing: " + str(path))
        content = (os.readlink(path).encode() if mode == "120000" else path.read_bytes())
        digest = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
        if digest != oid:
            raise RuntimeError("upstream export modified: " + str(path))
        verified[name.decode()] = oid
    extra = {str(path.relative_to(source)) for path in source.rglob("*")
             if path.is_file() or path.is_symlink()} - set(verified)
    if extra:
        raise RuntimeError("unexpected upstream source files: " + str(sorted(extra)))
    return hashlib.sha256(json.dumps(verified, sort_keys=True).encode()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--jobs", type=int, default=48)
    a = p.parse_args()
    if a.jobs not in (48, 32, 16, 8):
        p.error("--jobs must be 48, 32, 16, or 8; reduce only after OOM")
    out = a.out.resolve()
    if out == ROOT or ROOT in out.parents:
        p.error("use a build directory outside the source checkout")
    out.mkdir(parents=True, exist_ok=True)
    marker = out / ".mb-native-build"
    if not marker.exists() and any(out.iterdir()):
        p.error("build directory must be empty or owned by this builder")
    marker.write_text("metabypass-native-perf-v1\n")
    # An interrupted rebuild must never leave an apparently valid manifest.
    (out / "build.json").unlink(missing_ok=True)
    upstream = out / "upstream-source"
    if not upstream.exists():
        with tempfile.TemporaryDirectory(dir=out, prefix="extract-") as temp:
            archive = Path(temp) / "upstream.tar"
            with archive.open("wb") as stream:
                subprocess.run(["git", "-C", str(ROOT), "archive", UPSTREAM],
                               stdout=stream, check=True)
            source = Path(temp) / "source"
            source.mkdir()
            # The archive is from the pinned local, trusted upstream commit.
            with tarfile.open(archive) as tar:
                tar.extractall(source)
            source.rename(upstream)
    upstream_digest = verify_upstream(upstream)
    before = identity()
    metadata = {"protocol": "metabypass-native-perf-v1", "build_type": "Release",
                "created_unix": time.time(), "source": before,
                "upstream_commit": UPSTREAM, "jobs": a.jobs,
                "upstream_tree_sha256": upstream_digest,
                "git_status": git("status", "--short").decode(),
                "driver_sha256": sha256(HERE / "driver.cc"),
                "compiler": subprocess.check_output(
                    [os.environ.get("CXX", "c++"), "--version"], text=True),
                "commands": [], "binaries": {}}
    for name, source in (("current", ROOT), ("upstream", upstream)):
        directory = out / name
        commands = [
            ["cmake", "-S", str(HERE), "-B", str(directory),
             "-DCMAKE_BUILD_TYPE=Release", "-DPORTABLE=ON", "-DUSE_RTTI=OFF",
             "-DROCKSDB_SOURCE=" + str(source),
             "-DMB_UPSTREAM=" + ("ON" if name == "upstream" else "OFF")],
            ["cmake", "--build", str(directory), "--target", "mb_native",
             "--parallel", str(a.jobs)],
        ]
        log = out / (name + "-build.log")
        with log.open("w") as stream:
            for command in commands:
                print("Running:", " ".join(command), "log:", log, flush=True)
                metadata["commands"].append(command)
                result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT)
                if result.returncode and command[:2] == ["cmake", "--build"]:
                    stream.flush()
                    for jobs in (32, 16, 8):
                        if jobs >= int(command[-1]):
                            continue
                        evidence = log.read_text(errors="replace").lower()
                        if not any(x in evidence for x in (
                                "killed signal terminated program cc1plus",
                                "out of memory", "cannot allocate memory")):
                            break
                        command = command[:-1] + [str(jobs)]
                        metadata["commands"].append(command)
                        stream.write("\nOOM retry: " + " ".join(command) + "\n")
                        stream.flush()
                        result = subprocess.run(command, stdout=stream,
                                                stderr=subprocess.STDOUT)
                        if result.returncode == 0:
                            break
                result.check_returncode()
        binary = directory / "mb_native"
        metadata["binaries"][name] = {
            "path": str(binary), "sha256": sha256(binary),
            "cmake_cache_sha256": sha256(directory / "CMakeCache.txt"),
            "compile_commands_sha256": sha256(directory / "compile_commands.json")}
    if identity() != before:
        raise RuntimeError("source changed during build; rerun before measuring")
    (out / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print("Ready:", out / "build.json", flush=True)


if __name__ == "__main__":
    main()
