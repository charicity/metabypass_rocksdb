#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Independent clone with a fixed buffer and bounded unsynchronized writes.

This limits the userspace copy buffer and destination write batches. It does
not bound all kernel memory, drop cache pages, or guarantee absence of OOM.
Only within-clone hardlinks are reconstructed; seed inodes are never linked.
"""

import io
import json
import os
from pathlib import Path
import shutil
import stat
import sys


PREPARATION_PROTOCOL = "bounded-v2"
BUFFER_BYTES = 64 * 1024
SYNC_BATCH_BYTES = 1024 * 1024
OWNER = ".node-owner.json"


def signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_mode, info.st_uid, info.st_gid)


def require_directory(path):
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise ValueError("clone root or directory is not a real directory: " + str(path))


def copy_file(source, target, expected, buffer):
    """Retain partial output on failure; all writes use one reusable buffer."""
    source_fd = os.open(str(source), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        source_info = os.fstat(source_fd)
        if not stat.S_ISREG(source_info.st_mode) or signature(source_info) != signature(expected):
            raise ValueError("seed file changed before copy: " + str(source))
        target_fd = os.open(str(target), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                            stat.S_IMODE(source_info.st_mode) | stat.S_IWUSR)
        try:
            copied = unsynced = syncs = 0
            view = memoryview(buffer)
            # closefd=False leaves the descriptor available for final checks.
            with io.FileIO(source_fd, "rb", closefd=False) as stream:
                while True:
                    count = stream.readinto(buffer)
                    if not count:
                        break
                    offset = 0
                    while offset < count:
                        limit = min(count, offset + SYNC_BATCH_BYTES - unsynced)
                        written = os.write(target_fd, view[offset:limit])
                        if written <= 0:
                            raise OSError("copy write made no progress")
                        offset += written
                        copied += written
                        unsynced += written
                        if unsynced == SYNC_BATCH_BYTES:
                            os.fdatasync(target_fd)
                            syncs += 1
                            unsynced = 0
            os.fdatasync(target_fd)
            syncs += 1
            if copied != source_info.st_size or signature(os.fstat(source_fd)) != signature(source_info) or \
                    signature(source.lstat()) != signature(source_info):
                raise ValueError("seed file changed during copy: " + str(source))
            target_info = os.fstat(target_fd)
            if not stat.S_ISREG(target_info.st_mode) or target_info.st_size != copied or \
                    (target_info.st_dev, target_info.st_ino) == (source_info.st_dev, source_info.st_ino):
                raise ValueError("clone file is incomplete or shares a seed inode")
            # Match copy2's supported timestamps, mode and extended attributes.
            shutil.copystat(str(source), str(target), follow_symlinks=False)
            os.fchmod(target_fd, source_info.st_mode | stat.S_IWUSR)
            return copied, syncs
        finally:
            os.close(target_fd)
    finally:
        os.close(source_fd)


def clone_dataset(pairs):
    pairs = [(Path(source), Path(target)) for source, target in pairs]
    sources = [source.resolve() for source, _ in pairs]
    targets = [target.resolve() for _, target in pairs]
    for target in targets:
        if any(target == source or source in target.parents or target in source.parents for source in sources):
            raise ValueError("clone destination overlaps immutable seed")
    for index, target in enumerate(targets):
        if any(target == other or target in other.parents or other in target.parents for other in targets[:index]):
            raise ValueError("clone destinations overlap")
    copied = {}
    files = bytes_copied = links = syncs = 0
    buffer = bytearray(BUFFER_BYTES)
    for source, target in pairs:
        source, target = Path(source), Path(target)
        require_directory(source)
        target.mkdir(exist_ok=True)
        require_directory(target)
        for base, dirs, names in os.walk(str(source), followlinks=False):
            original_dir = Path(base)
            require_directory(original_dir)
            destination = target / original_dir.relative_to(source)
            destination.mkdir(exist_ok=True)
            require_directory(destination)
            for name in dirs:
                require_directory(original_dir / name)
                (destination / name).mkdir(exist_ok=True)
                require_directory(destination / name)
            for name in names:
                if name == OWNER:
                    continue
                original, new = original_dir / name, destination / name
                info = original.lstat()
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError("seed contains non-regular file: " + str(original))
                key = (info.st_dev, info.st_ino, destination.stat().st_dev)
                if key in copied:
                    first = copied[key]
                    first_info = first.lstat()
                    if not stat.S_ISREG(first_info.st_mode) or \
                            (first_info.st_dev, first_info.st_ino) == (info.st_dev, info.st_ino):
                        raise ValueError("internal link is not an independent clone file")
                    os.link(str(first), str(new), follow_symlinks=False)
                    links += 1
                else:
                    count, calls = copy_file(original, new, info, buffer)
                    copied[key] = new
                    bytes_copied += count
                    syncs += calls
                files += 1
    return {"ordinary_copy_bytes": bytes_copied, "files": files,
            "reconstructed_internal_links": links, "shared_with_seed": False,
            "preparation_protocol": PREPARATION_PROTOCOL, "buffer_bytes": BUFFER_BYTES,
            "sync_batch_bytes": SYNC_BATCH_BYTES, "fdatasync_calls": syncs,
            "directory_fsync": False,
            "memory_limitation": "userspace buffer and dirty batches only; kernel charge is not bounded"}


def main(arguments=None):
    arguments = sys.argv[1:] if arguments is None else arguments
    if len(arguments) != 5 or arguments[0] != "--clone":
        raise ValueError("expected --clone source_ssd source_hdd dest_ssd dest_hdd")
    source_ssd, source_hdd, dest_ssd, dest_hdd = arguments[1:]
    details = clone_dataset(((source_ssd, dest_ssd), (source_hdd, dest_hdd)))
    print("MB_SST_JSON " + json.dumps({"protocol": "node_v1", "event": "summary",
                                      "ok": True, "copy_complete": True, **details}), flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print("bounded clone failed: " + str(error), file=sys.stderr, flush=True)
        sys.exit(1)
