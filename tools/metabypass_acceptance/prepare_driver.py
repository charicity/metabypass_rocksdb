#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Build one explicit ABI arm from frozen driver + patch, without probing headers."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

from build_optcore import commands, git
from hdd_sync_compare import CORE

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FROZEN = ROOT / 'reports/e157-8g-20261117-e928/delivery/frozen-sources'
DRIVER = FROZEN / 'driver-9475062/tools/metabypass_acceptance'
CONTROLLER = FROZEN / 'controller-9f4dc2d'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare(destination):
    target = destination / 'tools/metabypass_acceptance'
    target.mkdir(parents=True, exist_ok=False)
    for name in ('driver.cc', 'protocol.h'):
        shutil.copy2(DRIVER / name, target / name)
    for name in ('driver.patch', 'write_breakdown.patch'):
        subprocess.run(['patch', '--batch', '--fuzz=0', '-p1', '-i', str(HERE / name)],
                       cwd=destination, check=True)
    if digest(target / 'protocol.h') != digest(DRIVER / 'protocol.h'):
        raise RuntimeError('shared protocol changed')
    return target


def build_commands(core, build, artifacts, capability, cc='clang-14', cxx='clang++-14', jobs=48):
    steps = commands(core, build, artifacts, CONTROLLER, cc, cxx, jobs, artifacts / 'driver-source')
    steps[2].insert(1, '-DMB_HAS_RECOVERY_TIME_SAMPLING=' + str(capability))
    return steps


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('core', 'build', 'artifacts'):
        p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--core-commit', required=True)
    p.add_argument('--has-recovery-time-sampling', type=int, choices=(0, 1), required=True)
    p.add_argument('--cc', default='clang-14')
    p.add_argument('--cxx', default='clang++-14')
    p.add_argument('--jobs', type=int, choices=(48, 32, 16, 8), default=48)
    p.add_argument('--reuse-core-artifacts', type=Path,
                   help='compile only the driver against a previous strict build')
    p.add_argument('--plan', action='store_true')
    args = p.parse_args()
    core, build, artifacts = (getattr(args, k).resolve() for k in ('core', 'build', 'artifacts'))
    capability = args.has_recovery_time_sampling
    if (capability == 0) != (args.core_commit == CORE):
        p.error('macro 0 is reserved for core52; macro 1 requires the final core')
    if git(core, 'rev-parse', 'HEAD') != args.core_commit or git(core, 'status', '--porcelain', '--untracked-files=no'):
        p.error('core must be a clean checkout at --core-commit')
    if core == build or core in build.parents or core == artifacts or core in artifacts.parents:
        p.error('build and artifacts must be outside core source')
    steps = dict(enumerate(build_commands(core, build, artifacts, capability,
                                         args.cc, args.cxx, args.jobs)))
    library_dir = build
    if args.reuse_core_artifacts:
        library_dir = args.reuse_core_artifacts.resolve(strict=True)
        previous = json.loads((library_dir / 'build-manifest.json').read_text())
        core_configure_command = previous.get('core_configure_command') or previous['commands'][0]
        if (previous['status'] != 'built' or previous['core_commit'] != args.core_commit or
                previous['mb_has_recovery_time_sampling'] != capability or
                previous['assert_status_checked'] is not True or
                previous['diagnostics_enabled'] is not False or
                core_configure_command != steps[0] or
                digest((library_dir / 'librocksdb.so').resolve(strict=True)) != previous['library_sha256']):
            p.error('reused core artifacts must match the requested core and build configuration')
        steps = {2: steps[2]}
    if args.plan:
        print(json.dumps(list(steps.values()), indent=2))
        return
    artifacts.mkdir(parents=True, exist_ok=False)
    source = prepare(artifacts / 'driver-source')
    manifest = dict(status='building', core_commit=args.core_commit,
        mb_has_recovery_time_sampling=capability, adapter_commit=git(ROOT, 'rev-parse', 'HEAD'),
        assert_status_checked=True, diagnostics_enabled=False, commands=list(steps.values()),
        driver_source_sha256=digest(source / 'driver.cc'), protocol_sha256=digest(source / 'protocol.h'),
        driver_patch_sha256=digest(HERE / 'driver.patch'),
        write_breakdown_patch_sha256=digest(HERE / 'write_breakdown.patch'))
    if args.reuse_core_artifacts:
        manifest.update(reused_core_artifacts=str(library_dir),
                        reused_library_sha256=previous['library_sha256'],
                        core_configure_command=core_configure_command)
    path = artifacts / 'build-manifest.json'
    def save():
        path.write_text(json.dumps(manifest, indent=2) + '\n')
    save()
    env = dict(os.environ)
    for key in ('LD_LIBRARY_PATH', 'CXXFLAGS', 'CFLAGS', 'LDFLAGS'):
        env.pop(key, None)
    for index, command in steps.items():
        if index == 2:
            compiled_file = library_dir / ('core-compile-commands.json' if args.reuse_core_artifacts
                                           else 'compile_commands.json')
            compiled = json.loads(compiled_file.read_text())
            if not compiled or any('ROCKSDB_ASSERT_STATUS_CHECKED' not in r['command'] or
                    'METABYPASS_PERF_DIAGNOSTICS' in r['command'] for r in compiled):
                raise RuntimeError('core strict Status / diagnostics OFF flags mismatch')
            shutil.copy2(compiled_file, artifacts / 'core-compile-commands.json')
            for library in sorted(library_dir.glob('librocksdb.so*')):
                if library.is_symlink():
                    (artifacts / library.name).symlink_to(library.resolve().name)
                else:
                    shutil.copy2(library, artifacts / library.name)
        with (artifacts / ('build-%d.log' % index)).open('w') as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    driver = artifacts / 'metabypass_acceptance'
    library = (artifacts / 'librocksdb.so').resolve(strict=True)
    linked = subprocess.check_output(['ldd', str(driver)], env=env, text=True)
    if str(artifacts / 'librocksdb.so.') not in linked or 'not found' in linked:
        raise RuntimeError('driver must resolve its adjacent core library')
    subprocess.run([str(driver), '--mode=selfcheck', '--recovery_time_sampling=' + str(1-capability)],
                   env=env, check=True)
    manifest.update(status='built', driver_sha256=digest(driver), library_sha256=digest(library),
                    library_name=library.name, ldd_without_ld_library_path=linked)
    save()


if __name__ == '__main__':
    main()
