# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Compile candidate utilities into test executables overriding the shared build."""
from pathlib import Path
import argparse
import json
import shlex
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--binaries', default='/tmp/mb-scheduling-ab')
p.add_argument('--variants', default='current,split,reserve,priority')
p.add_argument('--output', default='checks.json')
a = p.parse_args()
here = Path(__file__).resolve().parent
root = here.parents[4]
base = Path(a.binaries)
cfg = (root/'make_config.mk').read_text().splitlines()
flags = shlex.split(next(x.split('=', 1)[1] for x in cfg if x.startswith('PLATFORM_CXXFLAGS=')))
ld = shlex.split(next(x.split('=', 1)[1] for x in cfg if x.startswith('PLATFORM_LDFLAGS=')))
records = []
for variant in a.variants.split(','):
    d = base/variant
    objects = []
    for stem in ['backup', 'native_files', 'separated_storage', 'metabypass_db']:
        src = d/'utilities/metabypass'/(stem+'.cc')
        s = src.read_text()
        if stem == 'backup':
            s = s.replace('    e.bytes.assign(bytes.data(), bytes.size());',
                          '    TEST_SYNC_POINT_CALLBACK("AB::Reserved", &e.name);\n    e.bytes.assign(bytes.data(), bytes.size());')
        target = d/('check-'+stem+'.cc')
        target.write_text(s)
        obj = d/('check-'+stem+'.o')
        subprocess.run(['g++', '-O0', '-g', '-fno-rtti', *flags,
                        '-I'+str(d), '-I'+str(root), '-I'+str(root/'include'),
                        '-c', str(target), '-o', str(obj)], check=True)
        objects.append(str(obj))
    s = subprocess.check_output(
        ['git', 'show', 'bbd60f2ca:utilities/metabypass/metabypass_test.cc'],
        cwd=root, text=True)
    pos = s.index('}  // namespace\n}  // namespace ROCKSDB_NAMESPACE')
    s = s[:pos] + (here/'extra-tests.inc').read_text()+'\n'+s[pos:]
    target = d/'check-test.cc'
    target.write_text(s)
    exe = d/'check-test'
    cmd = ['g++', '-O0', '-g', '-fno-rtti', *flags,
           *(['-DAB_SPLIT'] if variant != 'current' else []),
           '-I'+str(d), '-I'+str(root), '-I'+str(root/'include'),
           '-I'+str(root/'third-party/gtest-1.8.1/fused-src'), str(target), *objects,
           '-L'+str(root), '-lrocksdb_test_debug', '-lrocksdb',
           '-Wl,-rpath,'+str(root), *ld, '-o', str(exe)]
    subprocess.run(cmd, check=True)
    listing = subprocess.check_output([str(exe), '--gtest_list_tests'], text=True)
    suite = ''
    for line in listing.splitlines():
        if not line.startswith(' '):
            suite = line.strip()
            continue
        test = suite + line.strip().split()[0]
        for repetition in range(10 if '.AB' in test else 1):
            proc = subprocess.run([str(exe), '--gtest_filter='+test], capture_output=True,
                                  text=True, timeout=60)
            records.append({'variant': variant, 'test': test, 'repetition': repetition,
                            'returncode': proc.returncode, 'stdout': proc.stdout, 'stderr': proc.stderr})
            (base/a.output).write_text(json.dumps(records, indent=2)+'\n')
            if proc.returncode:
                raise RuntimeError(records[-1])
    print('tests passed', variant, flush=True)
