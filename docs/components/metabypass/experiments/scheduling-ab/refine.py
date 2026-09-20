# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Follow-up: partition the private event window outside the queue mutex."""
from pathlib import Path
import argparse
import hashlib
import json
import shlex
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--binaries', default='/tmp/mb-scheduling-ab')
p.add_argument('--library', default='/tmp/metabypass-release-build/librocksdb.a')
a = p.parse_args()
here = Path(__file__).resolve().parent
root = here.parents[4]
base = Path(a.binaries)
lib = Path(a.library)
cfg = (lib.parent/'make_config.mk').read_text().splitlines()
flags = shlex.split(next(x.split('=', 1)[1] for x in cfg if x.startswith('PLATFORM_CXXFLAGS=')))
ld = shlex.split(next(x.split('=', 1)[1] for x in cfg if x.startswith('PLATFORM_LDFLAGS=')))
d = base/'priority_outside'
(d/'utilities/metabypass').mkdir(parents=True, exist_ok=True)
s = (base/'priority/utilities/metabypass/backup.cc').read_text()
assert '    window.swap(queue_);' in s
s = s.replace('    window.swap(queue_);', '    window.swap(queue_);\n    lock.unlock();')
s = s.replace('    while (!window.empty()) {', '    lock.lock();\n    while (!window.empty()) {')
src = d/'utilities/metabypass/backup.cc'
src.write_text(s)
for name in ['backup.h', 'native_files.cc', 'separated_storage.cc', 'metabypass_db.cc']:
    (d/'utilities/metabypass'/name).write_bytes((base/'priority/utilities/metabypass'/name).read_bytes())
obj = d/'backup.o'
subprocess.run(['g++', '-O2', '-DNDEBUG', '-fno-rtti', *flags, '-I'+str(d), '-I'+str(root),
                '-I'+str(root/'include'), '-c', str(src), '-o', str(obj)], check=True)
cmd = ['g++', '-O2', '-DNDEBUG', '-fno-rtti', *flags, '-I'+str(root), '-I'+str(root/'include'),
       str(here/'driver.cc'), str(obj),
       *[str(base/'priority'/(x+'.o')) for x in ['native_files', 'separated_storage', 'metabypass_db']],
       str(lib), *ld, '-o', str(d/'driver')]
subprocess.run(cmd, check=True)
m = json.loads((base/'build.json').read_text())
m['variants']['priority_outside'] = {'link': cmd, 'backup_cc_sha256': hashlib.sha256(s.encode()).hexdigest(),
    'backup_h_sha256': hashlib.sha256((d/'utilities/metabypass/backup.h').read_bytes()).hexdigest(),
    'binary_sha256': hashlib.sha256((d/'driver').read_bytes()).hexdigest()}
(base/'build.json').write_text(json.dumps(m, indent=2)+'\n')
