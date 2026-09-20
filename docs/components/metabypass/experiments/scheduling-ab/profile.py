# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Instrument foreground acquisition/capacity waits; not throughput evidence."""
from pathlib import Path
import argparse
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
instrumentation = r'''
#include <cstdio>
namespace {
thread_local bool ab_measure = false;
thread_local uint64_t ab_operations_ns = 0, ab_queue_ns = 0, ab_capacity_ns = 0;
using ABClock = std::chrono::steady_clock;
uint64_t ABElapsed(ABClock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(ABClock::now()-start).count();
}
class ABLock {
 public:
  ABLock(std::mutex& mutex, bool operation = false) : lock_(mutex, std::defer_lock) {
    auto t = ab_measure ? ABClock::now() : ABClock::time_point{};
    lock_.lock();
    if (ab_measure) (operation ? ab_operations_ns : ab_queue_ns) += ABElapsed(t);
  }
 private:
  std::unique_lock<std::mutex> lock_;
};
}
extern "C" void mb_profile_start() {
  ab_operations_ns = ab_queue_ns = ab_capacity_ns = 0;
  ab_measure = true;
}
extern "C" void mb_profile_stop() {
  ab_measure = false;
  printf("operation_lock_ns=%llu queue_lock_ns=%llu foreground_capacity_ns=%llu\n",
    (unsigned long long)ab_operations_ns, (unsigned long long)ab_queue_ns,
    (unsigned long long)ab_capacity_ns);
}
'''
records = {}
for variant in ['current', 'split', 'reserve', 'priority']:
    d = base / variant
    s = (d/'utilities/metabypass/backup.cc').read_text()
    s = s.replace('namespace ROCKSDB_NAMESPACE {', instrumentation+'\nnamespace ROCKSDB_NAMESPACE {', 1)
    import re
    s = re.sub(r'std::lock_guard<std::mutex> serial\((.*)\);', r'ABLock serial(\1, true);', s)
    s = s.replace('std::lock_guard<std::mutex> lock(mutex_);', 'ABLock lock(mutex_);')
    for signature in ['Status Backup::Reserve(size_t charge)', 'Status Backup::Reserve(size_t charge, bool wal)']:
        s = s.replace(signature+' {\n  std::unique_lock<std::mutex> lock(mutex_);', signature+''' {
  auto ab_start = ab_measure ? ABClock::now() : ABClock::time_point{};
  std::unique_lock<std::mutex> lock(mutex_);
  if (ab_measure) ab_queue_ns += ABElapsed(ab_start);''')
    s = s.replace('    space_cv_.wait(lock, [&] {', '''    auto ab_wait = ab_measure ? ABClock::now() : ABClock::time_point{};
    space_cv_.wait(lock, [&] {''')
    s = s.replace('    stats_.backpressure_micros += Now() - start;', '''    if (ab_measure) ab_capacity_ns += ABElapsed(ab_wait);
    stats_.backpressure_micros += Now() - start;''')
    src = d/'profile.cc'
    src.write_text(s)
    obj = d/'profile.o'
    subprocess.run(['g++', '-O2', '-DNDEBUG', '-fno-rtti', *flags, '-I'+str(d), '-I'+str(root),
                    '-I'+str(root/'include'), '-c', str(src), '-o', str(obj)], check=True)
    cmd = ['g++', '-O2', '-DNDEBUG', '-DMB_AB_PROFILE', '-fno-rtti', *flags,
           '-I'+str(root), '-I'+str(root/'include'), str(here/'driver.cc'), str(obj),
           *[str(d/(x+'.o')) for x in ['native_files', 'separated_storage', 'metabypass_db']],
           str(lib), *ld, '-o', str(d/'profile-driver')]
    subprocess.run(cmd, check=True)
    records[variant] = cmd
    print('profile built', variant, flush=True)
(base/'profile-build.json').write_text(json.dumps(records, indent=2)+'\n')
