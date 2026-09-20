# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Build isolated research variants against an existing release archive."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parent
p = argparse.ArgumentParser()
p.add_argument('--out', default='/tmp/mb-scheduling-ab')
p.add_argument('--library', default='/tmp/metabypass-release-build/librocksdb.a')
p.add_argument('--revision', default='bbd60f2ca', help='Historical source revision; WORKTREE for live sources')
p.add_argument('--variants', default='current,split,reserve,priority')
a = p.parse_args()
out = Path(a.out)
out.mkdir(parents=True, exist_ok=True)
lib = Path(a.library)
config = (lib.parent / 'make_config.mk').read_text()
flags = shlex.split(next(x.split('=', 1)[1] for x in config.splitlines()
                        if x.startswith('PLATFORM_CXXFLAGS=')))
ldflags = shlex.split(next(x.split('=', 1)[1] for x in config.splitlines()
                          if x.startswith('PLATFORM_LDFLAGS=')))
def source_bytes(rel):
    if a.revision == 'WORKTREE':
        return (ROOT / rel).read_bytes()
    return subprocess.check_output(['git', 'show', a.revision + ':' + rel], cwd=ROOT)

base_cc = source_bytes('utilities/metabypass/backup.cc').decode()
base_h = source_bytes('utilities/metabypass/backup.h').decode()

def replace(s, old, new):
    assert old in s, old
    return s.replace(old, new)

metadata = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT,
                                           text=True).strip(), 'revision': a.revision, 'variants': {}, 'source_hashes': {}}
for rel in ['utilities/metabypass/'+stem for stem in ['backup.cc', 'backup.h', 'native_files.cc', 'native_files.h', 'separated_storage.cc', 'separated_storage.h', 'metabypass_db.cc']] + ['include/rocksdb/utilities/metabypass.h']:
    metadata['source_hashes'][rel] = hashlib.sha256(source_bytes(rel)).hexdigest()
for variant in a.variants.split(','):
    if variant not in ['current', 'split', 'reserve', 'priority', 'channels']:
        raise ValueError(variant)
    d = out / variant
    (d / 'utilities/metabypass').mkdir(parents=True, exist_ok=True)
    for rel in ['utilities/metabypass/native_files.h',
                'utilities/metabypass/separated_storage.h',
                'include/rocksdb/utilities/metabypass.h']:
        target = d / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_bytes(rel))
    cc, h = base_cc, base_h
    if variant not in ['current', 'channels']:
        h = replace(h, 'Status Reserve(size_t charge);', '''Status Reserve(size_t charge, bool wal);
  static bool IsWal(const std::string& name) {
    return name.size() >= 4 && name.compare(name.size() - 4, 4, ".log") == 0;
  }
  std::mutex& Operations(const std::string& name) {
    return IsWal(name) ? wal_operations_ : operations_;
  }''')
        h = replace(h, 'std::mutex operations_;', 'std::mutex operations_, wal_operations_;')
        h = replace(h, 'size_t waiting_charge_ = 0;', 'size_t waiting_count_ = 0;')
        cc = replace(cc, 'serial(backup_->operations_)', 'serial(backup_->Operations(name_))')
        cc = replace(cc, 'backup_->Reserve(e.charge)', 'backup_->Reserve(e.charge, Backup::IsWal(name_))')
        cc = replace(cc, 'serial(operations_);', 'serial(Operations(p));')
        cc = replace(cc, '''std::lock_guard<std::mutex> serial(Operations(p));
  Event e{Kind::kRename''', '''std::scoped_lock serial(operations_, wal_operations_);
  Event e{Kind::kRename''')
        cc = replace(cc, 'Reserve(e.charge);', 'Reserve(e.charge, IsWal(e.name));')
        cc = replace(cc, 'Status Backup::Reserve(size_t charge)', 'Status Backup::Reserve(size_t charge, bool wal)')
        cc = replace(cc, '  const uint64_t start = Now();\n  const bool blocked', '''  const size_t limit = options_.queue_capacity;
  (void)wal;
  const uint64_t start = Now();
  const bool blocked''')
        cc = cc.replace('options_.queue_capacity - charge', 'limit - charge')
        cc = replace(cc, 'waiting_charge_ = charge;', '++waiting_count_;')
        cc = replace(cc, 'waiting_charge_ = 0;', '--waiting_count_;')
        cc = replace(cc, '''if (waiting_charge_ != 0 &&
          stats_.queued_bytes <= options_.queue_capacity - waiting_charge_)''',
                     'if (waiting_count_ != 0)')
        cc = cc.replace('waiting_charge_ != 0', 'waiting_count_ != 0')
        cc = cc.replace('space_cv_.notify_one()', 'space_cv_.notify_all()')
    if variant in ['reserve', 'priority']:
        cc = replace(cc, '''const size_t limit = options_.queue_capacity;
  (void)wal;''', '''// Reserve one eighth for WAL, but permit a single oversized metadata
  // operation to use the original capacity rather than rejecting it.
  const size_t metadata_limit = options_.queue_capacity - options_.queue_capacity / 8;
  const size_t limit = wal || charge > metadata_limit
                           ? options_.queue_capacity : metadata_limit;''')
    if variant == 'priority':
        cc = replace(cc, '#include <chrono>', '#include <algorithm>\n#include <chrono>')
        cc = replace(cc, '''    while (!queue_.empty() && queue_.front().seq <= boundary) {
      Event e = std::move(queue_.front());
      queue_.pop_front();''', '''    // Freeze a finite window. Reorder only append runs: create/close,
    // truncate/rename/delete are barriers. Stable partition preserves each
    // file's append order. Capture is allowed only after the whole window.
    std::deque<Event> window;
    window.swap(queue_);
    for (auto begin = window.begin(); begin != window.end();) {
      if (begin->kind != Kind::kAppend) { ++begin; continue; }
      auto end = begin;
      while (end != window.end() && end->kind == Kind::kAppend) ++end;
      std::stable_partition(begin, end, [](const Event& e) { return IsWal(e.name); });
      begin = end;
    }
    while (!window.empty()) {
      Event e = std::move(window.front());
      window.pop_front();''')
        cc = replace(cc, '      applied_ = e.seq;', '      // applied_ advances only after the complete frozen window.')
        cc = replace(cc, '''    if (!stats_.error.ok()) break;
    if (active_ && !candidate_busy_ && applied_ > captured_)''', '''    if (!stats_.error.ok()) break;
    applied_ = boundary;
    if (active_ && !candidate_busy_ && applied_ > captured_)''')
        cc = replace(cc, '''      if (!oldest_unpublished_micros_)
        oldest_unpublished_micros_ = e.queued_micros;''', '''      if (!oldest_unpublished_micros_ || e.queued_micros < oldest_unpublished_micros_)
        oldest_unpublished_micros_ = e.queued_micros;''')
    (d / 'utilities/metabypass/backup.cc').write_text(cc)
    (d / 'utilities/metabypass/backup.h').write_text(h)
    objects = []
    for stem in ['backup', 'native_files', 'separated_storage', 'metabypass_db']:
        source = d / 'utilities/metabypass' / (stem + '.cc')
        if stem != 'backup':
            source.write_bytes(source_bytes('utilities/metabypass/' + stem + '.cc'))
        obj = d / (stem + '.o')
        cmd = ['g++', '-O2', '-DNDEBUG', '-fno-rtti', *flags, '-I'+str(d),
               '-I'+str(d/'include'), '-I'+str(ROOT), '-I'+str(ROOT/'include'), '-c', str(source), '-o', str(obj)]
        subprocess.run(cmd, check=True)
        objects.append(str(obj))
    cmd = ['g++', '-O2', '-DNDEBUG', '-fno-rtti', *flags, '-I'+str(d/'include'), '-I'+str(ROOT/'include'),
           '-I'+str(ROOT), str(HERE/'driver.cc'), *objects, str(lib), *ldflags,
           '-o', str(d/'driver')]
    subprocess.run(cmd, check=True)
    metadata['variants'][variant] = {
        'binary_sha256': hashlib.sha256((d/'driver').read_bytes()).hexdigest(),
        'backup_cc_sha256': hashlib.sha256(cc.encode()).hexdigest(),
        'backup_h_sha256': hashlib.sha256(h.encode()).hexdigest(), 'link': cmd}
    print('built', variant, flush=True)
(out/'build.json').write_text(json.dumps(metadata, indent=2)+'\n')
