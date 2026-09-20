# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Add a separated-storage-only driver to existing channel comparison binaries."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

here = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('--controls', default='/tmp/mb-channels-ab')
parser.add_argument('--out', default='/tmp/mb-channels-no-backup')
parser.add_argument('--output', default='/tmp/mb-channels-no-backup-results.json')
parser.add_argument('--trials', type=int, default=5)
args = parser.parse_args()
controls, out = Path(args.controls), Path(args.out)
metadata = json.loads((controls / 'build.json').read_text())
for variant in ['current', 'split', 'channels']:
    (out / variant).mkdir(parents=True, exist_ok=True)
    shutil.copy2(controls / variant / 'driver', out / variant / 'driver')

source = (here / 'driver.cc').read_text()


def replace(old, new):
    global source
    assert old in source, old
    source = source.replace(old, new)


replace('#include "rocksdb/listener.h"',
        '#include "rocksdb/listener.h"\n#include "utilities/metabypass/separated_storage.h"')
replace('  std::unique_ptr<MetaBypassDB> db;', '''  auto storage = std::make_shared<metabypass::SeparatedStorage>(
      Env::Default()->GetFileSystem(), root + "/index", b.data_dir);
  Status locked = storage->Lock();
  if (!locked.ok()) { puts(locked.ToString().c_str()); return 1; }
  auto env = NewCompositeEnv(storage);
  o.env = env.get();
  std::unique_ptr<DB> db;''')
replace('if (mode == "restore")', 'if (mode == "verify")')
replace('    s = MetaBypassDB::Restore(o, b, root + "/index");',
        '    s = Status::OK();  // Verify the retained primary; no backup exists.')
replace('MetaBypassDB::Open(o, b, root + "/index", &db)',
        'DB::Open(o, root + "/index", &db)')
replace('restore_us=%llu', 'verify_open_us=%llu')
replace('  if (s.ok()) s = db->SyncBackup();',
        '  // No backup synchronization in the baseline.')
replace('  s.UpdateIfOk(db->Close());\n  const auto close_us',
        '  s.UpdateIfOk(db->Close());\n  db.reset();\n  const auto close_us')
replace('  auto stats = db->GetBackupStats();', '  MetaBypassStats stats;')
replace('  if (mode == "crash" && s.ok()) {',
        '  if (mode == "crash") { return 2; }\n  if (false) {')
(out / 'baseline').mkdir(exist_ok=True)
generated = out / 'baseline' / 'driver.cc'
generated.write_text(source)
cmd = metadata['variants']['channels']['link'].copy()
cmd = [str(generated) if x.endswith('/driver.cc') else x for x in cmd]
cmd[-1] = str(out / 'baseline' / 'driver')
subprocess.run(cmd, check=True)
metadata['variants']['baseline'] = {
    'source_sha256': hashlib.sha256(source.encode()).hexdigest(),
    'binary_sha256': hashlib.sha256((out / 'baseline' / 'driver').read_bytes()).hexdigest(),
    'link': cmd,
    'semantics': 'SeparatedStorage plus raw DB, no Backup, primary retained for verification',
}
(out / 'build.json').write_text(json.dumps(metadata, indent=2) + '\n')
subprocess.run([sys.executable, str(here / 'run.py'), '--binaries', str(out),
                '--variants', 'baseline,current,split,channels', '--trials', str(args.trials),
                '--output', args.output], check=True)
