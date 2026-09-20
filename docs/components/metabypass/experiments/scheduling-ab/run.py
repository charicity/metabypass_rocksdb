# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Rotated A/B trials; each successful write must restore with the A reader."""
import argparse
import json
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument('--binaries', default='/tmp/mb-scheduling-ab')
p.add_argument('--output', default='/tmp/mb-scheduling-results.json')
p.add_argument('--trials', type=int, default=5)
p.add_argument('--executable', choices=['driver', 'profile-driver'], default='driver')
p.add_argument('--count', type=int, default=100000)
p.add_argument('--variants', default='current,split,reserve,priority')
p.add_argument('--cases', default='default,mixed,pressure')
a = p.parse_args()
binaries = Path(a.binaries)
variants = a.variants.split(',')
cases = [('default', 64*1024*1024), ('mixed', 64*1024*1024), ('pressure', 2*1024*1024)]
selected_cases = a.cases.split(',')
if not set(selected_cases) <= {name for name, _ in cases}:
    raise ValueError(a.cases)
cases = [(name, capacity) for name, capacity in cases if name in selected_cases]
root = Path(tempfile.mkdtemp(prefix='mb-scheduling-runs-'))
result = {'root': str(root), 'count': a.count, 'trials': a.trials,
          'build': json.loads((binaries/'build.json').read_text()), 'runs': [], 'crashes': []}

def command(args):
    begin = time.time()
    proc = subprocess.run(list(map(str, args)), capture_output=True, text=True, timeout=60)
    return {'command': list(map(str, args)), 'returncode': proc.returncode,
            'stdout': proc.stdout, 'stderr': proc.stderr, 'wall_seconds': time.time()-begin}

def metrics(output):
    return {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', output)}

def save():
    Path(a.output).write_text(json.dumps(result, indent=2)+'\n')

for trial in range(-1, a.trials):
    for case, capacity in cases:
        for variant in variants[trial % len(variants):] + variants[:trial % len(variants)]:
            d = root / f'{trial}-{case}-{variant}'
            d.mkdir()
            count = min(10000, a.count) if trial == -1 else a.count
            row = {'trial': trial, 'case': case, 'variant': variant}
            row['write'] = command([binaries/variant/a.executable, 'write', d, count, case, capacity])
            row['metrics'] = metrics(row['write']['stdout'])
            result['runs'].append(row)
            save()
            if row['write']['returncode']:
                raise RuntimeError(row)
            if variant == 'baseline':
                row['verify'] = command([binaries/'baseline/driver', 'verify', d, count, case, capacity])
                checked = row['verify']
            else:
                shutil.rmtree(d/'index')
                row['restore'] = command([binaries/'current/driver', 'restore', d, count, case, capacity])
                checked = row['restore']
            save()
            if checked['returncode']:
                raise RuntimeError(row)
            print(trial, case, variant, row['metrics'], flush=True)
            shutil.rmtree(d)

# SIGKILL after a confirmed recovery point; no Close in the writer.
for variant in variants:
    if variant == 'baseline':
        continue
    d = root / ('crash-'+variant)
    d.mkdir()
    row = {'variant': variant}
    row['write'] = command([binaries/variant/a.executable, 'crash', d, 10000, 'mixed', 2*1024*1024])
    result['crashes'].append(row)
    save()
    assert row['write']['returncode'] == -9, row
    assert 'synced-before-sigkill' in row['write']['stdout'], row
    shutil.rmtree(d/'index')
    row['restore'] = command([binaries/'current/driver', 'restore', d, 10000, 'mixed', 2*1024*1024])
    save()
    assert row['restore']['returncode'] == 0, row
    shutil.rmtree(d)
result['summary'] = {}
for case, _ in cases:
    result['summary'][case] = {}
    for variant in variants:
        rows = [r for r in result['runs'] if r['trial'] >= 0 and r['case'] == case and r['variant'] == variant]
        data = [dict(r['metrics'], total_us=sum(r['metrics'][x] for x in ['foreground_us', 'sync_us', 'close_us']),
                     **({'verify_open_us': metrics(r['verify']['stdout'])['verify_open_us']}
                        if variant == 'baseline' else
                        {'restore_us': metrics(r['restore']['stdout'])['restore_us']})) for r in rows]
        result['summary'][case][variant] = {k: {'median': statistics.median(r[k] for r in data),
                                               'min': min(r[k] for r in data), 'max': max(r[k] for r in data)}
                                             for k in data[0]}
save()
print(json.dumps(result['summary'], indent=2))
