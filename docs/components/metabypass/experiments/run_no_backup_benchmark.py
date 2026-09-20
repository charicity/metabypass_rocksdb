# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Measure the same separated Blob Direct Write workload without backup."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--trials', type=int, default=5)
    args = parser.parse_args()
    if args.trials < 1:
        parser.error('trials must be positive')
    binary = args.binary.resolve()
    report = {'binary': str(binary),
              'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'runs': [], 'summary': {}}
    for trial in range(-1, args.trials):
        counts = [1000] if trial == -1 else [50000, 200000]
        if trial % 2:
            counts.reverse()
        for count in counts:
            root = Path(tempfile.mkdtemp(prefix='metabypass-no-backup-'))
            command = [str(binary), '--metabypass_mode=baseline',
                       '--db=' + str(root / 'index'),
                       '--metabypass_data_dir=' + str(root / 'data'),
                       '--num=' + str(count), '--value_size=1024', '--sync=false']
            result = subprocess.run(command, text=True, capture_output=True,
                                    timeout=60)
            metrics = {key: int(value) for key, value in
                       re.findall(r'(\w+)=(\d+)', result.stdout)}
            item = {'trial': trial, 'count': count, 'command': command,
                    'exit': result.returncode, 'stdout': result.stdout,
                    'stderr': result.stderr, 'metrics': metrics}
            report['runs'].append(item)
            args.output.write_text(json.dumps(report, indent=2) + '\n')
            assert result.returncode == 0 and 'status=OK' in result.stdout, item
            metrics['write_sync_close_us'] = sum(metrics[key] for key in
                                                ('foreground_us', 'sync_us',
                                                 'close_us'))
            print(count, trial, result.stdout.strip(), flush=True)
            shutil.rmtree(root)
    for count in [50000, 200000]:
        rows = [item['metrics'] for item in report['runs']
                if item['count'] == count and item['trial'] >= 0]
        report['summary'][str(count)] = {
            key: {'median': statistics.median(row[key] for row in rows),
                  'min': min(row[key] for row in rows),
                  'max': max(row[key] for row in rows)} for key in rows[0]}
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
