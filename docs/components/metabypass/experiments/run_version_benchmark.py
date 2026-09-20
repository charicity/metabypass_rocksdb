# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Compare original, incremental and notification-optimized native backups."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('original', 'incremental', 'fixed', 'output'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--trials', type=int, default=5)
    args = parser.parse_args()
    if args.trials < 1:
        parser.error('trials must be positive')
    binaries = {name: getattr(args, name).resolve()
                for name in ('original', 'incremental', 'fixed')}
    root = Path(tempfile.mkdtemp(prefix='metabypass-versions-'))
    report = {'data_root': str(root), 'binaries': {}, 'runs': [], 'summary': {}}
    for name, binary in binaries.items():
        report['binaries'][name] = {
            'path': str(binary),
            'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}

    def save():
        args.output.write_text(json.dumps(report, indent=2) + '\n')

    def run(variant, mode, folder, count, extra):
        command = [str(binaries[variant]), '--metabypass_mode=' + mode,
                   '--db=' + str(folder / 'index'),
                   '--metabypass_data_dir=' + str(folder / 'data'),
                   '--metabypass_backup_dir=' + str(folder / 'backup'),
                   '--num=' + str(count), '--value_size=1024', '--sync=false',
                   *extra]
        start = time.monotonic()
        result = subprocess.run(command, capture_output=True, text=True,
                                timeout=60)
        item = {'variant': variant, 'mode': mode, 'command': command,
                'elapsed_seconds': time.monotonic() - start,
                'exit': result.returncode, 'stdout': result.stdout,
                'stderr': result.stderr,
                'metrics': {key: int(value) for key, value in
                            re.findall(r'(\w+)=(\d+)', result.stdout)}}
        report['runs'].append(item)
        save()
        assert result.returncode == 0 and 'status=OK' in result.stdout, item
        return item

    for variant in binaries:
        folder = root / ('warmup-' + variant)
        folder.mkdir()
        run(variant, 'write', folder, 1000, ())
    scenarios = [('50k_default', 50000, ()), ('200k_default', 200000, ()),
                 ('50k_pressure', 50000,
                  ('--metabypass_queue_capacity=2097152',
                   '--metabypass_batch_bytes=65536'))]
    for scenario, count, extra in scenarios:
        samples = {variant: [] for variant in binaries}
        for trial in range(args.trials):
            order = list(binaries)
            shift = trial % len(order)
            order = order[shift:] + order[:shift]
            for variant in order:
                folder = root / (scenario + '-' + str(trial) + '-' + variant)
                folder.mkdir()
                item = run(variant, 'write', folder, count, extra)
                item.update(scenario=scenario, trial=trial)
                metrics = item['metrics']
                metrics['write_sync_close_us'] = sum(
                    metrics[key] for key in ('foreground_us', 'sync_us', 'close_us'))
                samples[variant].append(metrics)
                print(scenario, trial, variant, item['stdout'].strip(), flush=True)
                assert not list((folder / 'backup').rglob('*.blob'))
                # Restore with the pre-fix reader to verify format compatibility.
                shutil.rmtree(folder / 'index')
                reader = 'original'
                restored = run(reader, 'restore', folder, count, extra)
                metrics['restore_us'] = restored['metrics']['restore_us']
                run(variant, 'verify', folder, count, extra)
                shutil.rmtree(folder)
        report['summary'][scenario] = {
            variant: {key: {'median': statistics.median(row[key] for row in rows),
                            'min': min(row[key] for row in rows),
                            'max': max(row[key] for row in rows)}
                      for key in rows[0]}
            for variant, rows in samples.items()}
        save()
    print(json.dumps(report['summary'], indent=2), flush=True)


if __name__ == '__main__':
    main()
