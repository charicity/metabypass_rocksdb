# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Build and compare pinned notification/split controls with live channels."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

here = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('--out', default='/tmp/mb-channels-ab')
parser.add_argument('--library', default='/tmp/metabypass-release-build/librocksdb.a')
parser.add_argument('--output', default='/tmp/mb-channels-results.json')
parser.add_argument('--trials', type=int, default=5)
args = parser.parse_args()
out = Path(args.out)


def build(revision, variants):
    subprocess.run([sys.executable, str(here / 'build.py'), '--out', str(out),
                    '--library', args.library, '--revision', revision,
                    '--variants', variants], check=True)
    return json.loads((out / 'build.json').read_text())


controls = build('bbd60f2ca', 'current,split')
formal = build('WORKTREE', 'channels')
formal['controls'] = controls
(out / 'build.json').write_text(json.dumps(formal, indent=2) + '\n')
subprocess.run([sys.executable, str(here / 'run.py'), '--binaries', str(out),
                '--output', args.output, '--trials', str(args.trials),
                '--variants', 'current,split,channels'], check=True)
