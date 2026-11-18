#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Offline protocol tests using the frozen controller's accepted sample fixture."""
import copy
from pathlib import Path
import tempfile
import unittest

import hdd_sync_compare as hc
import prepare_driver as build
from optcore_compare_test import accepted


def sample(spec):
    row = accepted(spec)
    row['source_commit'] = hc.CORE if row['group'] == 'baseline' else 'f' * 40
    for event in (row['writer']['events'][0], row['recovery_result']):
        event['recovery_time_sampling'] = row['recovery_time_sampling']
    row['recovery_result'].update(has_time_lower_bound=False, has_time_upper_bound=False,
                                  time_lower_bound_micros=0, time_upper_bound_micros=0)
    return row


def document(rnd):
    return dict(optimization_case=hc.CASES[0], profile='node-optimize', status='completed',
        settings=dict(profile='node-optimize', suite='node-optimize', measure_seconds=180,
                      harness_commit='a' * 40), resource_limits=hc.RESOURCES.copy(),
        harness_actual=dict(commit='a' * 40),
        build_manifests={group: dict(adapter_commit='d' * 40,
            core_commit=hc.CORE if group == 'baseline' else 'f' * 40) for group in hc.GROUPS},
        rows=[sample(r) for r in hc.matrix(round_number=rnd)])


class HddSyncTests(unittest.TestCase):
    def test_matrix(self):
        rows = hc.matrix()
        self.assertEqual(len(rows), 48)
        self.assertEqual(len({r['id'] for r in rows}), 48)
        for rnd in (1, 2):
            rows = hc.matrix(round_number=rnd)
            for index in range(12):
                pair = rows[index*2:index*2+2]
                first = (index + rnd - 1) % 2
                self.assertEqual([r['group'] for r in pair], [hc.GROUPS[first], hc.GROUPS[1-first]])
                self.assertEqual(pair[0]['config_id'], pair[1]['config_id'])
                for row in pair:
                    self.assertEqual((row['key_count'], row['warm_seconds'], row['measure_seconds'], row['sample_count']),
                                     (1048576, 65, 180, 4096))
        short = hc.matrix(hc.CASES[1])
        self.assertEqual(len(short), 8)
        for row in short:
            self.assertEqual((row['batch_size'], row['key_count'], row['warm_seconds'], row['measure_seconds'], row['sample_count']),
                             (128, 16384, 1, 7, 256))
        with self.assertRaises(ValueError):
            hc.matrix(round_number=3)

    def test_commands(self):
        runner = hc.HddSyncRunner.__new__(hc.HddSyncRunner)
        runner.args = hc.parser().parse_args(['plan', '--driver', '/ssd/final', '--baseline-driver', '/ssd/base'])
        for row in hc.matrix() + hc.matrix(hc.CASES[1]):
            for mode in ('write', 'recover'):
                command = runner.command(row, mode, '/db', '/blob', '/backup', '/stats')
                self.assertEqual(command[0], '/ssd/base' if row['group'] == 'baseline' else '/ssd/final')
                for key in ('batch_size', 'value_size', 'warm_seconds', 'measure_seconds', 'sample_count',
                            'keyspace_bytes', 'mirror_interval_ms', 'mirror_batch_bytes', 'mirror_queue_capacity', 'interval_ms'):
                    self.assertIn('--%s=%s' % (key, row[key]), command)
                self.assertIn('--recovery_time_sampling=' + str(int(row['recovery_time_sampling'])), command)

    def test_acceptance(self):
        for spec in hc.matrix() + hc.matrix(hc.CASES[1]):
            hc.validate_sample(sample(spec))
        row = sample(next(r for r in hc.matrix() if r['group'] == 'optimized'))
        for key, value in (('recovery_time_sampling', True), ('has_time_lower_bound', True),
                           ('time_lower_bound_micros', 1), ('candidate_ordinal', 0)):
            bad = copy.deepcopy(row)
            bad['recovery_result'][key] = value
            with self.assertRaises(RuntimeError, msg=key):
                hc.validate_sample(bad)
        short = sample(hc.matrix(hc.CASES[1])[0])
        short['recovery_result'].update(recovered_sequence=short['key_count'], candidate_ordinal=0)
        with self.assertRaises(RuntimeError):
            hc.validate_sample(short)

    def test_report(self):
        result = hc.aggregate([document(1), document(2)])
        self.assertEqual((len(result['rows']), len(result['local_comparison']), len(result['cross_round_comparison'])), (48, 24, 12))
        for pair in result['local_comparison']:
            self.assertEqual((pair['current_variant'], pair['reference_variant']), ('optimized', 'baseline'))
        for row in result['rows']:
            if row['group'] == 'optimized':
                self.assertIsNone(row['rpo_visible_lower_seconds'])
                self.assertIsNone(row['rpo_ack_upper_seconds'])
        bad = document(1)
        bad['rows'][0]['mirror_batch_bytes'] = 1
        with self.assertRaises(ValueError):
            hc.aggregate([bad])
        with self.assertRaises(ValueError):
            hc.aggregate([document(1), document(1)])

    def test_declared_sources_must_match_actual_records(self):
        for record in ('controller', 'driver', 'core'):
            bad = document(2)
            if record == 'controller':
                bad['harness_actual']['commit'] = 'b' * 40
            else:
                key = 'adapter_commit' if record == 'driver' else 'core_commit'
                bad['build_manifests']['optimized'][key] = 'b' * 40
            with self.assertRaisesRegex(ValueError, 'actual controller/build records'):
                hc.aggregate([document(1), bad])

    def test_explicit_driver_patch_and_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = build.prepare(Path(tmp))
            text = (target / 'driver.cc').read_text()
            self.assertIn('baseline requires recovery_time_sampling=1', text)
            self.assertIn('mb_options.enable_recovery_time_sampling =', text)
            self.assertIn('db.RecoveryTimeSampling()', text)
            self.assertEqual((target / 'protocol.h').read_bytes(), (build.DRIVER / 'protocol.h').read_bytes())
        for capability in (0, 1):
            steps = build.build_commands(Path('/core'), Path('/build'), Path('/artifacts'), capability)
            self.assertIn('-DMB_HAS_RECOVERY_TIME_SAMPLING=' + str(capability), steps[2])
            self.assertIn('-I/core/include', steps[2])
            self.assertIn('-L/artifacts', steps[2])
            self.assertNotIn('-DMETABYPASS_PERF_DIAGNOSTICS', steps[2])


if __name__ == '__main__':
    unittest.main()
