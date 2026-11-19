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

import fine_wal as fw
import hdd_sync_compare as hc
import prepare_driver as build
from optcore_compare_test import accepted
import write_breakdown as wb


def sample(spec):
    row = accepted(spec)
    row['source_commit'] = hc.FINAL if row['group'] == 'optimized' else hc.CORE
    for event in (row['writer']['events'][0], row['recovery_result']):
        event['recovery_time_sampling'] = row['recovery_time_sampling']
    row['recovery_result'].update(has_time_lower_bound=False, has_time_upper_bound=False,
                                  time_lower_bound_micros=0, time_upper_bound_micros=0)
    return row


def document(rnd, case=hc.FORMAL):
    return dict(optimization_case=case, profile='node-optimize', status='completed',
        settings=dict(profile='node-optimize', suite='node-optimize', measure_seconds=180,
                      harness_commit='a' * 40), resource_limits=hc.RESOURCES.copy(),
        harness_actual=dict(commit='a' * 40),
        build_manifests={group: dict(adapter_commit='d' * 40,
            core_commit=hc.FINAL if group == 'optimized' else hc.CORE) for group in hc.GROUPS},
        rows=[sample(r) for r in hc.matrix(case, rnd)])


def add_breakdown_samples(row):
    final = row['final_stats']
    begin, end = final['measure_begin_ns'], final['measure_end_ns']
    events = row['writer']['events']
    events[0]['write_breakdown'] = True
    mb = row['engine'] == 'mb'
    for timestamp, value in ((begin-10, 1), (begin+10, 10),
                             (end-100, 30), (end, 999)):
        counters = dict.fromkeys(wb.BACKGROUND if mb else wb.NATIVE + wb.BE_BACKGROUND, value)
        counters.update(background_begin_ns=timestamp, background_end_ns=timestamp+1,
                        foreground_snapshot_ns=timestamp+2, sampled_calls=1,
                        sampled_records=row['batch_size'], sampled_api_ns=100,
                        sampled_wal_ns=20, sampled_memtable_ns=30)
        event = dict.fromkeys(wb.MB, value) if mb else {}
        event.update(event='sample', monotonic_begin_ns=timestamp, monotonic_end_ns=timestamp+3,
                     **{key: final[key] for key in fw.RUN_FIELDS}, write_breakdown=counters)
        events.append(event)


def add_fine_samples(row):
    add_breakdown_samples(row)
    row['writer']['events'][0].update(fine_wal=row['fine_wal'], fine_wal_clock_p95_ns=26)
    if not row['fine_wal']:
        for event in row['writer']['events'][-4:]:
            event['fine_wal_reference'] = dict(calls=1, records=row['batch_size'], api_ns=100,
                                               wal_ns=20, memtable_ns=30)
        return
    stages = fw.STAGES if row['engine'] == 'mb' else ('primary_append',)
    stages = [stage for stage in stages if stage != 'batch_validation' or row['batch_size'] > 1]
    for event in row['writer']['events'][-4:]:
        event['fine_wal'] = dict(calls=1, records=row['batch_size'], api_ns=100, wal_ns=20,
            memtable_ns=30, primary_append_bytes=88,
            stages={stage: dict(calls=2 if stage == 'primary_append' else 1, ns=10) for stage in stages})


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

    def test_be40_rounds_and_commands(self):
        self.assertEqual(len(hc.matrix(hc.BE_PREFLIGHT)), 12)
        self.assertEqual(len(hc.matrix(hc.BE_FORMAL)), 48)
        first = hc.matrix(hc.BE_FORMAL, 1)
        second = hc.matrix(hc.BE_FORMAL, 2)
        self.assertEqual((len(first), len(second)), (36, 12))
        runner = hc.HddSyncRunner.__new__(hc.HddSyncRunner)
        runner.args = hc.parser().parse_args(
            ['plan', '--case', hc.BE_FORMAL, '--driver', '/ssd/final', '--baseline-driver', '/ssd/base'])
        order = hc.GROUPS + (hc.BE,)
        for index, repeat in enumerate(second):
            three = first[index * 3:(index + 1) * 3]
            offset = index % 3
            self.assertEqual(tuple(r['group'] for r in three), order[offset:] + order[:offset])
            self.assertTrue(all(r['config_id'] == repeat['config_id'] for r in three))
            for row in three + [repeat]:
                cmd = runner.command(row, 'write', '/db', '/blob', '/backup', '/stats')
                self.assertIn('--write_breakdown=1', cmd)
                self.assertIn('--interval_ms=40000', cmd)
                self.assertNotIn('--write_breakdown=1', runner.command(row, 'recover', '/d', '/b', '/p', '/s'))
                if row['engine'] == 'be':
                    self.assertEqual(cmd[0], '/ssd/base')
                    self.assertIn('--be_retained_points=1', cmd)
                    self.assertFalse(any(x.startswith(('--mirror_', '--recovery_time_sampling=')) for x in cmd))
        for row in hc.matrix(hc.BE_FORMAL) + hc.matrix(hc.BE_PREFLIGHT):
            hc.validate_sample(sample(row))

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
        runner.args.write_breakdown = True
        self.assertIn('--write_breakdown=1', runner.command(row, 'write', '/d', '/b', '/p', '/s'))
        self.assertNotIn('--write_breakdown=1', runner.command(row, 'recover', '/d', '/b', '/p', '/s'))

    def test_write_breakdown_window_and_pairs(self):
        doc = document(1)
        doc['write_breakdown'] = True
        for row in doc['rows']:
            add_breakdown_samples(row)
        doc['rows'][1]['writer']['events'][-2]['write_breakdown']['mirror_wal_append_ns'] += 100
        result = hc.aggregate([doc], write_breakdown=True)
        self.assertEqual((len(result['rows']), len(result['local_comparison'])), (24, 12))
        self.assertEqual(result['cross_round_comparison'], [])
        self.assertEqual(result['run_status'], 'completed')
        for row in result['rows']:
            self.assertEqual(row['sampled_wal_ns_per_call'], 20)
            self.assertEqual(row['sampled_memtable_ns_per_call'], 30)
            self.assertEqual(row['flush_us'], 20)
            self.assertEqual(row['mirrored_wal_bytes'], 20)
            self.assertEqual(row['mirror_wal_append_calls'], 20)
            self.assertEqual(row['mirror_wal_append_bytes'], 20)
            self.assertEqual(row['breakdown_backup_count'], 20)
            self.assertEqual(row['backup_count'], 2)
            self.assertEqual(row['blob_write_bytes'], 20 if row['blob'] else None)
            self.assertEqual(row['background_observed_seconds'], (180 * 10**9 - 110) / 1e9)
            self.assertGreater(row['background_unexported_tail_seconds'], 0)
        self.assertGreater(result['local_comparison'][0]['mirror_wal_append_ms_per_s_delta'], 0)
        doc['rows'][0]['status'] = 'failed'
        result = hc.aggregate([doc], write_breakdown=True)
        self.assertEqual(result['run_status'], 'incomplete')
        self.assertIsNone(result['rows'][0].get('sampled_wal_ns_per_call'))
        self.assertIsNone(result['local_comparison'][0]['sampled_wal_ns_per_call_delta'])
        with self.assertRaises(ValueError):
            hc.aggregate([doc])

    def test_short_breakdown_requires_observed_paths(self):
        row = sample(next(r for r in hc.matrix(hc.CASES[1]) if r['blob']))
        add_breakdown_samples(row)
        samples = [e['write_breakdown'] for e in row['writer']['events']
                   if e.get('event') == 'sample' and 'write_breakdown' in e]
        for s in samples:
            # Prefill wrote the Blob. No short-window flush or compaction occurred.
            s.update(blob_write_bytes=100, flush_count=1, compaction_count=0)
        self.assertEqual(wb.summarize(row)['flush_count'], 0)
        self.assertEqual(wb.summarize(row)['blob_write_bytes'], 0)
        samples[-2]['mirror_wal_append_calls'] = samples[1]['mirror_wal_append_calls']
        with self.assertRaisesRegex(ValueError, 'mirror WAL append'):
            wb.summarize(row)
        samples[-2]['mirror_wal_append_calls'] += 1
        for s in samples:
            s['blob_write_bytes'] = 0
        with self.assertRaisesRegex(ValueError, 'native Blob bytes'):
            wb.summarize(row)

    def test_be40_preflight_measures_initial_point(self):
        row = sample(next(r for r in hc.matrix(hc.BE_PREFLIGHT) if r['group'] == hc.BE and r['blob']))
        add_breakdown_samples(row)
        for event in row['writer']['events']:
            if 'write_breakdown' in event and event.get('event') == 'sample':
                event['write_breakdown'].update(dict.fromkeys(wb.BE_BACKGROUND, 10))
        self.assertEqual(wb.summarize(row)['be_backup_sst_append_bytes'], 0)
        for event in row['writer']['events']:
            if event.get('event') == 'sample' and 'write_breakdown' in event:
                event['write_breakdown']['be_backup_dir_sync_calls'] = 0
        with self.assertRaisesRegex(ValueError, 'initial BE backup writes and syncs'):
            wb.summarize(row)

    def test_be40_report(self):
        rounds = [document(1, hc.BE_FORMAL), document(2, hc.BE_FORMAL)]
        history = document(1)
        history['harness_actual']['commit'] = hc.MB_HISTORY_ADAPTER
        for manifest in history['build_manifests'].values():
            manifest['adapter_commit'] = hc.MB_HISTORY_ADAPTER
        for row in history['rows']:
            row.update(harness_commit=hc.MB_HISTORY_ADAPTER, driver_source_commit=hc.MB_HISTORY_ADAPTER)
        for doc in rounds + [history]:
            doc['write_breakdown'] = True
            for row in doc['rows']:
                add_breakdown_samples(row)
        result = hc.aggregate(rounds, case=hc.BE_FORMAL, mb_history=history)
        self.assertEqual((len(result['rows']), len(result['local_comparison']),
                          len(result['be_across_rounds']), len(result['mb_historical_comparison'])),
                         (48, 36, 12, 24))
        self.assertEqual(result['run_status'], 'completed')
        for row in (r for r in result['rows'] if r['group'] == hc.BE):
            self.assertEqual(row['be_backup_wal_append_bytes'], 20)
            self.assertNotIn('mirror_wal_append_bytes', row)
            self.assertIsNone(row['recovery_time_sampling'])
        for pair in result['be_across_rounds']:
            self.assertEqual(pair['mib_per_s_valid_n'], 2)
            self.assertEqual(pair['current_round'], 2)
            self.assertEqual(pair['reference_round'], 1)
            self.assertEqual(pair['current_group'], hc.BE)
        for pair in result['mb_historical_comparison']:
            self.assertEqual(pair['current_group'], pair['reference_group'])
            self.assertEqual(pair['reference_harness_commit'], hc.MB_HISTORY_ADAPTER)
        rounds[1]['rows'][0]['status'] = 'failed'
        result = hc.aggregate(rounds, case=hc.BE_FORMAL, mb_history=history)
        repeat = result['be_across_rounds'][0]
        self.assertEqual(repeat['mib_per_s_valid_n'], 1)
        self.assertEqual(repeat['be_backup_wal_append_bytes_valid_n'], 1)
        self.assertIsNone(repeat['be_backup_wal_append_bytes_delta'])
        self.assertEqual(result['run_status'], 'incomplete')

    def test_fine_wal_matrix_and_commands(self):
        formal = hc.matrix(hc.FINE_FORMAL)
        short = hc.matrix(hc.FINE_PREFLIGHT)
        self.assertEqual((len(formal), len(short)), (9, 3))
        runner = hc.FineWalRunner.__new__(hc.FineWalRunner)
        runner.args = hc.parser().parse_args(['run', '--case', hc.FINE_FORMAL, '--driver', '/ssd/driver',
            '--build-manifest', '/ssd/build.json', '--source-commit', 'f'*40, '--driver-source-commit', 'd'*40,
            '--harness-commit', 'a'*40, '--ssd-root', '/ssd', '--hdd-root', '/hdd', '--ssd-accounting-root', '/ssd',
            '--hdd-accounting-root', '/hdd', '--output', '/hdd/output'])
        hc.validate_args(runner.args)
        for index, batch in enumerate((1, 8, 128)):
            group = formal[index*3:(index+1)*3]
            self.assertEqual(tuple(r['group'] for r in group), fw.GROUPS[index:] + fw.GROUPS[:index])
            self.assertEqual({(r['value_size'], r['blob'], r['batch_size'], r['measure_seconds'],
                               r['key_count'], r['sample_count']) for r in group},
                             {(64, False, batch, 180, 1048576, 4096)})
        for spec in short:
            self.assertEqual((spec['batch_size'], spec['warm_seconds'], spec['measure_seconds'],
                              spec['key_count'], spec['sample_count']), (8, 1, 7, 16384, 256))
        for spec in formal + short:
            hc.validate_sample(sample(spec))
            writer = runner.command(spec, 'write', '/db', '/blob', '/backup', '/stats')
            self.assertEqual(writer[0], '/ssd/driver')
            self.assertIn('--write_breakdown=1', writer)
            self.assertIn('--fine_wal=' + str(int(spec['fine_wal'])), writer)
            recovery = runner.command(spec, 'recover', '/db', '/blob', '/backup', '/stats')
            self.assertFalse(any(flag.startswith('--fine_wal=') for flag in recovery))
            if spec['engine'] == 'mb':
                self.assertIn('--recovery_time_sampling=0', recovery)
            else:
                self.assertIn('--be_retained_points=1', recovery)

    def test_fine_wal_interval_and_probe_scope(self):
        spec = next(row for row in hc.matrix(hc.FINE_FORMAL) if row['group'] == fw.MB_ON)
        row = sample(spec)
        add_fine_samples(row)
        begin = row['final_stats']['measure_begin_ns']
        # Move the second endpoint to one second. The next sampled interval
        # covers a backup, so keep the first interval only.
        row['writer']['events'][-3]['write_breakdown']['foreground_snapshot_ns'] = begin + 10**9
        row['writer']['events'][-2].update(last_backup_begin_ns=begin + 10*10**9,
                                           last_backup_end_ns=begin + 11*10**9)
        (full, filtered), intervals = fw.summarize(row)
        self.assertEqual(full['duration_seconds'], 180)
        self.assertEqual(filtered['duration_seconds'], 1)
        self.assertEqual([r['near_observed_backup'] for r in intervals], [False, True])
        append = next(r for r in fw.stage_rows((full,)) if r['stage'] == 'primary_append')
        self.assertEqual((append['calls'], append['sample_calls'], append['append_bytes']), (2, 1, 88))
        self.assertEqual(append['ns_per_call'], 10)
        self.assertEqual(append['ns_per_occurrence'], 5)
        self.assertEqual(append['resolution_status'], 'at_or_below_clock_p95')
        row['writer']['events'][-2]['fine_wal']['calls'] = 2
        with self.assertRaisesRegex(ValueError, 'outside the coarse sample'):
            fw.summarize(row)

    def test_fine_wal_disturbance_thresholds(self):
        rows = [dict(config_id='put', group=group, mib_per_s=rate, sampled_api_ns_per_call=latency)
                for group, rate, latency in zip(fw.GROUPS, (10, 10.29, 15), (100, 109, 60))]
        self.assertEqual(fw.disturbance(rows, ['put'])[0]['status'], 'within_threshold')
        rows[1]['mib_per_s'] = 10.3
        self.assertEqual(fw.disturbance(rows, ['put'])[0]['status'], 'within_threshold')
        rows[1]['sampled_api_ns_per_call'] = 90
        self.assertEqual(fw.disturbance(rows, ['put'])[0]['status'], 'disturbed')
        rows[1].update(mib_per_s=9.69, sampled_api_ns_per_call=100)
        self.assertEqual(fw.disturbance(rows, ['put'])[0]['status'], 'disturbed')
        rows[1]['mib_per_s'] = None
        self.assertEqual(fw.disturbance(rows, ['put'])[0]['status'], 'unknown')

    def test_fine_wal_report(self):
        doc = document(1, hc.FINE_FORMAL)
        doc['write_breakdown'] = True
        manifest = dict(adapter_commit='d' * 40, core_commit=hc.FINAL, fine_wal=True)
        doc['build_manifests'] = dict.fromkeys(fw.GROUPS, manifest)
        for row in doc['rows']:
            row['source_commit'] = hc.FINAL
            add_fine_samples(row)
        result = fw.aggregate(doc, hc.validate_sample, hc.validate_source_records, hc.write_pairs)
        self.assertEqual((len(result['rows']), len(result['local_comparison']),
                          len(result['fine_wal_disturbance']), len(result['fine_wal_windows'])), (9, 9, 3, 18))
        self.assertTrue(result['backup_retention_events'])
        self.assertEqual({r['stage'] for r in result['fine_wal_stages'] if r['group'] == fw.BE_ON}, {'primary_append'})
        self.assertFalse(any(r['group'] == fw.MB_OFF for r in result['fine_wal_stages']))
        self.assertEqual(len(result['fine_wal_window_pairs']), 18)
        matched = next(r for r in result['fine_wal_window_pairs'] if
                       r['current_group'] == fw.MB_ON and r['reference_group'] == fw.MB_OFF)
        self.assertEqual(matched['matched_api_ns_per_call_delta'], 0)
        self.assertEqual(matched['matched_wal_ns_per_call_delta'], 0)

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
        with tempfile.TemporaryDirectory() as tmp:
            target = build.prepare(Path(tmp), fine_wal=True)
            rendered = (target / 'driver.cc').read_text()
            self.assertIn('fine_wal_reference', rendered)
            self.assertIn('fine_wal_clock_p95_ns', rendered)
            self.assertEqual((target / 'protocol.h').read_bytes(), (build.DRIVER / 'protocol.h').read_bytes())
        for capability in (0, 1):
            steps = build.build_commands(Path('/core'), Path('/build'), Path('/artifacts'), capability)
            self.assertIn('-DMB_HAS_RECOVERY_TIME_SAMPLING=' + str(capability), steps[2])
            self.assertIn('-I/core/include', steps[2])
            self.assertIn('-L/artifacts', steps[2])
            self.assertNotIn('-DMETABYPASS_PERF_DIAGNOSTICS', steps[2])
        fine_steps = build.build_commands(Path('/core'), Path('/build'), Path('/artifacts'), 1, fine_wal=True)
        self.assertIn('-DCMAKE_CXX_FLAGS=-DROCKSDB_MB_WAL_BREAKDOWN', fine_steps[0])
        self.assertIn('-DROCKSDB_MB_WAL_BREAKDOWN', fine_steps[2])


if __name__ == '__main__':
    unittest.main()
