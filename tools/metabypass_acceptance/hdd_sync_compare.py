#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Thin two-core HDD sync experiment; frozen dependencies come from PYTHONPATH."""
import copy
from collections import Counter
import json
import hashlib
from pathlib import Path
from statistics import median

import optcore_compare as frozen
from node_workload import NodeRunner
from run import ResourceStop
import write_breakdown as breakdown

CORE = frozen.CORE
CASES = ('hdd-sync', 'hdd-sync-preflight')
GROUPS = ('baseline', 'optimized')
RESOURCES = frozen.RESOURCES


def matrix(case=CASES[0], round_number=None):
    if case not in CASES or round_number not in (None, 1, 2):
        raise ValueError('unknown case or round')
    short = case == CASES[1]
    if short and round_number is not None:
        raise ValueError('preflight has no round')
    rounds = (0,) if short else ((round_number,) if round_number else (1, 2))
    rows = []
    for rnd in rounds:
        source = frozen.matrix(frozen.CASES[1] if short else frozen.CASES[0], None if short else rnd)
        for index, row in enumerate(source):
            config, position = divmod(index, 2)
            group = GROUPS[(config + max(rnd - 1, 0) + position) % 2]
            row.update(id='hdd-sync-r%d-%03d' % (rnd, index + 1), group=group, variant=group,
                engine='mb', purpose='short_recovery_check' if short else 'paired_core_backup',
                mirror_interval_ms=5000, mirror_batch_bytes=536870912,
                mirror_queue_capacity=1073741824, recovery_time_sampling=group == 'baseline')
            rows.append(row)
    return rows


def validate_args(args):
    matrix(args.case, args.round)
    if args.write_breakdown and (args.round not in (None, 1) or
            (args.action == 'aggregate' and len(args.round_ledgers or ()) != 1)):
        raise ValueError('write breakdown uses one formal round')
    if args.phase_timeout != 1800:
        raise ValueError('phase timeout must remain 1800')
    if args.action == 'run':
        if args.case == CASES[0] and args.round is None:
            raise ValueError('run one formal --round at a time')
        if (args.baseline_source_commit != CORE or args.source_commit == CORE or
                not all(frozen.full_commit(x) for x in
                    (args.source_commit, args.driver_source_commit, args.harness_commit))):
            raise ValueError('requires core52 baseline and full final/driver/adapter commits')
        for name in ('driver', 'baseline_driver', 'build_manifest', 'baseline_build_manifest',
                     'ssd_root', 'hdd_root', 'ssd_accounting_root', 'hdd_accounting_root', 'output'):
            if not getattr(args, name) or not Path(getattr(args, name)).is_absolute():
                raise ValueError('run requires absolute ' + name)
    if args.action == 'aggregate' and (args.case != CASES[0] or args.round is not None or
                                      not args.round_ledgers or not args.output):
        raise ValueError('aggregate requires formal ledgers and new output')


def validate_sample(row):
    frozen.validate_sample(row)
    ready = next(e for e in row['writer']['events'] if e.get('event') == 'ready')
    recovery = row['recovery_result']
    expected = row['recovery_time_sampling']
    if any(e.get('recovery_time_sampling') is not expected for e in (ready, recovery)):
        raise RuntimeError('effective recovery time sampling mismatch')
    if not expected:
        if (recovery.get('has_time_lower_bound') is not False or
                recovery.get('has_time_upper_bound') is not False or
                recovery.get('time_lower_bound_micros') != 0 or
                recovery.get('time_upper_bound_micros') != 0):
            raise RuntimeError('optimized recovery reported time bounds')
        # Frozen controller derives additional windows from stdout samples.
        # Keep raw telemetry, but do not present these as optimized time bounds.
        for kind in ('visible', 'ack'):
            row['rpo_' + kind] = dict(status='disabled_recovery_time_sampling')


def validate_source_records(rows, actual, manifests):
    for row in rows:
        manifest = manifests.get(row['group'], {})
        if (row.get('harness_commit') != actual.get('commit') or
                row.get('driver_source_commit') != manifest.get('adapter_commit') or
                row.get('source_commit') != manifest.get('core_commit')):
            raise ValueError('declared source does not match actual controller/build records')


class HddSyncRunner(NodeRunner):
    child_timeout = frozen.OptcoreRunner.child_timeout
    check_delta = frozen.OptcoreRunner.check_delta
    writer_ready = frozen.OptcoreRunner.writer_ready
    writer_finished = frozen.OptcoreRunner.writer_finished

    def __init__(self, args):
        validate_args(args)
        super().__init__(args)
        self.ledger.update(optimization_case=args.case, round=args.round,
            retention='Both MB arms retain one point',
            comparison_direction='optimized/baseline')
        if args.write_breakdown:
            self.ledger['write_breakdown'] = True
        for row in self.ledger['rows']:
            row.update(source_commit=args.baseline_source_commit if row['group'] == 'baseline' else args.source_commit,
                       driver_source_commit=args.driver_source_commit, harness_commit=args.harness_commit)

    def planned_rows(self, args):
        return matrix(args.case, args.round)

    def driver_paths(self):
        return (('new', self.args.driver), ('old', self.args.baseline_driver))

    def driver_for_row(self, row):
        return self.args.baseline_driver if row['group'] == 'baseline' else self.args.driver

    def command(self, row, mode, front, blob, backup, stats):
        cmd = super().command(row, mode, front, blob, backup, stats) + [
            '--fixed_tail=0', '--recovery_time_sampling=' + str(int(row['recovery_time_sampling']))]
        if self.args.write_breakdown and mode == 'write':
            cmd.append('--write_breakdown=1')
        return cmd

    def validate_storage(self):
        super().validate_storage()
        root = Path(self.args.ssd_accounting_root).resolve(strict=True)
        for _, binary in self.driver_paths():
            driver = Path(binary).resolve(strict=True)
            if root not in driver.parents or driver.stat().st_dev != self.ssd.parent.stat().st_dev:
                raise ResourceStop('Both drivers and adjacent libraries must be in accounted SSD root')

    def validate_completed_sample(self, row):
        validate_sample(row)
        if self.args.write_breakdown:
            breakdown.summarize(row)

    def run(self):
        manifests = {}
        for group, name, commit, capability in (
                ('baseline', 'baseline_build_manifest', self.args.baseline_source_commit, 0),
                ('optimized', 'build_manifest', self.args.source_commit, 1)):
            manifest = json.loads(Path(getattr(self.args, name)).read_text())
            if (manifest['core_commit'] != commit or manifest['status'] != 'built' or
                    manifest['mb_has_recovery_time_sampling'] != capability):
                raise ValueError('build manifest does not match ' + group)
            driver = Path(self.args.baseline_driver if group == 'baseline' else self.args.driver)
            library = driver.parent / manifest['library_name']
            if (hashlib.sha256(driver.read_bytes()).hexdigest() != manifest['driver_sha256'] or
                    hashlib.sha256(library.read_bytes()).hexdigest() != manifest['library_sha256']):
                raise ValueError('driver/library digest mismatch for ' + group)
            manifests[group] = manifest
        if manifests['baseline']['driver_source_sha256'] != manifests['optimized']['driver_source_sha256']:
            raise ValueError('both arms must compile the same updated driver source')
        if manifests['baseline']['protocol_sha256'] != manifests['optimized']['protocol_sha256']:
            raise ValueError('protocol differs between arms')
        from local_compare import harness_identity
        validate_source_records(self.ledger['rows'], harness_identity(), manifests)
        self.ledger['build_manifests'] = manifests
        return super().run()


def aggregate(documents, write_breakdown=False):
    from report import local_pairs, memory_fields, summarize
    rounds, identities = {}, None
    for document in documents:
        observed = {r['round'] for r in document['rows']}
        if (document.get('optimization_case') != CASES[0] or len(observed) != 1 or
                bool(document.get('write_breakdown')) != write_breakdown):
            raise ValueError('one formal round per ledger required')
        rnd = next(iter(observed))
        if rnd not in (1, 2) or rnd in rounds:
            raise ValueError('duplicate or invalid round')
        expected = matrix(round_number=rnd)
        fields = set(expected[0]) - {'status'}
        if len(document['rows']) != 24 or any(any(r.get(k) != spec[k] for k in fields)
                for r, spec in zip(document['rows'], expected)):
            raise ValueError('fixed matrix mismatch')
        if document.get('resource_limits') != RESOURCES:
            raise ValueError('resource limits mismatch')
        identity = {(r['group'], r.get('source_commit'), r.get('driver_source_commit'),
                     r.get('harness_commit')) for r in document['rows']}
        if (len(identity) != 2 or any(not all(frozen.full_commit(x) for x in t[1:]) for t in identity)
                or next(t[1] for t in identity if t[0] == 'baseline') != CORE
                or next(t[1] for t in identity if t[0] == 'optimized') == CORE):
            raise ValueError('invalid source identities')
        validate_source_records(document['rows'], document.get('harness_actual', {}),
                                document.get('build_manifests', {}))
        if identities is not None and identities != identity:
            raise ValueError('cross-round identity mismatch')
        identities = identity
        for row in document['rows']:
            if row['status'] == 'passed':
                validate_sample(row)
        rounds[rnd] = document
    if write_breakdown and set(rounds) != {1}:
        raise ValueError('expected only round 1 for write breakdown')
    if not rounds:
        raise ValueError('at least one ledger required')
    expected_rounds = (1,) if write_breakdown else (1, 2)
    combined = copy.deepcopy(next(iter(rounds.values())))
    combined['rows'] = [copy.deepcopy(row) for rnd in expected_rounds for row in
                        (rounds[rnd]['rows'] if rnd in rounds else matrix(round_number=rnd))]
    combined['status'] = 'completed' if len(rounds) == len(expected_rounds) and all(
        row['status'] == 'passed' for row in combined['rows']) else 'incomplete'
    summaries = []
    for rnd in expected_rounds:
        part = dict(combined, rows=[r for r in combined['rows'] if r['round'] == rnd])
        summaries.append(summarize(part))
    result = summaries[0]
    for summary in summaries[1:]:
        for name in ('rows', 'publication_events', 'memory_samples', 'rpo_samples'):
            result[name].extend(summary[name])
    result['counts'] = dict(Counter(r['status'] for r in result['rows']))
    result['run_status'] = combined['status']
    result['optimization_case'] = CASES[0]
    result['available_rounds'] = sorted(rounds)
    result['local_comparison'] = []
    for source, row in zip(combined['rows'], result['rows']):
        for key in ('round', 'group', 'recovery_time_sampling', 'source_commit', 'driver_source_commit', 'harness_commit'):
            row[key] = source.get(key)
        row['raw_recovery'] = source.get('recovery_result', {})
        if write_breakdown and row['status'] == 'passed':
            row.update(breakdown.summarize(source))
        if source['group'] == 'optimized':
            for key in row:
                if key.startswith('rpo_'):
                    row[key] = 'disabled_recovery_time_sampling' if key.endswith('_status') else None
    for rnd in expected_rounds:
        pairs = local_pairs([r for r in result['rows'] if r['round'] == rnd], 'main',
                            (('optimized', 'baseline'),), memory_fields(result))
        result['local_comparison'].extend(dict(p, round=rnd) for p in pairs)
    result['cross_round_comparison'] = []
    for config in dict.fromkeys(p['config_id'] for p in result['local_comparison']):
        pairs = [p for p in result['local_comparison'] if p['config_id'] == config]
        entry = dict(config_id=config, current_group='optimized', reference_group='baseline', rounds=pairs)
        for metric in (k for k in pairs[0] if k.endswith('_ratio')):
            values = [p[metric] for p in pairs if p[metric] is not None]
            entry.update({metric + '_median': median(values) if values else None,
                          metric + '_min': min(values) if values else None,
                          metric + '_max': max(values) if values else None,
                          metric + '_valid_n': len(values)})
        result['cross_round_comparison'].append(entry)
    result['notes'] = [
        '48 samples / 24 within-round pairs / 12 configuration summaries; ratios are optimized/baseline.',
        'Both MB arms: CP40s, mirror5s, batch512MiB, queue1GiB, retain1, warm65s/measure180s.',
        'Baseline core52 samples recovery time; final three-fix core disables it and has no time windows.',
        'Sampled full values and batch-aligned sequence checks do not certify global continuity.',
        'Failures retain raw evidence; no automatic performance retries. Medians/min/max are observations, not confidence intervals.',
        'Cgroup v1 32GiB; SSD/HDD task caps 64/128GiB; minimum free 20/100GiB. Raw sampled memory is retained.']
    if write_breakdown:
        result['notes'][0] = '24 samples / 12 within-round pairs; ratios are optimized/baseline.'
        result['notes'][-2] = 'Failures retain raw evidence. Each configuration has one observation.'
        result['notes'].extend(breakdown.NOTES)
        result['cross_round_comparison'] = []
        by_id = {r['id']: r for r in result['rows']}
        for pair in result['local_comparison']:
            for metric in breakdown.COMPARISON:
                old = by_id[pair['reference_id']].get(metric)
                new = by_id[pair['current_id']].get(metric)
                pair.update({'reference_' + metric: old, 'current_' + metric: new,
                             metric + '_delta': new - old if old is not None and new is not None else None})
    return result


def export(paths, output, write_breakdown=False):
    from mb40_report import write_csv
    from report import md_table, number
    result = aggregate([json.loads(Path(p).read_text()) for p in paths], write_breakdown)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=False)
    names = ('rows', 'local_comparison', 'memory_samples', 'publication_events', 'rpo_samples')
    if not write_breakdown:
        names += ('cross_round_comparison',)
    for name in names:
        write_csv(out / (name + '.csv'), result[name])
    (out / 'summary.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    lines = ['# HDD sync: final / core52', '', 'Status: ' + result['run_status'], '']
    lines += ['- ' + note for note in result['notes']] + ['']
    if write_breakdown:
        md_table(lines, ('Config', 'Baseline MiB/s', 'Optimized MiB/s', 'Change %'),
            [(r['config_id'], number(r['reference_mib_per_s']), number(r['current_mib_per_s']),
              number(100 * r['mib_per_s_change_fraction'] if r['mib_per_s_change_fraction'] is not None else None))
             for r in result['local_comparison']])
        lines.append('')
        md_table(lines, ('Config', 'Arm', 'WAL ns/call', 'MemTable ns/call', 'Flush ms/s',
                         'Compaction ms/s', 'Mirror WAL append MiB/s', 'Mirror WAL append ms/s',
                         'Checkpoint publish ms/s',
                         'Blob HDD MiB/s', 'Blob append ms/s', 'Blob sync ms/s'),
            [(r['config_id'], r['group'], *(number(r.get(k)) for k in (
                'sampled_wal_ns_per_call', 'sampled_memtable_ns_per_call', 'flush_ms_per_s',
                'compaction_ms_per_s', 'mirror_wal_append_mib_per_s', 'mirror_wal_append_ms_per_s',
                'checkpoint_publish_ms_per_s',
                'blob_write_mib_per_s', 'blob_append_ms_per_s', 'blob_sync_ms_per_s'))) for r in result['rows']])
    else:
        md_table(lines, ('Config', 'Final/baseline MiB/s median', 'Min', 'Max', 'Valid rounds'),
            [(r['config_id'], number(r['mib_per_s_ratio_median']), number(r['mib_per_s_ratio_min']),
              number(r['mib_per_s_ratio_max']), r['mib_per_s_ratio_valid_n']) for r in result['cross_round_comparison']])
    (out / 'report.md').write_text('\n'.join(lines) + '\n')
    return result


def parser():
    p = frozen.parser(cases=CASES)
    p.add_argument('--baseline-driver')
    p.add_argument('--baseline-build-manifest')
    p.add_argument('--baseline-source-commit', default=CORE)
    p.add_argument('--write-breakdown', action='store_true')
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    try:
        validate_args(args)
        if args.action == 'plan':
            rnd = 1 if args.write_breakdown and args.case == CASES[0] else args.round
            print(json.dumps(dict(case=args.case, rows=matrix(args.case, rnd), resources=RESOURCES,
                                  write_breakdown=args.write_breakdown), indent=2))
            return 0
        if args.action == 'aggregate':
            result = export(args.round_ledgers, args.output, args.write_breakdown)
            return 0 if result['run_status'] == 'completed' else 1
        return HddSyncRunner(args).run()
    except (ValueError, RuntimeError) as exc:
        p.error(str(exc))


if __name__ == '__main__':
    raise SystemExit(main())
