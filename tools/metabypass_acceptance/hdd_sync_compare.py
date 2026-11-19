#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Two-core MB and BE40 experiments; frozen dependencies come from PYTHONPATH."""
from collections import Counter
import json
import hashlib
from pathlib import Path
from statistics import median

import optcore_compare as frozen
from node_workload import NodeRunner
from run import ResourceStop
import fine_wal
import write_breakdown as breakdown

CORE = frozen.CORE
FORMAL, PREFLIGHT = 'hdd-sync', 'hdd-sync-preflight'
BE_FORMAL, BE_PREFLIGHT = 'hdd-sync-be40', 'hdd-sync-be40-preflight'
FINE_FORMAL, FINE_PREFLIGHT = fine_wal.FORMAL, fine_wal.PREFLIGHT
FINE_CASES = (FINE_FORMAL, FINE_PREFLIGHT)
ALWAYS_BREAKDOWN = (BE_FORMAL, BE_PREFLIGHT) + FINE_CASES
CASES = (FORMAL, PREFLIGHT) + ALWAYS_BREAKDOWN
GROUPS = ('baseline', 'optimized')
BE = 'be40'
FINAL = '0330a5e84936e2e222cdce4de666d4698e0b936d'
MB_HISTORY_ADAPTER = 'dfab997216ce40d1e26f680c1cb04dd358d89ad9'
RESOURCES = frozen.RESOURCES


def matrix(case=CASES[0], round_number=None):
    if case not in CASES or round_number not in (None, 1, 2):
        raise ValueError('unknown case or round')
    if case in FINE_CASES:
        return fine_wal.matrix(case, round_number)
    short = case in (PREFLIGHT, BE_PREFLIGHT)
    with_be = case in (BE_FORMAL, BE_PREFLIGHT)
    if short and round_number is not None:
        raise ValueError('preflight has no round')
    rounds = (0,) if short else ((round_number,) if round_number else (1, 2))
    rows = []
    for rnd in rounds:
        source = frozen.matrix(frozen.CASES[1] if short else frozen.CASES[0], None if short else rnd)
        groups = ((BE,) if rnd == 2 else GROUPS + (BE,)) if with_be else GROUPS
        for config, template in enumerate(source[::2]):
            offset = (config + max(rnd - 1, 0)) % len(groups)
            for position, group in enumerate(groups[offset:] + groups[:offset], 1):
                mb = group != BE
                prefix = BE_FORMAL if with_be else FORMAL
                rows.append(dict(template, id='%s-r%d-%03d' % (prefix, rnd, config * len(groups) + position),
                    group=group, variant=group, engine='mb' if mb else 'be',
                    purpose='short_recovery_check' if short else 'paired_core_backup',
                    mirror_interval_ms=5000 if mb else None, mirror_batch_bytes=536870912 if mb else None,
                    mirror_queue_capacity=1073741824 if mb else None,
                    recovery_time_sampling=group == 'baseline' if mb else None))
    return rows


def validate_args(args):
    matrix(args.case, args.round)
    if args.case in (FORMAL, PREFLIGHT) and args.write_breakdown and (args.round not in (None, 1) or
            (args.action == 'aggregate' and len(args.round_ledgers or ()) != 1)):
        raise ValueError('write breakdown uses one formal round')
    if args.phase_timeout != 1800:
        raise ValueError('phase timeout must remain 1800')
    if args.action == 'run':
        if args.case in (FORMAL, BE_FORMAL) and args.round is None:
            raise ValueError('run one formal --round at a time')
        fine = args.case in FINE_CASES
        if (not all(frozen.full_commit(x) for x in
                    (args.source_commit, args.driver_source_commit, args.harness_commit)) or
                args.source_commit == CORE or (not fine and args.baseline_source_commit != CORE)):
            raise ValueError('fine WAL requires full core/driver/adapter commits' if fine else
                             'requires core52 baseline and full final/driver/adapter commits')
        if args.case in (BE_FORMAL, BE_PREFLIGHT) and args.source_commit != FINAL:
            raise ValueError('BE40 comparison requires the three-fix core')
        names = ('driver', 'build_manifest', 'ssd_root', 'hdd_root', 'ssd_accounting_root',
                 'hdd_accounting_root', 'output')
        if not fine:
            names += ('baseline_driver', 'baseline_build_manifest')
        for name in names:
            if not getattr(args, name) or not Path(getattr(args, name)).is_absolute():
                raise ValueError('run requires absolute ' + name)
    if args.action == 'aggregate' and (args.case not in (FORMAL, BE_FORMAL, FINE_FORMAL) or args.round is not None or
                                      not args.round_ledgers or not args.output):
        raise ValueError('aggregate requires formal ledgers and new output')
    if args.action == 'aggregate' and args.case == BE_FORMAL and not args.mb_history_ledger:
        raise ValueError('BE40 aggregate requires --mb-history-ledger')
    if args.action == 'aggregate' and args.case == FINE_FORMAL and len(args.round_ledgers) != 1:
        raise ValueError('fine WAL aggregate requires exactly one formal ledger')


def validate_sample(row):
    frozen.validate_sample(row)
    ready = next(e for e in row['writer']['events'] if e.get('event') == 'ready')
    recovery = row['recovery_result']
    if row['engine'] == 'be':
        if any(e.get('recovery_time_sampling') is not None for e in (ready, recovery)):
            raise RuntimeError('BE40 reported the MB recovery time option')
        return
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
        manifest = manifests.get('baseline' if row['group'] == BE else row['group'], {})
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
            retention='All groups retain one point',
            comparison_direction='first-round groups and same-group repeats' if args.case == BE_FORMAL else 'optimized/baseline')
        if args.write_breakdown or args.case in ALWAYS_BREAKDOWN:
            self.ledger['write_breakdown'] = True
        for row in self.ledger['rows']:
            row.update(source_commit=args.source_commit if row['group'] == 'optimized' else args.baseline_source_commit,
                       driver_source_commit=args.driver_source_commit, harness_commit=args.harness_commit)

    def planned_rows(self, args):
        return matrix(args.case, args.round)

    def driver_paths(self):
        return (('new', self.args.driver), ('old', self.args.baseline_driver))

    def driver_for_row(self, row):
        return self.args.driver if row['group'] == 'optimized' else self.args.baseline_driver

    def command(self, row, mode, front, blob, backup, stats):
        cmd = super().command(row, mode, front, blob, backup, stats) + ['--fixed_tail=0']
        if row['engine'] == 'mb':
            cmd.append('--recovery_time_sampling=' + str(int(row['recovery_time_sampling'])))
        if (self.args.write_breakdown or self.args.case in ALWAYS_BREAKDOWN) and mode == 'write':
            cmd.append('--write_breakdown=1')
        return cmd

    def validate_storage(self):
        super().validate_storage()
        root = Path(self.args.ssd_accounting_root).resolve(strict=True)
        for _, binary in self.driver_paths():
            driver = Path(binary).resolve(strict=True)
            if root not in driver.parents or driver.stat().st_dev != self.ssd.parent.stat().st_dev:
                raise ResourceStop('Drivers and adjacent libraries must be in accounted SSD root')

    def validate_completed_sample(self, row):
        validate_sample(row)
        if self.ledger.get('write_breakdown'):
            breakdown.summarize(row)

    def run(self):
        manifests = {}
        for group, name, commit, capability in (
                ('baseline', 'baseline_build_manifest', self.args.baseline_source_commit, 0),
                ('optimized', 'build_manifest', self.args.source_commit, 1)):
            driver = Path(self.args.baseline_driver if group == 'baseline' else self.args.driver)
            manifests[group] = load_manifest(getattr(self.args, name), driver, commit, capability)
        if manifests['baseline']['driver_source_sha256'] != manifests['optimized']['driver_source_sha256']:
            raise ValueError('both arms must compile the same updated driver source')
        if manifests['baseline']['protocol_sha256'] != manifests['optimized']['protocol_sha256']:
            raise ValueError('protocol differs between arms')
        from local_compare import harness_identity
        validate_source_records(self.ledger['rows'], harness_identity(), manifests)
        self.ledger['build_manifests'] = manifests
        return super().run()


def load_manifest(path, driver, commit, capability):
    manifest = json.loads(Path(path).read_text())
    if (manifest['core_commit'] != commit or manifest['status'] != 'built' or
            manifest['mb_has_recovery_time_sampling'] != capability):
        raise ValueError('build manifest does not match ' + str(driver))
    library = driver.parent / manifest['library_name']
    if (hashlib.sha256(driver.read_bytes()).hexdigest() != manifest['driver_sha256'] or
            hashlib.sha256(library.read_bytes()).hexdigest() != manifest['library_sha256']):
        raise ValueError('driver/library digest mismatch for ' + str(driver))
    return manifest


class FineWalRunner(HddSyncRunner):
    def __init__(self, args):
        super().__init__(args)
        self.ledger['comparison_direction'] = 'MB fine on and off; BE fine on'
        for row in self.ledger['rows']:
            row['source_commit'] = args.source_commit

    def driver_paths(self):
        return (('new', self.args.driver),)

    def driver_for_row(self, row):
        return self.args.driver

    def command(self, row, mode, front, blob, backup, stats):
        cmd = super().command(row, mode, front, blob, backup, stats)
        if mode == 'write':
            cmd.append('--fine_wal=' + str(int(row['fine_wal'])))
        return cmd

    def validate_completed_sample(self, row):
        super().validate_completed_sample(row)
        fine_wal.summarize(row)

    def run(self):
        from local_compare import harness_identity
        manifest = load_manifest(self.args.build_manifest, Path(self.args.driver), self.args.source_commit, 1)
        if manifest.get('fine_wal') is not True:
            raise ValueError('build manifest is missing fine WAL support')
        self.ledger['build_manifests'] = {group: manifest for group in fine_wal.GROUPS}
        validate_source_records(self.ledger['rows'], harness_identity(), self.ledger['build_manifests'])
        return NodeRunner.run(self)


def write_pairs(pairs, rows, with_be=False):
    by_id = {r['id']: r for r in rows}
    for pair in pairs:
        for metric in breakdown.COMPARISON + (breakdown.BE_COMPARISON if with_be else ()):
            old = by_id.get(pair['reference_id'], {}).get(metric)
            new = by_id.get(pair['current_id'], {}).get(metric)
            pair.update({'reference_' + metric: old, 'current_' + metric: new,
                         metric + '_delta': new - old if old is not None and new is not None else None})


def compare_repeat(current, reference, fields, reference_campaign, with_be=False):
    from report import local_pairs
    current_rows = [dict(row, variant='current') for row in current]
    reference_rows = [dict(row, variant='reference') for row in reference]
    pairs = local_pairs(current_rows + reference_rows, 'main', (('current', 'reference'),), fields)
    write_pairs(pairs, current_rows + reference_rows, with_be)
    current_by_config = {r['config_id']: r for r in current}
    reference_by_config = {r['config_id']: r for r in reference}
    for pair in pairs:
        for label, lookup in (('current', current_by_config), ('reference', reference_by_config)):
            row = lookup[pair['config_id']]
            for key in ('group', 'round', 'source_commit', 'harness_commit'):
                pair[label + '_' + key] = row[key]
            pair[label + '_variant'] = row['group']
        pair['reference_campaign'] = reference_campaign
        metrics = [key[:-6] for key in pair if key.endswith(('_ratio', '_delta'))]
        for metric in metrics:
            values = [pair[key] for key in ('current_' + metric, 'reference_' + metric)
                      if pair[key] is not None]
            pair.update({metric + '_min': min(values) if values else None,
                         metric + '_max': max(values) if values else None,
                         metric + '_valid_n': len(values)})
    return pairs


def compare_be_rounds(result, mb_history):
    from report import memory_fields
    if mb_history is None:
        raise ValueError('BE40 comparison requires the previous MB write breakdown ledger')
    history = aggregate([mb_history], write_breakdown=True)
    if any(row['harness_commit'] != MB_HISTORY_ADAPTER or row['driver_source_commit'] != MB_HISTORY_ADAPTER or
           row['source_commit'] != (FINAL if row['group'] == 'optimized' else CORE) for row in history['rows']):
        raise ValueError('unexpected historical MB write breakdown source')
    fields = memory_fields(result)
    rows = result['rows']
    result['be_across_rounds'] = compare_repeat(
        [r for r in rows if r['round'] == 2],
        [r for r in rows if r['round'] == 1 and r['group'] == BE],
        fields, BE_FORMAL, with_be=True)
    result['mb_historical_comparison'] = [
        pair for group in GROUPS for pair in compare_repeat(
            [r for r in rows if r['group'] == group],
            [r for r in history['rows'] if r['group'] == group], fields, 'previous-mb-breakdown')]


def aggregate(documents, write_breakdown=False, case=FORMAL, mb_history=None):
    from report import local_pairs, memory_fields, summarize
    with_be = case == BE_FORMAL
    write_breakdown = write_breakdown or with_be
    rounds, identities = {}, {}
    for document in documents:
        observed = {r['round'] for r in document['rows']}
        if (document.get('optimization_case') != case or len(observed) != 1 or
                bool(document.get('write_breakdown')) != write_breakdown):
            raise ValueError('one formal round per ledger required')
        rnd = next(iter(observed))
        if rnd not in (1, 2) or rnd in rounds:
            raise ValueError('duplicate or invalid round')
        expected = matrix(case, rnd)
        fields = set(expected[0]) - {'status'}
        if len(document['rows']) != len(expected) or any(any(r.get(k) != spec[k] for k in fields)
                for r, spec in zip(document['rows'], expected)):
            raise ValueError('fixed matrix mismatch')
        if document.get('resource_limits') != RESOURCES:
            raise ValueError('resource limits mismatch')
        identity = {(r['group'], r.get('source_commit'), r.get('driver_source_commit'),
                     r.get('harness_commit')) for r in document['rows']}
        groups = {r['group'] for r in expected}
        if len(identity) != len(groups) or any(not all(frozen.full_commit(x) for x in t[1:]) for t in identity):
            raise ValueError('invalid source identities')
        cores = {t[0]: t[1] for t in identity}
        if any(cores[group] != CORE for group in groups - {'optimized'}) or (
                'optimized' in cores and (cores['optimized'] == CORE or (with_be and cores['optimized'] != FINAL))):
            raise ValueError('unexpected core commit')
        validate_source_records(document['rows'], document.get('harness_actual', {}),
                                document.get('build_manifests', {}))
        for group, *commits in identity:
            if identities.setdefault(group, commits) != commits:
                raise ValueError('cross-round identity mismatch')
        for row in document['rows']:
            if row['status'] == 'passed':
                validate_sample(row)
        rounds[rnd] = document
    if write_breakdown and not with_be and set(rounds) != {1}:
        raise ValueError('expected only round 1 for write breakdown')
    if not rounds:
        raise ValueError('at least one ledger required')
    expected_rounds = (1,) if write_breakdown and not with_be else (1, 2)
    combined = dict(next(iter(rounds.values())))
    combined['rows'] = [dict(row) for rnd in expected_rounds for row in
                        (rounds[rnd]['rows'] if rnd in rounds else matrix(case, rnd))]
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
    result['optimization_case'] = case
    result['available_rounds'] = sorted(rounds)
    result['local_comparison'] = []
    result['backup_retention_events'] = []
    for source, row in zip(combined['rows'], result['rows']):
        for key in ('round', 'group', 'recovery_time_sampling', 'source_commit', 'driver_source_commit', 'harness_commit'):
            row[key] = source.get(key)
        row['raw_recovery'] = source.get('recovery_result', {})
        if with_be:
            for key in ('backup_initial_allocated_bytes', 'backup_after_kill_allocated_bytes',
                        'retention_observation'):
                row[key] = source.get(key)
            result['backup_retention_events'].extend(
                dict(event, sample_id=row['id'], round=row['round'], group=row['group'])
                for event in source.get('writer', {}).get('events', [])
                if event.get('event') == 'backup_retention')
        if write_breakdown and row['status'] == 'passed':
            row.update(breakdown.summarize(source))
        if source['group'] == 'optimized':
            for key in row:
                if key.startswith('rpo_'):
                    row[key] = 'disabled_recovery_time_sampling' if key.endswith('_status') else None
    directions = (('optimized', 'baseline'),)
    if with_be:
        directions += (('baseline', BE), ('optimized', BE))
    for rnd in ((1,) if with_be else expected_rounds):
        pairs = local_pairs([r for r in result['rows'] if r['round'] == rnd], 'main',
                            directions, memory_fields(result))
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
        write_pairs(result['local_comparison'], result['rows'], with_be)
    if with_be:
        result['notes'][0] = '48 planned samples / 36 round-1 pairs / 12 BE repeats / 24 historical MB comparisons.'
        result['notes'][1] += ' BE40 uses the core52 library, with WAL backup, sync and one retained point.'
        result['notes'][4] = 'Failures retain raw evidence. Repeats retain both values and observed ranges.'
        result['notes'].extend(breakdown.BE_NOTES)
        compare_be_rounds(result, mb_history)
    return result


def export(paths, output, write_breakdown=False, case=FORMAL, mb_history_ledger=None):
    from mb40_report import write_csv
    from report import md_table, number
    with_be = case == BE_FORMAL
    write_breakdown = write_breakdown or with_be
    history = json.loads(Path(mb_history_ledger).read_text()) if mb_history_ledger else None
    result = aggregate([json.loads(Path(p).read_text()) for p in paths], write_breakdown, case, history)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=False)
    names = ('rows', 'local_comparison', 'memory_samples', 'publication_events', 'rpo_samples')
    if with_be:
        names += ('be_across_rounds', 'mb_historical_comparison', 'backup_retention_events')
    elif not write_breakdown:
        names += ('cross_round_comparison',)
    for name in names:
        write_csv(out / (name + '.csv'), result[name])
    (out / 'summary.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    title = 'Write breakdown: core52 MB40 / three-fix MB40 / BE40' if with_be else 'HDD sync: final / core52'
    lines = ['# ' + title, '', 'Status: ' + result['run_status'], '']
    lines += ['- ' + note for note in result['notes']] + ['']
    if with_be:
        lines += ['## Round 1 throughput', '']
        md_table(lines, ('Config', 'Current / reference', 'Current MiB/s', 'Reference MiB/s', 'Change %'),
            [(r['config_id'], r['current_variant'] + ' / ' + r['reference_variant'],
              number(r['current_mib_per_s']), number(r['reference_mib_per_s']),
              number(100 * r['mib_per_s_change_fraction'] if r['mib_per_s_change_fraction'] is not None else None))
             for r in result['local_comparison']])
        for title, name in (('BE40 round 2 / round 1', 'be_across_rounds'),
                            ('MB40 current / prior instrumented run', 'mb_historical_comparison')):
            lines += ['', '## ' + title, '']
            md_table(lines, ('Config', 'Group', 'Current MiB/s', 'Reference MiB/s', 'Min', 'Max', 'Valid'),
                [(r['config_id'], r['current_group'], number(r['current_mib_per_s']),
                  number(r['reference_mib_per_s']), number(r['mib_per_s_min']),
                  number(r['mib_per_s_max']), r['mib_per_s_valid_n']) for r in result[name]])
        lines += ['', '## Write phases', '',
                  'The row CSV and JSON contain both engine-specific breakdowns, with backup events and all paired values in separate CSVs.', '']
        md_table(lines, ('Sample', 'Round', 'Config', 'Group', 'WAL ns/call', 'MemTable ns/call', 'Flush ms/s',
                        'Compaction ms/s', 'MB mirror WAL append ms/s', 'MB publish ms/s', 'BE create s',
                        'BE purge s', 'Business Blob MiB/s', 'Blob append ms/s', 'Blob sync ms/s'),
            [(r['id'], r['round'], r['config_id'], r['group'], *(number(r.get(key)) for key in (
                'sampled_wal_ns_per_call', 'sampled_memtable_ns_per_call', 'flush_ms_per_s', 'compaction_ms_per_s',
                'mirror_wal_append_ms_per_s', 'checkpoint_publish_ms_per_s', 'be_create_seconds', 'be_purge_seconds',
                'blob_write_mib_per_s', 'blob_append_ms_per_s', 'blob_sync_ms_per_s'))) for r in result['rows']])
    elif write_breakdown:
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
    p.add_argument('--mb-history-ledger')
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    try:
        validate_args(args)
        if args.action == 'plan':
            rnd = 1 if args.write_breakdown and args.case == FORMAL else args.round
            print(json.dumps(dict(case=args.case, rows=matrix(args.case, rnd), resources=RESOURCES,
                                  write_breakdown=args.write_breakdown or args.case in ALWAYS_BREAKDOWN), indent=2))
            return 0
        if args.action == 'aggregate':
            if args.case == FINE_FORMAL:
                result = fine_wal.export(args.round_ledgers[0], args.output, validate_sample,
                                         validate_source_records, write_pairs)
            else:
                result = export(args.round_ledgers, args.output, args.write_breakdown, args.case, args.mb_history_ledger)
            return 0 if result['run_status'] == 'completed' else 1
        runner = FineWalRunner if args.case in FINE_CASES else HddSyncRunner
        return runner(args).run()
    except (ValueError, RuntimeError) as exc:
        p.error(str(exc))


if __name__ == '__main__':
    raise SystemExit(main())
