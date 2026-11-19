# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

"""One-core MB40/BE40 foreground WAL timing."""
from collections import Counter
import json
from math import isclose
from pathlib import Path

import optcore_compare as frozen
import write_breakdown
from report import local_pairs, md_table, memory_fields, number, publication_events
from report import summarize as summarize_run

FORMAL, PREFLIGHT = 'hdd-sync-fine-wal', 'hdd-sync-fine-wal-preflight'
MB_OFF, MB_ON, BE_ON = GROUPS = ('mb40-fine-off', 'mb40-fine-on', 'be40-fine-on')
STAGES = ('operations_wait', 'event_build', 'reserve_wait', 'reserve_bookkeeping',
          'capacity_wait', 'payload_copy', 'primary_append', 'finish_wait',
          'finish_bookkeeping', 'batch_validation', 'error_check', 'write_fence_wait',
          'completed_wait', 'completed_bookkeeping')
MATCHED_FIELDS = ('calls', 'records', 'api_ns', 'wal_ns', 'memtable_ns')
FINE_FIELDS = MATCHED_FIELDS + ('primary_append_bytes',)
RUN_FIELDS = ('measure_calls', 'measure_records', 'measure_bytes')
COARSE_FIELDS = write_breakdown.FOREGROUND
BACKUP_PADDING_NS = 2 * 10**9
MIB = 2**20
DIRECTIONS = ((MB_ON, MB_OFF), (MB_OFF, BE_ON), (MB_ON, BE_ON))


def matrix(case, round_number):
    short = case == PREFLIGHT
    if (short and round_number is not None) or (not short and round_number not in (None, 1)):
        raise ValueError('fine WAL preflight has no round and formal uses only round 1')
    rnd = 0 if short else 1
    templates = frozen.matrix(frozen.CASES[1] if short else frozen.CASES[0], None if short else 1)
    rows = []
    for index, template in enumerate(row for row in templates
                                     if row['engine'] == 'mb' and row['value_size'] == 64 and not row['blob']):
        offset = index % len(GROUPS)
        for group in GROUPS[offset:] + GROUPS[:offset]:
            mb = group != BE_ON
            rows.append(dict(template, id='%s-r%d-%03d' % (FORMAL, rnd, len(rows) + 1),
                             group=group, variant=group, engine='mb' if mb else 'be',
                             batch_size=8 if short else template['batch_size'],
                             config_id='value64-bloboff-batch8' if short else template['config_id'],
                             mirror_interval_ms=5000 if mb else None,
                             mirror_batch_bytes=536870912 if mb else None,
                             mirror_queue_capacity=1073741824 if mb else None,
                             recovery_time_sampling=False if mb else None, fine_wal=group != MB_OFF))
    return rows


def coarse_snapshot(event, row, stamp):
    coarse = event['write_breakdown']
    return dict(time_ns=stamp, **{k: event[k] for k in RUN_FIELDS}, **{k: coarse[k] for k in COARSE_FIELDS})


def snapshot(event, row, stamp):
    coarse = event['write_breakdown']
    data = coarse_snapshot(event, row, stamp)
    if not row['fine_wal']:
        if event.get('fine_wal') is not None:
            raise ValueError('fine WAL timing present with probe disabled')
        counters = event.get('fine_wal_reference')
        prefix, fields = 'reference_', MATCHED_FIELDS
    else:
        counters = event.get('fine_wal')
        prefix, fields = 'fine_', FINE_FIELDS
    if not isinstance(counters, dict):
        raise ValueError('fine WAL or matched reference counters are missing')
    for key in fields:
        data[prefix + key] = counters[key]
    if (not 0 <= counters['calls'] <= coarse['sampled_calls'] <= event['measure_calls'] or
            counters['records'] != counters['calls'] * row['batch_size']):
        raise ValueError('fine WAL counters are outside the coarse sample')
    if not row['fine_wal']:
        return data
    for stage in STAGES if row['engine'] == 'mb' else ('primary_append',):
        values = counters['stages'].get(stage, {})
        for unit in ('calls', 'ns'):
            data[stage + '_' + unit] = values.get(unit, 0)
    if row['engine'] == 'be' and any(
            fields.get('calls', 0) or fields.get('ns', 0)
            for stage, fields in counters['stages'].items() if stage != 'primary_append'):
        raise ValueError('BE included stages outside primary WAL append')
    return data


def padded_backup_windows(row, begin, end):
    return [(e['begin_ns'] - BACKUP_PADDING_NS, e['end_ns'] + BACKUP_PADDING_NS)
            for e in publication_events(row)
            if e['begin_ns'] and e['begin_ns'] <= e['end_ns'] and
            e['end_ns'] + BACKUP_PADDING_NS > begin and e['begin_ns'] - BACKUP_PADDING_NS < end]


def window_row(row, kind, intervals, fields, clock_p95, duration_ns=None, total=None):
    if duration_ns is None:
        duration_ns = sum(s['duration_ns'] for s in intervals)
    if total is None:
        total = {k: sum(s[k] for s in intervals) if intervals else None for k in fields}
    info = dict(sample_id=row['id'], config_id=row['config_id'], group=row['group'], batch_size=row['batch_size'],
                window=kind, duration_seconds=duration_ns / 1e9, intervals=len(intervals),
                clock_p95_ns=clock_p95, **total)
    info['mib_per_s'] = total['measure_bytes'] / MIB / info['duration_seconds'] if duration_ns else None
    prefixes = ('sampled_', 'fine_', 'reference_') if 'fine_wal' in row else ('sampled_',)
    for prefix in prefixes:
        calls = total.get(prefix + 'calls')
        for metric in ('api_ns', 'wal_ns', 'memtable_ns'):
            key = prefix + metric
            info[key + '_per_call'] = total[key] / calls if calls else None
    if 'fine_wal' in row:
        matched = 'reference_' if row['group'] == MB_OFF else 'fine_'
        info['matched_calls'] = total.get(matched + 'calls')
        for metric in ('api', 'wal', 'memtable'):
            info['matched_' + metric + '_ns_per_call'] = info[matched + metric + '_ns_per_call']
    return info


def foreground_windows(row, collect=coarse_snapshot, clock_p95=None):
    stats = row['final_stats']
    begin, end = stats['measure_begin_ns'], stats['measure_end_ns']
    windows = padded_backup_windows(row, begin, end)
    samples = []
    for event in row['writer']['events']:
        if event.get('event') != 'sample' or 'write_breakdown' not in event:
            continue
        time_ns = event['write_breakdown']['foreground_snapshot_ns']
        if begin <= time_ns < end:
            samples.append(collect(event, row, time_ns))
    if not samples:
        raise ValueError('no in-window WAL snapshots')

    coverage_end = samples[-1]['time_ns']
    fields = tuple(k for k in samples[0] if k != 'time_ns')
    previous = dict.fromkeys(fields, 0)
    previous['time_ns'] = begin
    intervals = []
    for sample in samples:
        stamp = sample['time_ns']
        prev = previous['time_ns']
        delta = {k: sample[k] - previous[k] for k in fields}
        if stamp == begin == prev and not any(delta.values()):
            continue
        if stamp <= prev or any(value < 0 for value in delta.values()):
            raise ValueError('WAL snapshots are non-monotonic')
        intervals.append(dict(sample_id=row['id'], config_id=row['config_id'], group=row['group'],
                              begin_ns=prev, end_ns=stamp, duration_ns=stamp - prev,
                              near_observed_backup=any(prev < right and stamp > left for left, right in windows),
                              **delta))
        previous = sample

    totals = {k: samples[-1][k] for k in fields}
    totals.update({k: stats[k] for k in RUN_FIELDS})
    full = window_row(row, 'full', intervals, fields, clock_p95, end - begin, totals)
    full['sample_coverage_seconds'] = (coverage_end - begin) / 1e9
    without_backup = [s for s in intervals if not s['near_observed_backup']]
    filtered = window_row(row, 'outside_observed_backups', without_backup, fields, clock_p95)
    return (full, filtered), intervals


def summarize(row):
    ready = next(e for e in row['writer']['events'] if e.get('event') == 'ready')
    if ready.get('fine_wal') is not row['fine_wal']:
        raise ValueError('effective fine WAL option mismatch')
    clock_p95 = ready.get('fine_wal_clock_p95_ns')
    if row['fine_wal'] and (type(clock_p95) is not int or clock_p95 <= 0):
        raise ValueError('fine WAL clock baseline is missing')
    windows, intervals = foreground_windows(row, snapshot, clock_p95 if row['fine_wal'] else None)
    full = windows[0]
    if row['fine_wal'] and any(full.get(key, 0) <= 0 for key in
            ('fine_calls', 'fine_api_ns', 'fine_wal_ns', 'fine_memtable_ns', 'primary_append_calls',
             'primary_append_ns', 'fine_primary_append_bytes')):
        raise ValueError('fine primary WAL Appends or foreground timing were not observed')
    if not row['fine_wal'] and any(full.get('reference_' + key, 0) <= 0 for key in MATCHED_FIELDS):
        raise ValueError('matched fine WAL reference samples were not observed')
    needed_stages = ('event_build', 'reserve_bookkeeping', 'payload_copy', 'finish_bookkeeping',
                     'error_check', 'completed_bookkeeping')
    if row['batch_size'] > 1:
        needed_stages += ('batch_validation',)
    if row['fine_wal'] and row['engine'] == 'mb' and any(full[key + '_calls'] <= 0 for key in
                                                         needed_stages):
        raise ValueError('fine MB admission stages were not observed')

    return windows, intervals


def stage_rows(windows):
    stages = []
    for window in windows:
        if window['group'] == MB_OFF:
            continue
        for stage in STAGES if window['group'] == MB_ON else ('primary_append',):
            calls, ns = window[stage + '_calls'], window[stage + '_ns']
            mean = ns / calls if calls else None
            if stage == 'batch_validation' and window['batch_size'] == 1:
                resolution = 'not_applicable'
            elif not window['fine_calls']:
                resolution = 'no_samples'
            elif not calls:
                resolution = 'zero_occurrences'
            elif mean <= window['clock_p95_ns']:
                resolution = 'at_or_below_clock_p95'
            else:
                resolution = 'above_clock_p95'
            stages.append(dict(sample_id=window['sample_id'], config_id=window['config_id'],
                               group=window['group'], window=window['window'], stage=stage,
                               calls=calls, ns=ns, sample_calls=window['fine_calls'],
                               sample_records=window['fine_records'], clock_p95_ns=window['clock_p95_ns'],
                               ns_per_occurrence=mean, resolution_status=resolution,
                               ns_per_call=ns / window['fine_calls'] if window['fine_calls'] else None,
                               ns_per_record=ns / window['fine_records'] if window['fine_records'] else None,
                               append_bytes=window['fine_primary_append_bytes'] if stage == 'primary_append' else None))
    return stages


def disturbance(rows, configs):
    by_group = {(r['config_id'], r['group']): r for r in rows}
    results = []
    for config in configs:
        off, on, be = [by_group.get((config, group), {}) for group in GROUPS]
        throughput = None
        api_delta = None
        api_gap = None
        if off.get('mib_per_s') and on.get('mib_per_s') is not None:
            throughput = on['mib_per_s'] / off['mib_per_s'] - 1
        if off.get('sampled_api_ns_per_call') is not None:
            if on.get('sampled_api_ns_per_call') is not None:
                api_delta = on['sampled_api_ns_per_call'] - off['sampled_api_ns_per_call']
            if be.get('sampled_api_ns_per_call') is not None:
                api_gap = off['sampled_api_ns_per_call'] - be['sampled_api_ns_per_call']
        limit = abs(api_gap) / 4 if api_gap is not None else None
        throughput_trigger = (throughput is not None and abs(throughput) > 0.03 and
                              not isclose(abs(throughput), 0.03))
        api_trigger = api_delta is not None and limit is not None and (
            abs(api_delta) >= limit or isclose(abs(api_delta), limit))
        disturbed = throughput_trigger or api_trigger
        status = ('disturbed' if disturbed else 'unknown' if None in (throughput, api_delta, api_gap)
                  else 'within_threshold')
        results.append(dict(config_id=config, status=status, throughput_change_fraction=throughput,
                            coarse_api_change_ns=api_delta, coarse_mb_off_minus_be_ns=api_gap,
                            api_change_limit_ns=limit,
                            stage_attribution='eligible' if status == 'within_threshold' else 'deferred'))
    return results


def window_pairs(windows, configs):
    by_group = {(r['config_id'], r['window'], r['group']): r for r in windows}
    comparisons = []
    for config in configs:
        for kind in ('full', 'outside_observed_backups'):
            for current, reference in DIRECTIONS:
                pair = dict(config_id=config, window=kind, current_group=current, reference_group=reference)
                wr, rr = (by_group.get((config, kind, group), {}) for group in (current, reference))
                for metric in ('mib_per_s', 'sampled_api_ns_per_call', 'sampled_wal_ns_per_call',
                               'sampled_memtable_ns_per_call', 'matched_api_ns_per_call',
                               'matched_wal_ns_per_call', 'matched_memtable_ns_per_call',
                               'duration_seconds', 'matched_calls'):
                    new, old = wr.get(metric), rr.get(metric)
                    pair.update({'current_' + metric: new, 'reference_' + metric: old,
                                 metric + '_delta': new - old if new is not None and old is not None else None})
                comparisons.append(pair)
    return comparisons


def qualify_windows(result, sources):
    intervals, windows, retention_events = [], [], []
    for source, row in zip(sources, result['rows']):
        for key in ('source_commit', 'driver_source_commit', 'harness_commit', 'fine_wal',
                    'recovery_time_sampling', 'retention_observation', 'backup_initial_allocated_bytes',
                    'backup_after_kill_allocated_bytes'):
            row[key] = source.get(key)
        row['raw_recovery'] = source.get('recovery_result', {})
        retention_events.extend(
            dict(event, sample_id=row['id'], group=row['group'])
            for event in source.get('writer', {}).get('events', []) if event.get('event') == 'backup_retention')
        if row['status'] != 'passed':
            continue
        if row['engine'] == 'mb':
            for key in row:
                if key.startswith('rpo_'):
                    row[key] = 'disabled_recovery_time_sampling' if key.endswith('_status') else None
        row.update(write_breakdown.summarize(source))
        report_windows, seconds = summarize(source)
        windows.extend(report_windows)
        intervals.extend(seconds)
    result.update(fine_wal_intervals=intervals, fine_wal_windows=windows, fine_wal_stages=stage_rows(windows),
                  backup_retention_events=retention_events)


def aggregate(document, validate_sample, validate_source_records, write_pairs):
    expected = matrix(FORMAL, 1)
    sources = document['rows']
    if (document.get('optimization_case') != FORMAL or not document.get('write_breakdown') or
            len(expected) != len(sources) or document.get('resource_limits') != frozen.RESOURCES):
        raise ValueError('fine WAL formal ledger or resource limits mismatch')
    if any(any(row.get(k) != spec[k] for k in spec if k != 'status')
           for row, spec in zip(sources, expected)):
        raise ValueError('fine WAL formal matrix mismatch')
    manifests = document.get('build_manifests', {})
    manifest = manifests.get(MB_OFF, {})
    if (manifest.get('fine_wal') is not True or
            not all(frozen.full_commit(manifest.get(key)) for key in ('core_commit', 'adapter_commit')) or
            not frozen.full_commit(document.get('harness_actual', {}).get('commit')) or
            any(manifests.get(group) != manifest for group in GROUPS)):
        raise ValueError('fine WAL groups need identical driver/core manifests')
    validate_source_records(sources, document.get('harness_actual', {}), manifests)
    for row in sources:
        if row['status'] == 'passed':
            validate_sample(row)

    result = summarize_run(document)
    result.update(optimization_case=FORMAL, counts=dict(Counter(row['status'] for row in sources)))
    qualify_windows(result, sources)
    rows = result['rows']
    result['local_comparison'] = local_pairs(rows, 'main', DIRECTIONS, memory_fields(result))
    write_pairs(result['local_comparison'], rows, with_be=True)
    config_ids = list(dict.fromkeys(row['config_id'] for row in rows))
    result['fine_wal_disturbance'] = disturbance(rows, config_ids)
    result['fine_wal_window_pairs'] = window_pairs(result['fine_wal_windows'], config_ids)
    assessment = {r['config_id']: r['stage_attribution'] for r in result['fine_wal_disturbance']}
    for phase in result['fine_wal_stages']:
        phase['stage_attribution'] = assessment[phase['config_id']]
    if not all(row['status'] == 'passed' for row in rows):
        result['run_status'] = 'incomplete'
    return result


def markdown(result):
    lines = ['# Foreground WAL timing for MB40 and BE40', '', 'Run status: ' + result['run_status'], '',
             'The full throughput window uses final acknowledged totals. Foreground durations and stage metrics '
             'end at the last exported sample. Stage occurrence counts differ from sampled API call counts. '
             'Stage ns/call and ns/record divide by the sampled API calls and records, respectively.', '',
             'The filtered window keeps complete intervals that do not overlap observed successful backup '
             'intervals padded by two seconds at each end. Unobserved or unfinished backup intervals can remain. '
             'Neither the filtered rate nor the stage measurements are adjusted for clock cost.', '',
             'Disturbance uses coarse API means from both MB groups and BE. A throughput change above 3% in either '
             'direction or an API mean change at least one quarter of the absolute MB-off/BE gap defers stage attribution.', '',
             '## Disturbance check', '']
    md_table(lines, ('Config', 'Status', 'On/off throughput change %', 'Coarse on minus off ns/call',
                     'Coarse off minus BE ns/call', 'Absolute API change limit ns'),
             [(r['config_id'], r['status'], number(r['throughput_change_fraction'] * 100
                         if r['throughput_change_fraction'] is not None else None),
               number(r['coarse_api_change_ns']), number(r['coarse_mb_off_minus_be_ns']),
               number(r['api_change_limit_ns'])) for r in result['fine_wal_disturbance']])
    lines += ['', '## Throughput and timing coverage', '']
    md_table(lines, ('Config', 'Group', 'Window', 'Seconds', 'MiB/s', 'Coarse API ns/call',
                     'Coarse WAL ns/call', 'Fine calls', 'Fine WAL ns/call', 'Clock p95 ns'),
             [(r['config_id'], r['group'], r['window'], number(r['duration_seconds']),
               number(r['mib_per_s']), number(r['sampled_api_ns_per_call']),
               number(r['sampled_wal_ns_per_call']), r.get('fine_calls'),
               number(r['fine_wal_ns_per_call']), r['clock_p95_ns'])
              for r in result['fine_wal_windows']])
    lines += ['', '## Matched MB subset', '',
              'The MB-off reference uses the same approximately 1/1024 sampling rule as fine-on. '
              'It records native timings without stage timers. The disturbance thresholds above use the broader coarse sample.', '']
    md_table(lines, ('Config', 'Window', 'On matched calls', 'Off matched calls', 'API on minus off ns/call',
                     'WAL on minus off ns/call'),
             [(r['config_id'], r['window'], r['current_matched_calls'], r['reference_matched_calls'],
               number(r['matched_api_ns_per_call_delta']), number(r['matched_wal_ns_per_call_delta']))
              for r in result['fine_wal_window_pairs'] if (r['current_group'], r['reference_group']) == (MB_ON, MB_OFF)])
    lines += ['', '## Sampled stages', '',
              'Fine-on rows retain unadjusted measurements even when timing is disturbed. Stage timings '
              'can overlap with native stopwatch intervals. Timings at or below the p95 empty-clock interval '
              'remain unresolved.', '']
    md_table(lines, ('Config', 'Group', 'Window', 'Stage', 'Occurrences', 'ns/call', 'ns/record', 'Append bytes',
                     'Resolution', 'Stage attribution'),
             [(r['config_id'], r['group'], r['window'], r['stage'], r['calls'], number(r['ns_per_call']),
               number(r['ns_per_record']), r['append_bytes'], r['resolution_status'], r['stage_attribution'])
              for r in result['fine_wal_stages']])
    return '\n'.join(lines) + '\n'


def export(path, output, validate_sample, validate_source_records, write_pairs):
    from mb40_report import write_csv
    result = aggregate(json.loads(Path(path).read_text()), validate_sample, validate_source_records, write_pairs)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=False)
    for key in ('rows', 'local_comparison', 'memory_samples', 'publication_events', 'rpo_samples',
                'backup_retention_events', 'fine_wal_windows', 'fine_wal_intervals',
                'fine_wal_stages', 'fine_wal_disturbance', 'fine_wal_window_pairs'):
        write_csv(out / (key + '.csv'), result[key])
    (out / 'summary.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    (out / 'report.md').write_text(markdown(result))
    return result
