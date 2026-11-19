# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

"""Paired MB40 comparison for the mirror WAL payload allocation change."""
from fine_wal import foreground_windows, padded_backup_windows
from report import md_table, number, proc_kib, publication_events

FORMAL, PREFLIGHT = 'hdd-sync-wal-payload', 'hdd-sync-wal-payload-preflight'
BASELINE = '1e7dd4903ad790d22869b27ce59df8da432b33ef'
WINDOW_METRICS = ('mib_per_s', 'sampled_api_ns_per_call', 'sampled_wal_ns_per_call',
                  'sampled_memtable_ns_per_call', 'sampled_calls', 'duration_seconds')
PEAK_METRICS = ('mirror_queue_sample_peak_bytes', 'writer_measurement_rss_sample_peak_kib',
                'writer_measurement_rss_anon_sample_peak_kib',
                'writer_wait4_max_rss_kib', 'measurement_cp_completed')


def select_matrix(case, round_number, old_matrix):
    short = case == PREFLIGHT
    if (short and round_number is not None) or (not short and round_number not in (None, 1)):
        raise ValueError('WAL payload preflight has no round and formal uses only round 1')
    templates = old_matrix('hdd-sync-preflight' if short else 'hdd-sync', None if short else 1)
    selected = []
    for row in templates:
        if row['value_size'] != 64 or row['blob'] or (not short and row['batch_size'] not in (1, 8)):
            continue
        selected.append(dict(row, id='%s-r%d-%03d' % (FORMAL, row['round'], len(selected) + 1),
                             batch_size=8 if short else row['batch_size'],
                             config_id='value64-bloboff-batch8' if short else row['config_id'],
                             recovery_time_sampling=False))
    return selected


def validate_manifests(manifests):
    arms = [manifests.get(group, {}) for group in ('baseline', 'optimized')]
    if any(manifest.get('fine_wal') is not False or manifest.get('mb_has_recovery_time_sampling') != 1 or
           manifest.get('assert_status_checked') is not True or manifest.get('diagnostics_enabled') is not False or
           manifest.get('status') != 'built' for manifest in arms):
        raise ValueError('WAL payload requires two strict ordinary builds with fine WAL disabled')
    for key in ('driver_source_sha256', 'protocol_sha256'):
        if not arms[0].get(key) or arms[0][key] != arms[1].get(key):
            raise ValueError('WAL payload requires the same driver source and protocol')


def foreground(row):
    windows, intervals = foreground_windows(row)
    # Coarse metrics and ACK counters share the snapshot lock.
    full = windows[0]
    if not 0 < full['sampled_calls'] <= full['measure_calls']:
        raise ValueError('invalid WAL payload foreground samples')
    return windows, intervals


def peak_metrics(row):
    stats = row['final_stats']
    begin, end = stats['measure_begin_ns'], stats['measure_end_ns']
    writer = row['writer']
    padded = padded_backup_windows(row, begin, end)
    queues, queues_filtered = [], []
    for event in writer['events']:
        coarse = event.get('write_breakdown')
        queued = event.get('queued_bytes')
        if event.get('event') != 'sample' or coarse is None or queued is None:
            continue
        left, right = coarse['background_begin_ns'], coarse['background_end_ns']
        if begin <= left <= right < end:
            queues.append(queued)
            if not any(left < stop and right > start for start, stop in padded):
                queues_filtered.append(queued)
    status = [s.get('status', {}) for s in writer.get('telemetry', ())
              if begin <= (s.get('monotonic_ns') or 0) < end]
    rss, rss_anon = ([value for sample in status if (value := proc_kib(sample.get(field))) is not None]
                     for field in ('VmRSS', 'RssAnon'))
    return dict(mirror_queue_sample_peak_bytes=max(queues, default=None),
                mirror_queue_sample_count=len(queues),
                mirror_queue_outside_backup_sample_peak_bytes=max(queues_filtered, default=None),
                mirror_queue_outside_backup_sample_count=len(queues_filtered),
                writer_measurement_rss_sample_peak_kib=max(rss, default=None),
                writer_measurement_rss_sample_count=len(rss),
                writer_measurement_rss_anon_sample_peak_kib=max(rss_anon, default=None),
                writer_measurement_rss_anon_sample_count=len(rss_anon),
                measurement_cp_completed=sum(begin <= e['end_ns'] < end for e in publication_events(row)))


def add_results(result, windows, intervals):
    result.update(payload_windows=windows, payload_intervals=intervals, payload_window_pairs=[])
    lookup = {(r['config_id'], r['group'], r['window']): r for r in windows}
    rows = {r['id']: r for r in result['rows']}
    for pair in result['local_comparison']:
        for metric in PEAK_METRICS + ('mirror_queue_outside_backup_sample_peak_bytes',):
            old = rows.get(pair['reference_id'], {}).get(metric) if pair['reference_status'] == 'passed' else None
            new = rows.get(pair['current_id'], {}).get(metric) if pair['current_status'] == 'passed' else None
            pair.update({'reference_' + metric: old, 'current_' + metric: new,
                         metric + '_delta': new - old if new is not None and old is not None else None})
        for kind in ('full', 'outside_observed_backups'):
            config = pair['config_id']
            base, opt = (lookup.get((config, group, kind), {}) for group in ('baseline', 'optimized'))
            window_pair = dict(config_id=config, window=kind, reference_id=pair['reference_id'],
                               current_id=pair['current_id'], reference_status=pair['reference_status'],
                               current_status=pair['current_status'])
            for key in WINDOW_METRICS:
                old, new = base.get(key), opt.get(key)
                window_pair.update({'reference_' + key: old, 'current_' + key: new,
                                    key + '_delta': new - old if new is not None and old is not None else None,
                                    key + '_change_fraction': new / old - 1 if new is not None and old else None})
            result['payload_window_pairs'].append(window_pair)
    result['notes'] = [
        'Four ordinary-build MB40 observations: one baseline and one optimized run per 64 B Blob-off workload.',
        'Both arms disable recovery-time sampling and fine WAL probes.',
        'Full throughput uses acknowledged counters through the 180-second measurement deadline. '
        'Coarse API and WAL timing use samples through the last exported foreground snapshot.',
        'The filtered table excludes complete sampled intervals overlapping observed successful checkpoints, '
        'with two seconds of padding on each end. Unobserved or unfinished checkpoints can remain.',
        'Mirror queue peaks measure logical charges across WAL and metadata, including in-flight '
        'reservations. Queue, total RSS and anonymous RSS peaks are maxima of observed samples. '
        'The writer wait4 RSS is the lifetime high-water mark. Checkpoint counts use observed completion events. '
        'Recovered sequences and checked samples reflect the same SSD-loss and recovery protocol.',
        'Each configuration has one pair. Failed observations retain raw evidence and are excluded from comparisons.']


def markdown(result):
    lines = ['# MB40 WAL payload A/B', '', 'Run status: ' + result['run_status'], '']
    lines += ['- ' + text for text in result['notes']] + ['']
    md_table(lines, ('Config', 'Window', 'Base seconds', 'Optimized seconds', 'Base MiB/s',
                     'Optimized MiB/s', 'Throughput change %', 'Base WAL ns/call', 'Optimized WAL ns/call',
                     'WAL change ns/call', 'Base API ns/call', 'Optimized API ns/call'),
             [(r['config_id'], r['window'], number(r['reference_duration_seconds']),
               number(r['current_duration_seconds']), number(r['reference_mib_per_s']), number(r['current_mib_per_s']),
               number(100 * r['mib_per_s_change_fraction'] if r['mib_per_s_change_fraction'] is not None else None),
               number(r['reference_sampled_wal_ns_per_call']), number(r['current_sampled_wal_ns_per_call']),
               number(r['sampled_wal_ns_per_call_delta']), number(r['reference_sampled_api_ns_per_call']),
               number(r['current_sampled_api_ns_per_call'])) for r in result['payload_window_pairs']])
    lines += ['', '## Queue, memory and recovery', '']
    md_table(lines, ('Config', 'Group', 'Status', 'Mirror queue sampled peak MiB', 'Writer sampled RSS peak MiB',
                     'Writer sampled anonymous RSS peak MiB',
                     'Writer wait4 RSS MiB', 'Observed CPs in window', 'Recovered sequence',
                     'Checked keys', 'Recovery seconds'),
             [(r['config_id'], r['group'], r['status'],
               scaled(r, 'mirror_queue_sample_peak_bytes', 2**20), scaled(r, 'writer_measurement_rss_sample_peak_kib', 2**10),
               scaled(r, 'writer_measurement_rss_anon_sample_peak_kib', 2**10),
               scaled(r, 'writer_wait4_max_rss_kib', 2**10), r.get('measurement_cp_completed'), r.get('recovered_sequence'),
               r.get('sampled_keys'), number(r.get('rto_seconds'))) for r in result['rows']])
    return '\n'.join(lines) + '\n'


def scaled(row, key, divisor):
    value = row.get(key)
    return number(value / divisor if value is not None else None)
