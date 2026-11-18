# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

"""Summarize writer samples without combining foreground and background time."""

FOREGROUND = ('sampled_calls', 'sampled_records', 'sampled_api_ns',
              'sampled_wal_ns', 'sampled_memtable_ns')
BACKGROUND = ('flush_count', 'flush_us', 'flush_write_bytes',
              'compaction_count', 'compaction_us', 'compaction_read_bytes', 'compaction_write_bytes',
              'blob_append_count', 'blob_append_us', 'blob_sync_count', 'blob_sync_us', 'blob_write_bytes',
              'wal_write_bytes', 'stall_us', 'backpressure_us',
              'mirror_wal_append_calls', 'mirror_wal_append_bytes', 'mirror_wal_append_ns',
              'hdd_append_calls', 'hdd_append_us', 'hdd_sync_calls', 'hdd_sync_us',
              'hdd_dirsync_calls', 'hdd_dirsync_us', 'blob_gc_us')
MB = ('mirrored_wal_bytes', 'candidate_copied_bytes', 'backup_count',
      'checkpoint_capture_ns', 'checkpoint_publish_ns', 'checkpoint_persist_ns')
COMPARISON = ('sampled_api_ns_per_call', 'sampled_wal_ns_per_call', 'sampled_memtable_ns_per_call',
              'wal_write_mib_per_s', 'flush_count', 'flush_ms_per_s', 'flush_write_mib_per_s',
              'compaction_count', 'compaction_ms_per_s', 'compaction_read_mib_per_s',
              'compaction_write_mib_per_s', 'mirrored_wal_mib_per_s',
              'mirror_wal_append_calls', 'mirror_wal_append_mib_per_s', 'mirror_wal_append_ms_per_s',
              'backpressure_ms_per_s', 'breakdown_backup_count', 'candidate_copied_bytes',
              'checkpoint_capture_ms_per_s', 'checkpoint_publish_ms_per_s', 'checkpoint_persist_ms_per_s',
              'blob_write_mib_per_s', 'blob_append_ms_per_s', 'blob_sync_ms_per_s',
              'hdd_sync_ms_per_s', 'hdd_dirsync_ms_per_s')
NOTES = (
    'Foreground WAL and MemTable means use the same approximately 1/64 API sample. '
    'Calls enter the counters by successful return time in the measurement window, through the last exported snapshot.',
    'Native foreground WAL timing includes synchronous mirror queue admission, including any queue wait. '
    'Mirror backpressure is also available as a separate cumulative counter.',
    'Background counters use two snapshots whose read intervals fit inside the measurement window. '
    'Completed jobs can include work before the first snapshot. The unexported tail and unfinished jobs are outside these counters.',
    'Background rates are cumulative task or I/O milliseconds per elapsed second. Tasks can overlap one another and the writer.',
    'Redundancy conversion covers WAL mirroring and checkpoints. The mirror probe measures Append calls on active HDD WAL files. '
    'The time includes buffered writes and counts no checkpoint copies or durability waits. '
    'Native mirrored WAL bytes and mirror successful Append bytes are recorded at different points.',
    'Checkpoint capture times native file discovery. Publish includes Persist. Publication events show the observed completed builds. '
    'Aggregate HDD call times cover all observed HDD paths.',
    'Data offload covers ordinary Blob value placement on HDD. With Blob enabled, native Blob append/sync time and bytes describe that placement '
    'inside flush and compaction. Native flush/compaction write bytes can include Blob bytes.',
)


def summarize(row):
    final = row.get('final_stats', {})
    begin, end = final.get('measure_begin_ns'), final.get('measure_end_ns')
    events = row.get('writer', {}).get('events', ())
    ready = next((e for e in events if e['event'] == 'ready'), {})
    if ready.get('write_breakdown') is not True or not begin or not end:
        raise ValueError('missing write breakdown configuration or measurement window')
    samples = [(e, e['write_breakdown']) for e in events
               if e['event'] == 'sample' and 'write_breakdown' in e]
    usable = [(e, s) for e, s in samples
              if begin <= s['background_begin_ns'] <= s['background_end_ns'] < end]
    fgs = [(e, s) for e, s in samples if begin <= s['foreground_snapshot_ns'] < end]
    if len(usable) < 2 or not fgs:
        raise ValueError('insufficient write breakdown snapshots inside measurement window')
    first_event, first = usable[0]
    last_event, last = usable[-1]
    fg_event, fg = fgs[-1]
    span = (last['background_end_ns'] - first['background_end_ns']) / 1e9
    calls = fg['sampled_calls']
    if (span <= 0 or not 0 < calls <= fg_event['measure_calls'] <= final['measure_calls'] or
            min(fg['sampled_wal_ns'], fg['sampled_memtable_ns']) <= 0 or
            fg['sampled_records'] != row['batch_size'] * calls):
        raise ValueError('invalid write breakdown sampling counters')

    delta = {k: last[k] - first[k] for k in BACKGROUND}
    delta.update({k: last_event[k] - first_event[k] for k in MB})
    if any(v < 0 for v in delta.values()):
        raise ValueError('decreasing cumulative write breakdown counter')
    if row['purpose'] == 'short_recovery_check':
        if min(delta['mirror_wal_append_calls'], delta['mirror_wal_append_bytes'],
               delta['mirror_wal_append_ns']) <= 0:
            raise ValueError('short check did not observe a mirror WAL append')
        if row['blob'] and last['blob_write_bytes'] <= 0:
            raise ValueError('short check did not observe native Blob bytes')
    delta['breakdown_backup_count'] = delta.pop('backup_count')
    result = {k: fg[k] for k in FOREGROUND}
    result.update(delta)
    result.update(data_offload_status='measured' if row['blob'] else 'not_applicable',
                  foreground_ack_calls_observed=fg_event['measure_calls'],
                  foreground_snapshot_ns=fg['foreground_snapshot_ns'],
                  foreground_unexported_tail_seconds=(end - fg['foreground_snapshot_ns']) / 1e9,
                  background_first_begin_ns=first['background_begin_ns'],
                  background_first_end_ns=first['background_end_ns'],
                  background_last_begin_ns=last['background_begin_ns'],
                  background_last_end_ns=last['background_end_ns'],
                  background_observed_seconds=span,
                  background_unexported_tail_seconds=(end - last['background_end_ns']) / 1e9)
    for part in ('api', 'wal', 'memtable'):
        result['sampled_' + part + '_ns_per_call'] = fg['sampled_' + part + '_ns'] / calls
    for stage in ('flush', 'compaction', 'blob_append', 'blob_sync', 'hdd_sync',
                  'hdd_dirsync', 'blob_gc', 'stall', 'backpressure'):
        result[stage + '_ms_per_s'] = delta[stage + '_us'] / 1000 / span
    for stage in ('checkpoint_capture', 'checkpoint_publish', 'checkpoint_persist', 'mirror_wal_append'):
        result[stage + '_ms_per_s'] = delta[stage + '_ns'] / 1e6 / span
    for stage in ('wal_write', 'flush_write', 'compaction_read', 'compaction_write',
                  'mirrored_wal', 'mirror_wal_append', 'blob_write'):
        result[stage + '_mib_per_s'] = delta[stage + '_bytes'] / 2**20 / span
    if not row['blob']:
        for key in ('blob_append_count', 'blob_append_us', 'blob_sync_count', 'blob_sync_us',
                    'blob_write_bytes', 'blob_append_ms_per_s', 'blob_sync_ms_per_s', 'blob_write_mib_per_s'):
            result[key] = None
    return result
