# Foreground-time investigation

Linux/glibc-only diagnostic artifacts for `../../foreground-profile.md`.
No production source is modified. The notification patch is an experimental
control, not a validated production fix.

The driver performs the same 200,000 sequential decimal-key, 1 KiB value writes
as the component benchmark and samples `getrusage(RUSAGE_THREAD)` only around
the foreground loop. The preload library times foreground mutex acquisition,
condition-variable broadcast, and file write/read calls by return address.
The driver explicitly enables/disables hooks around that loop. Timers perturb
execution; compare separate uninstrumented trials for throughput claims.
`pthread_cond_broadcast` must resolve GLIBC_2.3.2 with `dlvsym`: unversioned
resolution on this host selected the incompatible legacy implementation.

## Recorded data

- `reproduce.json`: five alternating trials with the original release binaries.
- `results.json`: first driver series (five ext4, three instrumented, three
  tmpfs trials per variant). The three baseline rows labeled `hooks` had no
  active instrumentation because that driver was linked before the phase
  hook was added. **Exclude them from hook comparisons.** Plain and tmpfs
  measurements remain valid. Use `confirmation.json` for complete hook data.
- `confirmation.json`: corrected baseline driver, current driver, and combined
  notification control; five plain and three instrumented trials each.
  First plain trial per variant deletes the entire primary index and then
  restores/verifies all 200,000 values with the unchanged production reader.
- `metadata.json`: source commit and final driver hashes. The initial baseline
  driver was subsequently relinked to enable hooks; plain measurements are
  unaffected, but its final hash does not identify that earlier binary.
- `diagnostic-notifications.patch`: current -> combined notification control.

The `symbols` field in early `results.json` includes inline frames, so there
may be multiple lines per hook row. Resolve a row's `offset` directly with
`addr2line -Cf -e BINARY 0xOFFSET` if necessary. Small offsets from shared-library
callers do not resolve against the executable; they are excluded from the
RocksDB call-site breakdown.

## Reproduction on the recorded host

For the historical diagnosis, utility sources must match the commit in
`metadata.json` (296800123), before the production notification fix.
These scripts intentionally retain the recorded absolute paths. They require
this checkout at `/home/lj/metabypass_rocksdb`, GCC, addr2line, tcmalloc and the
same RocksDB release dependency libraries. `/tmp/metabypass-release-build`
must contain the production release `librocksdb.a` and `make_config.mk`, built
with `AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j48 static_lib`. The build script compiles
only the four utility objects per variant against that archive, using its
platform flags and `-O2 -DNDEBUG -fno-rtti`. Baseline utility sources/headers
come from commit `e7663cf90`; the other variants use the current checkout.

Copy `*.py`, `driver.cc`, and `hooks.c` into `/tmp/mb-foreground-profile/`, then:

```sh
python3 /tmp/mb-foreground-profile/build.py
python3 /tmp/mb-foreground-profile/build-extra.py
cc -shared -fPIC -ftls-model=initial-exec -O2 \
  /tmp/mb-foreground-profile/hooks.c -ldl \
  -o /tmp/mb-foreground-profile/hooks.so
python3 /tmp/mb-foreground-profile/run.py
python3 /tmp/mb-foreground-profile/confirm.py
```

`confirm.py` also expects the original production reader at
`/tmp/metabypass-scan-ablation-bin/async-incremental`. `reproduce.py` expects
that directory's original `baseline` and `async-incremental` binaries, whose
hashes are recorded in `../pipeline-results.json`.
Run builds before timings, with no competing experiment. Each subprocess has
a 60-second timeout. Runners create private directories and delete only those
directories; tmpfs trials do not represent durable-storage performance.
