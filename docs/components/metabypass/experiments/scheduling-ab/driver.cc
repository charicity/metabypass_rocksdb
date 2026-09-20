// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
// Linux research benchmark; per-operation clocks are common to all variants.
#include <signal.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/listener.h"
#include "rocksdb/utilities/metabypass.h"

#ifdef MB_AB_PROFILE
extern "C" void mb_profile_start();
extern "C" void mb_profile_stop();
#endif
using namespace ROCKSDB_NAMESPACE;
using Clock = std::chrono::steady_clock;
static uint64_t Micros(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                               start)
      .count();
}
static long Cpu(timeval t) { return t.tv_sec * 1000000 + t.tv_usec; }
struct Events : EventListener {
  std::atomic<uint64_t> flushes{0}, compactions{0}, sst_bytes{0};
  void OnFlushCompleted(DB*, const FlushJobInfo& info) override {
    ++flushes;
    sst_bytes +=
        info.table_properties.data_size + info.table_properties.index_size;
  }
  void OnCompactionCompleted(DB*, const CompactionJobInfo&) override {
    ++compactions;
  }
};
static std::string Key(int i, bool mixed) {
  std::string key = std::to_string(i);
  if (mixed) key += std::string(128 - key.size(), 'k');
  return key;
}
int main(int argc, char** argv) {
  if (argc != 6) return 2;
  const std::string mode = argv[1], root = argv[2], workload = argv[4];
  const int count = std::stoi(argv[3]);
  const bool mixed = workload != "default";
  Options o;
  o.create_if_missing = true;
  o.allow_concurrent_memtable_write = false;
  o.enable_blob_files = true;
  o.enable_blob_direct_write = true;
  o.min_blob_size = 0;
  o.compression = kNoCompression;
  if (mixed) {
    o.write_buffer_size = 256 * 1024;
    o.target_file_size_base = 256 * 1024;
    o.max_bytes_for_level_base = 1024 * 1024;
    o.level0_file_num_compaction_trigger = 2;
    o.max_background_jobs = 4;
  }
  auto events = std::make_shared<Events>();
  o.listeners.push_back(events);
  MetaBypassOptions b;
  b.data_dir = root + "/data";
  b.backup_dir = root + "/backup";
  b.queue_capacity = std::stoull(argv[5]);
  b.batch_bytes = std::min<size_t>(256 * 1024, b.queue_capacity / 4);
  std::unique_ptr<MetaBypassDB> db;
  Status s;
  const std::string value(1024, 'v');
  if (mode == "restore") {
    auto start = Clock::now();
    s = MetaBypassDB::Restore(o, b, root + "/index");
    if (s.ok()) s = MetaBypassDB::Open(o, b, root + "/index", &db);
    const auto restore_us = Micros(start);
    std::string actual;
    for (int i = 0; s.ok() && i < count; ++i) {
      s = db->Get(ReadOptions(), Key(i, mixed), &actual);
      if (s.ok() && actual != value) s = Status::Corruption("value mismatch");
    }
    // Continue writing after recovery, then close/reopen and check again.
    if (s.ok()) s = db->Put(WriteOptions(), "after-restore", "ok");
    if (db) s.UpdateIfOk(db->Close());
    db.reset();
    if (s.ok()) s = MetaBypassDB::Open(o, b, root + "/index", &db);
    if (s.ok()) s = db->Get(ReadOptions(), "after-restore", &actual);
    if (s.ok() && actual != "ok") s = Status::Corruption("reopen mismatch");
    if (db) s.UpdateIfOk(db->Close());
    printf("restore_us=%llu status=%s\n", (unsigned long long)restore_us,
           s.ToString().c_str());
    return s.ok() ? 0 : 1;
  }
  s = MetaBypassDB::Open(o, b, root + "/index", &db);
  if (!s.ok()) {
    puts(s.ToString().c_str());
    return 1;
  }
  std::vector<uint64_t> latency;
  latency.reserve(count);
  rusage before{}, after{};
  getrusage(RUSAGE_THREAD, &before);
#ifdef MB_AB_PROFILE
  mb_profile_start();
#endif
  auto start = Clock::now();
  for (int i = 0; s.ok() && i < count; ++i) {
    auto key = Key(i, mixed);
    auto t = Clock::now();
    s = db->Put(WriteOptions(), key, value);
    latency.push_back(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t)
            .count());
  }
  const auto foreground_us = Micros(start);
#ifdef MB_AB_PROFILE
  mb_profile_stop();
#endif
  getrusage(RUSAGE_THREAD, &after);
  const auto foreground_flushes = events->flushes.load();
  const auto foreground_compactions = events->compactions.load();
  start = Clock::now();
  if (s.ok()) s = db->SyncBackup();
  const auto sync_us = Micros(start);
  if (mode == "crash" && s.ok()) {
    puts("synced-before-sigkill");
    fflush(stdout);
    kill(getpid(), SIGKILL);
  }
  start = Clock::now();
  s.UpdateIfOk(db->Close());
  const auto close_us = Micros(start);
  auto stats = db->GetBackupStats();
  s.UpdateIfOk(stats.error);
  std::sort(latency.begin(), latency.end());
  auto percentile = [&](double q) {
    return latency[std::min(latency.size() - 1, size_t(q * latency.size()))];
  };
  printf(
      "foreground_us=%llu sync_us=%llu close_us=%llu p50_ns=%llu p95_ns=%llu "
      "p99_ns=%llu max_ns=%llu "
      "backpressure_us=%llu peak_bytes=%llu lag_us=%llu points=%llu "
      "mirrored_bytes=%llu "
      "flushes=%llu compactions=%llu sst_bytes=%llu user_us=%ld system_us=%ld "
      "voluntary=%ld involuntary=%ld status=%s\n",
      (unsigned long long)foreground_us, (unsigned long long)sync_us,
      (unsigned long long)close_us, (unsigned long long)percentile(.50),
      (unsigned long long)percentile(.95), (unsigned long long)percentile(.99),
      (unsigned long long)latency.back(),
      (unsigned long long)stats.backpressure_micros,
      (unsigned long long)stats.peak_queued_bytes,
      (unsigned long long)stats.last_point_lag_micros,
      (unsigned long long)stats.recovery_points,
      (unsigned long long)stats.mirrored_bytes,
      (unsigned long long)foreground_flushes,
      (unsigned long long)foreground_compactions,
      (unsigned long long)events->sst_bytes.load(),
      Cpu(after.ru_utime) - Cpu(before.ru_utime),
      Cpu(after.ru_stime) - Cpu(before.ru_stime),
      after.ru_nvcsw - before.ru_nvcsw, after.ru_nivcsw - before.ru_nivcsw,
      s.ToString().c_str());
  return s.ok() ? 0 : 1;
}
