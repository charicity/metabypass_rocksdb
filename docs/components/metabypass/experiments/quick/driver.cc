// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
// Standalone Linux experiment. Never linked into the RocksDB library.
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#ifndef MB_UPSTREAM
#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/separated_storage.h"
#endif

using namespace ROCKSDB_NAMESPACE;
namespace {
using Clock = std::chrono::steady_clock;
uint64_t Micros(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                               start)
      .count();
}
std::string Quote(const std::string& s) {
  std::string result = "\"";
  constexpr char hex[] = "0123456789abcdef";
  for (unsigned char c : s) {
    if (c == '\\' || c == '"') {
      result += '\\';
      result += c;
    } else if (c < 32) {
      result += "\\u00";
      result += hex[c / 16];
      result += hex[c % 16];
    } else {
      result += c;
    }
  }
  return result + '"';
}
struct DelayCounters {
  std::atomic<uint64_t> append{0}, sync{0}, fsync{0}, named_sync{0};
  std::atomic<uint64_t> wait_us{0};
  uint64_t delay_us = 0;
  void Pause(std::atomic<uint64_t>& count) {
    ++count;
    if (delay_us != 0) {
      auto start = Clock::now();
      std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
      wait_us += Micros(start);
    }
  }
};
class DelayedFile : public FSWritableFileOwnerWrapper {
 public:
  DelayedFile(std::unique_ptr<FSWritableFile> file, DelayCounters* counters)
      : FSWritableFileOwnerWrapper(std::move(file)), counters_(counters) {}
  IOStatus Append(const Slice& b, const IOOptions& o,
                  IODebugContext* d) override {
    counters_->Pause(counters_->append);
    return target()->Append(b, o, d);
  }
  IOStatus Append(const Slice& b, const IOOptions& o,
                  const DataVerificationInfo& v, IODebugContext* d) override {
    counters_->Pause(counters_->append);
    return target()->Append(b, o, v, d);
  }
  IOStatus Sync(const IOOptions& o, IODebugContext* d) override {
    counters_->Pause(counters_->sync);
    return target()->Sync(o, d);
  }
  IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
    counters_->Pause(counters_->fsync);
    return target()->Fsync(o, d);
  }

 private:
  DelayCounters* counters_;
};
class DelayedFS : public FileSystemWrapper {
 public:
  DelayedFS(std::vector<std::string> paths, DelayCounters* counters)
      : FileSystemWrapper(FileSystem::Default()),
        paths_(std::move(paths)),
        counters_(counters) {}
  const char* Name() const override { return "MetaBypassQuickDelay"; }
  IOStatus NewWritableFile(const std::string& p, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* f,
                           IODebugContext* d) override {
    auto s = target()->NewWritableFile(p, o, f, d);
    if (s.ok() && Slow(p)) f->reset(new DelayedFile(std::move(*f), counters_));
    return s;
  }
  IOStatus ReopenWritableFile(const std::string& p, const FileOptions& o,
                              std::unique_ptr<FSWritableFile>* f,
                              IODebugContext* d) override {
    auto s = target()->ReopenWritableFile(p, o, f, d);
    if (s.ok() && Slow(p)) f->reset(new DelayedFile(std::move(*f), counters_));
    return s;
  }
  // SeparatedStorage uses named-file synchronization for blob dependencies.
  // Calling the underlying FS avoids counting the same sync twice.
  IOStatus SyncFile(const std::string& p, const FileOptions& f,
                    const IOOptions& o, bool full, IODebugContext* d) override {
    if (Slow(p)) counters_->Pause(counters_->named_sync);
    return target()->SyncFile(p, f, o, full, d);
  }

 private:
  bool Slow(const std::string& p) const {
    for (const auto& root : paths_) {
      if (p.compare(0, root.size() + 1, root + "/") == 0) return true;
    }
    return false;
  }
  std::vector<std::string> paths_;
  DelayCounters* counters_;
};
struct Events : EventListener {
  std::atomic<uint64_t> flushes{0}, compactions{0};
  void OnFlushCompleted(DB*, const FlushJobInfo&) override { ++flushes; }
  void OnCompactionCompleted(DB*, const CompactionJobInfo&) override {
    ++compactions;
  }
};
class Database {
 public:
  Database(std::string variant, std::string root, bool pressure,
           bool backpressure, std::shared_ptr<FileSystem> fs)
      : variant_(std::move(variant)),
        root_(std::move(root)),
        fs_(std::move(fs)) {
    options_.create_if_missing = true;
    options_.allow_concurrent_memtable_write = false;
    options_.compression = kNoCompression;
    options_.enable_blob_garbage_collection = false;
    options_.listeners.push_back(events);
    if (pressure) {
      options_.write_buffer_size = 256 * 1024;
      options_.target_file_size_base = 256 * 1024;
      options_.max_bytes_for_level_base = 1024 * 1024;
      options_.level0_file_num_compaction_trigger = 2;
      options_.max_background_jobs = 4;
    }
#ifndef MB_UPSTREAM
    options_.enable_blob_files = true;
    options_.enable_blob_direct_write = true;
    options_.min_blob_size = 0;
    bypass_.data_dir = root_ + "/data";
    bypass_.backup_dir = root_ + "/backup";
    if (pressure || backpressure) {
      bypass_.queue_capacity = 2 * 1024 * 1024;
      bypass_.batch_bytes = 64 * 1024;
    }
#endif
  }
  Status Open(bool fresh) {
    options_.error_if_exists = fresh;
    options_.create_if_missing = fresh;
#ifndef MB_UPSTREAM
    if (variant_ == "current") {
      env_ = NewCompositeEnv(fs_);
      options_.env = env_.get();
      return MetaBypassDB::Open(options_, bypass_, root_ + "/index", &mb_);
    }
    storage_ = std::make_shared<metabypass::SeparatedStorage>(
        fs_, root_ + "/index", root_ + "/data");
    auto s = storage_->Lock();
    if (!s.ok()) return s;
    env_ = NewCompositeEnv(storage_);
#else
    env_ = NewCompositeEnv(fs_);
#endif
    options_.env = env_.get();
    return DB::Open(options_, root_ + "/index", &raw_);
  }
  Status Restore() {
#ifndef MB_UPSTREAM
    env_ = NewCompositeEnv(fs_);
    options_.env = env_.get();
    return MetaBypassDB::Restore(options_, bypass_, root_ + "/index");
#else
    return Status::NotSupported("upstream has no Metabypass backup");
#endif
  }
  Status Put(const std::string& k, const std::string& v) {
#ifndef MB_UPSTREAM
    if (mb_) return mb_->Put(WriteOptions(), k, v);
#endif
    return raw_->Put(WriteOptions(), k, v);
  }
  Status Get(const std::string& k, std::string* v) {
#ifndef MB_UPSTREAM
    if (mb_) return mb_->Get(ReadOptions(), k, v);
#endif
    return raw_->Get(ReadOptions(), k, v);
  }
  Status Sync() {
#ifndef MB_UPSTREAM
    if (mb_) return mb_->SyncBackup();
#endif
    return Status::OK();
  }
  Status BackupBackpressure(uint64_t* micros) const {
#ifndef MB_UPSTREAM
    if (mb_) {
      auto stats = mb_->GetBackupStats();
      if (stats.error.ok()) *micros = stats.backpressure_micros;
      return stats.error;
    }
#endif
    return Status::NotSupported("backup stats require current Metabypass");
  }
  Status Close(std::map<std::string, uint64_t>* metrics) {
    Status s;
#ifndef MB_UPSTREAM
    if (mb_) {
      s = mb_->Close();
      auto stats = mb_->GetBackupStats();
      s.UpdateIfOk(stats.error);
      if (metrics) {
        (*metrics)["queue_peak_bytes"] = stats.peak_queued_bytes;
        (*metrics)["queue_capacity_bytes"] = bypass_.queue_capacity;
        (*metrics)["backpressure_us"] = stats.backpressure_micros;
        (*metrics)["mirrored_bytes"] = stats.mirrored_bytes;
        (*metrics)["recovery_points"] = stats.recovery_points;
        (*metrics)["last_point_build_us"] = stats.last_build_micros;
        (*metrics)["last_point_lag_us"] = stats.last_point_lag_micros;
        (*metrics)["retained_index_bytes"] = stats.retained_index_bytes;
        (*metrics)["validated_blob_bytes"] = stats.validated_blob_bytes;
        (*metrics)["reused_tables"] = stats.reused_tables;
      }
      mb_.reset();
    }
#endif
    if (raw_) {
      s.UpdateIfOk(raw_->Close());
      raw_.reset();
    }
    env_.reset();
#ifndef MB_UPSTREAM
    storage_.reset();
#else
    (void)metrics;
#endif
    return s;
  }
  std::shared_ptr<Events> events = std::make_shared<Events>();

 private:
  std::string variant_, root_;
  std::shared_ptr<FileSystem> fs_;
  Options options_;
#ifndef MB_UPSTREAM
  MetaBypassOptions bypass_;
  std::shared_ptr<metabypass::SeparatedStorage> storage_;
#endif
  std::unique_ptr<Env> env_;
  std::unique_ptr<DB> raw_;
#ifndef MB_UPSTREAM
  std::unique_ptr<MetaBypassDB> mb_;
#endif
};
std::string Key(uint64_t i, bool pressure) {
  std::string k = std::to_string(i);
  if (pressure) k.resize(128, 'k');
  return k;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc != 7) {
    std::cerr << "usage: mb_quick VARIANT write|verify ROOT COUNT "
                 "control|slow|pressure|backpressure DELAY_US\n";
    return 2;
  }
  try {
    const std::string variant = argv[1], action = argv[2], scenario = argv[5];
    const std::string root = std::filesystem::canonical(argv[3]).string();
    const uint64_t count = std::stoull(argv[4]), delay = std::stoull(argv[6]);
    if (count == 0 || count > 10000000 || delay > 1000000 ||
        (action != "write" && action != "verify") ||
        (scenario != "control" && scenario != "slow" &&
         scenario != "pressure" && scenario != "backpressure")) {
      throw std::invalid_argument("invalid workload argument");
    }
    if (scenario == "backpressure" && variant != "current") {
      throw std::invalid_argument("backpressure requires current variant");
    }
#ifdef MB_UPSTREAM
    if (variant != "upstream") throw std::invalid_argument("upstream only");
#else
    if (variant != "current" && variant != "baseline") {
      throw std::invalid_argument("current or baseline required");
    }
#endif
    if (action == "write" && !std::filesystem::is_empty(root)) {
      throw std::invalid_argument("write requires an empty run directory");
    }
    DelayCounters counters;
    counters.delay_us = delay;
    std::vector<std::string> paths =
        scenario == "backpressure" ? std::vector<std::string>{root + "/backup"}
        : variant == "upstream"
            ? std::vector<std::string>{root + "/index"}
            : std::vector<std::string>{root + "/data", root + "/backup"};
    auto fs = std::make_shared<DelayedFS>(paths, &counters);
    const bool pressure = scenario == "pressure";
    const bool backpressure = scenario == "backpressure";
    Database db(variant, root, pressure, backpressure, fs);
    std::map<std::string, uint64_t> m;
    const std::string value(1024, 'v');
    auto begin = Clock::now();
    Status s;
    if (action == "verify" && variant == "current") s = db.Restore();
    if (s.ok()) s = db.Open(action == "write");
    m[action == "verify" && variant == "current" ? "restore_us" : "open_us"] =
        Micros(begin);
    uint64_t before_backpressure = 0;
    if (s.ok() && action == "write" && backpressure) {
      s = db.BackupBackpressure(&before_backpressure);
    }
    std::vector<uint64_t> latencies;
    latencies.reserve(count);
    begin = Clock::now();
    for (uint64_t i = 0; s.ok() && i < count; ++i) {
      const auto key = Key(i, pressure || backpressure);
      if (action == "write") {
        const auto t = Clock::now();
        s = db.Put(key, value);
        const uint64_t ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                 t)
                .count();
        if (s.ok()) latencies.push_back(ns);
      } else {
        std::string actual;
        s = db.Get(key, &actual);
        if (s.ok() && actual != value) s = Status::Corruption("value mismatch");
      }
      if (s.ok()) ++m["successful_ops"];
    }
    if (action == "write") {
      m["foreground_us"] = Micros(begin);
      if (backpressure) {
        uint64_t after_backpressure = 0;
        s.UpdateIfOk(db.BackupBackpressure(&after_backpressure));
        if (s.ok() && after_backpressure < before_backpressure) {
          s = Status::Corruption("backpressure counter decreased");
        }
        if (s.ok()) {
          m["foreground_backpressure_us"] =
              after_backpressure - before_backpressure;
        }
      }
      m["foreground_flushes"] = db.events->flushes.load();
      m["foreground_compactions"] = db.events->compactions.load();
      begin = Clock::now();
      if (s.ok()) s = db.Sync();
      m["sync_us"] = variant == "current" ? Micros(begin) : 0;
      begin = Clock::now();
      s.UpdateIfOk(db.Close(&m));
      m["close_us"] = Micros(begin);
      m["total_us"] = m["foreground_us"] + m["sync_us"] + m["close_us"];
      // Sorting is outside all measured write phases.
      std::sort(latencies.begin(), latencies.end());
      if (!latencies.empty()) {
        m["p50_ns"] = latencies[(latencies.size() - 1) * 50 / 100];
        m["p99_ns"] = latencies[(latencies.size() - 1) * 99 / 100];
      }
    } else {
      m["verify_us"] = Micros(begin);
      if (s.ok()) s = db.Put("after-verify", "ok");
      s.UpdateIfOk(db.Close(nullptr));
      if (s.ok()) s = db.Open(false);
      std::string actual;
      if (s.ok()) s = db.Get("after-verify", &actual);
      if (s.ok() && actual != "ok") s = Status::Corruption("reopen mismatch");
      s.UpdateIfOk(db.Close(nullptr));
    }
    m["flushes"] = db.events->flushes.load();
    m["compactions"] = db.events->compactions.load();
    m["delayed_append_calls"] = counters.append.load();
    m["delayed_sync_calls"] = counters.sync.load();
    m["delayed_fsync_calls"] = counters.fsync.load();
    m["delayed_named_sync_calls"] = counters.named_sync.load();
    m["injected_wait_us"] = counters.wait_us.load();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    m["cpu_user_us"] = usage.ru_utime.tv_sec * 1000000 + usage.ru_utime.tv_usec;
    m["cpu_system_us"] =
        usage.ru_stime.tv_sec * 1000000 + usage.ru_stime.tv_usec;
    m["peak_rss_kib"] = usage.ru_maxrss;
    std::cout << "{\"status\":" << Quote(s.ToString()) << ",\"metrics\":{";
    bool first = true;
    for (const auto& [key, v] : m) {
      if (!first) std::cout << ',';
      first = false;
      std::cout << Quote(key) << ':' << v;
    }
    std::cout << "}}\n";
    return s.ok() ? 0 : 1;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 2;
  }
}
