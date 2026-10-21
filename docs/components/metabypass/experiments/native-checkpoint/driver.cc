// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
// Standalone Linux experiment, not part of the library.
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "verification_sample.h"

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#ifndef MB_UPSTREAM
#include "rocksdb/utilities/metabypass.h"
#endif
using namespace ROCKSDB_NAMESPACE;
namespace {
using Clock = std::chrono::steady_clock;
uint64_t Us(Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t)
      .count();
}
std::string Q(const std::string& s) {
  std::string r = "\"";
  const char* hex = "0123456789abcdef";
  for (unsigned char c : s) {
    if (c == '"' || c == '\\') {
      r += '\\';
      r += c;
    } else if (c < 32) {
      r += "\\u00";
      r += hex[c / 16];
      r += hex[c % 16];
    } else
      r += c;
  }
  return r + '"';
}
std::mutex output_mutex;
void Emit(const std::string& event, const std::string& fields = "") {
  std::lock_guard<std::mutex> lock(output_mutex);
  std::cout
      << "MB_NATIVE_JSON {\"protocol\":\"metabypass-native-perf-v1\",\"event\":"
      << Q(event) << fields << "}" << std::endl;
}
void Check(const Status& s) {
  if (!s.ok()) throw std::runtime_error(s.ToString());
}
struct Config {
  std::map<std::string, std::string> flags{
      {"variant", "D"},
      {"action", "run"},
      {"workload", "insert"},
      {"index", ""},
      {"data", ""},
      {"backup", ""},
      {"staging", ""},
      {"state", ""},
      {"verify-acks", ""},
      {"verify-samples", "0"},
      {"crash-stage", ""},
      {"keys", "1024"},
      {"ops", "1024"},
      {"value-size", "1024"},
      {"threads", "1"},
      {"warmup-seconds", "0"},
      {"measure-seconds", "0"},
      {"sync", "false"},
      {"flush", "true"},
      {"fresh", "false"},
      {"staging-capacity", "268435456"},
      {"sst-mode", "disabled"},
      {"sst-budget", "0"},
      {"gc", "true"},
      {"disable-auto-compactions", "false"},
      {"cache-bytes", "268435456"},
      {"flush-every", "0"},
      {"seed", "20261017"},
      {"gc-wait-seconds", "0"},
      {"direct-write", "false"},
      {"mirror-interval-ms", "1000"},
      {"checkpoint-interval-ms", "1000"}};
  std::string S(const std::string& k) const { return flags.at(k); }
  uint64_t N(const std::string& k) const {
    auto s = S(k);
    if (s.empty() || s.find_first_not_of("0123456789") != std::string::npos)
      throw std::runtime_error("invalid integer: " + k);
    return std::stoull(s);
  }
  bool B(const std::string& k) const {
    auto s = S(k);
    if (s != "true" && s != "false")
      throw std::runtime_error("invalid bool: " + k);
    return s == "true";
  }
  Config(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
      std::string a = argv[i];
      auto p = a.find('=');
      if (a.rfind("--", 0) != 0 || p == std::string::npos ||
          !flags.count(a.substr(2, p - 2)))
        throw std::runtime_error("unknown argument: " + a);
      flags[a.substr(2, p - 2)] = a.substr(p + 1);
    }
    for (auto k : {"keys", "ops", "value-size", "threads", "warmup-seconds",
                   "measure-seconds", "staging-capacity", "sst-budget",
                   "cache-bytes", "flush-every", "seed", "gc-wait-seconds",
                   "mirror-interval-ms", "checkpoint-interval-ms", "verify-samples"})
      N(k);
    for (auto k : {"sync", "flush", "fresh", "gc", "disable-auto-compactions",
                   "direct-write"})
      B(k);
    if (!std::set<std::string>{"U", "B", "D", "S"}.count(S("variant")) ||
        !std::set<std::string>{"prepare", "run", "verify", "restore",
                               "append-reopen", "crashwrite"}
             .count(S("action")) ||
        !std::set<std::string>{"insert", "overwrite", "randomread", "hotspot",
                               "scan", "mixed", "sync", "gc", "sst"}
             .count(S("workload")))
      throw std::runtime_error("invalid variant/action/workload");
    if (N("keys") == 0 || N("keys") > 100000000 || N("threads") == 0 ||
        N("threads") > 64 || N("keys") < N("threads") || N("value-size") < 32 ||
        N("value-size") > 64 * 1024 * 1024 || N("measure-seconds") > 86400 ||
        N("warmup-seconds") > 86400 || N("gc-wait-seconds") > 60)
      throw std::runtime_error("argument outside supported bounds");
    if (N("mirror-interval-ms") == 0 || N("checkpoint-interval-ms") == 0 ||
        N("mirror-interval-ms") > 86400000 ||
        N("checkpoint-interval-ms") > 86400000)
      throw std::runtime_error("interval outside supported bounds");
    if (N("verify-samples") != 0 && !S("verify-acks").empty())
      throw std::runtime_error("sample verification does not support verify-acks");
    if (B("direct-write") && S("variant") == "U")
      throw std::runtime_error("upstream does not support direct-write");
    if (!std::set<std::string>{"disabled", "observe", "adaptive"}.count(
            S("sst-mode")))
      throw std::runtime_error("invalid sst-mode");
    if (S("sst-mode") != "disabled" && N("sst-budget") == 0)
      throw std::runtime_error("sst-budget required");
    for (auto k : {"index", "state"})
      if (S(k).empty() || !std::filesystem::path(S(k)).is_absolute())
        throw std::runtime_error(std::string(k) + " must be absolute");
    if (S("variant") != "U" && S("data").empty())
      throw std::runtime_error("data required");
    if ((S("variant") == "D" || S("variant") == "S") && S("backup").empty())
      throw std::runtime_error("backup required");
    if (S("variant") == "S" &&
        (S("staging").empty() || N("staging-capacity") == 0))
      throw std::runtime_error("staging required");
    if (!std::set<std::string>{"", "write", "publish", "migration"}.count(
            S("crash-stage")))
      throw std::runtime_error("invalid crash-stage");
    if (!S("crash-stage").empty() &&
        (S("action") != "crashwrite" ||
         (S("variant") != "D" && S("variant") != "S")))
      throw std::runtime_error("crash-stage requires D/S crashwrite");
    if (S("crash-stage") == "migration" && S("variant") != "S")
      throw std::runtime_error("migration gate requires staging");
    std::vector<std::filesystem::path> dirs;
    for (auto name : {"index", "data", "backup", "staging"}) {
      if (S(name).empty()) continue;
      std::filesystem::path p(S(name));
      if (!p.is_absolute() || p.lexically_normal() != p || p.filename().empty())
        throw std::runtime_error("paths must be absolute and normalized");
      for (const auto& d : dirs) {
        auto sub = p.lexically_relative(d).string();
        auto reverse = d.lexically_relative(p).string();
        if (sub == "." || sub.rfind("..", 0) != 0 ||
            reverse.rfind("..", 0) != 0)
          throw std::runtime_error("database directories overlap");
      }
      auto state_relative =
          std::filesystem::path(S("state")).lexically_relative(p).string();
      if (state_relative.rfind("..", 0) != 0)
        throw std::runtime_error("state must be outside database directories");
      dirs.push_back(p);
    }
#ifdef MB_UPSTREAM
    if (S("variant") != "U")
      throw std::runtime_error("upstream binary only accepts U");
#else
    if (S("variant") == "U")
      throw std::runtime_error("U requires pinned upstream binary");
#endif
  }
};
// Fault gates belong solely to this experiment's filesystem adapter.
struct CrashGate {
  std::string stage, backup, data;
  std::atomic<bool> armed{false}, hit{false};
  void Wait(const std::string& at, const std::string& path) {
    if (!armed || stage != at || hit.exchange(true)) return;
    Emit("ready", ",\"stage\":" + Q(at) + ",\"path\":" + Q(path));
    char token;
    while (true) {
      auto n = ::read(STDIN_FILENO, &token, 1);
      if (n == 1 && token == 'c') return;
      if (n <= 0) {
        Emit("error",
             ",\"status\":\"failed\",\"message\":\"crash gate stdin closed\"");
        _exit(2);
      }
    }
  }
};
class GateFS : public FileSystemWrapper {
 public:
  explicit GateFS(std::shared_ptr<CrashGate> gate)
      : FileSystemWrapper(FileSystem::Default()), gate_(std::move(gate)) {}
  const char* Name() const override { return "NativePerfCrashGate"; }
  IOStatus RenameFile(const std::string& from, const std::string& to,
                      const IOOptions& o, IODebugContext* d) override {
    if (to == gate_->backup + "/LATEST") gate_->Wait("publish", to);
    return target()->RenameFile(from, to, o, d);
  }
  IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* f,
                           IODebugContext* d) override {
    if (path.rfind(gate_->data + "/segment-", 0) == 0)
      gate_->Wait("migration", path);
    return target()->NewWritableFile(path, o, f, d);
  }

 private:
  std::shared_ptr<CrashGate> gate_;
};
// Only native .blob files directly under index are routed. Native DeleteFile
// reaches the physical file; this adapter has no backup or retention policy.
class BlobFS : public FileSystemWrapper {
 public:
  BlobFS(std::string index, std::string data)
      : FileSystemWrapper(FileSystem::Default()),
        index_(std::move(index)),
        data_(std::move(data)) {}
  const char* Name() const override { return "NativePerfBlobRouting"; }
  std::string P(const std::string& p) const {
    std::filesystem::path x(p);
    return x.parent_path() == index_ && x.extension() == ".blob"
               ? (std::filesystem::path(data_) / x.filename()).string()
               : p;
  }
#define OPEN_METHOD(M, T)                                         \
  IOStatus M(const std::string& p, const FileOptions& o,          \
             std::unique_ptr<T>* r, IODebugContext* d) override { \
    return target()->M(P(p), o, r, d);                            \
  }
  OPEN_METHOD(NewSequentialFile, FSSequentialFile)
  OPEN_METHOD(NewRandomAccessFile, FSRandomAccessFile)
  OPEN_METHOD(NewWritableFile, FSWritableFile)
  OPEN_METHOD(ReopenWritableFile, FSWritableFile)
  OPEN_METHOD(NewRandomRWFile, FSRandomRWFile)
#undef OPEN_METHOD
#define ONE_METHOD(M)                                                     \
  IOStatus M(const std::string& p, const IOOptions& o, IODebugContext* d) \
      override {                                                          \
    return target()->M(P(p), o, d);                                       \
  }
  ONE_METHOD(FileExists)
#undef ONE_METHOD
  IOStatus DeleteFile(const std::string& p, const IOOptions& o,
                      IODebugContext* d) override {
    const auto physical = P(p);
    auto status = target()->DeleteFile(physical, o, d);
    if (status.ok() && physical != p)
      native_blob_deletes.fetch_add(1, std::memory_order_relaxed);
    return status;
  }
  std::atomic<uint64_t> native_blob_deletes{0};
  IOStatus ReuseWritableFile(const std::string& p, const std::string& old,
                             const FileOptions& o,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* d) override {
    return target()->ReuseWritableFile(P(p), P(old), o, r, d);
  }
  IOStatus GetFileSize(const std::string& p, const IOOptions& o, uint64_t* r,
                       IODebugContext* d) override {
    return target()->GetFileSize(P(p), o, r, d);
  }
  IOStatus GetFileModificationTime(const std::string& p, const IOOptions& o,
                                   uint64_t* r, IODebugContext* d) override {
    return target()->GetFileModificationTime(P(p), o, r, d);
  }
  IOStatus Truncate(const std::string& p, size_t n, const IOOptions& o,
                    IODebugContext* d) override {
    return target()->Truncate(P(p), n, o, d);
  }
  IOStatus RenameFile(const std::string& p, const std::string& q,
                      const IOOptions& o, IODebugContext* d) override {
    return target()->RenameFile(P(p), P(q), o, d);
  }
  IOStatus LinkFile(const std::string& p, const std::string& q,
                    const IOOptions& o, IODebugContext* d) override {
    return target()->LinkFile(P(p), P(q), o, d);
  }
  IOStatus NumFileLinks(const std::string& p, const IOOptions& o, uint64_t* r,
                        IODebugContext* d) override {
    return target()->NumFileLinks(P(p), o, r, d);
  }
  IOStatus AreFilesSame(const std::string& p, const std::string& q,
                        const IOOptions& o, bool* r,
                        IODebugContext* d) override {
    return target()->AreFilesSame(P(p), P(q), o, r, d);
  }
  IOStatus SyncFile(const std::string& p, const FileOptions& f,
                    const IOOptions& o, bool full, IODebugContext* d) override {
    return target()->SyncFile(P(p), f, o, full, d);
  }
  IOStatus GetChildren(const std::string& p, const IOOptions& o,
                       std::vector<std::string>* r,
                       IODebugContext* d) override {
    auto s = target()->GetChildren(p, o, r, d);
    if (!s.ok() || p != index_) return s;
    std::vector<std::string> b;
    s = target()->GetChildren(data_, o, &b, d);
    if (!s.ok()) return s;
    for (auto& n : b)
      if (std::filesystem::path(n).extension() == ".blob") r->push_back(n);
    std::sort(r->begin(), r->end());
    r->erase(std::unique(r->begin(), r->end()), r->end());
    return IOStatus::OK();
  }
  IOStatus GetChildrenFileAttributes(const std::string& p, const IOOptions& o,
                                     std::vector<FileAttributes>* r,
                                     IODebugContext* d) override {
    return FileSystem::GetChildrenFileAttributes(p, o, r, d);
  }
  class Dirs : public FSDirectory {
   public:
    std::unique_ptr<FSDirectory> a, b;
    IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
      auto s = b->Fsync(o, d);
      if (!s.ok()) return s;
      return a->Fsync(o, d);
    }
  };
  IOStatus NewDirectory(const std::string& p, const IOOptions& o,
                        std::unique_ptr<FSDirectory>* r,
                        IODebugContext* d) override {
    if (p != index_) return target()->NewDirectory(p, o, r, d);
    auto dirs = std::make_unique<Dirs>();
    auto s = target()->NewDirectory(p, o, &dirs->a, d);
    if (!s.ok()) return s;
    s = target()->NewDirectory(data_, o, &dirs->b, d);
    if (!s.ok()) return s;
    *r = std::move(dirs);
    return IOStatus::OK();
  }

 private:
  std::string index_, data_;
};
struct Events : EventListener {
  std::atomic<uint64_t> flushes{0}, compactions{0}, flush_bytes{0},
      compaction_bytes{0};
  void OnFlushCompleted(DB*, const FlushJobInfo& i) override {
    ++flushes;
    flush_bytes += i.table_properties.data_size;
  }
  void OnCompactionCompleted(DB*, const CompactionJobInfo& i) override {
    ++compactions;
    compaction_bytes += i.stats.total_output_bytes;
  }
};
class Database {
 public:
  explicit Database(const Config& c) : c_(c) {
    gate->stage = c.S("crash-stage");
    gate->backup = c.S("backup");
    gate->data = c.S("data");
    o_.compression = kNoCompression;
    o_.write_buffer_size = 128 * 1024 * 1024;
    o_.max_write_buffer_number = 2;
    o_.max_background_jobs = 4;
    o_.disable_auto_compactions = c.B("disable-auto-compactions");
    o_.enable_blob_files = c.S("variant") != "U";
    o_.min_blob_size = 0;
    o_.enable_blob_garbage_collection = false;
    o_.statistics = CreateDBStatistics();
    o_.listeners.push_back(events);
    BlockBasedTableOptions table;
    table.block_cache = NewLRUCache(c.N("cache-bytes"));
    o_.table_factory.reset(NewBlockBasedTableFactory(table));
#ifndef MB_UPSTREAM
    o_.enable_blob_direct_write = c.B("direct-write");
    o_.allow_concurrent_memtable_write = !c.B("direct-write");
    b_.interval_ms = c.N("mirror-interval-ms");
    b_.checkpoint_interval_ms = c.N("checkpoint-interval-ms");
    b_.data_dir = c.S("data");
    b_.backup_dir = c.S("backup");
    b_.enable_blob_file_gc = c.B("gc");
    if (c.S("variant") == "S") {
      b_.staging_dir = c.S("staging");
      b_.staging_capacity = c.N("staging-capacity");
    }
    b_.sst_tiering.mode =
        c.S("sst-mode") == "adaptive"  ? SstTieringMode::kAdaptive
        : c.S("sst-mode") == "observe" ? SstTieringMode::kObserveOnly
                                       : SstTieringMode::kDisabled;
    b_.sst_tiering.ssd_capacity_bytes = c.N("sst-budget");
#endif
    std::shared_ptr<FileSystem> fs = FileSystem::Default();
    if (c.S("variant") == "B") {
      blob_fs_ = std::make_shared<BlobFS>(c.S("index"), c.S("data"));
      fs = blob_fs_;
    }
    if (!c.S("crash-stage").empty()) fs = std::make_shared<GateFS>(gate);
    env_ = NewCompositeEnv(fs);
    o_.env = env_.get();
  }
  void Open(bool fresh) {
    o_.create_if_missing = fresh;
    o_.error_if_exists = fresh;
    if (c_.S("variant") == "B")
      std::filesystem::create_directories(c_.S("data"));
#ifndef MB_UPSTREAM
    if (c_.S("variant") == "D" || c_.S("variant") == "S") {
      Check(MetaBypassDB::Open(o_, b_, c_.S("index"), &mb_));
      return;
    }
#endif
    Check(DB::Open(o_, c_.S("index"), &raw_));
  }
  void Restore() {
#ifndef MB_UPSTREAM
    if (c_.S("variant") == "D" || c_.S("variant") == "S") {
      Check(MetaBypassDB::Restore(o_, b_, c_.S("index")));
      return;
    }
#endif
    throw std::runtime_error("restore requires D/S");
  }
  Status Put(const std::string& k, const std::string& v, bool sync) {
    WriteOptions w;
    w.sync = sync;
#ifndef MB_UPSTREAM
    if (mb_) return mb_->Put(w, k, v);
#endif
    return raw_->Put(w, k, v);
  }
  Status Delete(const std::string& k, bool sync) {
    WriteOptions w;
    w.sync = sync;
#ifndef MB_UPSTREAM
    if (mb_) return mb_->Delete(w, k);
#endif
    return raw_->Delete(w, k);
  }
  Status Get(const std::string& k, std::string* v) {
#ifndef MB_UPSTREAM
    if (mb_) return mb_->Get(ReadOptions(), k, v);
#endif
    return raw_->Get(ReadOptions(), k, v);
  }
  std::unique_ptr<Iterator> Iter() {
#ifndef MB_UPSTREAM
    if (mb_) return std::unique_ptr<Iterator>(mb_->NewIterator(ReadOptions()));
#endif
    return std::unique_ptr<Iterator>(raw_->NewIterator(ReadOptions()));
  }
  void Flush() {
    FlushOptions f;
    f.wait = true;
#ifndef MB_UPSTREAM
    if (mb_) {
      Check(mb_->Flush(f));
      return;
    }
#endif
    Check(raw_->Flush(f));
  }
  void Compact() {
#ifndef MB_UPSTREAM
    if (mb_) {
      Check(mb_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      return;
    }
#endif
    Check(raw_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  }
  void Sync() {
#ifndef MB_UPSTREAM
    if (mb_) Check(mb_->SyncBackup());
#endif
  }
  void Stats(const std::string& boundary = "snapshot") {
    std::ostringstream s;
    s << ",\"boundary\":" << Q(boundary);
    s << ",\"native_blob_deletes\":"
      << (blob_fs_
              ? blob_fs_->native_blob_deletes.load(std::memory_order_relaxed)
              : 0)
      << ",\"flushes\":" << events->flushes
      << ",\"compactions\":" << events->compactions
      << ",\"flush_data_bytes\":" << events->flush_bytes
      << ",\"compaction_output_bytes\":" << events->compaction_bytes
      << ",\"wal_bytes\":" << o_.statistics->getTickerCount(WAL_FILE_BYTES);
#ifndef MB_UPSTREAM
    if (mb_) {
      auto v = mb_->GetBackupStats();
      Check(v.error);
#define FIELD(n) s << ",\"" #n "\":" << v.n
      FIELD(staging_bytes);
      FIELD(peak_staging_bytes);
      FIELD(migrated_blob_bytes);
      FIELD(staging_backpressure_micros);
      FIELD(sync_write_micros);
      FIELD(queued_bytes);
      FIELD(peak_queued_bytes);
      FIELD(backpressure_micros);
      FIELD(mirrored_bytes);
      FIELD(mirrored_wal_bytes);
      FIELD(recovery_points);
      FIELD(checkpoint_wait_micros);
      FIELD(checkpoint_captures);
      FIELD(checkpoint_capture_micros);
      FIELD(checkpoint_flush_micros);
      FIELD(checkpoint_persist_micros);
      FIELD(checkpoint_publish_micros);
      FIELD(staging_spill_files);
      FIELD(staging_spill_bytes);
      FIELD(tier_queued_bytes);
      FIELD(peak_tier_queued_bytes);
      FIELD(hdd_extents);
      FIELD(last_publish_micros);
      FIELD(last_build_micros);
      FIELD(last_point_lag_micros);
      FIELD(retained_index_bytes);
      FIELD(pending_gc_points);
      FIELD(pending_gc_bytes);
      FIELD(gc_micros);
      FIELD(pending_blob_gc_files);
      FIELD(pending_blob_gc_bytes);
      FIELD(reclaimed_blob_files);
      FIELD(reclaimed_blob_bytes);
      FIELD(blob_gc_micros);
      FIELD(candidate_copied_bytes);
#undef FIELD
#define SF(n) s << ",\"sst_" #n "\":" << v.sst_tiering.n
      SF(ssd_bytes);
      SF(peak_ssd_bytes);
      SF(hdd_bytes);
      SF(sampled_reads);
      SF(dropped_samples);
      SF(promotions);
      SF(demotions);
      SF(promoted_bytes);
      SF(demoted_bytes);
      SF(migration_errors);
      SF(queued_migrations);
      SF(observed_promotions);
      SF(observed_demotions);
      SF(over_budget_micros);
#undef SF
      if (!v.sst_tiering.error.empty())
        throw std::runtime_error(v.sst_tiering.error);
      if (c_.S("variant") == "S" &&
          v.peak_staging_bytes > c_.N("staging-capacity"))
        throw std::runtime_error("staging capacity exceeded");
    }
#endif
    Emit("stats", s.str());
  }
  void Close() {
#ifndef MB_UPSTREAM
    if (mb_) {
      Check(mb_->Close());
      Stats();
      mb_.reset();
    }
#endif
    if (raw_) {
      Check(raw_->Close());
      Stats();
      raw_.reset();
    }
  }
  std::shared_ptr<CrashGate> gate = std::make_shared<CrashGate>();
  std::shared_ptr<Events> events = std::make_shared<Events>();

 private:
  const Config& c_;
  Options o_;
  std::shared_ptr<BlobFS> blob_fs_;
  std::unique_ptr<Env> env_;
  std::unique_ptr<DB> raw_;
#ifndef MB_UPSTREAM
  MetaBypassOptions b_;
  std::unique_ptr<MetaBypassDB> mb_;
#endif
};
uint64_t Hash(const char* p, size_t n) {
  uint64_t h = 1469598103934665603ULL;
  for (size_t i = 0; i < n; ++i) {
    h ^= uint8_t(p[i]);
    h *= 1099511628211ULL;
  }
  return h;
}
void Put64(char* p, uint64_t n) {
  for (int i = 0; i < 8; ++i) {
    p[i] = char(n & 255);
    n >>= 8;
  }
}
uint64_t Get64(const char* p) {
  uint64_t n = 0;
  for (int i = 7; i >= 0; --i) n = (n << 8) | uint8_t(p[i]);
  return n;
}
std::string Key(uint64_t n) {
  std::string k(16, '0');
  for (int i = 15; i >= 0; --i) {
    k[i] = "0123456789abcdef"[n & 15];
    n >>= 4;
  }
  return k;
}
std::string Value(uint64_t key, uint64_t version, size_t n) {
  std::string v(n, 'x');
  Put64(v.data(), key);
  Put64(v.data() + 8, version);
  uint64_t x = key ^ (version * 0x9e3779b97f4a7c15ULL);
  for (size_t i = 16; i < n - 8; ++i) {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    v[i] = char(x);
  }
  Put64(v.data() + n - 8, Hash(v.data(), n - 8));
  return v;
}
void Validate(const std::string& v, uint64_t key, uint64_t version, size_t n,
              bool minimum = false) {
  if (v.size() != n || Get64(v.data()) != key ||
      Get64(v.data() + n - 8) != Hash(v.data(), n - 8) ||
      (minimum ? Get64(v.data() + 8) < version
               : Get64(v.data() + 8) != version))
    throw std::runtime_error("value mismatch key=" + std::to_string(key) +
                             " expected_version=" + std::to_string(version));
}
constexpr uint64_t kDeleted = uint64_t(1) << 63;
bool Absent(uint64_t state) { return state == 0 || (state & kDeleted) != 0; }
struct State {
  std::vector<uint64_t> version;
  explicit State(const Config& c, bool fresh) : version(c.N("keys"), 0) {
    if (fresh) return;
    std::ifstream f(c.S("state"), std::ios::binary);
    std::array<char, 24> h{};
    f.read(h.data(), h.size());
    if (!f || std::string(h.data(), 8) != "MBNPST01" ||
        Get64(h.data() + 8) != version.size() ||
        Get64(h.data() + 16) != c.N("value-size"))
      throw std::runtime_error("invalid state header");
    std::array<char, 8> b;
    for (auto& v : version) {
      f.read(b.data(), 8);
      if (!f) throw std::runtime_error("truncated state");
      v = Get64(b.data());
    }
    if (f.peek() != std::char_traits<char>::eof())
      throw std::runtime_error("state trailing bytes");
  }
  void Save(const Config& c) {
    auto p = c.S("state") + ".tmp";
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    std::array<char, 24> h{};
    std::memcpy(h.data(), "MBNPST01", 8);
    Put64(h.data() + 8, version.size());
    Put64(h.data() + 16, c.N("value-size"));
    f.write(h.data(), h.size());
    std::array<char, 8> b;
    for (auto v : version) {
      Put64(b.data(), v);
      f.write(b.data(), 8);
    }
    f.close();
    if (!f) throw std::runtime_error("state write failed");
    int fd = open(p.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("state open failed");
    int rc = fsync(fd);
    close(fd);
    if (rc) throw std::runtime_error("state fsync failed");
    std::filesystem::rename(p, c.S("state"));
    fd = open(std::filesystem::path(c.S("state")).parent_path().c_str(),
              O_RDONLY | O_DIRECTORY);
    if (fd < 0) throw std::runtime_error("state directory open failed");
    rc = fsync(fd);
    close(fd);
    if (rc) throw std::runtime_error("state directory fsync failed");
  }
};
struct Histogram {
  std::array<uint64_t, 2048> bins{};
  uint64_t count = 0;
  static size_t Bucket(uint64_t us) {
    if (us < 32) return us;
    size_t power = 0;
    uint64_t v = us;
    while (v >= 64) {
      v >>= 1;
      ++power;
    }
    return std::min<size_t>(2047, 32 + power * 32 + (v - 32));
  }
  static uint64_t Upper(size_t b) {
    if (b < 32) return b;
    size_t power = (b - 32) / 32;
    if (power >= 58) return std::numeric_limits<uint64_t>::max();
    return ((uint64_t(33 + (b - 32) % 32)) << power) - 1;
  }
  void Add(uint64_t us) {
    ++bins[Bucket(us)];
    ++count;
  }
  void Merge(const Histogram& h) {
    count += h.count;
    for (size_t i = 0; i < bins.size(); ++i) bins[i] += h.bins[i];
  }
  uint64_t Percent(unsigned p) const {
    uint64_t goal = (count * p + 99) / 100, n = 0;
    for (size_t i = 0; i < bins.size(); ++i) {
      n += bins[i];
      if (n >= goal) return Upper(i);
    }
    return 0;
  }
  std::string Json() const {
    std::ostringstream s;
    s << "[";
    bool first = true;
    for (size_t i = 0; i < bins.size(); ++i)
      if (bins[i]) {
        if (!first) s << ",";
        first = false;
        s << "[" << Upper(i) << "," << bins[i] << "]";
      }
    return s.str() + "]";
  }
};
uint64_t Random(uint64_t& x) {
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  return x;
}
void Phase(const std::string& name, Clock::time_point t) {
  Emit("phase",
       ",\"name\":" + Q(name) + ",\"elapsed_us\":" + std::to_string(Us(t)));
}
uint64_t Verify(Database& db, const Config& c, State& state) {
  bool minimum = !c.S("verify-acks").empty();
  if (minimum) {
    std::ifstream ack(c.S("verify-acks"));
    if (!ack) throw std::runtime_error("ACK file unavailable");
    uint64_t k, v;
    while (ack >> k >> v) {
      if (k >= state.version.size() || v == 0)
        throw std::runtime_error("invalid ACK");
      state.version[k] = std::max(state.version[k], v);
    }
    if (!ack.eof())
      throw std::runtime_error("invalid ACK file; expected key version lines");
  }
  const bool sampled = c.N("verify-samples") != 0;
  std::vector<uint64_t> selected;
  if (sampled) {
    selected = mb_native::VerificationKeys(state.version, c.N("threads"),
                                           c.N("verify-samples"), c.N("seed"));
  }
  uint64_t live = 0, deleted = 0, unwritten = 0;
  std::string v;
  auto t = Clock::now();
  bool first = true;
  const uint64_t count = sampled ? selected.size() : state.version.size();
  for (uint64_t offset = 0; offset < count; ++offset) {
    const auto k = sampled ? selected[offset] : offset;
    auto s = db.Get(Key(k), &v);
    if (Absent(state.version[k])) {
      if (!s.IsNotFound()) {
        Check(s);
        throw std::runtime_error("deleted key present");
      }
      if (state.version[k] == 0) ++unwritten;
      else ++deleted;
    } else {
      Check(s);
      Validate(v, k, state.version[k], c.N("value-size"), minimum);
      if (minimum) state.version[k] = Get64(v.data() + 8);
      ++live;
    }
    if (first) {
      Phase("first_verified_read", t);
      first = false;
    }
  }
  if (!sampled) {
    auto it = db.Iter();
    uint64_t seen = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) ++seen;
    Check(it->status());
    if (seen != live) throw std::runtime_error("unexpected key count");
  } else {
    std::ostringstream fields;
    fields << ",\"verification_protocol\":\"metabypass-key-sample-v1\""
           << ",\"mode\":\"sample\",\"population\":" << state.version.size()
           << ",\"requested\":" << c.N("verify-samples")
           << ",\"actual\":" << selected.size()
           << ",\"derived_seed\":" << mb_native::VerificationSeed(c.N("seed"))
           << ",\"live_checked\":" << live << ",\"deleted_checked\":" << deleted
           << ",\"unwritten_checked\":" << unwritten
           << ",\"full_keycount_checked\":false,\"keys\":[";
    for (size_t index = 0; index < selected.size(); ++index) {
      if (index != 0) fields << ",";
      fields << selected[index];
    }
    fields << "]";
    Emit("verification", fields.str());
  }
  Phase("verify", t);
  return count;
}
void Work(Database& db, const Config& c, State& state, bool warmup = false) {
  const auto workload = c.S("action") == "prepare" ? "insert" : c.S("workload");
  const bool ack = c.S("action") == "crashwrite";
  const uint64_t seconds =
      c.S("action") == "prepare"
          ? 0
          : (warmup ? c.N("warmup-seconds") : c.N("measure-seconds"));
  const uint64_t threads = c.N("threads"), keys = c.N("keys"),
                 ops = c.S("action") == "prepare" ? keys : c.N("ops");
  const bool timed = seconds > 0;
  db.Stats(warmup ? "warmup_begin" : "foreground_begin");
  auto start = Clock::now();
  std::atomic<bool> failed{false};
  std::string error;
  std::mutex error_mutex;
  std::vector<Histogram> hist(threads), gets(threads), puts(threads),
      deletes(threads);
  std::vector<uint64_t> rows(threads, 0), hot_count(threads, 0),
      cold_count(threads, 0);
  std::vector<std::thread> workers;
  for (uint64_t id = 0; id < threads; ++id)
    workers.emplace_back([&, id] {
      try {
        uint64_t lo = keys * id / threads, hi = keys * (id + 1) / threads,
                 width = hi - lo;
        uint64_t rng = c.N("seed") + id + 1, seq = 0;
        std::string value;
        uint64_t quota = ops * (id + 1) / threads - ops * id / threads;
        if (workload == "insert") quota = width;
        while (!failed &&
               (timed ? Us(start) < seconds * 1000000 : seq < quota)) {
          auto r = Random(rng);
          uint64_t k = lo + (r % width);
          if (workload == "insert" || workload == "gc" || ack)
            k = lo + (seq % width);
          if (workload == "hotspot" || workload == "sst") {
            uint64_t hot = std::max<uint64_t>(1, width / 4);
            bool last =
                workload == "sst" && seconds && Us(start) > seconds * 500000;
            bool in_hot = Random(rng) % 10 < 8 || width == hot;
            if (in_hot) {
              k = lo + (last ? width - hot : 0) + Random(rng) % hot;
              ++hot_count[id];
            } else {
              k = lo + (last ? 0 : hot) + Random(rng) % (width - hot);
              ++cold_count[id];
            }
          }
          auto key = Key(k);
          bool put = workload == "insert" || workload == "overwrite" ||
                     workload == "sync" || workload == "gc" || ack;
          bool del = false;
          if (workload == "mixed") {
            auto choice = Random(rng) % 100;
            put = choice >= 90 && choice < 95;
            del = choice >= 95;
          }
          uint64_t version = (state.version[k] & ~kDeleted) + 1;
          if (version >= kDeleted) throw std::runtime_error("version overflow");
          if (put) value = Value(k, version, c.N("value-size"));
          auto t = Clock::now();
          if (put) {
            Check(db.Put(key, value, c.B("sync") || workload == "sync" || ack));
            auto latency = Us(t);
            hist[id].Add(latency);
            puts[id].Add(latency);
            state.version[k] = version;
            if (ack)
              Emit("ack", ",\"key\":" + std::to_string(k) +
                              ",\"version\":" + std::to_string(version));
            if (ack) db.gate->Wait("write", key);
          } else if (del) {
            Check(db.Delete(key, c.B("sync")));
            auto latency = Us(t);
            hist[id].Add(latency);
            deletes[id].Add(latency);
            state.version[k] = version | kDeleted;
          } else if (workload == "scan") {
            auto it = db.Iter();
            uint64_t n = 0;
            for (it->Seek(key); it->Valid() && n < 100; it->Next(), ++n) {
              auto ik = it->key().ToString();
              if (ik.size() != 16)
                throw std::runtime_error("scan malformed key");
              auto actual = std::stoull(ik, nullptr, 16);
              if (actual >= keys)
                throw std::runtime_error("scan key outside range");
              Validate(it->value().ToString(), actual, state.version[actual],
                       c.N("value-size"));
            }
            Check(it->status());
            rows[id] += n;
            hist[id].Add(Us(t));
          } else {
            auto s = db.Get(key, &value);
            auto latency = Us(t);
            hist[id].Add(latency);
            gets[id].Add(latency);
            if (Absent(state.version[k])) {
              if (!s.IsNotFound()) {
                Check(s);
                throw std::runtime_error("expected deletion");
              }
            } else {
              Check(s);
              Validate(value, k, state.version[k], c.N("value-size"));
            }
          }
          ++seq;
          if (c.N("flush-every") && seq % c.N("flush-every") == 0) db.Flush();
        }
      } catch (const std::exception& e) {
        failed = true;
        std::lock_guard<std::mutex> lock(error_mutex);
        error = e.what();
      }
    });
  for (auto& t : workers) t.join();
  if (failed) throw std::runtime_error(error);
  auto elapsed = Us(start);
  Histogram total, get, put, del;
  uint64_t scanned = 0, hot_total = 0, cold_total = 0;
  for (size_t i = 0; i < hist.size(); ++i) {
    total.Merge(hist[i]);
    get.Merge(gets[i]);
    put.Merge(puts[i]);
    del.Merge(deletes[i]);
    scanned += rows[i];
    hot_total += hot_count[i];
    cold_total += cold_count[i];
  }
  std::ostringstream s;
  s << ",\"name\":" << Q(warmup ? "warmup" : "foreground")
    << ",\"ops\":" << total.count << ",\"elapsed_us\":" << elapsed
    << ",\"ops_per_sec\":"
    << (elapsed ? double(total.count) * 1000000 / elapsed : 0)
    << ",\"p50_us\":" << total.Percent(50)
    << ",\"p95_us\":" << total.Percent(95)
    << ",\"p99_us\":" << total.Percent(99) << ",\"hist_us\":" << total.Json()
    << ",\"get_hist_us\":" << get.Json() << ",\"put_hist_us\":" << put.Json()
    << ",\"delete_hist_us\":" << del.Json() << ",\"scanned_rows\":" << scanned
    << ",\"hot_ops\":" << hot_total << ",\"cold_ops\":" << cold_total;
  Emit("sample", s.str());
  db.Stats(warmup ? "warmup_end" : "foreground_end");
}
}  // namespace
int main(int argc, char** argv) {
  try {
    Config c(argc, argv);
    bool fresh = c.B("fresh") || c.S("action") == "prepare";
    if (fresh && std::filesystem::exists(c.S("state")))
      throw std::runtime_error("refuse existing state for fresh database");
    State state(c, fresh);
    Database db(c);
    uint64_t verified = 0;
    std::ostringstream options;
    options << ",\"variant\":" << Q(c.S("variant"))
            << ",\"effective_options\":{\"compression\":\"none\",\"write_"
               "buffer_size\":134217728,\"max_write_buffer_number\":2,\"max_"
               "background_jobs\":4,\"block_cache_bytes\":"
            << c.N("cache-bytes") << ",\"disable_auto_compactions\":"
            << (c.B("disable-auto-compactions") ? "true" : "false")
            << ",\"enable_blob_files\":"
            << (c.S("variant") == "U" ? "false" : "true")
            << ",\"enable_blob_direct_write\":"
            << (c.B("direct-write") ? "true" : "false")
            << ",\"allow_concurrent_memtable_write\":"
            << (c.B("direct-write") ? "false" : "true")
            << ",\"mirror_interval_ms\":" << c.N("mirror-interval-ms")
            << ",\"checkpoint_interval_ms\":" << c.N("checkpoint-interval-ms")
            << ",\"sync\":" << (c.B("sync") ? "true" : "false")
            << ",\"min_blob_size\":0,\"blob_"
               "value_gc\":false,\"sst_mode\":"
            << Q(c.S("sst-mode")) << ",\"sst_budget\":" << c.N("sst-budget")
            << ",\"staging_capacity\":"
            << (c.S("variant") == "S" ? c.N("staging-capacity") : 0)
            << ",\"whole_blob_gc\":" << (c.B("gc") ? "true" : "false") << "}";
    Emit("options", options.str());
    if (c.S("action") == "restore") {
      auto t = Clock::now();
      db.Restore();
      Phase("restore", t);
    }
    auto t = Clock::now();
    db.Open(fresh);
    Phase("open", t);
    db.gate->armed = true;
    auto life = Clock::now();
    if (c.S("action") == "verify" || c.S("action") == "restore")
      verified = Verify(db, c, state);
    else if (c.S("action") == "append-reopen") {
      auto version = (state.version[0] & ~kDeleted) + 1;
      Check(db.Put(Key(0), Value(0, version, c.N("value-size")), true));
      state.version[0] = version;
      db.Close();
      db.Open(false);
      verified = Verify(db, c, state);
      state.Save(c);
    } else {
      if (c.S("action") != "prepare" && c.N("warmup-seconds"))
        Work(db, c, state, true);
      if (c.S("action") != "prepare" && c.S("workload") == "gc") {
        for (int gen = 0; gen < 3; ++gen) {
          Work(db, c, state);
          t = Clock::now();
          db.Flush();
          Phase("generation_flush", t);
          t = Clock::now();
          db.Compact();
          Phase("generation_compact", t);
          db.Sync();
          db.Stats();
        }
      } else
        Work(db, c, state);
    }
    if (c.S("action") == "prepare") {
      if (!std::all_of(state.version.begin(), state.version.end(),
                       [](uint64_t v) { return v == 1; }))
        throw std::runtime_error(
            "prepare did not initialize every key exactly once");
      Emit("prepared", ",\"keys\":" + std::to_string(state.version.size()));
    }
    // Read-only measurements do not force flush or publish an explicit point.
    const bool mutating =
        c.S("action") == "prepare" || c.S("action") == "append-reopen" ||
        c.S("action") == "crashwrite" ||
        (c.S("action") == "run" &&
         std::set<std::string>{"insert", "overwrite", "mixed", "sync", "gc"}
             .count(c.S("workload")));
    if (mutating) {
      if (c.B("flush")) {
        t = Clock::now();
        db.Flush();
        Phase("flush", t);
      }
      t = Clock::now();
      db.Sync();
      Phase("sync_backup", t);
    }
    if (c.N("gc-wait-seconds")) {
      t = Clock::now();
      auto until = Clock::now() + std::chrono::seconds(c.N("gc-wait-seconds"));
      while (Clock::now() < until) {
        db.Stats();
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      Phase("background_tail", t);
    }
    db.Stats();
    t = Clock::now();
    db.Close();
    Phase("close", t);
    Phase("lifecycle", life);
    if (mutating || !c.S("verify-acks").empty()) state.Save(c);
    struct rusage usage {};
    getrusage(RUSAGE_SELF, &usage);
    Emit("resources",
         ",\"peak_rss_kib\":" + std::to_string(usage.ru_maxrss) +
             ",\"user_cpu_us\":" +
             std::to_string(uint64_t(usage.ru_utime.tv_sec) * 1000000 +
                            usage.ru_utime.tv_usec) +
             ",\"system_cpu_us\":" +
             std::to_string(uint64_t(usage.ru_stime.tv_sec) * 1000000 +
                            usage.ru_stime.tv_usec));
    Emit("done", ",\"status\":\"pass\",\"variant\":" + Q(c.S("variant")) +
                     ",\"action\":" + Q(c.S("action")) +
                     ",\"workload\":" + Q(c.S("workload")) +
                     ",\"verified_keys\":" + std::to_string(verified));
    return 0;
  } catch (const std::exception& e) {
    Emit("error", ",\"status\":\"failed\",\"message\":" + Q(e.what()));
    return 1;
  }
}
