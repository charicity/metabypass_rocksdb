// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <latch>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/core_workload.h"
#include "dataset.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/metabypass.h"
#include "rocksdb/version.h"
#include "rocksdb/write_batch.h"

namespace {
namespace rdb = ROCKSDB_NAMESPACE;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using std::chrono_literals::operator""s;

constexpr size_t kMiB = 1024 * 1024;
constexpr size_t kCacheBytes = 256 * kMiB;
constexpr size_t kWriteBufferBytes = 128 * kMiB;
constexpr int kWriteBuffers = 3;
constexpr int kBackgroundJobs = 4;
constexpr uint64_t kMirrorMs = 1000;
constexpr size_t kMirrorBatchBytes = 512 * kMiB;
constexpr size_t kWalQueueBytes = 896 * kMiB;
constexpr size_t kMetadataQueueBytes = 128 * kMiB;

uint64_t Nanos(Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             t.time_since_epoch())
      .count();
}

uint64_t WallMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string Quote(std::string_view value) {
  std::string result = "\"";
  constexpr char kHex[] = "0123456789abcdef";
  for (unsigned char ch : value) {
    if (ch == '"' || ch == '\\') {
      result += '\\';
      result += ch;
    } else if (ch < 32 || ch >= 127) {
      result += "\\u00";
      result += kHex[ch >> 4];
      result += kHex[ch & 15];
    } else {
      result += ch;
    }
  }
  result += '"';
  return result;
}

void Emit(std::string_view event, const std::string& fields = {}) {
  static std::mutex output_mutex;
  std::lock_guard<std::mutex> lock(output_mutex);
  std::cout << "{\"event\":" << Quote(event) << fields << '}' << std::endl;
}

template <typename Integer>
std::string Number(std::string_view field, Integer value) {
  return "," + Quote(field) + ":" + std::to_string(value);
}

std::string String(std::string_view field, std::string_view value) {
  return "," + Quote(field) + ":" + Quote(value);
}

std::string OptionalNumber(std::string_view field, uint64_t value,
                           bool present) {
  return present ? Number(field, value) : "," + Quote(field) + ":null";
}

void Require(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

template <typename Status>
void Check(const Status& status, const char* context) {
  if (!status.ok()) {
    throw std::runtime_error(std::string(context) + ": " + status.ToString());
  }
}

uint64_t Parse(std::string_view value, std::string_view label, uint64_t min,
               uint64_t max) {
  uint64_t result = 0;
  const auto end = value.data() + value.size();
  const auto parsed = std::from_chars(value.data(), end, result);
  Require(parsed.ec == std::errc{} && parsed.ptr == end && result >= min &&
              result <= max,
          "invalid " + std::string(label));
  return result;
}

std::string Absolute(const char* path) {
  return fs::absolute(path).lexically_normal().string();
}

enum class Engine { kMetaBypass, kBE, kNative };

Engine ParseEngine(std::string_view engine) {
  if (engine == "mb") {
    return Engine::kMetaBypass;
  }
  if (engine == "be") {
    return Engine::kBE;
  }
  Require(engine == "native", "engine must be mb, be or native");
  return Engine::kNative;
}

rdb::Options DBOptions(bool create) {
  rdb::Options options;
  options.create_if_missing = create;
  options.error_if_exists = create;
  options.compression = rdb::kNoCompression;
  options.enable_blob_files = false;
  options.write_buffer_size = kWriteBufferBytes;
  options.max_write_buffer_number = kWriteBuffers;
  options.max_background_jobs = kBackgroundJobs;
  rdb::BlockBasedTableOptions table;
  table.block_cache = rdb::NewLRUCache(kCacheBytes);
  options.table_factory.reset(rdb::NewBlockBasedTableFactory(table));
  return options;
}

rdb::BackupEngineOptions BEOptions(const std::string& backup) {
  rdb::BackupEngineOptions options(backup);
  options.share_table_files = true;
  options.sync = true;
  options.backup_log_files = true;
  return options;
}

template <typename Options>
void SetDataDirectory(Options& bypass) {
  if constexpr (requires { bypass.data_dir; }) {
    // Historical cores require the identity/lock directory with Blob off.
    bypass.data_dir =
        (fs::path(bypass.backup_dir).parent_path() / "data").string();
  }
}

struct SstAsyncConfig {
  int min_level = -1;
  uint64_t delay_ms = 1000;
};

SstAsyncConfig SstAsyncFromEnvironment() {
  SstAsyncConfig config;
  if (const char* level = std::getenv("MB_YCSB_SST_ASYNC_MIN_LEVEL")) {
    if (std::string_view(level) != "-1") {
      config.min_level =
          static_cast<int>(Parse(level, "SST async minimum level", 1,
                                 std::numeric_limits<int>::max()));
    }
  }
  if (const char* delay = std::getenv("MB_YCSB_SST_ASYNC_DELAY_MS")) {
    config.delay_ms = Parse(delay, "SST async delay milliseconds", 0,
                            std::numeric_limits<uint64_t>::max());
  }
  return config;
}

template <typename Options>
void SetSstAsyncConfig(Options& bypass, const SstAsyncConfig& config) {
  if constexpr (requires {
                  bypass.sst_async_min_level;
                  bypass.sst_async_delay_ms;
                }) {
    bypass.sst_async_min_level = config.min_level;
    bypass.sst_async_delay_ms = config.delay_ms;
  } else {
    Require(config.min_level == -1 && config.delay_ms == 1000,
            "this RocksDB core does not support SST async staging options");
  }
}

template <typename Stats>
std::string BackupStatsFields(const Stats& stats) {
  auto fields = Number("recovery_points", stats.recovery_points) +
                Number("last_publish_micros", stats.last_publish_micros) +
                Number("last_build_micros", stats.last_build_micros) +
                Number("peak_queued_bytes", stats.peak_queued_bytes);
  if constexpr (requires { stats.io_backup_write_bytes; }) {
    fields += Number("io_backup_write_bytes", stats.io_backup_write_bytes);
  }
  if constexpr (requires { stats.last_checkpoint_sst_copy_bytes; }) {
    fields +=
        Number("last_checkpoint_sst_copy_bytes",
               stats.last_checkpoint_sst_copy_bytes) +
        Number("last_checkpoint_sst_ready_bytes",
               stats.last_checkpoint_sst_ready_bytes) +
        Number("last_checkpoint_sst_wait_micros",
               stats.last_checkpoint_sst_wait_micros) +
        Number("last_checkpoint_sst_micros", stats.last_checkpoint_sst_micros) +
        Number("sst_async_copy_bytes", stats.sst_async_copy_bytes) +
        Number("sst_async_discarded_bytes", stats.sst_async_discarded_bytes) +
        Number("sst_async_cancelled_bytes", stats.sst_async_cancelled_bytes) +
        Number("sst_async_staged_bytes", stats.sst_async_staged_bytes) +
        Number("wal_backpressure_micros", stats.wal_backpressure_micros) +
        Number("wal_queued_bytes", stats.wal_queued_bytes);
  }
  return fields;
}

class Database {
 public:
  Database(Engine engine, std::string backup, uint64_t checkpoint_ms,
           SstAsyncConfig sst_async = {})
      : engine_(engine), sst_async_(sst_async) {
    bypass_.backup_dir = std::move(backup);
    bypass_.interval_ms = kMirrorMs;
    bypass_.batch_bytes = kMirrorBatchBytes;
    bypass_.wal_queue_capacity = kWalQueueBytes;
    bypass_.metadata_queue_capacity = kMetadataQueueBytes;
    if (engine == Engine::kMetaBypass) {
      SetDataDirectory(bypass_);
      SetSstAsyncConfig(bypass_, sst_async_);
    }
    if (checkpoint_ms != 0) {
      bypass_.checkpoint_interval_ms = checkpoint_ms;
    }
  }

  void Open(const std::string& path, bool create) {
    auto options = DBOptions(create);
    if (engine_ == Engine::kMetaBypass) {
      Check(rdb::MetaBypassDB::Open(options, bypass_, path, &mb_), "MB Open");
    } else {
      Check(rdb::DB::Open(options, path, &native_), "DB Open");
      if (create && engine_ == Engine::kBE) {
        rdb::BackupEngine* backup = nullptr;
        Check(rdb::BackupEngine::Open(BEOptions(bypass_.backup_dir),
                                      options.env, &backup),
              "BE Open");
        be_.reset(backup);
      }
    }
  }

  void Close() { Check(mb_ ? mb_->Close() : native_->Close(), "DB Close"); }

  void Load(const mb_ycsb::Dataset& dataset) {
    rdb::WriteBatch batch;
    const auto flush_batch = [&] {
      Check(mb_ ? mb_->Write({}, &batch) : native_->Write({}, &batch),
            "initial Write");
      batch.Clear();
    };
    dataset.Replay([&](const std::string& key, const std::string& value) {
      Check(batch.Put(key, value), "initial WriteBatch Put");
      if (batch.Count() == 1024 || batch.GetDataSize() >= 8 * kMiB) {
        flush_batch();
      }
    });
    if (batch.Count() != 0) {
      flush_batch();
    }
    rdb::FlushOptions flush;
    flush.wait = true;
    Check(mb_ ? mb_->Flush(flush) : native_->Flush(flush), "initial Flush");
  }

  rdb::Status Restore(const std::string& target,
                      rdb::MetaBypassRecoveryReport* report) {
    if (engine_ == Engine::kMetaBypass) {
      return rdb::MetaBypassDB::Restore(DBOptions(false), bypass_, target,
                                        report);
    }
    rdb::BackupEngineReadOnly* impl = nullptr;
    auto status = rdb::BackupEngineReadOnly::Open(BEOptions(bypass_.backup_dir),
                                                  rdb::Env::Default(), &impl);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<rdb::BackupEngineReadOnly> backup(impl);
    return backup->RestoreDBFromLatestBackup(rdb::RestoreOptions{}, target,
                                             target);
  }

  rdb::Status Get(const std::string& key, std::string* value) {
    return mb_ ? mb_->Get({}, key, value) : native_->Get({}, key, value);
  }

  rdb::Status Put(const std::string& key, const std::string& value) {
    return mb_ ? mb_->Put({}, key, value) : native_->Put({}, key, value);
  }

  std::unique_ptr<rdb::Iterator> Iterator(bool fill_cache = true) {
    rdb::ReadOptions options;
    options.fill_cache = fill_cache;
    return std::unique_ptr<rdb::Iterator>(mb_ ? mb_->NewIterator(options)
                                              : native_->NewIterator(options));
  }

  uint64_t Sequence() {
    if (mb_) {
      rdb::MetaBypassRecoveryProgress current;
      Check(mb_->GetRecoveryProgress(&current), "MB progress");
      return current.visible_sequence;
    }
    return native_->GetLatestSequenceNumber();
  }

  void ReportSequence() {
    const auto begin = Clock::now();
    const auto wall_begin = WallMicros();
    const auto sequence = Sequence();
    const auto wall_end = WallMicros();
    const auto end = Clock::now();
    auto fields = Number("query_begin_ns", Nanos(begin)) +
                  Number("query_end_ns", Nanos(end)) +
                  Number("query_begin_wall_us", wall_begin) +
                  Number("query_end_wall_us", wall_end) +
                  Number("sequence", sequence);
    Emit("progress", fields + ReportBackupStats());
  }

  std::string ReportBackupStats() const {
    if (!mb_) {
      return {};
    }
    const auto stats = mb_->GetBackupStats();
    const auto sample_time = Clock::now();
    Check(stats.error, "MB background backup");
    return Number("stats_sample_monotonic_ns", Nanos(sample_time)) +
           BackupStatsFields(stats);
  }

  void Backup(bool initial) {
    if (engine_ == Engine::kNative) {
      return;
    }
    const auto start = Clock::now();
    rdb::BackupID id = 0;
    std::string stats;
    if (mb_) {
      Check(mb_->SyncBackup(), "MB initial backup");
      stats = ReportBackupStats();
    } else {
      rdb::CreateBackupOptions options;
      options.flush_before_backup = false;
      Check(be_->CreateNewBackup(options, native_.get(), &id), "BE backup");
      Check(be_->PurgeOldBackups(1), "BE purge");
    }
    const auto end = Clock::now();
    Emit("backup", std::string(",\"initial\":") + (initial ? "true" : "false") +
                       ",\"completed\":true" +
                       OptionalNumber("backup_id", id, be_ != nullptr) +
                       Number("begin_ns", Nanos(start)) +
                       Number("end_ns", Nanos(end)) +
                       (mb_ ? stats : ",\"recovery_points\":null"));
  }

  std::string ReportOptions() const {
    auto fields = Number("cache_bytes", kCacheBytes) +
                  Number("write_buffer_bytes", kWriteBufferBytes) +
                  Number("max_write_buffer_number", kWriteBuffers) +
                  Number("max_background_jobs", kBackgroundJobs) +
                  ",\"wal\":true,\"sync\":false,\"compression\":\"none\"" +
                  ",\"blob\":false,\"staging\":false,\"sst_tiering\":false";
    if (engine_ == Engine::kMetaBypass) {
      fields +=
          Number("mirror_ms", bypass_.interval_ms) +
          Number("wal_queue_bytes", bypass_.wal_queue_capacity) +
          Number("metadata_queue_bytes", bypass_.metadata_queue_capacity) +
          Number("mirror_batch_bytes", bypass_.batch_bytes) +
          Number("sst_async_min_level", sst_async_.min_level) +
          Number("sst_async_delay_ms", sst_async_.delay_ms);
    } else {
      fields += ",\"mirror_ms\":null";
    }
    return fields;
  }

 private:
  Engine engine_;
  SstAsyncConfig sst_async_;
  rdb::MetaBypassOptions bypass_;
  std::unique_ptr<rdb::MetaBypassDB> mb_;
  std::unique_ptr<rdb::DB> native_;
  std::unique_ptr<rdb::BackupEngine> be_;
};

struct OperationIO {
  uint64_t get_calls = 0;
  uint64_t put_calls = 0;
  uint64_t scan_calls = 0;
  uint64_t read_bytes = 0;
  uint64_t write_bytes = 0;
  uint64_t scan_rows = 0;
};

class Adapter final : public ycsbc::DB {
 public:
  explicit Adapter(Database& db) : db_(db) {}

  Status Read(const std::string&, const std::string& key,
              const std::vector<std::string>*,
              std::vector<Field>& result) override {
    std::string value;
    ++io.get_calls;
    // CoreWorkload's read-modify-write does not inspect the Read return code.
    Check(db_.Get(key, &value), "YCSB Get");
    io.read_bytes += value.size();
    result.push_back({"field0", std::move(value)});
    return kOK;
  }

  Status Scan(const std::string&, const std::string& key, int length,
              const std::vector<std::string>*,
              std::vector<std::vector<Field>>& result) override {
    ++io.scan_calls;
    auto iterator = db_.Iterator();
    for (iterator->Seek(key); iterator->Valid() && length > 0;
         iterator->Next(), --length) {
      const auto value = iterator->value();
      ++io.scan_rows;
      io.read_bytes += value.size();
      result.push_back({{"field0", value.ToString()}});
    }
    Check(iterator->status(), "YCSB Scan");
    return kOK;
  }

  Status Insert(const std::string&, const std::string& key,
                std::vector<Field>& fields) override {
    return Put(key, fields);
  }

  Status Update(const std::string&, const std::string& key,
                std::vector<Field>& fields) override {
    return Put(key, fields);
  }

  Status Delete(const std::string&, const std::string&) override {
    throw std::runtime_error("delete is not supported by YCSB A-F");
  }

  OperationIO io;

 private:
  Status Put(const std::string& key, const std::vector<Field>& fields) {
    ++io.put_calls;
    const auto& value = fields.front().value;
    Check(db_.Put(key, value), "YCSB Put");
    io.write_bytes += value.size();
    return kOK;
  }

  Database& db_;
};

struct Profile {
  Profile(const char* path, const mb_ycsb::Metadata& metadata) {
    std::ifstream file(path);
    Require(file.is_open(), "cannot open YCSB properties");
    raw.assign(std::istreambuf_iterator<char>(file),
               std::istreambuf_iterator<char>());
    file.clear();
    file.seekg(0);
    props.Load(file);
    mb_ycsb::SetDatasetProperties(props, metadata.record_count,
                                  metadata.value_size);
    props.SetProperty("readallfields", "true");
    props.SetProperty("writeallfields", "true");
  }

  std::string Effective() const {
    return ",\"effective_workload\":{\"properties_file\":" + Quote(raw) +
           ",\"profile\":\"one_raw_field_hashed_user_v1\"" +
           Number("recordcount", std::stoull(props["recordcount"])) +
           Number("fieldlength", std::stoull(props["fieldlength"])) +
           ",\"readallfields\":true,\"writeallfields\":true}";
  }

  bool MayUpdate() const {
    return std::stod(props.GetProperty(
               ycsbc::CoreWorkload::UPDATE_PROPORTION_PROPERTY,
               ycsbc::CoreWorkload::UPDATE_PROPORTION_DEFAULT)) > 0 ||
           std::stod(props.GetProperty(
               ycsbc::CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY,
               ycsbc::CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT)) > 0;
  }

  std::string raw;
  ycsbc::utils::Properties props;
};

// Each counter has exactly one worker publishing it. The controller snapshots
// completed (published) operations at the deadline, even if other operations
// remain blocked in RocksDB. It never waits on a worker to finish an operation.
struct Published {
  struct Counter {
    void Add(uint64_t value) {
      count.store(Read() + value, std::memory_order_relaxed);
    }

    uint64_t Read() const { return count.load(std::memory_order_relaxed); }

    std::atomic<uint64_t> count{0};
  };

  void Add(const OperationIO& io, uint64_t latency_ns) {
    gets.Add(io.get_calls);
    puts.Add(io.put_calls);
    scans.Add(io.scan_calls);
    read.Add(io.read_bytes);
    written.Add(io.write_bytes);
    rows.Add(io.scan_rows);
    // Bit width buckets have upper bounds 0, 1, 3, 7, ... nanoseconds.
    latencies[std::bit_width(latency_ns)].Add(1);
  }

  std::array<Counter, 65> latencies;
  Counter gets, puts, scans, read, written, rows;
};

std::string Summarize(const std::vector<Published>& all) {
  std::array<uint64_t, 65> latency{};
  OperationIO io;
  uint64_t count = 0;
  for (const auto& worker : all) {
    for (size_t i = 0; i < latency.size(); ++i) {
      const auto part = worker.latencies[i].Read();
      latency[i] += part;
      count += part;
    }
    io.get_calls += worker.gets.Read();
    io.put_calls += worker.puts.Read();
    io.scan_calls += worker.scans.Read();
    io.read_bytes += worker.read.Read();
    io.write_bytes += worker.written.Read();
    io.scan_rows += worker.rows.Read();
  }
  auto percentile = [&](uint64_t p) {
    const auto target = (count * p + 99) / 100;
    uint64_t seen = 0;
    for (size_t i = 0; i < latency.size() - 1; ++i) {
      seen += latency[i];
      if (seen >= target) {
        return (uint64_t{1} << i) - 1;
      }
    }
    return std::numeric_limits<uint64_t>::max();
  };
  return Number("attempted", count) + Number("succeeded", count) +
         Number("failed", 0) + Number("get_calls", io.get_calls) +
         Number("put_calls", io.put_calls) +
         Number("scan_calls", io.scan_calls) +
         Number("read_bytes", io.read_bytes) +
         Number("write_bytes", io.write_bytes) +
         Number("scan_rows", io.scan_rows) +
         OptionalNumber("p50_ns", percentile(50), count != 0) +
         OptionalNumber("p95_ns", percentile(95), count != 0) +
         OptionalNumber("p99_ns", percentile(99), count != 0);
}

std::string DatasetFields(const mb_ycsb::Metadata& metadata) {
  return String("dataset_sha256", metadata.file_sha256) +
         String("dataset_logical_sha256", metadata.logical_sha256) +
         Number("initial_records", metadata.record_count) +
         Number("value_bytes", metadata.value_size);
}

[[noreturn]] void ChildFailure(const std::exception& error) {
  Emit("error", String("message", error.what()));
  std::_Exit(EXIT_FAILURE);
}

void Run(char** argv) {
  const auto engine = ParseEngine(argv[4]);
  const auto backup =
      engine == Engine::kNative ? std::string{} : Absolute(argv[6]);
  const auto threads = Parse(argv[7], "thread count", 1, 1024);
  const auto warmup =
      std::chrono::seconds(Parse(argv[8], "warmup seconds", 0, 86400));
  const auto measure =
      std::chrono::seconds(Parse(argv[9], "measurement seconds", 1, 86400));
  const auto interval = std::chrono::milliseconds(
      Parse(argv[10], "checkpoint milliseconds",
            engine == Engine::kNative ? 0 : 1, 86400000));
  const auto sst_async = engine == Engine::kMetaBypass
                             ? SstAsyncFromEnvironment()
                             : SstAsyncConfig{};
  Database db(engine, backup, interval.count(), sst_async);
  db.Open(Absolute(argv[5]), true);

  const auto load_start = Clock::now();
  mb_ycsb::Metadata initial;
  uint64_t verify_ns;
  uint64_t load_ns;
  {
    const mb_ycsb::Dataset dataset(argv[2]);
    db.Load(dataset);
    const auto verify_start = Clock::now();
    load_ns = Nanos(verify_start) - Nanos(load_start);
    initial = dataset.Verify(*db.Iterator(false));
    verify_ns = Nanos(Clock::now()) - Nanos(verify_start);
  }
  const auto sequence = db.Sequence();
  db.Backup(true);

  Profile profile(argv[3], initial);
  ycsbc::CoreWorkload workload;
  workload.Init(profile.props);
  std::vector<Published> results(threads);
  std::vector<std::thread> workers;
  workers.reserve(threads);
  std::latch ready(threads);
  std::latch start(1);
  Clock::time_point begin, end;
  for (size_t i = 0; i < threads; ++i) {
    workers.emplace_back([&, i] {
      try {
        Adapter adapter(db);
        ready.count_down();
        start.wait();
        for (;;) {
          adapter.io = {};
          const auto op_begin = Clock::now();
          Require(workload.DoTransaction(adapter), "YCSB transaction failed");
          const auto op_end = Clock::now();
          if (op_end >= begin && op_end < end) {
            results[i].Add(adapter.io, Nanos(op_end) - Nanos(op_begin));
          }
        }
      } catch (const std::exception& e) {
        ChildFailure(e);
      }
    });
  }
  ready.wait();
  Emit("ready", DatasetFields(initial) + ",\"initial_verified\":true" +
                    String("initial_sha256", initial.logical_sha256) +
                    Number("initial_sequence", sequence) +
                    Number("load_ns", load_ns) +
                    Number("initial_verify_ns", verify_ns) +
                    String("engine", argv[4]) + Number("threads", threads) +
                    Number("warmup_seconds", warmup.count()) +
                    Number("measure_seconds", measure.count()) +
                    Number("checkpoint_ms", interval.count()) +
                    profile.Effective() + db.ReportOptions());

  const auto workload_start = Clock::now();
  begin = workload_start + warmup;
  end = begin + measure;
  start.count_down();
  std::thread reporter([&] {
    try {
      for (auto next = workload_start;; next += 1s) {
        std::this_thread::sleep_until(next);
        db.ReportSequence();
        next = std::max(next, Clock::now() - 1s);
      }
    } catch (const std::exception& e) {
      ChildFailure(e);
    }
  });
  std::unique_ptr<std::thread> backup_worker;
  if (engine == Engine::kBE) {
    backup_worker = std::make_unique<std::thread>([&] {
      try {
        auto next = workload_start + interval;
        for (;;) {
          std::this_thread::sleep_until(next);
          db.Backup(false);
          const auto elapsed = Clock::now() - workload_start;
          next = workload_start + interval * (elapsed / interval + 1);
        }
      } catch (const std::exception& e) {
        ChildFailure(e);
      }
    });
  }
  std::this_thread::sleep_until(begin);
  Emit("measurement_start",
       Number("monotonic_ns", Nanos(begin)) + db.ReportBackupStats());
  std::this_thread::sleep_until(end);
  const auto end_stats = db.ReportBackupStats();
  Emit("measurement_end", Number("monotonic_ns", Nanos(end)) +
                              Number("elapsed_ns", Nanos(end) - Nanos(begin)) +
                              Summarize(results) + end_stats);
  // The controller kills this process to model an abrupt SSD failure.
  // Continue the workload until the signal rather than closing either store.
  reporter.join();
}

void Recover(char** argv) {
  const auto engine = ParseEngine(argv[4]);
  Require(engine != Engine::kNative, "native has no backup to restore");
  const auto target = Absolute(argv[5]);
  const auto backup = Absolute(argv[6]);
  Require(!fs::exists(target), "restore target must be new");
  const auto initial = Parse(argv[7], "initial sequence", 1,
                             std::numeric_limits<uint64_t>::max());

  std::vector<std::pair<std::string, std::string>> samples;
  bool may_update;
  {
    const mb_ycsb::Dataset dataset(argv[2],
                                   mb_ycsb::Dataset::Access::kSamplesOnly);
    may_update = Profile(argv[3], dataset.metadata()).MayUpdate();
    const auto records = dataset.metadata().record_count;
    const auto count =
        std::min(records, Parse(argv[8], "sample count", 1,
                                std::numeric_limits<uint64_t>::max()));
    std::unordered_set<uint64_t> selected;
    std::mt19937_64 rng(0x79637362);
    std::uniform_int_distribution<uint64_t> ordinal(0, records - 1);
    while (samples.size() < count) {
      const auto id = ordinal(rng);
      if (selected.insert(id).second) {
        samples.push_back(dataset.ReadRecord(id));
      }
    }
  }

  Database db(engine, backup, 0);
  rdb::MetaBypassRecoveryReport report;
  const auto restore_begin = Clock::now();
  Check(db.Restore(target, &report), "Restore");
  const auto restore_end = Clock::now();
  db.Open(target, false);
  const auto open_end = Clock::now();
  // Read immediately after Open. Comparing to the initial fixture is valid
  // after insert-only/read-only workloads or when no mutations were recovered.
  const auto recovered = db.Sequence();
  Require(recovered >= initial, "restored sequence precedes the initial data");
  const auto mb = engine == Engine::kMetaBypass;
  Require(!mb || recovered == report.recovered_sequence,
          "open sequence differs from MB Restore report");
  const bool exact = !may_update || recovered == initial;
  uint64_t first_get_ns = 0;
  size_t checked = 0;
  for (const auto& [key, expected] : samples) {
    std::string value;
    Check(db.Get(key, &value), "recovery Get");
    Require(value.size() == expected.size(), "recovered value width mismatch");
    Require(!exact || value == expected, "recovered initial value mismatch");
    if (++checked == 1) {
      first_get_ns = Nanos(Clock::now()) - Nanos(open_end);
    }
  }
  const auto verify_end = Clock::now();
  db.Close();
  const auto close_end = Clock::now();
  Emit("recovery",
       Number("initial_sequence", initial) +
           Number("recovered_sequence", recovered) +
           OptionalNumber("checkpoint_sequence", report.checkpoint_sequence,
                          mb) +
           String("stop_reason", report.stop_reason) +
           Number("restore_start_ns", Nanos(restore_begin)) +
           Number("restore_ns", Nanos(restore_end) - Nanos(restore_begin)) +
           Number("open_ns", Nanos(open_end) - Nanos(restore_end)) +
           Number("first_get_ns", first_get_ns) +
           Number("verify_ns", Nanos(verify_end) - Nanos(open_end)) +
           Number("close_ns", Nanos(close_end) - Nanos(verify_end)) +
           Number("sampled", checked) +
           Number("byte_verified", exact ? checked : 0) +
           OptionalNumber("recovered_time_lower_us",
                          report.time_lower_bound_micros,
                          report.has_time_lower_bound) +
           OptionalNumber("recovered_time_upper_us",
                          report.time_upper_bound_micros,
                          report.has_time_upper_bound));
}

void Verify(const char* path, const char* db_path) {
  const mb_ycsb::Dataset dataset(path);
  std::unique_ptr<rdb::DB> db;
  Check(rdb::DB::OpenForReadOnly(DBOptions(false), Absolute(db_path), &db),
        "DB OpenForReadOnly");
  rdb::ReadOptions read;
  read.fill_cache = false;
  std::unique_ptr<rdb::Iterator> iterator(db->NewIterator(read));
  const auto metadata = dataset.Verify(*iterator);
  Emit("verify", DatasetFields(metadata) + ",\"initial_verified\":true" +
                     String("initial_sha256", metadata.logical_sha256));
}
}  // namespace

int main(int argc, char** argv) {
  try {
    const std::string_view command = argc > 1 ? argv[1] : "";
    if (command == "identity" && argc == 2) {
      const auto& properties = rdb::GetRocksBuildProperties();
      const auto git_sha = properties.find("rocksdb_build_git_sha");
      const auto embedded_sha =
          git_sha == properties.end() ? std::string{} : git_sha->second;
      Require(
          embedded_sha.size() != 40 || embedded_sha == MB_YCSB_CORE_REVISION,
          "linked RocksDB git revision differs from the build configuration");
      Emit("identity",
           String("ycsb_commit", MB_YCSB_UPSTREAM_REVISION) +
               String("ycsb_patch_sha256", MB_YCSB_PATCH_SHA256) +
               String("adapter_sha256", MB_YCSB_ADAPTER_SHA256) +
               String("core_commit", MB_YCSB_CORE_REVISION) +
               String("embedded_core_commit", embedded_sha) +
               String("core_library_sha256", MB_YCSB_CORE_LIBRARY_SHA256));
    } else if (command == "dataset" && argc == 5) {
      const auto count =
          Parse(argv[3], "record count", 1, std::numeric_limits<int>::max());
      const auto width =
          Parse(argv[4], "value width", 1, std::numeric_limits<int>::max());
      const auto directory = Absolute(argv[2]);
      Require(fs::create_directory(directory),
              "dataset directory already exists");
      const auto path = (fs::path(directory) / "dataset.bin").string();
      mb_ycsb::GenerateDataset(path, count, static_cast<uint32_t>(width));
      const mb_ycsb::Dataset dataset(path);
      Emit("dataset",
           String("dataset", path) + DatasetFields(dataset.metadata()));
    } else if (command == "run" && argc == 11) {
      Run(argv);
    } else if (command == "recover" && argc == 9) {
      Recover(argv);
    } else if (command == "verify" && argc == 4) {
      Verify(argv[2], argv[3]);
    } else {
      throw std::runtime_error(
          "usage: identity | dataset DIR RECORDS VALUE_BYTES | verify DATASET "
          "DB "
          "| run DATASET PROPERTIES mb|be|native DB BACKUP|- THREADS WARMUP_S "
          "MEASURE_S CHECKPOINT_MS | recover DATASET PROPERTIES mb|be TARGET "
          "BACKUP INITIAL_SEQUENCE SAMPLES");
    }
    return EXIT_SUCCESS;
  } catch (const std::exception& e) {
    Emit("error", String("message", e.what()));
    return EXIT_FAILURE;
  }
}
