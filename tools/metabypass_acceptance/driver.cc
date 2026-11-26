//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/native_files.h"
#include "utilities/metabypass/wal_breakdown.h"

#ifdef ROCKSDB_MB_WAL_BREAKDOWN
#include "file/filename.h"
#endif

namespace {
namespace fs = std::filesystem;
namespace rdb = ROCKSDB_NAMESPACE;
using Clock = std::chrono::steady_clock;
using rdb::Status;

const char kUsage[] =
    "metabypass_acceptance --arm mb|be --index DIR --backup DIR --restore DIR\n"
    "  [--records N] [--value-bytes N] [--batch N] [--seed N]\n"
    "  [--checkpoint-records N] [--sample-every N] [--sst-budget-bytes N]\n"
    "  [--fine-wal]\n";

enum class Arm { kMetaBypass, kBackupEngine };

struct Config {
  Arm arm = Arm::kMetaBypass;
  fs::path index, backup, restore;
  uint64_t records = 100000;
  uint64_t value_bytes = 64;
  uint64_t batch = 8;
  uint64_t seed = 1;
  uint64_t checkpoint_records = 50000;
  uint64_t sample_every = 64;
  uint64_t sst_budget_bytes = 0;
  bool fine = false;
};

uint64_t Number(std::string_view arg) {
  uint64_t number = 0;
  auto result = std::from_chars(arg.data(), arg.data() + arg.size(), number);
  if (arg.empty() || result.ec != std::errc{} ||
      result.ptr != arg.data() + arg.size()) {
    throw std::runtime_error("invalid unsigned integer: " + std::string(arg));
  }
  return number;
}

Config Parse(int argc, char** argv) {
  Config c;
  bool have_arm = false;
  for (int i = 1; i < argc; ++i) {
    const std::string_view flag(argv[i]);
    if (flag == "--fine-wal") {
      c.fine = true;
      continue;
    }
    if (++i == argc) {
      throw std::runtime_error("missing value for " + std::string(flag));
    }
    const std::string value(argv[i]);
    if (flag == "--arm") {
      if (value == "mb") {
        c.arm = Arm::kMetaBypass;
      } else if (value == "be") {
        c.arm = Arm::kBackupEngine;
      } else {
        throw std::runtime_error("--arm must be mb or be");
      }
      have_arm = true;
    } else if (flag == "--index") {
      c.index = value;
    } else if (flag == "--backup") {
      c.backup = value;
    } else if (flag == "--restore") {
      c.restore = value;
    } else if (flag == "--records") {
      c.records = Number(value);
    } else if (flag == "--value-bytes") {
      c.value_bytes = Number(value);
    } else if (flag == "--batch") {
      c.batch = Number(value);
    } else if (flag == "--seed") {
      c.seed = Number(value);
    } else if (flag == "--checkpoint-records") {
      c.checkpoint_records = Number(value);
    } else if (flag == "--sample-every") {
      c.sample_every = Number(value);
    } else if (flag == "--sst-budget-bytes") {
      c.sst_budget_bytes = Number(value);
    } else {
      throw std::runtime_error("unknown argument: " + std::string(flag));
    }
  }
  if (!have_arm || c.index.empty() || c.backup.empty() || c.restore.empty()) {
    throw std::runtime_error(
        "--arm, --index, --backup and --restore are required");
  }
  if (c.records == 0 || c.records >= (uint64_t{1} << 56) || c.batch == 0 ||
      c.batch > std::numeric_limits<int>::max() || c.value_bytes == 0 ||
      c.value_bytes > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error(
        "invalid record count, batch size or value length");
  }
  if (c.sst_budget_bytes != 0 && c.arm == Arm::kBackupEngine) {
    throw std::runtime_error("--sst-budget-bytes requires --arm mb");
  }
  if (c.fine && c.sample_every == 0) {
    throw std::runtime_error("--fine-wal requires positive --sample-every");
  }
#ifndef ROCKSDB_MB_WAL_BREAKDOWN
  if (c.fine) {
    throw std::runtime_error("--fine-wal requires a probe-enabled build");
  }
#endif
  return c;
}

bool Contains(const fs::path& root, const fs::path& path) {
  auto current = path.begin();
  for (const auto& component : root) {
    if (current == path.end() || *current++ != component) return false;
  }
  return true;
}

void PrepareDirectories(Config* c) {
  std::array<fs::path*, 3> paths{&c->index, &c->backup, &c->restore};
  for (auto* path : paths) {
    *path = fs::weakly_canonical(fs::absolute(*path));
    if (path->filename().empty()) *path = path->parent_path();
    if (fs::exists(fs::symlink_status(*path))) {
      throw std::runtime_error("directory already exists: " + path->string());
    }
  }
  for (size_t i = 0; i < paths.size(); ++i) {
    for (size_t j = 0; j < i; ++j) {
      if (Contains(*paths[i], *paths[j]) || Contains(*paths[j], *paths[i])) {
        throw std::runtime_error("index, backup and restore must be disjoint");
      }
    }
  }
  for (const auto* path : paths) {
    if (!fs::create_directories(*path)) {
      throw std::runtime_error("cannot create directory: " + path->string());
    }
  }
}

void Check(const Status& s, const char* operation) {
  if (!s.ok())
    throw std::runtime_error(std::string(operation) + ": " + s.ToString());
}

uint64_t ElapsedNs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                              start)
      .count();
}

uint64_t Mix(uint64_t n) {
  n = (n ^ (n >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
  n = (n ^ (n >> 27)) * UINT64_C(0x94d049bb133111eb);
  return n ^ (n >> 31);
}

std::string Key(uint64_t row) {
  std::string key(8, '\0');
  for (size_t i = key.size(); i != 0; --i) {
    key[i - 1] = static_cast<char>(row & 255);
    row >>= 8;
  }
  return key;
}

std::string Value(const Config& c, uint64_t row) {
  std::string value(c.value_bytes, '\0');
  uint64_t state = Mix(c.seed ^ row);
  for (size_t i = 0; i < value.size(); ++i) {
    if (i % 8 == 0) {
      state = Mix(state + UINT64_C(0x9e3779b97f4a7c15));
    }
    value[i] = static_cast<char>((state >> (8 * (i % 8))) & 255);
  }
  return value;
}

#ifdef ROCKSDB_MB_WAL_BREAKDOWN
namespace wal = rdb::metabypass::wal_probe;

class PrimaryWal : public rdb::FSWritableFileOwnerWrapper {
 public:
  using FSWritableFileOwnerWrapper::FSWritableFileOwnerWrapper;

  rdb::IOStatus Append(const rdb::Slice& b, const rdb::IOOptions& o,
                       rdb::IODebugContext* debug) override {
    return Measure(b.size(), [&] { return target()->Append(b, o, debug); });
  }
  rdb::IOStatus Append(const rdb::Slice& b, const rdb::IOOptions& o,
                       const rdb::DataVerificationInfo& v,
                       rdb::IODebugContext* debug) override {
    return Measure(b.size(), [&] { return target()->Append(b, o, v, debug); });
  }

 private:
  template <typename F>
  rdb::IOStatus Measure(size_t bytes, F append) {
    wal::Span span(wal::primary_append, true);
    rdb::IOStatus s = append();
    span.End(s.ok() ? bytes : 0);
    return s;
  }
};

class PrimaryWalFS : public rdb::FileSystemWrapper {
 public:
  PrimaryWalFS(std::shared_ptr<rdb::FileSystem> base, std::string index)
      : FileSystemWrapper(std::move(base)), prefix_(std::move(index) + "/") {}
  const char* Name() const override { return "AcceptancePrimaryWalFS"; }

  rdb::IOStatus NewWritableFile(const std::string& path,
                                const rdb::FileOptions& o,
                                std::unique_ptr<rdb::FSWritableFile>* out,
                                rdb::IODebugContext* debug) override {
    auto s = target()->NewWritableFile(path, o, out, debug);
    if (s.ok()) Wrap(path, out);
    return s;
  }
  rdb::IOStatus ReopenWritableFile(const std::string& path,
                                   const rdb::FileOptions& o,
                                   std::unique_ptr<rdb::FSWritableFile>* out,
                                   rdb::IODebugContext* debug) override {
    auto s = target()->ReopenWritableFile(path, o, out, debug);
    if (s.ok()) Wrap(path, out);
    return s;
  }

 private:
  void Wrap(const std::string& path,
            std::unique_ptr<rdb::FSWritableFile>* out) {
    uint64_t number;
    rdb::FileType type;
    if (path.starts_with(prefix_) &&
        rdb::ParseFileName(path.substr(prefix_.size()), &number, &type) &&
        type == rdb::kWalFile) {
      *out = std::make_unique<PrimaryWal>(std::move(*out));
    }
  }
  const std::string prefix_;
};

struct Stage {
  uint64_t calls = 0, total_ns = 0, max_ns = 0, bytes = 0;

  void Record(const wal::Trace::Interval& interval) {
    const uint64_t elapsed = interval.end - interval.begin;
    ++calls;
    total_ns += elapsed;
    max_ns = std::max(max_ns, elapsed);
    bytes += interval.bytes;
  }
};
#endif

struct Result {
  uint64_t calls = 0, sampled_calls = 0, sampled_records = 0;
  uint64_t write_ns = 0, sample_api_ns = 0, sample_wal_ns = 0;
  uint64_t sample_memtable_ns = 0;
  uint64_t checkpoints = 0, checkpoint_ns = 0, purge_ns = 0;
  uint64_t load_ns = 0, durable_load_ns = 0, flush_ns = 0, close_ns = 0;
  uint64_t restore_ns = 0;
  uint64_t verify_ns = 0, flush_job_us = 0, compaction_job_us = 0;
  uint64_t flush_bytes = 0, compaction_read_bytes = 0,
           compaction_write_bytes = 0;
  rdb::MetaBypassStats stats;
  rdb::MetaBypassRecoveryReport recovery;
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
  uint64_t fine_samples = 0, fine_records = 0, trace_overflows = 0;
  std::array<Stage, wal::stage_count> stages{};
  std::array<wal::BackgroundSample::Snapshot, wal::background_stage_count>
      background{};
#endif
};

template <typename DB>
void WriteRows(DB* db, const Config& c, uint64_t first, uint64_t limit,
               Result* r) {
  for (uint64_t row = first; row < limit;) {
    const uint64_t count = std::min(c.batch, limit - row);
    rdb::WriteBatch batch;
    std::string key, value;
    if (c.batch == 1) {
      key = Key(row);
      value = Value(c, row);
    } else {
      for (uint64_t i = 0; i < count; ++i) {
        Check(batch.Put(Key(row + i), Value(c, row + i)), "build write batch");
      }
    }
    const bool sample = c.sample_every != 0 && r->calls % c.sample_every == 0;
    if (sample) {
      rdb::SetPerfLevel(rdb::PerfLevel::kEnableTime);
      rdb::get_perf_context()->Reset();
    }
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
    const bool fine_sample = c.fine && sample && (r->sampled_calls % 16 == 0);
    wal::Trace trace;
    wal::SampleScope scope(fine_sample ? &trace : nullptr);
#endif
    const auto started = Clock::now();
    const auto s = c.batch == 1 ? db->Put(rdb::WriteOptions(), key, value)
                                : db->Write(rdb::WriteOptions(), &batch);
    const auto elapsed = ElapsedNs(started);
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
    scope.End();
#endif
    if (sample) {
      const auto& perf = *rdb::get_perf_context();
      ++r->sampled_calls;
      r->sampled_records += count;
      r->sample_api_ns += elapsed;
      r->sample_wal_ns += perf.write_wal_time;
      r->sample_memtable_ns += perf.write_memtable_time;
      rdb::SetPerfLevel(rdb::PerfLevel::kDisable);
    }
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
    if (fine_sample) {
      if (trace.overflow) {
        ++r->trace_overflows;
      } else {
        ++r->fine_samples;
        r->fine_records += count;
        for (size_t i = 0; i < trace.size; ++i) {
          const auto& interval = trace.intervals[i];
          r->stages[interval.stage].Record(interval);
        }
      }
    }
#endif
    Check(s, "write");
    ++r->calls;
    r->write_ns += elapsed;
    row += count;
  }
}

template <typename DB, typename Checkpoint>
void RunWrites(DB* db, const Config& c, Checkpoint checkpoint, Result* r) {
  const auto load_started = Clock::now();
  const auto stride =
      c.checkpoint_records == 0 ? c.records : c.checkpoint_records;
  for (uint64_t first = 0; first < c.records;) {
    const uint64_t limit = first + std::min(stride, c.records - first);
    WriteRows(db, c, first, limit, r);
    if (limit < c.records) checkpoint();
    first = limit;
  }
  r->load_ns = ElapsedNs(load_started);
  const auto flush_started = Clock::now();
  Check(db->Flush(rdb::FlushOptions()), "flush SSTs");
  r->flush_ns = ElapsedNs(flush_started);
  checkpoint();
  r->durable_load_ns = ElapsedNs(load_started);
}

template <typename DB>
void Verify(DB* db, const Config& c) {
  std::unique_ptr<rdb::Iterator> it(db->NewIterator(rdb::ReadOptions()));
  it->SeekToFirst();
  for (uint64_t row = 0; row < c.records; ++row) {
    if (!it->Valid() || it->key() != Key(row) || it->value() != Value(c, row)) {
      Check(it->status(), "iterate restored DB");
      throw std::runtime_error("restored key/value mismatch at row " +
                               std::to_string(row));
    }
    it->Next();
  }
  Check(it->status(), "iterate restored DB");
  if (it->Valid()) throw std::runtime_error("unexpected tail in restored DB");
}

void CollectNative(const rdb::Statistics& stats, Result* r) {
  rdb::HistogramData histogram{};
  stats.histogramData(rdb::FLUSH_TIME, &histogram);
  r->flush_job_us = histogram.sum;
  stats.histogramData(rdb::COMPACTION_TIME, &histogram);
  r->compaction_job_us = histogram.sum;
  r->flush_bytes = stats.getTickerCount(rdb::FLUSH_WRITE_BYTES);
  r->compaction_read_bytes = stats.getTickerCount(rdb::COMPACT_READ_BYTES);
  r->compaction_write_bytes = stats.getTickerCount(rdb::COMPACT_WRITE_BYTES);
}

#ifdef ROCKSDB_MB_WAL_BREAKDOWN
void CollectBackground(const Config& c, Result* r) {
  if (!c.fine) return;
  for (size_t i = 0; i < r->background.size(); ++i) {
    r->background[i] = wal::background_samples[i].Read();
  }
}
#endif

void RunMetaBypass(const Config& c, rdb::Options options, Result* r) {
  rdb::MetaBypassOptions bypass;
  bypass.backup_dir = c.backup.generic_string();
  bypass.enable_recovery_time_sampling = false;
  if (c.sst_budget_bytes != 0) {
    bypass.sst_tiering.mode = rdb::SstTieringMode::kAdaptive;
    bypass.sst_tiering.ssd_capacity_bytes = c.sst_budget_bytes;
  }
  std::unique_ptr<rdb::MetaBypassDB> db;
  Check(rdb::MetaBypassDB::Open(options, bypass, c.index.generic_string(), &db),
        "open Metabypass");
  RunWrites(
      db.get(), c,
      [&] {
        const auto started = Clock::now();
        Check(db->SyncBackup(), "publish Metabypass checkpoint");
        r->checkpoint_ns += ElapsedNs(started);
        ++r->checkpoints;
      },
      r);
  r->stats = db->GetBackupStats();
  Check(r->stats.error, "Metabypass background work");
  CollectNative(*options.statistics, r);
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
  CollectBackground(c, r);
#endif
  auto started = Clock::now();
  Check(db->Close(), "close Metabypass");
  db.reset();
  r->close_ns = ElapsedNs(started);
  options.create_if_missing = false;
  started = Clock::now();
  Check(rdb::MetaBypassDB::Restore(options, bypass, c.restore.generic_string(),
                                   &r->recovery),
        "restore Metabypass");
  r->restore_ns = ElapsedNs(started);
  started = Clock::now();
  Check(
      rdb::MetaBypassDB::Open(options, bypass, c.restore.generic_string(), &db),
      "open restored Metabypass");
  Verify(db.get(), c);
  r->verify_ns = ElapsedNs(started);
  Check(db->Close(), "close restored Metabypass");
}

void RunBackupEngine(const Config& c, rdb::Options options,
                     const std::shared_ptr<rdb::metabypass::IoStats>& io_stats,
                     Result* r) {
  std::unique_ptr<rdb::DB> db;
  Check(rdb::DB::Open(options, c.index.generic_string(), &db), "open RocksDB");
  rdb::BackupEngineOptions backup_options(c.backup.generic_string());
  backup_options.sync = true;
  backup_options.backup_log_files = true;
  backup_options.share_table_files = true;
  rdb::BackupEngine* engine = nullptr;
  Check(rdb::BackupEngine::Open(backup_options, options.env, &engine),
        "open BackupEngine");
  std::unique_ptr<rdb::BackupEngine> backup(engine);
  rdb::CreateBackupOptions create_options;
  create_options.flush_before_backup = false;
  rdb::BackupID final_id = 0;
  RunWrites(
      db.get(), c,
      [&] {
        auto started = Clock::now();
        Check(backup->CreateNewBackup(create_options, db.get(), &final_id),
              "publish BackupEngine backup");
        r->checkpoint_ns += ElapsedNs(started);
        ++r->checkpoints;
        started = Clock::now();
        Check(backup->PurgeOldBackups(1), "purge old backups");
        r->purge_ns += ElapsedNs(started);
      },
      r);
  io_stats->AddStats(&r->stats);
  CollectNative(*options.statistics, r);
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
  CollectBackground(c, r);
#endif
  auto started = Clock::now();
  Check(db->Close(), "close RocksDB");
  db.reset();
  r->close_ns = ElapsedNs(started);
  const auto restore = c.restore.generic_string();
  started = Clock::now();
  Check(backup->RestoreDBFromBackup(rdb::RestoreOptions(), final_id, restore,
                                    restore),
        "restore BackupEngine backup");
  r->restore_ns = ElapsedNs(started);
  options.create_if_missing = false;
  started = Clock::now();
  Check(rdb::DB::OpenForReadOnly(options, restore, &db),
        "open restored RocksDB");
  Verify(db.get(), c);
  r->verify_ns = ElapsedNs(started);
  Check(db->Close(), "close restored RocksDB");
}

void PrintResult(const Config& c, const Result& r) {
  std::cout << "{\n  \"arm\":\"" << (c.arm == Arm::kMetaBypass ? "mb" : "be")
            << "\"";
  const auto field = [](const char* name, auto value) {
    std::cout << ",\n  \"" << name << "\":" << value;
  };
  field("records", c.records);
  field("verified_records", c.records);
  field("value_bytes", c.value_bytes);
  field("batch", c.batch);
  field("seed", c.seed);
  field("checkpoint_records", c.checkpoint_records);
  field("sample_every", c.sample_every);
  field("sst_budget_bytes", c.sst_budget_bytes);
  field("write_calls", r.calls);
  field("sampled_calls", r.sampled_calls);
  field("sampled_records", r.sampled_records);
  field("sample_api_ns", r.sample_api_ns);
  field("sample_wal_ns", r.sample_wal_ns);
  field("sample_memtable_ns", r.sample_memtable_ns);
  field("write_api_ns", r.write_ns);
  field("load_ns", r.load_ns);
  field("records_per_second",
        r.load_ns > 0 ? 1e9 * static_cast<double>(c.records) / r.load_ns : 0);
  field("durable_load_ns", r.durable_load_ns);
  field("durable_records_per_second",
        r.durable_load_ns > 0
            ? 1e9 * static_cast<double>(c.records) / r.durable_load_ns
            : 0);
  field("flush_ns", r.flush_ns);
  field("checkpoints", r.checkpoints);
  field("checkpoint_ns", r.checkpoint_ns);
  field("purge_ns", r.purge_ns);
  field("close_ns", r.close_ns);
  field("restore_ns", r.restore_ns);
  field("verify_ns", r.verify_ns);
  field("flush_job_us", r.flush_job_us);
  field("compaction_job_us", r.compaction_job_us);
  field("flush_bytes", r.flush_bytes);
  field("compaction_read_bytes", r.compaction_read_bytes);
  field("compaction_write_bytes", r.compaction_write_bytes);
  const auto& s = r.stats;
  field("io_index_write_bytes", s.io_index_write_bytes);
  field("io_backup_write_bytes", s.io_backup_write_bytes);
  field("io_caller_write_bytes", s.io_caller_write_bytes);
  field("io_bulk_write_bytes", s.io_bulk_write_bytes);
  field("io_barrier_write_bytes", s.io_barrier_write_bytes);
  field("io_failed_writes", s.io_failed_writes);
  field("hdd_append_calls", s.hdd_append_calls);
  field("hdd_flush_calls", s.hdd_flush_calls);
  field("hdd_sync_calls", s.hdd_sync_calls);
  field("hdd_dirsync_calls", s.hdd_dirsync_calls);
  field("hdd_read_calls", s.hdd_read_calls);
  field("hdd_append_micros", s.hdd_append_micros);
  field("hdd_flush_micros", s.hdd_flush_micros);
  field("hdd_sync_micros", s.hdd_sync_micros);
  field("hdd_dirsync_micros", s.hdd_dirsync_micros);
  field("hdd_read_micros", s.hdd_read_micros);
  if (c.arm == Arm::kMetaBypass) {
    field("queued_bytes", s.queued_bytes);
    field("peak_queued_bytes", s.peak_queued_bytes);
    field("backpressure_micros", s.backpressure_micros);
    field("mirrored_wal_bytes", s.mirrored_wal_bytes);
    field("recovery_points", s.recovery_points);
    field("checkpoint_capture_micros", s.checkpoint_capture_micros);
    field("checkpoint_publish_micros", s.checkpoint_publish_micros);
    field("checkpoint_wait_micros", s.checkpoint_wait_micros);
    field("candidate_copied_bytes", s.candidate_copied_bytes);
    field("gc_micros", s.gc_micros);
    field("checkpoint_sequence", r.recovery.checkpoint_sequence);
    field("recovered_sequence", r.recovery.recovered_sequence);
  }
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
  if (c.fine) {
    constexpr std::array foreground{
        "operations_wait",      "event_build",   "reserve_wait",
        "reserve_bookkeeping",  "capacity_wait", "payload_copy",
        "primary_append",       "finish_wait",   "finish_bookkeeping",
        "batch_validation",     "error_check",   "completed_wait",
        "completed_bookkeeping"};
    constexpr std::array background{"mirror_selection_hold",
                                    "mirror_completion_hold"};
    static_assert(foreground.size() == wal::stage_count);
    static_assert(background.size() == wal::background_stage_count);
    field("fine_samples", r.fine_samples);
    field("fine_records", r.fine_records);
    field("fine_trace_overflows", r.trace_overflows);
    const auto print_stage = [](const char* name, uint64_t count,
                                uint64_t total_ns, uint64_t max_ns,
                                uint64_t bytes, bool last) {
      std::cout << "\n    \"" << name << "\":{\"count\":" << count
                << ",\"total_ns\":" << total_ns << ",\"max_ns\":" << max_ns
                << ",\"bytes\":" << bytes << "}" << (last ? "" : ",");
    };
    std::cout << ",\n  \"fine_stages\":{";
    for (size_t i = 0; i < foreground.size(); ++i) {
      const auto& stage = r.stages[i];
      print_stage(foreground[i], stage.calls, stage.total_ns, stage.max_ns,
                  stage.bytes, i + 1 == foreground.size());
    }
    std::cout << "\n  },\n  \"fine_background\":{";
    for (size_t i = 0; i < background.size(); ++i) {
      const auto& stage = r.background[i];
      print_stage(background[i], stage.count, stage.total_ns, stage.max_ns, 0,
                  i + 1 == background.size());
    }
    std::cout << "\n  }";
  }
#endif
  std::cout << "\n}\n";
}
}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string_view(argv[1]) == "--help") {
    std::cout << kUsage;
    return 0;
  }
  try {
    auto c = Parse(argc, argv);
    PrepareDirectories(&c);
    rdb::Options options;
    options.create_if_missing = true;
    options.compression = rdb::kNoCompression;
    options.statistics = rdb::CreateDBStatistics();
    auto base = options.env->GetFileSystem();
    auto io_stats = std::make_shared<rdb::metabypass::IoStats>();
    if (c.arm == Arm::kBackupEngine) {
      base = rdb::metabypass::ObserveIo(base, c.index.generic_string(),
                                        c.backup.generic_string(), io_stats);
    }
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
    if (c.fine) {
      base = std::make_shared<PrimaryWalFS>(base, c.index.generic_string());
    }
#endif
    auto env = rdb::NewCompositeEnv(base);
    options.env = env.get();
    Result result;
    if (c.arm == Arm::kMetaBypass) {
      RunMetaBypass(c, options, &result);
    } else {
      RunBackupEngine(c, options, io_stats, &result);
    }
    Check(result.stats.error, "backup statistics");
    PrintResult(c, result);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n' << kUsage;
    return 1;
  }
}
