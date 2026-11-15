//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <limits>
#include <mutex>
#include <sstream>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_index.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/listener.h"
#include "rocksdb/utilities/metabypass.h"
#include "rocksdb/wal_filter.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "utilities/metabypass/backup.h"

namespace ROCKSDB_NAMESPACE {
namespace {
constexpr char kFormat[] = "MBN6\n";
// Refuse legacy directories before taking locks or initializing storage.
Status CheckFormat(FileSystem* fs, const std::string& path, bool required) {
  std::string version;
  Status s = metabypass::Read(fs, path + "/METABYPASS-FORMAT", &version);
  if (s.ok())
    return version == kFormat ? Status::OK()
                              : Status::NotSupported("Metabypass format", path);
  if (!s.IsNotFound()) return s;
  if (required) return Status::NotSupported("missing Metabypass format", path);
  std::vector<std::string> children;
  s = fs->GetChildren(path, IOOptions(), &children, nullptr);
  if (s.IsNotFound()) return Status::OK();
  if (!s.ok()) return s;
  for (const auto& child : children)
    if (child != "." && child != "..")
      return Status::NotSupported("unrecognized Metabypass directory", path);
  return Status::OK();
}
Status CheckStorageLayout(FileSystem* fs, const MetaBypassOptions& options) {
  std::string version;
  Status s =
      metabypass::Read(fs, options.data_dir + "/METABYPASS-TIERED", &version);
  if (!s.ok() && !s.IsNotFound()) return s;
  const bool tiered = s.ok();
  if (tiered != !options.staging_dir.empty())
    return Status::InvalidArgument("cannot change Metabypass storage format");
  if (tiered && version != "MBT5\n")
    return Status::NotSupported("tiered storage format");
  return Status::OK();
}
Status WriteFormat(FileSystem* fs, const std::string& path) {
  Status s = metabypass::EnsureDir(fs, path);
  if (s.ok()) s = metabypass::Write(fs, path + "/METABYPASS-FORMAT", kFormat);
  if (s.ok()) s = metabypass::SyncDir(fs, path);
  return s;
}
Status Validate(const Options& o, const MetaBypassOptions& m,
                const std::string& index) {
  if (!o.env) return Status::InvalidArgument("null Env");
  if (!o.enable_blob_files &&
      (o.enable_blob_direct_write || !m.staging_dir.empty()))
    return Status::NotSupported(
        "Metabypass Direct Write and blob staging require enable_blob_files");
  auto valid_path = [](const std::string& p) {
    const std::filesystem::path path(p);
    return path.is_absolute() && !path.filename().empty() &&
           path.lexically_normal().generic_string() == path.generic_string();
  };
  if (!valid_path(index) || !valid_path(m.data_dir) ||
      !valid_path(m.backup_dir))
    return Status::InvalidArgument(
        "Metabypass requires normalized absolute directories");
  auto overlap = [](const std::string& a, const std::string& b) {
    return a == b || a.compare(0, b.size() + 1, b + "/") == 0 ||
           b.compare(0, a.size() + 1, a + "/") == 0;
  };
  if (overlap(index, m.data_dir) || overlap(index, m.backup_dir) ||
      overlap(m.data_dir, m.backup_dir))
    return Status::InvalidArgument("Metabypass directories must be disjoint");
  if (m.staging_latency_budget_us == 0 ||
      (m.staging_policy != MetaBypassStagingPolicy::kAlways &&
       m.staging_policy != MetaBypassStagingPolicy::kAdaptive))
    return Status::InvalidArgument("invalid staging policy/budget");
  if (!m.staging_dir.empty()) {
    if (!valid_path(m.staging_dir) || m.staging_capacity < 8192 ||
        overlap(m.staging_dir, index) || overlap(m.staging_dir, m.data_dir) ||
        overlap(m.staging_dir, m.backup_dir))
      return Status::InvalidArgument("invalid tiered staging configuration");
  } else if (m.staging_capacity != 0) {
    return Status::InvalidArgument(
        "staging capacity requires staging directory");
  }
  if (m.queue_capacity < 4096 || m.batch_bytes == 0 ||
      m.batch_bytes > m.queue_capacity || m.interval_ms == 0 ||
      m.interval_ms > std::numeric_limits<int64_t>::max())
    return Status::InvalidArgument("invalid Metabypass queue configuration");
  const auto max_interval =
      std::chrono::duration_cast<std::chrono::duration<uint64_t, std::milli>>(
          std::chrono::steady_clock::duration::max())
          .count() /
      2;
  if (!m.checkpoint_interval_ms || m.checkpoint_interval_ms > max_interval)
    return Status::InvalidArgument("invalid Metabypass checkpoint interval");
  const auto& tier = m.sst_tiering;
  if (tier.mode != SstTieringMode::kDisabled &&
      (tier.mode != SstTieringMode::kObserveOnly &&
       tier.mode != SstTieringMode::kAdaptive))
    return Status::InvalidArgument("invalid SST tiering mode");
  if (tier.mode != SstTieringMode::kDisabled &&
      (!tier.ssd_capacity_bytes || tier.reserve_percent >= 100 ||
       !tier.sample_one_in || !tier.sample_buffer_capacity ||
       !tier.interval_ms || !tier.heat_half_life_ms || !tier.promote_rounds ||
       !tier.demote_rounds || !tier.migration_queue_capacity ||
       tier.replacement_margin < 0 || !std::isfinite(tier.replacement_margin) ||
       tier.interval_ms > UINT64_MAX / 1000 ||
       tier.heat_half_life_ms > UINT64_MAX / 1000 ||
       tier.min_residency_ms > UINT64_MAX / 1000))
    return Status::InvalidArgument("invalid SST tiering configuration");
  if (tier.mode != SstTieringMode::kDisabled && o.allow_mmap_reads)
    return Status::NotSupported("SST tiering requires buffered reads");
  if (!o.write_identity_file)
    return Status::NotSupported("Metabypass requires write_identity_file");
  if (o.compaction_service)
    return Status::NotSupported("Metabypass remote compaction service");
  if (o.enable_blob_direct_write && (o.allow_concurrent_memtable_write ||
                                     o.experimental_mempurge_threshold > 0.0))
    return Status::NotSupported(
        "Metabypass Direct Write requires concurrent memtable writes and "
        "MemPurge to be disabled");
  if (o.enable_pipelined_write || o.unordered_write || o.two_write_queues ||
      o.manual_wal_flush || o.sst_file_manager ||
      o.use_direct_io_for_flush_and_compaction || o.use_direct_reads ||
      o.allow_mmap_writes || !o.db_paths.empty() || !o.cf_paths.empty() ||
      !o.wal_dir.empty() || o.recycle_log_file_num || o.WAL_ttl_seconds ||
      o.WAL_size_limit_MB || o.wal_filter || o.track_and_verify_wals ||
      o.track_and_verify_wals_in_manifest || o.merge_operator ||
      o.compaction_filter || o.compaction_filter_factory ||
      o.comparator != BytewiseComparator() ||
      o.enable_blob_garbage_collection || o.best_efforts_recovery ||
      o.persist_stats_to_disk || o.allow_ingest_behind ||
      o.cf_allow_ingest_behind || o.atomic_flush || o.allow_2pc)
    return Status::NotSupported(
        "unsupported Metabypass configuration; ordered single-CF writes "
        "required");
  return Status::OK();
}
// Recovery owns only destination index files. Blob sources remain immutable
// until the native database has flushed and closed successfully.
class RecoveryStorage : public metabypass::SeparatedStorage {
 public:
  using SeparatedStorage::SeparatedStorage;
  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* dbg) override {
    if (IsBlob(path)) return IOStatus::NotSupported("read-only recovery blob");
    return SeparatedStorage::NewWritableFile(path, options, file, dbg);
  }
  IOStatus ReopenWritableFile(const std::string& path,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* file,
                              IODebugContext* dbg) override {
    if (IsBlob(path)) return IOStatus::NotSupported("read-only recovery blob");
    return SeparatedStorage::ReopenWritableFile(path, options, file, dbg);
  }
  IOStatus Truncate(const std::string& path, size_t size,
                    const IOOptions& options, IODebugContext* dbg) override {
    if (IsBlob(path)) return IOStatus::NotSupported("read-only recovery blob");
    return target()->Truncate(path, size, options, dbg);
  }
  IOStatus ReuseWritableFile(const std::string& path, const std::string& old,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* file,
                             IODebugContext* dbg) override {
    if (IsBlob(path) || IsBlob(old))
      return IOStatus::NotSupported("read-only recovery blob");
    return target()->ReuseWritableFile(path, old, options, file, dbg);
  }
  IOStatus RenameFile(const std::string& from, const std::string& to,
                      const IOOptions& options, IODebugContext* dbg) override {
    if (IsBlob(from) || IsBlob(to))
      return IOStatus::NotSupported("read-only recovery blob");
    return target()->RenameFile(from, to, options, dbg);
  }
};
class RecoveryBatch : public WriteBatch::Handler {
 public:
  RecoveryBatch(const Options& options, WriteBatch* output)
      : options_(options), output_(output) {}
  Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
    return WriteBatchInternal::Put(output_, cf, key, value);
  }
  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return WriteBatchInternal::Delete(output_, cf, key);
  }
  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return WriteBatchInternal::SingleDelete(output_, cf, key);
  }
  Status DeleteRangeCF(uint32_t cf, const Slice& begin,
                       const Slice& end) override {
    return WriteBatchInternal::DeleteRange(output_, cf, begin, end);
  }
  Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                        const Slice& value) override {
    BlobIndex index;
    Status s = index.DecodeFrom(value);
    if (!s.ok()) return s;
    if (index.IsInlined() || index.HasTTL())
      return Status::NotSupported("unsupported recovery blob index");
    std::unique_ptr<BlobFileReader> reader;
    s = BlobFileReader::Create(ImmutableOptions(options_), ReadOptions(),
                               FileOptions(), cf, nullptr, index.file_number(),
                               nullptr, true, &reader);
    std::unique_ptr<BlobContents> contents;
    uint64_t bytes = 0;
    if (s.ok())
      s = reader->GetBlob(ReadOptions(), key, index.offset(), index.size(),
                          index.compression(), nullptr, nullptr, &contents,
                          &bytes);
    return s.ok() ? PutCF(cf, key, contents->data()) : s;
  }

 private:
  const Options& options_;
  WriteBatch* output_;
};
// Inspect every record without copying keys or values. Handler's default Merge
// can succeed without consuming an update, so the recognized count is required.
class RecoveryBatchShape : public WriteBatch::Handler {
 public:
  uint64_t updates = 0;
  bool has_blob_index = false;
  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    ++updates;
    return Status::OK();
  }
  Status DeleteCF(uint32_t, const Slice&) override {
    ++updates;
    return Status::OK();
  }
  Status SingleDeleteCF(uint32_t, const Slice&) override {
    ++updates;
    return Status::OK();
  }
  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override {
    ++updates;
    return Status::OK();
  }
  Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override {
    ++updates;
    has_blob_index = true;
    return Status::OK();
  }
};
class RecoveryFilter : public WalFilter {
 public:
  RecoveryFilter(const Options& options, MetaBypassRecoveryReport* report)
      : options_(options), report_(report) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::RecoveryFilterCreated", this);
  }
  const char* Name() const override { return "MetaBypassContinuousRecovery"; }
  WalProcessingOption LogRecordFound(unsigned long long, const std::string&,
                                     const WriteBatch& batch,
                                     WriteBatch* output,
                                     bool* changed) override {
    const uint64_t sequence = WriteBatchInternal::Sequence(&batch);
    const uint64_t count = WriteBatchInternal::Count(&batch);
    *changed = false;
    if (sequence <= report_->checkpoint_sequence || !count)
      return WalProcessingOption::kContinueProcessing;
    if (sequence != report_->recovered_sequence + 1) {
      report_->stop_reason = "sequence_gap";
      return WalProcessingOption::kStopReplay;
    }
    RecoveryBatchShape shape;
    Status s = batch.Iterate(&shape);
    if (!s.ok() || shape.updates != count) {
      report_->stop_reason = "unsupported_batch";
      return WalProcessingOption::kStopReplay;
    }
    if (shape.has_blob_index) {
      TEST_SYNC_POINT("MetaBypass::RecoveryBatchRebuilt");
      RecoveryBatch rewrite(options_, output);
      s = batch.Iterate(&rewrite);
      if (!s.ok()) {
        report_->stop_reason = "missing_dependency: " + s.ToString();
        return WalProcessingOption::kStopReplay;
      }
      if (WriteBatchInternal::Count(output) != count) {
        report_->stop_reason = "unsupported_batch";
        return WalProcessingOption::kStopReplay;
      }
      WriteBatchInternal::SetSequence(output, sequence);
      *changed = true;
    }
    report_->recovered_sequence = sequence + count - 1;
    return WalProcessingOption::kContinueProcessing;
  }

 private:
  const Options& options_;
  MetaBypassRecoveryReport* report_;
};
class SupportedBatch : public WriteBatch::Handler {
 public:
  bool supported = true;
  void LogData(const Slice&) override { supported = false; }
  Status PutCF(uint32_t cf, const Slice&, const Slice&) override {
    return CF(cf);
  }
  Status DeleteCF(uint32_t cf, const Slice&) override { return CF(cf); }
  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    return Status::NotSupported("Metabypass Merge");
  }
  Status SingleDeleteCF(uint32_t, const Slice&) override {
    return Status::NotSupported("Metabypass SingleDelete");
  }

 private:
  Status CF(uint32_t cf) {
    return cf == 0 ? Status::OK()
                   : Status::NotSupported("Metabypass multiple CFs");
  }
};
class BackupListener : public EventListener {
 public:
  explicit BackupListener(std::weak_ptr<metabypass::Backup> backup)
      : backup_(std::move(backup)) {}
  const char* Name() const override { return "MetaBypassBackupListener"; }
  void OnFlushCompleted(DB*, const FlushJobInfo&) override { Notify(); }
  void OnCompactionCompleted(DB*, const CompactionJobInfo& info) override {
    if (info.status.ok()) Notify();
  }

 private:
  void Notify() {
    if (auto backup = backup_.lock()) backup->NativeFilesChanged();
  }
  const std::weak_ptr<metabypass::Backup> backup_;
};
}  // namespace
struct MetaBypassDB::Impl {
  std::shared_ptr<metabypass::HddPressure> pressure;
  mutable std::mutex progress_close_mutex;
  std::string recovery_identity;
  std::string recovery_session;
  std::shared_ptr<metabypass::SstStorage> sst;
  std::shared_ptr<metabypass::TieredStorage> tier;
  std::atomic<uint64_t> sync_write_us{0};
  std::shared_ptr<metabypass::SeparatedStorage> storage;
  std::shared_ptr<metabypass::Backup> backup;
  std::unique_ptr<Env> env;
  std::unique_ptr<DB> db;
  Status closed;
  ~Impl() {
    if (sst) sst->Stop();
    if (tier) {
      tier->SetFailureHandler({});
      tier->Stop().PermitUncheckedError();
    }
    closed.PermitUncheckedError();
  }
};
MetaBypassDB::MetaBypassDB(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
MetaBypassDB::~MetaBypassDB() { Close().PermitUncheckedError(); }
Status MetaBypassDB::Open(const Options& options, const MetaBypassOptions& m,
                          const std::string& index,
                          std::unique_ptr<MetaBypassDB>* result) {
  if (!result) return Status::InvalidArgument("null MetaBypassDB result");
  result->reset();
  Status s = Validate(options, m, index);
  if (!s.ok()) return s;
  auto fs = options.env->GetFileSystem();
  for (const auto& path : {index, m.data_dir, m.backup_dir}) {
    s = CheckFormat(fs.get(), path, false);
    if (!s.ok()) return s;
  }
  s = fs->FileExists(index + "/METABYPASS-RESTORING", IOOptions(), nullptr);
  if (s.ok())
    return Status::Incomplete("resume interrupted Metabypass Restore first");
  if (!s.IsNotFound()) return s;
  s = fs->FileExists(index + "/CURRENT", IOOptions(), nullptr);
  const bool existing = s.ok();
  if (!s.ok() && !s.IsNotFound()) return s;
  if (existing && options.error_if_exists)
    return Status::InvalidArgument("Metabypass index already exists");
  if (!existing && !options.create_if_missing)
    return Status::InvalidArgument("Metabypass index does not exist");
  if (!existing) {
    std::vector<std::string> directories{m.data_dir, m.backup_dir};
    if (!m.staging_dir.empty()) directories.push_back(m.staging_dir);
    for (const auto& path : directories) {
      std::vector<std::string> children;
      s = fs->GetChildren(path, IOOptions(), &children, nullptr);
      if (s.IsNotFound()) continue;
      if (!s.ok()) return s;
      for (const auto& child : children) {
        if (child != "." && child != ".." && child != "METABYPASS-LOCK" &&
            child != "METABYPASS-FORMAT")
          return Status::InvalidArgument(
              "new Metabypass requires empty data and backup directories");
      }
    }
  }
  std::string identity;
  if (existing) {
    s = CheckStorageLayout(fs.get(), m);
    if (!s.ok()) return s;
    std::string marker;
    s = metabypass::Read(fs.get(), index + "/METABYPASS", &marker);
    if (!s.ok())
      return Status::InvalidArgument(
          "ordinary existing DB cannot attach to Metabypass");
    s = metabypass::Read(fs.get(), index + "/IDENTITY", &identity);
    if (!s.ok()) return s;
    if (marker != identity)
      return Status::Corruption("Metabypass identity mismatch");
    for (const auto& path : {m.data_dir, m.backup_dir}) {
      std::string owner;
      s = metabypass::Read(fs.get(), path + "/METABYPASS-IDENTITY", &owner);
      if (!s.ok()) return s;
      if (owner != identity)
        return Status::Corruption("Metabypass directory identity mismatch");
    }
    if (!m.staging_dir.empty()) {
      std::string staging_owner;
      s = metabypass::Read(fs.get(), m.staging_dir + "/METABYPASS-IDENTITY",
                           &staging_owner);
      if (!s.ok()) return s;
      if (staging_owner != identity)
        return Status::Corruption("staging identity mismatch");
    }
  }
  if (existing) {
    s = fs->FileExists(m.backup_dir + "/LATEST", IOOptions(), nullptr);
    if (s.ok())
      s = metabypass::Backup::CheckRestoreFormat(fs.get(), m.backup_dir);
    else if (s.IsNotFound())
      s = Status::OK();
    if (!s.ok()) return s;
  }
  auto impl = std::make_unique<Impl>();
  impl->pressure =
      std::make_shared<metabypass::HddPressure>(m.staging_latency_budget_us);
  fs = metabypass::ObserveHdd(fs, m.data_dir, m.backup_dir, impl->pressure,
                              index, m.staging_dir);
  if (!m.staging_dir.empty())
    impl->tier = std::make_shared<metabypass::TieredStorage>(fs, m, identity,
                                                             impl->pressure);
  if (m.sst_tiering.mode != SstTieringMode::kDisabled)
    impl->sst = std::make_shared<metabypass::SstStorage>(
        impl->tier ? std::shared_ptr<FileSystem>(impl->tier) : fs, index, m);
  if (impl->tier) {
    s = impl->tier->Preflight(false);
    if (!s.ok()) return s;
  }
  impl->storage = std::make_shared<metabypass::SeparatedStorage>(
      fs, index, m.data_dir, impl->tier, impl->sst);
  if (existing && impl->tier) {
    Status pointer =
        fs->FileExists(m.backup_dir + "/LATEST", IOOptions(), nullptr);
    if (pointer.ok()) {
      s = metabypass::Backup::PreflightCheckpoint(fs.get(), m.backup_dir,
                                                  *impl->storage);
      if (!s.ok()) return s;
    } else if (!pointer.IsNotFound()) {
      return pointer;
    }
  }
  s = impl->storage->Lock();
  if (!s.ok()) return s;
  impl->backup =
      std::make_shared<metabypass::Backup>(impl->storage, index, m, options);
  s = impl->backup->Lock();
  if (!s.ok()) return s;
  // Repeat dependency validation under both ownership locks before recovery
  // can rewrite local state or perform orphan cleanup.
  if (existing && impl->tier) {
    Status pointer =
        fs->FileExists(m.backup_dir + "/LATEST", IOOptions(), nullptr);
    if (pointer.ok()) {
      s = metabypass::Backup::PreflightCheckpoint(fs.get(), m.backup_dir,
                                                  *impl->storage);
      if (!s.ok()) return s;
    } else if (!pointer.IsNotFound()) {
      return pointer;
    }
  }
  if (!impl->sst) {
    s = fs->FileExists(index + "/SST-PLACEMENT", IOOptions(), nullptr);
    if (s.ok())
      return Status::InvalidArgument("SST placement requires tiering enabled");
    if (!s.IsNotFound()) return s;
  }
  const std::string format_path = m.data_dir + "/METABYPASS-TIERED";
  if (impl->tier) {
    s = impl->tier->Initialize(false);
    if (!s.ok()) return s;
    if (!existing) {
      s = metabypass::Write(fs.get(), format_path, "MBT5\n");
      if (s.ok()) s = metabypass::SyncDir(fs.get(), m.data_dir);
      if (!s.ok()) return s;
    }
  }
  if (existing && impl->sst) {
    s = impl->sst->Initialize(identity, false);
    if (!s.ok()) return s;
  }
  if (!existing) {
    for (const auto& path : {index, m.data_dir, m.backup_dir}) {
      s = WriteFormat(fs.get(), path);
      if (!s.ok()) return s;
    }
  }
  if (impl->sst) {
    std::weak_ptr<metabypass::Backup> weak = impl->backup;
    impl->sst->SetFailureHandler([weak](const Status& error) {
      if (auto backup = weak.lock()) backup->StorageFailed(error);
    });
  }
  if (impl->tier) {
    std::weak_ptr<metabypass::Backup> weak = impl->backup;
    impl->tier->SetFailureHandler([weak](const Status& error) {
      if (auto backup = weak.lock()) backup->StorageFailed(error);
    });
    impl->tier->Start();
  }
  s = impl->backup->Start(existing);
  if (!s.ok()) return s;
  impl->env = NewCompositeEnv(impl->backup);
  Options configured = options;
  configured.env = impl->env.get();
  configured.listeners.push_back(
      std::make_shared<BackupListener>(impl->backup));
  if (configured.enable_blob_files) {
    configured.min_blob_size = 0;
    configured.enable_blob_garbage_collection = false;
  }
  s = DB::Open(configured, index, &impl->db);
  if (s.ok()) {
    s = metabypass::Read(fs.get(), index + "/IDENTITY", &identity);
    // Reopening must not truncate the identity marker validated above.
    if (s.ok() && !existing)
      s = metabypass::Write(fs.get(), index + "/METABYPASS", identity);
    if (s.ok()) s = metabypass::SyncDir(fs.get(), index);
    if (s.ok() && impl->tier && !existing)
      s = metabypass::Write(fs.get(), m.staging_dir + "/METABYPASS-IDENTITY",
                            identity);
    if (s.ok() && impl->tier) s = metabypass::SyncDir(fs.get(), m.staging_dir);
    if (s.ok() && impl->sst && !existing)
      s = impl->sst->Initialize(identity, false);
    if (s.ok() && impl->tier) impl->tier->SetIdentity(identity);
    if (s.ok()) s = impl->backup->Activate(identity, impl->db.get());
    if (s.ok()) s = impl->backup->Sync();
  }
  if (s.ok()) s = impl->db->GetDbIdentity(impl->recovery_identity);
  if (s.ok()) {
    impl->recovery_session = options.env->GenerateUniqueId();
    if (impl->recovery_identity.empty() || impl->recovery_session.empty())
      s = Status::Corruption(
          "missing Metabypass recovery observation identity");
  }
  if (!s.ok()) {
    impl->backup->DetachDB();
    if (impl->db) {
      impl->db->Close().PermitUncheckedError();
      impl->db.reset();
    }
    impl->backup->Stop().PermitUncheckedError();
    return s;
  }
  if (impl->sst) impl->sst->Start();
  result->reset(new MetaBypassDB(std::move(impl)));
  return Status::OK();
}
Status MetaBypassDB::Restore(const Options& options, const MetaBypassOptions& m,
                             const std::string& index) {
  MetaBypassRecoveryReport report;
  return Restore(options, m, index, &report);
}
Status MetaBypassDB::Restore(const Options& options, const MetaBypassOptions& m,
                             const std::string& index,
                             MetaBypassRecoveryReport* report) {
  if (!report) return Status::InvalidArgument("null recovery report");
  *report = MetaBypassRecoveryReport();
  Status s = Validate(options, m, index);
  if (!s.ok()) return s;
  auto fs = options.env->GetFileSystem();
  for (const auto& path : {m.data_dir, m.backup_dir}) {
    s = CheckFormat(fs.get(), path, true);
    if (!s.ok()) return s;
  }
  s = CheckFormat(fs.get(), index, false);
  if (s.ok()) s = CheckStorageLayout(fs.get(), m);
  if (!s.ok()) return s;
  std::string identity;
  s = metabypass::Read(fs.get(), m.backup_dir + "/METABYPASS-IDENTITY",
                       &identity);
  if (!s.ok()) return s;
  std::string data_identity;
  s = metabypass::Read(fs.get(), m.data_dir + "/METABYPASS-IDENTITY",
                       &data_identity);
  if (!s.ok()) return s;
  if (data_identity != identity)
    return Status::Corruption("data and backup identity mismatch");
  s = metabypass::Backup::CheckRestoreFormat(fs.get(), m.backup_dir);
  if (!s.ok()) return s;
  bool staging_identity_exists = false;
  if (!m.staging_dir.empty()) {
    std::vector<std::string> names;
    s = fs->GetChildren(m.staging_dir, IOOptions(), &names, nullptr);
    if (s.IsNotFound()) s = Status::OK();
    if (!s.ok()) return s;
    for (const auto& name : names) {
      if (name == "METABYPASS-IDENTITY") {
        std::string owner;
        s = metabypass::Read(fs.get(), m.staging_dir + "/" + name, &owner);
        if (!s.ok()) return s;
        if (owner != identity)
          return Status::Corruption("restore staging identity mismatch");
        staging_identity_exists = true;
      } else if (name != "." && name != ".." && name != "METABYPASS-LOCK" &&
                 name != "METABYPASS-IDENTITY.tmp") {
        return Status::InvalidArgument("restore staging is not empty");
      }
    }
  }
  auto tier =
      m.staging_dir.empty()
          ? nullptr
          : std::make_shared<metabypass::TieredStorage>(fs, m, identity);
  std::shared_ptr<metabypass::SstStorage> sst;
  if (m.sst_tiering.mode != SstTieringMode::kDisabled)
    sst = std::make_shared<metabypass::SstStorage>(
        tier ? std::shared_ptr<FileSystem>(tier) : fs, index, m);
  auto storage =
      std::make_shared<RecoveryStorage>(fs, index, m.data_dir, tier, sst);
  s = storage->Lock();
  if (!s.ok()) return s;
  FileLock* backup_lock = nullptr;
  s = fs->LockFile(m.backup_dir + "/LOCK", IOOptions(), &backup_lock, nullptr);
  if (!s.ok()) return s;
  auto restore = [&]() -> Status {
    Status status;
    if (!m.staging_dir.empty())
      status = metabypass::EnsureDir(fs.get(), m.staging_dir);
    if (!status.ok()) return status;
    status = metabypass::EnsureDir(fs.get(), index);
    if (!status.ok()) return status;
    std::vector<std::string> files;
    status = fs->GetChildren(index, IOOptions(), &files, nullptr);
    if (!status.ok()) return status;
    bool nonempty = false;
    for (const auto& file : files)
      if (file != "." && file != ".." && file != "METABYPASS-FORMAT")
        nonempty = true;
    if (nonempty) {
      std::string marker;
      status =
          metabypass::Read(fs.get(), index + "/METABYPASS-RESTORING", &marker);
      if (!status.ok() || marker != identity)
        return Status::InvalidArgument("restore destination is not resumable");
    }
    std::string persisted, point;
    status =
        metabypass::Read(fs.get(), index + "/METABYPASS-RECOVERED", &persisted);
    const bool finalized_native = status.ok();
    if (!status.ok() && !status.IsNotFound()) return status;
    if (finalized_native) {
      std::istringstream in(persisted);
      if (!std::getline(in, point) ||
          !(in >> report->checkpoint_sequence >> report->recovered_sequence))
        return Status::Corruption("restore progress marker");
      if (!(in >> report->has_time_lower_bound >>
            report->time_lower_bound_micros >> report->has_time_upper_bound >>
            report->time_upper_bound_micros))
        return Status::Corruption("restore time marker");
      std::getline(in, report->stop_reason);
      if (!report->stop_reason.empty()) report->stop_reason.erase(0, 1);
    } else {
      // Prior preparation may have opened and flushed native files, but its
      // source mappings remain untouched until the persisted-stage marker.
      for (const auto& file : files) {
        if (file == "." || file == ".." || file == "METABYPASS-FORMAT" ||
            file == "METABYPASS-RESTORING")
          continue;
        status = fs->DeleteFile(index + "/" + file, IOOptions(), nullptr);
        if (!status.ok()) return status;
      }
      if (!nonempty) {
        status = WriteFormat(fs.get(), index);
        if (status.ok())
          status = metabypass::Write(fs.get(), index + "/METABYPASS-RESTORING",
                                     identity);
      } else {
        status = Status::OK();
      }
      if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
      if (!status.ok()) return status;
      if (sst) {
        status = sst->Initialize(identity, true);
        if (!status.ok()) return status;
      }
      bool has_boundary;
      status = metabypass::Backup::PrepareRecovery(fs.get(), m.backup_dir,
                                                   index, &point, report,
                                                   &has_boundary, sst.get());
      if (!status.ok()) return status;
      TEST_SYNC_POINT("MetaBypass::RecoveryPrepared");
      if (tier) {
        status = tier->OpenRecoveryView(point + "/BLOB-MAP");
        if (!status.ok()) return status;
      }
      auto source_tier =
          m.staging_dir.empty()
              ? nullptr
              : std::make_shared<metabypass::TieredStorage>(fs, m, identity);
      if (source_tier) {
        status = source_tier->OpenRecoveryView("");
        if (!status.ok()) return status;
      }
      auto source =
          std::make_shared<RecoveryStorage>(fs, index, m.data_dir, source_tier);
      auto source_env = NewCompositeEnv(source);
      Options read_options = options;
      read_options.env = source_env.get();
      read_options.cf_paths = {DbPath(index, UINT64_MAX)};
      RecoveryFilter filter(read_options, report);
      auto env = NewCompositeEnv(storage);
      Options recovery = options;
      recovery.env = env.get();
      recovery.enable_blob_direct_write = false;
      recovery.enable_blob_files = false;
      recovery.disable_auto_compactions = true;
      recovery.avoid_flush_during_recovery = false;
      recovery.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
      recovery.wal_filter = has_boundary ? &filter : nullptr;
      recovery.create_if_missing = false;
      recovery.error_if_exists = false;
      std::unique_ptr<DB> native;
      status = DB::Open(recovery, index, &native);
      if (!status.ok()) return status;
      if (!has_boundary) {
        report->checkpoint_sequence = native->GetLatestSequenceNumber();
        report->recovered_sequence = report->checkpoint_sequence;
        report->stop_reason = "checkpoint_only_no_boundary";
      } else if (report->stop_reason.empty()) {
        report->stop_reason = "native_replay_end";
      }
      status = native->Close();
      native.reset();
      if (!status.ok()) return status;
      TEST_SYNC_POINT("MetaBypass::RecoveryNativeClosed");
      std::string samples;
      Status sample_status =
          metabypass::Read(fs.get(), m.backup_dir + "/PROGRESS", &samples);
      if (sample_status.ok())
        metabypass::Backup::EstimateRecoveryTime(samples, report);
      status = metabypass::Write(
          fs.get(), index + "/METABYPASS-RECOVERED.tmp",
          point + "\n" + std::to_string(report->checkpoint_sequence) + " " +
              std::to_string(report->recovered_sequence) + " " +
              std::to_string(report->has_time_lower_bound) + " " +
              std::to_string(report->time_lower_bound_micros) + " " +
              std::to_string(report->has_time_upper_bound) + " " +
              std::to_string(report->time_upper_bound_micros) + " " +
              report->stop_reason + "\n");
      if (status.ok())
        status = fs->RenameFile(index + "/METABYPASS-RECOVERED.tmp",
                                index + "/METABYPASS-RECOVERED", IOOptions(),
                                nullptr);
      if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
      if (!status.ok()) return status;
    }
    TEST_SYNC_POINT("MetaBypass::RecoveryPersisted");
    if (tier) {
      status = tier->LoadCheckpoint(point + "/BLOB-MAP", true);
      if (!status.ok()) return status;
      if (!staging_identity_exists) {
        const std::string marker = m.staging_dir + "/METABYPASS-IDENTITY";
        status = metabypass::Write(fs.get(), marker + ".tmp", identity);
        if (status.ok())
          status =
              fs->RenameFile(marker + ".tmp", marker, IOOptions(), nullptr);
      }
      if (status.ok()) status = metabypass::SyncDir(fs.get(), m.staging_dir);
      if (!status.ok()) return status;
    }
    // A restore starts a new native sequence timeline. The report above is
    // durable before resetting samples so finalize retries retain its bounds.
    status = metabypass::Write(fs.get(), m.backup_dir + "/PROGRESS", "");
    if (status.ok()) status = metabypass::SyncDir(fs.get(), m.backup_dir);
    if (status.ok())
      status = metabypass::Write(fs.get(), index + "/METABYPASS", identity);
    if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
    TEST_SYNC_POINT("MetaBypass::RecoveryFinalized");
    if (status.ok())
      status =
          fs->DeleteFile(index + "/METABYPASS-RESTORING", IOOptions(), nullptr);
    if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
    return status;
  };
  s = restore();
  s.UpdateIfOk(fs->UnlockFile(backup_lock, IOOptions(), nullptr));
  return s;
}
Status MetaBypassDB::Put(const WriteOptions& o, const Slice& k,
                         const Slice& v) {
  WriteBatch b;
  Status s = b.Put(k, v);
  return s.ok() ? Write(o, &b) : s;
}
Status MetaBypassDB::Delete(const WriteOptions& o, const Slice& k) {
  WriteBatch b;
  Status s = b.Delete(k);
  return s.ok() ? Write(o, &b) : s;
}
Status MetaBypassDB::Write(const WriteOptions& o, WriteBatch* b) {
  if (!impl_->db) return Status::ShutdownInProgress();
  if (o.disableWAL) return Status::NotSupported("Metabypass requires WAL");
  if (!b) return Status::InvalidArgument("null WriteBatch");
  if (!b->GetWalTerminationPoint().is_cleared())
    return Status::NotSupported(
        "Metabypass requires the complete batch in WAL");
  SupportedBatch check;
  Status s = b->Iterate(&check);
  if (s.ok() && !check.supported)
    s = Status::NotSupported("Metabypass LogData");
  if (s.ok()) s = impl_->backup->Error();
  if (!s.ok()) return s;
  {
    std::shared_lock<std::shared_mutex> fence(impl_->backup->WriteFence());
    s = impl_->db->Write(o, b);
    if (s.ok()) impl_->backup->WriteCompleted();
  }
  // Count successful primary writes even if the following backup barrier fails.
  if (s.ok() && o.sync) {
    TEST_SYNC_POINT("MetaBypass::BeforeSyncWriteBarrier");
    const auto start = std::chrono::steady_clock::now();
    s = impl_->backup->Sync();
    impl_->sync_write_us.fetch_add(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count(),
        std::memory_order_relaxed);
  }
  return s;
}
Status MetaBypassDB::Get(const ReadOptions& o, const Slice& k, std::string* v) {
  return impl_->db ? impl_->db->Get(o, k, v) : Status::ShutdownInProgress();
}
Iterator* MetaBypassDB::NewIterator(const ReadOptions& o) {
  return impl_->db ? impl_->db->NewIterator(o)
                   : NewErrorIterator(Status::ShutdownInProgress());
}
Status MetaBypassDB::Flush(const FlushOptions& o) {
  Status s = impl_->backup->Error();
  if (s.ok())
    s = impl_->db ? impl_->db->Flush(o) : Status::ShutdownInProgress();
  return s;
}
Status MetaBypassDB::CompactRange(const CompactRangeOptions& o, const Slice* a,
                                  const Slice* b) {
  if (o.blob_garbage_collection_policy == BlobGarbageCollectionPolicy::kForce)
    return Status::NotSupported("Metabypass does not relocate blob values");
  Status s = impl_->backup->Error();
  if (s.ok())
    s = impl_->db ? impl_->db->CompactRange(o, a, b)
                  : Status::ShutdownInProgress();
  return s;
}
Status MetaBypassDB::SyncBackup() { return impl_->backup->Sync(); }
MetaBypassStats MetaBypassDB::GetBackupStats() const {
  auto stats = impl_->backup->Stats();
  if (impl_->pressure) impl_->pressure->AddStats(&stats);
  if (impl_->tier) impl_->tier->AddStats(&stats);
  if (impl_->sst) impl_->sst->AddStats(&stats);
  stats.sync_write_micros =
      impl_->sync_write_us.load(std::memory_order_relaxed);
  return stats;
}
Status MetaBypassDB::GetRecoveryProgress(
    MetaBypassRecoveryProgress* output) const {
  if (!output) return Status::InvalidArgument("null recovery progress output");
  std::lock_guard<std::mutex> lock(impl_->progress_close_mutex);
  if (!impl_->db) return Status::ShutdownInProgress("Metabypass DB closed");
  output->db_identity = impl_->recovery_identity;
  output->open_session_id = impl_->recovery_session;
  output->visible_sequence = impl_->db->GetLatestSequenceNumber();
  return Status::OK();
}
Status MetaBypassDB::Close() {
  std::lock_guard<std::mutex> lock(impl_->progress_close_mutex);
  if (!impl_->db) return impl_->closed;
  if (impl_->sst) impl_->sst->Stop();
  Status s = impl_->backup->Sync();
  impl_->backup->DetachDB();
  s.UpdateIfOk(impl_->db->Close());
  impl_->db.reset();
  s.UpdateIfOk(impl_->backup->Stop());
  // DB close releases cached readers and may enqueue final logical deletions.
  if (impl_->sst) impl_->sst->Stop();
  if (impl_->tier) s.UpdateIfOk(impl_->tier->Stop());
  if (impl_->pressure) impl_->pressure->Stop();
  impl_->closed = s;
  return impl_->closed;
}
}  // namespace ROCKSDB_NAMESPACE
