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
#include <utility>
#include <vector>

#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/listener.h"
#include "rocksdb/utilities/metabypass.h"
#include "rocksdb/wal_filter.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "utilities/metabypass/backup.h"
#include "utilities/metabypass/sst_storage.h"
#include "utilities/metabypass/wal_breakdown.h"

namespace ROCKSDB_NAMESPACE {
namespace {
constexpr char kFormat[] = "MBN7\n";
constexpr char kIndexLock[] = "METABYPASS-LOCK";
constexpr char kBackupLock[] = "LOCK";
constexpr char kFormatFile[] = "METABYPASS-FORMAT";
constexpr char kRestoring[] = "METABYPASS-RESTORING";

bool Missing(const Status& status) {
  return status.IsNotFound() || status.IsPathNotFound();
}
bool DirectoryMetadata(const std::string& name, const char* lock) {
  return name == "." || name == ".." || name == lock || name == kFormatFile;
}
Status CheckEmpty(FileSystem* fs, const std::string& path, const char* lock) {
  std::vector<std::string> children;
  Status s = fs->GetChildren(path, IOOptions(), &children, nullptr);
  if (Missing(s)) return Status::OK();
  if (!s.ok()) return s;
  for (const auto& child : children)
    if (!DirectoryMetadata(child, lock))
      return Status::InvalidArgument("Metabypass requires empty directory",
                                     path);
  return Status::OK();
}
// Refuse legacy directories before taking locks or initializing storage.
Status CheckFormat(FileSystem* fs, const std::string& path, const char* lock,
                   bool required) {
  std::string version;
  Status s = metabypass::Read(fs, path + "/" + kFormatFile, &version);
  if (s.ok())
    return version == kFormat ? Status::OK()
                              : Status::NotSupported("Metabypass format", path);
  if (!Missing(s)) return s;
  if (required) return Status::NotSupported("missing Metabypass format", path);
  s = CheckEmpty(fs, path, lock);
  return s.IsInvalidArgument()
             ? Status::NotSupported("unrecognized Metabypass directory", path)
             : s;
}
Status WriteFormat(FileSystem* fs, const std::string& path) {
  Status s = metabypass::Write(fs, path + "/" + kFormatFile, kFormat);
  if (s.ok()) s = metabypass::SyncDir(fs, path);
  return s;
}
Status Validate(const Options& o, const MetaBypassOptions& m,
                const std::string& index) {
  if (!o.env) return Status::InvalidArgument("null Env");
  if (o.enable_blob_files || o.enable_blob_direct_write ||
      o.enable_blob_garbage_collection)
    return Status::NotSupported("Metabypass requires Blob options disabled");
  auto valid_path = [](const std::string& p) {
    const std::filesystem::path path(p);
    return path.is_absolute() && !path.filename().empty() &&
           path.lexically_normal().generic_string() == path.generic_string();
  };
  if (!valid_path(index) || !valid_path(m.backup_dir))
    return Status::InvalidArgument(
        "Metabypass requires normalized absolute directories");
  auto overlap = [](const std::string& a, const std::string& b) {
    return a == b || a.compare(0, b.size() + 1, b + "/") == 0 ||
           b.compare(0, a.size() + 1, a + "/") == 0;
  };
  if (overlap(index, m.backup_dir))
    return Status::InvalidArgument("Metabypass directories must be disjoint");
  if (m.wal_queue_capacity == 0 || m.metadata_queue_capacity == 0 ||
      m.wal_queue_capacity >
          std::numeric_limits<size_t>::max() - m.metadata_queue_capacity)
    return Status::InvalidArgument("invalid Metabypass queue capacities");
  const size_t total_capacity =
      m.wal_queue_capacity + m.metadata_queue_capacity;
  const auto max_interval =
      std::chrono::duration_cast<std::chrono::duration<uint64_t, std::milli>>(
          std::chrono::steady_clock::duration::max())
          .count() /
      2;
  if (total_capacity < 4096 || m.batch_bytes == 0 ||
      m.batch_bytes > total_capacity || m.interval_ms == 0 ||
      m.interval_ms > max_interval)
    return Status::InvalidArgument("invalid Metabypass queue configuration");
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
  if (o.enable_pipelined_write || o.unordered_write || o.two_write_queues ||
      o.manual_wal_flush || o.sst_file_manager ||
      o.use_direct_io_for_flush_and_compaction || o.use_direct_reads ||
      o.allow_mmap_writes || !o.db_paths.empty() || !o.cf_paths.empty() ||
      !o.wal_dir.empty() || o.recycle_log_file_num || o.WAL_ttl_seconds ||
      o.WAL_size_limit_MB || o.wal_filter || o.track_and_verify_wals ||
      o.track_and_verify_wals_in_manifest || o.merge_operator ||
      o.compaction_filter || o.compaction_filter_factory ||
      o.comparator != BytewiseComparator() || o.best_efforts_recovery ||
      o.persist_stats_to_disk || o.allow_ingest_behind ||
      o.cf_allow_ingest_behind || o.atomic_flush || o.allow_2pc)
    return Status::NotSupported(
        "unsupported Metabypass configuration; ordered single-CF writes "
        "required");
  return Status::OK();
}
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
class RecoveryFilter : public WalFilter {
 public:
  explicit RecoveryFilter(MetaBypassRecoveryReport* report) : report_(report) {
    TEST_SYNC_POINT_CALLBACK("MetaBypass::RecoveryFilterCreated", this);
  }
  const char* Name() const override { return "MetaBypassContinuousRecovery"; }
  WalProcessingOption LogRecordFound(unsigned long long, const std::string&,
                                     const WriteBatch& batch, WriteBatch*,
                                     bool* changed) override {
    *changed = false;
    const uint64_t sequence = WriteBatchInternal::Sequence(&batch);
    if (sequence <= report_->checkpoint_sequence)
      return WalProcessingOption::kContinueProcessing;
    const uint64_t count = WriteBatchInternal::Count(&batch);
    if (count && sequence != report_->recovered_sequence + 1) {
      report_->stop_reason = "sequence_gap";
      return WalProcessingOption::kStopReplay;
    }
    SupportedBatch check;
    Status s = batch.Iterate(&check);
    if (!s.ok() || !check.supported) {
      report_->stop_reason = "unsupported_batch";
      return WalProcessingOption::kStopReplay;
    }
    if (count) report_->recovered_sequence = sequence + count - 1;
    return WalProcessingOption::kContinueProcessing;
  }

 private:
  MetaBypassRecoveryReport* report_;
};

class DirectoryLock {
 public:
  DirectoryLock() = default;
  DirectoryLock(const DirectoryLock&) = delete;
  DirectoryLock& operator=(const DirectoryLock&) = delete;
  ~DirectoryLock() { Unlock().PermitUncheckedError(); }

  Status Lock(FileSystem* fs, const std::string& path) {
    fs_ = fs;
    return fs_->LockFile(path, IOOptions(), &lock_, nullptr);
  }
  Status Unlock() {
    return lock_ ? fs_->UnlockFile(std::exchange(lock_, nullptr), IOOptions(),
                                   nullptr)
                 : IOStatus::OK();
  }

 private:
  FileSystem* fs_ = nullptr;
  FileLock* lock_ = nullptr;
};

Status CheckOpenDirectory(FileSystem* fs, const Options& options,
                          const std::string& index, bool* existing) {
  Status s = CheckFormat(fs, index, kIndexLock, false);
  if (!s.ok()) return s;
  s = fs->FileExists(index + "/" + kRestoring, IOOptions(), nullptr);
  if (s.ok())
    return Status::Incomplete("resume interrupted Metabypass Restore first");
  if (!Missing(s)) return s;
  s = fs->FileExists(index + "/CURRENT", IOOptions(), nullptr);
  *existing = s.ok();
  if (!*existing && !Missing(s)) return s;
  if (!*existing) {
    if (!options.create_if_missing)
      return Status::InvalidArgument("Metabypass index does not exist");
    return CheckEmpty(fs, index, kIndexLock);
  }
  return options.error_if_exists
             ? Status::InvalidArgument("Metabypass index already exists")
             : Status::OK();
}

Status CheckExistingOwner(FileSystem* fs, const MetaBypassOptions& m,
                          const std::string& index, std::string* identity) {
  Status s;
  std::string marker;
  s = metabypass::Read(fs, index + "/METABYPASS", &marker);
  if (!s.ok()) return s;
  s = metabypass::Read(fs, index + "/IDENTITY", identity);
  if (!s.ok()) return s;
  if (marker != *identity)
    return Status::Corruption("Metabypass identity mismatch");
  s = metabypass::Read(fs, m.backup_dir + "/METABYPASS-IDENTITY", &marker);
  if (!s.ok()) return s;
  if (marker != *identity)
    return Status::Corruption("Metabypass directory identity mismatch");
  if (m.sst_tiering.mode == SstTieringMode::kDisabled) {
    s = fs->FileExists(index + "/SST-PLACEMENT", IOOptions(), nullptr);
    if (s.ok())
      return Status::InvalidArgument("SST placement requires tiering enabled");
    if (!Missing(s)) return s;
  }
  return Status::OK();
}

Status CheckRestoreDestination(FileSystem* fs, const std::string& index,
                               const std::string& identity,
                               std::vector<std::string>* files,
                               bool* resuming) {
  Status s = CheckFormat(fs, index, kIndexLock, false);
  if (!s.ok()) return s;
  *resuming = false;
  files->clear();
  s = fs->GetChildren(index, IOOptions(), files, nullptr);
  if (Missing(s)) return Status::OK();
  if (!s.ok()) return s;
  for (const auto& file : *files) {
    if (DirectoryMetadata(file, kIndexLock)) continue;
    std::string marker;
    s = metabypass::Read(fs, index + "/" + kRestoring, &marker);
    if (Missing(s) || (s.ok() && marker != identity))
      return Status::InvalidArgument("restore destination is not resumable");
    if (!s.ok()) return s;
    *resuming = true;
    break;
  }
  return Status::OK();
}

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
  std::shared_ptr<metabypass::IoStats> io_stats;
  mutable std::mutex progress_close_mutex;
  std::string recovery_identity;
  std::string recovery_session;
  std::shared_ptr<metabypass::SstStorage> sst;
  std::atomic<uint64_t> sync_write_us{0};
  std::shared_ptr<metabypass::Backup> backup;
  DirectoryLock index_lock;
  std::unique_ptr<Env> env;
  std::unique_ptr<DB> db;
  Status closed;
  ~Impl() {
    if (sst) sst->Stop();
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
  std::string identity;
  bool existing;
  s = CheckOpenDirectory(fs.get(), options, index, &existing);
  if (s.ok()) s = CheckFormat(fs.get(), m.backup_dir, kBackupLock, existing);
  if (s.ok() && !existing) s = CheckEmpty(fs.get(), m.backup_dir, kBackupLock);
  if (!s.ok()) return s;

  auto impl = std::make_unique<Impl>();
  impl->io_stats = std::make_shared<metabypass::IoStats>();
  fs = metabypass::ObserveIo(fs, index, m.backup_dir, impl->io_stats);
  if (m.sst_tiering.mode != SstTieringMode::kDisabled)
    impl->sst = std::make_shared<metabypass::SstStorage>(fs, index, m);
  impl->backup = std::make_shared<metabypass::Backup>(fs, index, m, impl->sst);
  // Restore takes the same locks in the same order.
  s = impl->backup->Lock();
  if (s.ok()) s = CheckFormat(fs.get(), m.backup_dir, kBackupLock, existing);
  if (s.ok()) s = impl->backup->LoadLatest();
  if (s.ok()) s = metabypass::EnsureDir(fs.get(), index);
  if (s.ok()) s = impl->index_lock.Lock(fs.get(), index + "/" + kIndexLock);
  if (!s.ok()) return s;
  s = CheckOpenDirectory(fs.get(), options, index, &existing);
  if (s.ok()) {
    s = existing ? CheckExistingOwner(fs.get(), m, index, &identity)
                 : CheckEmpty(fs.get(), m.backup_dir, kBackupLock);
  }
  if (!s.ok()) return s;

  if (existing && impl->sst) {
    s = impl->sst->Initialize(identity, false);
    if (!s.ok()) return s;
  }
  if (!existing) {
    s = WriteFormat(fs.get(), index);
    if (s.ok()) s = WriteFormat(fs.get(), m.backup_dir);
    if (!s.ok()) return s;
  }
  if (impl->sst) {
    std::weak_ptr<metabypass::Backup> weak = impl->backup;
    impl->sst->SetFailureHandler([weak](const Status& error) {
      if (auto backup = weak.lock()) backup->StorageFailed(error);
    });
  }
  s = impl->backup->Start(existing);
  if (!s.ok()) return s;
  impl->env = NewCompositeEnv(impl->backup);
  Options configured = options;
  configured.env = impl->env.get();
  configured.listeners.push_back(
      std::make_shared<BackupListener>(impl->backup));
  s = DB::Open(configured, index, &impl->db);
  if (s.ok()) {
    s = metabypass::Read(fs.get(), index + "/IDENTITY", &identity);
    // Reopening must not truncate the identity marker validated above.
    if (s.ok() && !existing)
      s = metabypass::Write(fs.get(), index + "/METABYPASS", identity);
    if (s.ok()) s = metabypass::SyncDir(fs.get(), index);
    if (s.ok() && impl->sst && !existing)
      s = impl->sst->Initialize(identity, false);
    if (s.ok())
      s = impl->backup->Activate(identity, impl->db.get(),
                                 options.env->GetSystemClock());
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
  metabypass::Backup::RecoveryPoint point;
  std::vector<std::string> files;
  bool resuming;
  s = CheckFormat(fs.get(), m.backup_dir, kBackupLock, true);
  if (s.ok()) s = CheckFormat(fs.get(), index, kIndexLock, false);
  if (!s.ok()) return s;

  DirectoryLock backup_lock, index_lock;
  s = backup_lock.Lock(fs.get(), m.backup_dir + "/" + kBackupLock);
  if (s.ok()) s = CheckFormat(fs.get(), m.backup_dir, kBackupLock, true);
  if (s.ok())
    s = metabypass::Backup::ReadRecoveryPoint(fs.get(), m.backup_dir, &point);
  if (s.ok())
    s = CheckRestoreDestination(fs.get(), index, point.identity, &files,
                                &resuming);
  if (s.ok()) s = metabypass::EnsureDir(fs.get(), index);
  if (s.ok()) s = index_lock.Lock(fs.get(), index + "/" + kIndexLock);
  if (!s.ok()) return s;
  s = CheckRestoreDestination(fs.get(), index, point.identity, &files,
                              &resuming);
  if (!s.ok()) return s;

  auto restore = [&]() -> Status {
    std::string persisted;
    Status status;
    status =
        metabypass::Read(fs.get(), index + "/METABYPASS-RECOVERED", &persisted);
    const bool finalized_native = status.ok();
    if (!finalized_native && !Missing(status)) return status;
    if (finalized_native) {
      std::istringstream in(persisted);
      if (!(in >> report->checkpoint_sequence >> report->recovered_sequence))
        return Status::Corruption("restore progress marker");
      if (!(in >> report->has_time_lower_bound >>
            report->time_lower_bound_micros >> report->has_time_upper_bound >>
            report->time_upper_bound_micros))
        return Status::Corruption("restore time marker");
      if (!m.enable_recovery_time_sampling) {
        report->has_time_lower_bound = report->has_time_upper_bound = false;
        report->time_lower_bound_micros = report->time_upper_bound_micros = 0;
      }
      std::getline(in, report->stop_reason);
      if (!report->stop_reason.empty()) report->stop_reason.erase(0, 1);
    } else {
      // Prior preparation may have opened and flushed native files, but its
      // source mappings remain untouched until the persisted-stage marker.
      for (const auto& file : files) {
        if (DirectoryMetadata(file, kIndexLock) || file == kRestoring) continue;
        status = fs->DeleteFile(index + "/" + file, IOOptions(), nullptr);
        if (!status.ok()) return status;
      }
      if (!resuming) {
        status = WriteFormat(fs.get(), index);
        if (status.ok())
          status = metabypass::Write(fs.get(), index + "/" + kRestoring,
                                     point.identity);
      } else {
        status = Status::OK();
      }
      if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
      if (!status.ok()) return status;
      std::shared_ptr<metabypass::SstStorage> sst;
      if (m.sst_tiering.mode != SstTieringMode::kDisabled) {
        sst = std::make_shared<metabypass::SstStorage>(fs, index, m);
        status = sst->Initialize(point.identity, true);
        if (!status.ok()) return status;
      }
      status = metabypass::Backup::PrepareRecovery(
          fs.get(), m.backup_dir, point, index, report, sst.get());
      if (!status.ok()) return status;
      TEST_SYNC_POINT("MetaBypass::RecoveryPrepared");
      RecoveryFilter filter(report);
      auto env = NewCompositeEnv(sst ? std::shared_ptr<FileSystem>(sst) : fs);
      Options recovery = options;
      recovery.env = env.get();
      recovery.disable_auto_compactions = true;
      recovery.avoid_flush_during_recovery = false;
      recovery.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
      recovery.wal_filter = &filter;
      recovery.create_if_missing = false;
      recovery.error_if_exists = false;
      std::unique_ptr<DB> native;
      status = DB::Open(recovery, index, &native);
      if (!status.ok()) return status;
      if (report->stop_reason.empty())
        report->stop_reason = "native_replay_end";
      status = native->Close();
      native.reset();
      if (!status.ok()) return status;
      TEST_SYNC_POINT("MetaBypass::RecoveryNativeClosed");
      if (m.enable_recovery_time_sampling) {
        std::string samples;
        Status sample_status =
            metabypass::Read(fs.get(), m.backup_dir + "/PROGRESS", &samples);
        if (sample_status.ok())
          metabypass::Backup::EstimateRecoveryTime(samples, report);
      }
      status = metabypass::Write(
          fs.get(), index + "/METABYPASS-RECOVERED.tmp",
          std::to_string(report->checkpoint_sequence) + " " +
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
    // A restore starts a new native sequence timeline. The report above is
    // durable before resetting samples so finalize retries retain its bounds.
    status = metabypass::Write(fs.get(), m.backup_dir + "/PROGRESS", "");
    if (status.ok()) status = metabypass::SyncDir(fs.get(), m.backup_dir);
    if (status.ok())
      status =
          metabypass::Write(fs.get(), index + "/METABYPASS", point.identity);
    if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
    TEST_SYNC_POINT("MetaBypass::RecoveryFinalized");
    if (status.ok())
      status = fs->DeleteFile(index + "/" + kRestoring, IOOptions(), nullptr);
    if (status.ok()) status = metabypass::SyncDir(fs.get(), index);
    return status;
  };
  s = restore();
  s.UpdateIfOk(index_lock.Unlock());
  s.UpdateIfOk(backup_lock.Unlock());
  return s;
}
Status MetaBypassDB::Put(const WriteOptions& o, const Slice& k,
                         const Slice& v) {
  WriteBatch b;
  Status s = b.Put(k, v);
  return s.ok() ? WriteImpl(o, &b, false) : s;
}
Status MetaBypassDB::Delete(const WriteOptions& o, const Slice& k) {
  WriteBatch b;
  Status s = b.Delete(k);
  return s.ok() ? WriteImpl(o, &b, false) : s;
}
Status MetaBypassDB::Write(const WriteOptions& o, WriteBatch* b) {
  return WriteImpl(o, b, true);
}
Status MetaBypassDB::WriteImpl(const WriteOptions& o, WriteBatch* b,
                               bool check_batch) {
  if (!impl_->db) return Status::ShutdownInProgress();
  if (o.disableWAL) return Status::NotSupported("Metabypass requires WAL");
  if (!b) return Status::InvalidArgument("null WriteBatch");
  if (!b->GetWalTerminationPoint().is_cleared())
    return Status::NotSupported(
        "Metabypass requires the complete batch in WAL");
  Status s;
  if (check_batch) {
    MB_WAL_SPAN(validation, batch_validation, true);
    SupportedBatch check;
    s = b->Iterate(&check);
    if (s.ok() && !check.supported)
      s = Status::NotSupported("Metabypass LogData");
  }
  if (s.ok()) {
    MB_WAL_SPAN(error, error_check, true);
    s = impl_->backup->Error();
  }
  if (!s.ok()) return s;
  {
    MB_WAL_SPAN(wait, write_fence_wait, true);
    auto writer = impl_->backup->EnterWrite();
    MB_WAL_END(wait);
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
  Status s = impl_->backup->Error();
  if (s.ok())
    s = impl_->db ? impl_->db->CompactRange(o, a, b)
                  : Status::ShutdownInProgress();
  return s;
}
Status MetaBypassDB::SyncBackup() { return impl_->backup->Sync(); }
MetaBypassStats MetaBypassDB::GetBackupStats() const {
  auto stats = impl_->backup->Stats();
  impl_->io_stats->AddStats(&stats);
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
  impl_->closed = s;
  return impl_->closed;
}
}  // namespace ROCKSDB_NAMESPACE
