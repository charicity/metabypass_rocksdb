//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <atomic>
#include <chrono>
#include <filesystem>
#include <limits>
#include <mutex>

#include "db/blob/blob_file_partition_manager.h"
#include "db/column_family.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/metabypass.h"
#include "rocksdb/write_batch.h"
#include "util/cast_util.h"
#include "utilities/metabypass/backup.h"

namespace ROCKSDB_NAMESPACE {
namespace {
Status Validate(const Options& o, const MetaBypassOptions& m,
                const std::string& index) {
  if (!o.env) return Status::InvalidArgument("null Env");
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
  if (!o.write_identity_file)
    return Status::NotSupported("Metabypass requires write_identity_file");
  if (o.compaction_service)
    return Status::NotSupported("Metabypass remote compaction service");
  if (o.allow_concurrent_memtable_write || o.enable_pipelined_write ||
      o.unordered_write || o.two_write_queues || o.manual_wal_flush ||
      o.sst_file_manager || o.use_direct_io_for_flush_and_compaction ||
      o.use_direct_reads || o.allow_mmap_writes || !o.db_paths.empty() ||
      !o.cf_paths.empty() || !o.wal_dir.empty() || o.recycle_log_file_num ||
      o.WAL_ttl_seconds || o.WAL_size_limit_MB || o.wal_filter ||
      o.track_and_verify_wals || o.track_and_verify_wals_in_manifest ||
      o.merge_operator || o.compaction_filter || o.compaction_filter_factory ||
      o.comparator != BytewiseComparator() ||
      o.enable_blob_garbage_collection || o.best_efforts_recovery ||
      o.persist_stats_to_disk || o.allow_ingest_behind ||
      o.cf_allow_ingest_behind || o.atomic_flush ||
      (o.enable_blob_files && !o.enable_blob_direct_write))
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
}  // namespace
struct MetaBypassDB::Impl {
  std::shared_ptr<metabypass::TieredStorage> tier;
  std::mutex admission;
  std::atomic<uint64_t> sync_write_us{0};
  std::shared_ptr<metabypass::SeparatedStorage> storage;
  std::shared_ptr<metabypass::Backup> backup;
  std::unique_ptr<Env> env;
  std::unique_ptr<DB> db;
  Status closed;
  ~Impl() {
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
  auto impl = std::make_unique<Impl>();
  auto fs = options.env->GetFileSystem();
  if (!m.staging_dir.empty())
    impl->tier = std::make_shared<metabypass::TieredStorage>(fs, m);
  impl->storage = std::make_shared<metabypass::SeparatedStorage>(
      fs, index, m.data_dir, impl->tier);
  s = impl->storage->Lock();
  if (!s.ok()) return s;
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
    if (impl->tier) directories.push_back(m.staging_dir);
    for (const auto& path : directories) {
      std::vector<std::string> children;
      s = fs->GetChildren(path, IOOptions(), &children, nullptr);
      if (s.IsNotFound()) continue;
      if (!s.ok()) return s;
      for (const auto& child : children) {
        if (child != "." && child != ".." && child != "METABYPASS-LOCK")
          return Status::InvalidArgument(
              "new Metabypass requires empty data and backup directories");
      }
    }
  }
  const std::string format_path = m.data_dir + "/METABYPASS-TIERED";
  Status format = fs->FileExists(format_path, IOOptions(), nullptr);
  if (!format.ok() && !format.IsNotFound()) return format;
  if (existing && format.ok() != bool(impl->tier))
    return Status::InvalidArgument("cannot change Metabypass storage format");
  if (impl->tier) {
    if (existing) {
      std::string version;
      s = metabypass::Read(fs.get(), format_path, &version);
      if (!s.ok()) return s;
      if (version != "MBT1\n")
        return Status::NotSupported("tiered storage format");
    }
    s = impl->tier->Initialize(false);
    if (!s.ok()) return s;
    if (!existing) {
      s = metabypass::Write(fs.get(), format_path, "MBT1\n");
      if (s.ok()) s = metabypass::SyncDir(fs.get(), m.data_dir);
      if (!s.ok()) return s;
    }
  }
  if (existing) {
    std::string marker, identity;
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
    if (impl->tier) {
      std::string staging_owner;
      s = metabypass::Read(fs.get(), m.staging_dir + "/METABYPASS-IDENTITY",
                           &staging_owner);
      if (!s.ok()) return s;
      if (staging_owner != identity)
        return Status::Corruption("staging identity mismatch");
    }
    s = impl->storage->PrepareRecovery(index);
    if (!s.ok()) return s;
  }
  impl->backup =
      std::make_shared<metabypass::Backup>(impl->storage, index, m, options);
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
  configured.enable_blob_files = true;
  configured.enable_blob_direct_write = true;
  configured.min_blob_size = 0;
  configured.enable_blob_garbage_collection = false;
  configured.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  s = DB::Open(configured, index, &impl->db);
  if (s.ok()) {
    std::string identity;
    s = metabypass::Read(fs.get(), index + "/IDENTITY", &identity);
    // Reopening must not truncate the identity marker validated above.
    if (s.ok() && !existing)
      s = metabypass::Write(fs.get(), index + "/METABYPASS", identity);
    if (s.ok()) s = metabypass::SyncDir(fs.get(), index);
    if (s.ok() && impl->tier && !existing)
      s = metabypass::Write(fs.get(), m.staging_dir + "/METABYPASS-IDENTITY",
                            identity);
    if (s.ok() && impl->tier) s = metabypass::SyncDir(fs.get(), m.staging_dir);
    if (s.ok()) s = impl->backup->Activate(identity);
    if (s.ok()) s = impl->backup->Sync();
  }
  if (!s.ok()) {
    if (impl->db) {
      impl->db->Close().PermitUncheckedError();
      impl->db.reset();
    }
    impl->backup->Stop().PermitUncheckedError();
    return s;
  }
  result->reset(new MetaBypassDB(std::move(impl)));
  return Status::OK();
}
Status MetaBypassDB::Restore(const Options& options, const MetaBypassOptions& m,
                             const std::string& index) {
  Status s = Validate(options, m, index);
  if (!s.ok()) return s;
  auto fs = options.env->GetFileSystem();
  std::shared_ptr<metabypass::TieredStorage> tier;
  if (!m.staging_dir.empty())
    tier = std::make_shared<metabypass::TieredStorage>(fs, m);
  metabypass::SeparatedStorage storage(fs, index, m.data_dir, tier);
  s = storage.Lock();
  if (!s.ok()) return s;
  FileLock* backup_lock = nullptr;
  s = fs->LockFile(m.backup_dir + "/LOCK", IOOptions(), &backup_lock, nullptr);
  if (!s.ok()) return s;
  if (tier) {
    std::string version;
    s = metabypass::Read(fs.get(), m.data_dir + "/METABYPASS-TIERED", &version);
    if (s.ok() && version != "MBT1\n")
      s = Status::NotSupported("tiered storage format");
    if (s.ok()) s = tier->Initialize(true);
    if (!s.ok()) {
      fs->UnlockFile(backup_lock, IOOptions(), nullptr).PermitUncheckedError();
      return s;
    }
  } else {
    Status format =
        fs->FileExists(m.data_dir + "/METABYPASS-TIERED", IOOptions(), nullptr);
    if (!format.IsNotFound()) {
      fs->UnlockFile(backup_lock, IOOptions(), nullptr).PermitUncheckedError();
      return format.ok()
                 ? Status::InvalidArgument("tiered restore requires staging")
                 : format;
    }
  }
  std::string data_owner, backup_owner;
  s = metabypass::Read(fs.get(), m.data_dir + "/METABYPASS-IDENTITY",
                       &data_owner);
  if (s.ok())
    s = metabypass::Read(fs.get(), m.backup_dir + "/METABYPASS-IDENTITY",
                         &backup_owner);
  if (s.ok() && data_owner != backup_owner)
    s = Status::Corruption("data and backup identity mismatch");
  bool staging_identity_exists = false;
  if (s.ok() && tier) {
    std::vector<std::string> staging_files;
    s = fs->GetChildren(m.staging_dir, IOOptions(), &staging_files, nullptr);
    for (const auto& file : staging_files) {
      if (!s.ok()) break;
      if (file == "." || file == ".." || file == "METABYPASS-LOCK" ||
          file == "METABYPASS-IDENTITY.tmp")
        continue;
      if (file != "METABYPASS-IDENTITY") {
        s = Status::InvalidArgument("restore staging is not empty");
        break;
      }
      std::string owner;
      s = metabypass::Read(fs.get(), m.staging_dir + "/" + file, &owner);
      if (s.ok() && owner != backup_owner)
        s = Status::Corruption("restore staging identity mismatch");
      if (s.ok()) staging_identity_exists = true;
    }
  }
  if (s.ok()) s = metabypass::EnsureDir(fs.get(), index);
  std::vector<std::string> children;
  if (s.ok()) s = fs->GetChildren(index, IOOptions(), &children, nullptr);
  bool nonempty = false;
  for (const auto& f : children)
    if (f != "." && f != "..") nonempty = true;
  if (s.ok() && nonempty) {
    std::string marker;
    s = metabypass::Read(fs.get(), index + "/METABYPASS-RESTORING", &marker);
    if (!s.ok() || marker != backup_owner)
      s = Status::InvalidArgument(
          "restore destination is not empty or resumable");
  }
  // A resumed restore already has a verified marker. Preserve it so an
  // interruption cannot invalidate the next retry.
  if (s.ok() && !nonempty)
    s = metabypass::Write(fs.get(), index + "/METABYPASS-RESTORING",
                          backup_owner);
  if (s.ok()) s = metabypass::SyncDir(fs.get(), index);
  if (s.ok())
    s = metabypass::Backup::RestoreFiles(storage.target(), m.backup_dir, index,
                                         storage);
  if (s.ok()) s = storage.PrepareRecovery(index);
  if (s.ok() && tier && !staging_identity_exists) {
    const std::string marker = m.staging_dir + "/METABYPASS-IDENTITY";
    s = metabypass::Write(fs.get(), marker + ".tmp", backup_owner);
    if (s.ok())
      s = fs->RenameFile(marker + ".tmp", marker, IOOptions(), nullptr);
  }
  if (s.ok() && tier) s = metabypass::SyncDir(fs.get(), m.staging_dir);
  if (s.ok()) {
    std::string identity;
    s = metabypass::Read(fs.get(), index + "/IDENTITY", &identity);
    if (s.ok())
      s = metabypass::Write(fs.get(), index + "/METABYPASS", identity);
    if (s.ok()) s = metabypass::SyncDir(fs.get(), index);
  }
  if (s.ok())
    s = fs->DeleteFile(index + "/METABYPASS-RESTORING", IOOptions(), nullptr);
  if (s.ok()) s = metabypass::SyncDir(fs.get(), index);
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
  SupportedBatch check;
  Status s = b->Iterate(&check);
  if (s.ok() && !check.supported)
    s = Status::NotSupported("Metabypass LogData");
  if (s.ok()) s = impl_->backup->Error();
  if (!s.ok()) return s;
  if (!impl_->tier) {
    s = impl_->db->Write(o, b);
  } else {
    // Admission serializes tiered writes only, before acquiring any DB lock.
    // A conservative compression and per-record envelope bounds staging growth.
    std::unique_lock<std::mutex> admission(impl_->admission);
    const uint64_t bytes = b->GetDataSize();
    const uint64_t count = b->Count();
    if (bytes > (UINT64_MAX - 4096) / 2 ||
        count > (UINT64_MAX - bytes * 2 - 4096) / 128)
      return Status::InvalidArgument("batch staging charge overflow");
    const uint64_t charge = bytes * 2 + count * 128 + 4096;
    auto* cf = static_cast_with_check<ColumnFamilyHandleImpl>(
        impl_->db->DefaultColumnFamily());
    s = impl_->tier->Reserve(charge, [&] {
      auto* manager = cf->cfd()->blob_partition_manager();
      return manager ? manager->SealForSpace(WriteOptions()) : Status::OK();
    });
    if (s.ok()) {
      s = impl_->backup->Error();
      if (s.ok()) s = impl_->db->Write(o, b);
      impl_->tier->ReleaseReservation();
    }
  }
  // The primary write and any tiered admission lock finish before publication.
  if (s.ok() && o.sync) {
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
  if (impl_->tier) impl_->tier->AddStats(&stats);
  stats.sync_write_micros =
      impl_->sync_write_us.load(std::memory_order_relaxed);
  return stats;
}
Status MetaBypassDB::Close() {
  if (!impl_->db) return impl_->closed;
  Status s = impl_->db->Close();
  impl_->db.reset();
  s.UpdateIfOk(impl_->backup->Stop());
  if (impl_->tier) s.UpdateIfOk(impl_->tier->Stop());
  impl_->closed = s;
  return impl_->closed;
}
}  // namespace ROCKSDB_NAMESPACE
