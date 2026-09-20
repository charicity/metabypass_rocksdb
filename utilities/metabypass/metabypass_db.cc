//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <filesystem>
#include <limits>

#include "rocksdb/convenience.h"
#include "rocksdb/utilities/metabypass.h"
#include "rocksdb/write_batch.h"
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
  std::shared_ptr<metabypass::SeparatedStorage> storage;
  std::shared_ptr<metabypass::Backup> backup;
  std::unique_ptr<Env> env;
  std::unique_ptr<DB> db;
  Status closed;
  ~Impl() { closed.PermitUncheckedError(); }
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
  impl->storage =
      std::make_shared<metabypass::SeparatedStorage>(fs, index, m.data_dir);
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
    for (const auto& path : {m.data_dir, m.backup_dir}) {
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
    s = impl->storage->PrepareRecovery(index);
    if (!s.ok()) return s;
  }
  impl->backup =
      std::make_shared<metabypass::Backup>(impl->storage, index, m, options);
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
  metabypass::SeparatedStorage storage(fs, index, m.data_dir);
  s = storage.Lock();
  if (!s.ok()) return s;
  FileLock* backup_lock = nullptr;
  s = fs->LockFile(m.backup_dir + "/LOCK", IOOptions(), &backup_lock, nullptr);
  if (!s.ok()) return s;
  std::string data_owner, backup_owner;
  s = metabypass::Read(fs.get(), m.data_dir + "/METABYPASS-IDENTITY",
                       &data_owner);
  if (s.ok())
    s = metabypass::Read(fs.get(), m.backup_dir + "/METABYPASS-IDENTITY",
                         &backup_owner);
  if (s.ok() && data_owner != backup_owner)
    s = Status::Corruption("data and backup identity mismatch");
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
    s = metabypass::Backup::RestoreFiles(fs.get(), m.backup_dir, index,
                                         storage);
  if (s.ok()) s = storage.PrepareRecovery(index);
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
  if (s.ok()) s = impl_->db->Write(o, b);
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
  return impl_->backup->Stats();
}
Status MetaBypassDB::Close() {
  if (!impl_->db) return impl_->closed;
  Status s = impl_->db->Close();
  impl_->db.reset();
  s.UpdateIfOk(impl_->backup->Stop());
  impl_->closed = s;
  return impl_->closed;
}
}  // namespace ROCKSDB_NAMESPACE
