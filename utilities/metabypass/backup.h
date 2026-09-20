//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <set>
#include <thread>

#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/separated_storage.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
class Backup : public FileSystemWrapper {
 public:
  Backup(std::shared_ptr<SeparatedStorage> storage, std::string index,
         MetaBypassOptions options, Options db_options = Options());
  ~Backup() override;
  const char* Name() const override { return "MetaBypassBackup"; }
  Status Start(bool existing);
  Status Activate(const std::string& identity);
  Status Sync();
  Status Stop();
  MetaBypassStats Stats() const;
  Status Error() const;
  static Status RestoreFiles(FileSystem*, const std::string& backup,
                             const std::string& destination,
                             const SeparatedStorage& storage);
  IOStatus NewWritableFile(const std::string&, const FileOptions&,
                           std::unique_ptr<FSWritableFile>*,
                           IODebugContext*) override;
  IOStatus ReopenWritableFile(const std::string&, const FileOptions&,
                              std::unique_ptr<FSWritableFile>*,
                              IODebugContext*) override;
  IOStatus ReuseWritableFile(const std::string&, const std::string&,
                             const FileOptions&,
                             std::unique_ptr<FSWritableFile>*,
                             IODebugContext*) override;
  IOStatus DeleteFile(const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus RenameFile(const std::string&, const std::string&, const IOOptions&,
                      IODebugContext*) override;
  IOStatus LinkFile(const std::string&, const std::string&, const IOOptions&,
                    IODebugContext*) override;

 private:
  friend class MirrorWriter;
  enum class Kind { kCreate, kAppend, kTruncate, kClose, kDelete, kRename };
  struct Event {
    Kind kind;
    std::string name, other, bytes;
    uint64_t size = 0;
    uint64_t seq = 0;
    uint64_t queued_micros = 0;
    size_t charge = 0;
  };
  bool Tracked(const std::string& path) const;
  std::string Base(const std::string& path) const;
  Status Reserve(size_t charge);
  void Fail(const Status& status);
  void Finish(Event&& event, const Status& primary);
  Status Apply(const Event& event);
  struct Candidate {
    uint64_t seq = 0, started = 0, oldest = 0, work_bytes = 0;
    std::string name, path;
    std::set<std::string> closed;
    std::map<std::string, uint64_t> epochs;
  };
  Status Capture(Candidate* candidate);
  Status Publish(const Candidate& candidate);
  // Called with mutex_ held. Each wait channel has a distinct predicate.
  bool MirrorReady() const;
  void WakeMirror();
  void Run();
  void ValidateLoop();
  struct TableValidation {
    uint64_t epoch = 0, size = 0;
    std::map<uint64_t, uint64_t> blobs;
  };
  struct FileDigest {
    uint64_t epoch = 0, length = 0;
    uint32_t crc = 0;
  };
  // Accessed only by the validator; never persisted or shared with recovery.
  std::map<std::string, TableValidation> table_cache_;
  std::map<std::string, FileDigest> digest_cache_;
  std::map<std::string, uint64_t> validation_epochs_;
  WalValidationCache wal_cache_;
  BlobValidationCache blob_cache_;
  std::shared_ptr<SeparatedStorage> storage_;
  FileSystem* disk_;
  const std::string index_;
  const MetaBypassOptions options_;
  const Options db_options_;
  const std::string work_;
  // Serializes successful primary operations and queue insertion. Worker never
  // acquires this mutex or any DB lock, including while persisting
  // dependencies.
  std::mutex operations_;
  // Orders failure recording with pointer commit, without holding the queue
  // mutex across filesystem I/O. Never acquired by successful producers.
  std::mutex publication_mutex_;
  mutable std::mutex mutex_;
  std::condition_variable mirror_cv_, validator_cv_, space_cv_, sync_cv_;
  bool mirror_waiting_ = false;
  // operations_ serializes reservations, so at most one producer waits.
  size_t waiting_charge_ = 0;
  std::deque<Event> queue_;
  MetaBypassStats stats_;
  uint64_t accepted_ = 0, applied_ = 0, published_ = 0, requested_ = 0;
  uint64_t generation_ = 0;
  uint64_t oldest_unpublished_micros_ = 0;
  bool stopping_ = false;
  bool active_ = false;
  bool candidate_busy_ = false, mirror_done_ = false;
  uint64_t captured_ = 0;
  std::unique_ptr<Candidate> candidate_;
  std::thread thread_, validator_;
  uint64_t next_epoch_ = 0;
  std::map<std::string, uint64_t> epochs_;
  std::set<std::string> closed_;
  std::map<std::string, std::unique_ptr<FSWritableFile>> writers_;
  std::deque<std::string> points_;
  FileLock* lock_ = nullptr;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
