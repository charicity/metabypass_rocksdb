//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/backup.h"

#include <cassert>
#include <chrono>
#include <limits>
#include <sstream>

#include "file/filename.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
uint64_t Now() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
IOStatus AsIO(const Status& s) {
  return s.ok() ? IOStatus::OK() : IOStatus::IOError(s.ToString());
}
bool SafeName(const std::string& s) {
  return !s.empty() && s != "." && s != ".." &&
         s.find_first_of("/\\ \t\r\n") == std::string::npos;
}
Status ReadPublishedInventory(FileSystem* fs, const std::string& backup,
                              std::string* point, std::string* inventory) {
  std::string owner;
  Status s = Read(fs, backup + "/METABYPASS-IDENTITY", &owner);
  if (!s.ok()) return s;
  std::string pointer;
  s = Read(fs, backup + "/LATEST", &pointer);
  if (!s.ok()) return s;
  std::istringstream p(pointer);
  std::string name;
  uint32_t inventory_crc;
  if (!(p >> name >> inventory_crc) || !SafeName(name) ||
      name.compare(0, 6, "point-") != 0)
    return Status::Corruption("invalid published pointer");
  *point = backup + "/" + name;
  std::string identity;
  s = Read(fs, *point + "/IDENTITY", &identity);
  if (!s.ok()) return s;
  if (identity != owner) return Status::Corruption("backup identity mismatch");
  s = Read(fs, *point + "/INVENTORY", inventory);
  if (!s.ok()) return s;
  if (crc32c::Value(inventory->data(), inventory->size()) != inventory_crc)
    return Status::Corruption("published inventory checksum");
  if (inventory->compare(0, 5, "MBI2\n") != 0)
    return Status::NotSupported("Metabypass inventory format");
  return Status::OK();
}
Status RemoveRetiredPoint(FileSystem* fs, const std::string& path) {
  std::vector<std::string> files;
  Status s = fs->GetChildren(path, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  for (const auto& file : files) {
    if (file == "." || file == "..") continue;
    s = fs->DeleteFile(path + "/" + file, IOOptions(), nullptr);
    if (!s.ok()) return s;
    TEST_SYNC_POINT("MetaBypass::GCFileDeleted");
  }
  return fs->DeleteDir(path, IOOptions(), nullptr);
}
}  // namespace
class MirrorWriter : public FSWritableFileOwnerWrapper {
 public:
  MirrorWriter(std::unique_ptr<FSWritableFile> file, Backup* backup,
               std::string name, Backup::Channel& channel)
      : FSWritableFileOwnerWrapper(std::move(file)),
        backup_(backup),
        name_(std::move(name)),
        channel_(channel) {}
  IOStatus Append(const Slice& bytes, const IOOptions& o,
                  IODebugContext* d) override {
    return AppendInternal(bytes, o, nullptr, d);
  }
  IOStatus Append(const Slice& b, const IOOptions& o,
                  const DataVerificationInfo& verification,
                  IODebugContext* d) override {
    return AppendInternal(b, o, &verification, d);
  }
  IOStatus PositionedAppend(const Slice&, uint64_t, const IOOptions&,
                            IODebugContext*) override {
    return IOStatus::NotSupported("Metabypass positioned writes");
  }
  IOStatus PositionedAppend(const Slice& b, uint64_t n, const IOOptions& o,
                            const DataVerificationInfo&,
                            IODebugContext* d) override {
    return PositionedAppend(b, n, o, d);
  }
  IOStatus Truncate(uint64_t n, const IOOptions& o,
                    IODebugContext* d) override {
    return Control(Backup::Kind::kTruncate, n, o, d);
  }
  IOStatus Close(const IOOptions& o, IODebugContext* d) override {
    return Control(Backup::Kind::kClose, 0, o, d);
  }

 private:
  IOStatus AppendInternal(const Slice& bytes, const IOOptions& o,
                          const DataVerificationInfo* verification,
                          IODebugContext* d) {
    std::lock_guard<std::mutex> serial(channel_.operations);
    Backup::Event e{Backup::Kind::kAppend, name_, {}, {}};
    e.charge = sizeof(e) + name_.size() + bytes.size();
    Status s = backup_->Reserve(channel_, e.charge);
    if (!s.ok()) return AsIO(s);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::PrimaryReserved", &e.name);
    e.bytes.assign(bytes.data(), bytes.size());
    IOStatus primary = verification
                           ? target()->Append(bytes, o, *verification, d)
                           : target()->Append(bytes, o, d);
    backup_->Finish(channel_, std::move(e), primary);
    return primary;
  }
  IOStatus Control(Backup::Kind kind, uint64_t n, const IOOptions& o,
                   IODebugContext* d) {
    std::lock_guard<std::mutex> serial(channel_.operations);
    Backup::Event e{kind, name_, {}, {}};
    e.size = n;
    e.charge = sizeof(e) + name_.size();
    Status s = backup_->Reserve(channel_, e.charge);
    if (!s.ok()) {
      if (kind == Backup::Kind::kClose)
        target()->Close(o, d).PermitUncheckedError();
      return AsIO(s);
    }
    IOStatus primary = kind == Backup::Kind::kClose
                           ? target()->Close(o, d)
                           : target()->Truncate(n, o, d);
    backup_->Finish(channel_, std::move(e), primary);
    return primary;
  }
  Backup* backup_;
  std::string name_;
  Backup::Channel& channel_;
};
Backup::Backup(std::shared_ptr<SeparatedStorage> storage, std::string index,
               MetaBypassOptions options, Options db_options)
    : FileSystemWrapper(storage),
      storage_(std::move(storage)),
      disk_(storage_->target()),
      index_(std::move(index)),
      options_(std::move(options)),
      db_options_(std::move(db_options)),
      work_(options_.backup_dir + "/work") {}
Backup::~Backup() {
  Stop().PermitUncheckedError();
  if (lock_)
    disk_->UnlockFile(lock_, IOOptions(), nullptr).PermitUncheckedError();
  stats_.error.PermitUncheckedError();
  capture_status_.PermitUncheckedError();
}
std::string Backup::Base(const std::string& path) const {
  return path.substr(index_.size() + 1);
}
bool Backup::Tracked(const std::string& path) const {
  if (path.compare(0, index_.size() + 1, index_ + "/") != 0) return false;
  uint64_t number;
  FileType type;
  if (!ParseFileName(Base(path), &number, &type)) return false;
  return type == kWalFile || type == kTableFile || type == kDescriptorFile ||
         type == kCurrentFile || type == kOptionsFile ||
         type == kIdentityFile || type == kTempFile;
}
Backup::Channel& Backup::ChannelFor(const std::string& name) {
  uint64_t number;
  FileType type;
  const bool parsed = ParseFileName(name, &number, &type);
  assert(parsed);
  return parsed && type == kWalFile ? wal_channel_ : metadata_channel_;
}
Status Backup::Lock() {
  if (lock_) return Status::OK();
  Status s = EnsureDir(disk_, options_.backup_dir);
  if (s.ok())
    s = disk_->LockFile(options_.backup_dir + "/LOCK", IOOptions(), &lock_,
                        nullptr);
  return s;
}
Status Backup::Start(bool existing) {
  Status s = Lock();
  if (!s.ok()) return s;
  std::vector<std::string> files;
  s = disk_->GetChildren(options_.backup_dir, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  // Generation names are monotonic even after interrupted publication.
  for (const auto& f : files) {
    if (f.compare(0, 6, "point-") == 0) {
      std::istringstream in(f.substr(6));
      uint64_t n;
      if (in >> n) generation_ = std::max(generation_, n);
    }
  }
  std::string pointer;
  s = Read(disk_, options_.backup_dir + "/LATEST", &pointer);
  if (s.ok()) {
    std::istringstream in(pointer);
    std::string p;
    uint32_t inventory_crc;
    while (in >> p >> inventory_crc) {
      if (!SafeName(p) || p.compare(0, 6, "point-") != 0)
        return Status::Corruption("invalid recovery point pointer");
      Point point{p + " " + std::to_string(inventory_crc), 0, {}};
      std::string inventory;
      s = Read(disk_, options_.backup_dir + "/" + p + "/INVENTORY", &inventory);
      if (!s.ok()) return s;
      if (crc32c::Value(inventory.data(), inventory.size()) != inventory_crc)
        return Status::Corruption("published inventory checksum");
      if (inventory.compare(0, 5, "MBI2\n") != 0)
        return Status::NotSupported("Metabypass inventory format");
      std::istringstream entries(inventory.substr(5));
      std::string kind, file;
      uint64_t length;
      uint32_t crc;
      while (entries >> kind >> file >> length >> crc) {
        if (kind == "B") {
          uint64_t number;
          std::istringstream id(file);
          if (!(id >> number) || !id.eof())
            return Status::Corruption("invalid blob inventory entry");
          point.blobs.insert(number);
        } else if (kind != "I" && kind != "D") {
          return Status::Corruption("invalid inventory entry");
        }
      }
      if (!entries.eof()) return Status::Corruption("invalid inventory");
      points_.push_back(std::move(point));
    }
    if (points_.empty() || points_.size() > 2)
      return Status::Corruption("invalid recovery point pointer");
  } else if (!s.IsNotFound())
    return s;
  for (const auto& f : files) {
    if (f.compare(0, 6, "point-") != 0) continue;
    bool retained = false;
    for (const auto& point : points_)
      if (point.Name() == f) retained = true;
    if (!retained) {
      s = RemoveDir(disk_, options_.backup_dir + "/" + f);
      if (!s.ok()) return s;
    }
  }
  s = disk_->FileExists(work_, IOOptions(), nullptr);
  if (s.ok())
    s = RemoveDir(disk_, work_);
  else if (s.IsNotFound())
    s = Status::OK();
  if (s.ok()) s = disk_->CreateDir(work_, IOOptions(), nullptr);
  if (!s.ok()) return s;
  for (auto& point : points_) {
    std::vector<FileAttributes> attributes;
    s = disk_->GetChildrenFileAttributes(
        options_.backup_dir + "/" + point.Name(), IOOptions(), &attributes,
        nullptr);
    if (!s.ok()) return s;
    for (const auto& file : attributes) point.bytes += file.size_bytes;
  }
  if (existing) {
    s = disk_->GetChildren(index_, IOOptions(), &files, nullptr);
    if (!s.ok()) return s;
    for (const auto& f : files) {
      if (!Tracked(index_ + "/" + f)) continue;
      uint64_t size;
      s = disk_->GetFileSize(index_ + "/" + f, IOOptions(), &size, nullptr);
      if (s.ok()) {
        uint64_t number;
        FileType type;
        if (storage_->sst() && ParseFileName(f, &number, &type) &&
            type == kTableFile) {
          s = disk_->LinkFile(index_ + "/" + f, work_ + "/" + f, IOOptions(),
                              nullptr);
          if (!s.ok()) s = Copy(disk_, index_ + "/" + f, work_ + "/" + f, size);
        } else {
          s = Copy(disk_, index_ + "/" + f, work_ + "/" + f, size);
        }
      }
      if (!s.ok()) return s;
      closed_.insert(f);
      epochs_[f] = ++next_epoch_;
    }
  }
  garbage_collector_ = std::thread(&Backup::GarbageCollectLoop, this);
  validator_ = std::thread(&Backup::ValidateLoop, this);
  thread_ = std::thread(&Backup::Run, this);
  return Status::OK();
}
Status Backup::Activate(const std::string& identity, DB* db) {
  for (const auto& dir : {options_.data_dir, options_.backup_dir}) {
    const std::string path = dir + "/METABYPASS-IDENTITY";
    std::string owner;
    Status s = Read(disk_, path, &owner);
    if (s.IsNotFound()) {
      s = Write(disk_, path, identity);
      if (s.ok()) s = SyncDir(disk_, dir);
    } else if (s.ok() && owner != identity) {
      return Status::Corruption("Metabypass directory identity mismatch");
    }
    if (!s.ok()) return s;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  db_ = db;
  identity_ = identity;
  active_ = true;
  validator_cv_.notify_one();
  WakeMirror();
  return Status::OK();
}
Status Backup::Reserve(Channel& channel, size_t charge) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (charge > options_.queue_capacity)
    return Status::InvalidArgument(
        "file operation exceeds Metabypass queue capacity");
  const uint64_t start = Now();
  const bool blocked = stats_.queued_bytes > options_.queue_capacity - charge;
  if (blocked) {
    assert(channel.waiting_charge == 0);
    channel.waiting_charge = charge;
    // A reservation can run out of space before reaching batch_bytes.
    // Drain immediately rather than waiting for the batching timer.
    WakeMirror();
    TEST_SYNC_POINT("MetaBypass::Backpressure");
    channel.capacity.wait(lock, [&] {
      return stopping_ || !stats_.error.ok() ||
             stats_.queued_bytes <= options_.queue_capacity - charge;
    });
    channel.waiting_charge = 0;
    stats_.backpressure_micros += Now() - start;
  }
  if (!stats_.error.ok()) return stats_.error;
  if (stopping_) return Status::ShutdownInProgress();
  stats_.queued_bytes += charge;
  stats_.peak_queued_bytes =
      std::max(stats_.peak_queued_bytes, stats_.queued_bytes);
  return Status::OK();
}
void Backup::Fail(const Status& status) {
  if (storage_->tier()) storage_->tier()->Cancel(status);
  std::lock_guard<std::mutex> publish(publication_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  if (stats_.error.ok()) stats_.error = status;
  WakeMirror();
  validator_cv_.notify_one();
  gc_cv_.notify_all();
  WakeCapacity();
  sync_cv_.notify_all();
}
void Backup::Finish(Channel& channel, Event&& e, const Status& primary) {
  if (!primary.ok()) Fail(primary);
  std::lock_guard<std::mutex> lock(mutex_);
  if (primary.ok() && stats_.error.ok() && !stopping_) {
    e.seq = ++accepted_;
    e.queued_micros = Now();
    channel.queue.push_back(std::move(e));
  } else {
    // Failure cleanup owns queued events, but a producer still owns its
    // reservation until Finish. Do not enqueue after either worker has failed.
    assert(stats_.queued_bytes >= e.charge);
    stats_.queued_bytes -= e.charge;
    WakeCapacity();
  }
  WakeMirror();
}
IOStatus Backup::NewWritableFile(const std::string& p, const FileOptions& o,
                                 std::unique_ptr<FSWritableFile>* r,
                                 IODebugContext* d) {
  if (!Tracked(p)) return target()->NewWritableFile(p, o, r, d);
  Channel& channel = ChannelFor(Base(p));
  std::lock_guard<std::mutex> serial(channel.operations);
  Event e{Kind::kCreate, Base(p), {}, {}};
  e.charge = sizeof(e) + e.name.size();
  Status s = Reserve(channel, e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->NewWritableFile(p, o, r, d);
  Finish(channel, std::move(e), primary);
  if (primary.ok())
    r->reset(new MirrorWriter(std::move(*r), this, Base(p), channel));
  return primary;
}
IOStatus Backup::ReopenWritableFile(const std::string& p, const FileOptions& o,
                                    std::unique_ptr<FSWritableFile>* r,
                                    IODebugContext* d) {
  if (!Tracked(p)) return target()->ReopenWritableFile(p, o, r, d);
  Channel& channel = ChannelFor(Base(p));
  std::lock_guard<std::mutex> serial(channel.operations);
  Status s = Error();
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->ReopenWritableFile(p, o, r, d);
  if (primary.ok())
    r->reset(new MirrorWriter(std::move(*r), this, Base(p), channel));
  return primary;
}
IOStatus Backup::ReuseWritableFile(const std::string&, const std::string&,
                                   const FileOptions&,
                                   std::unique_ptr<FSWritableFile>*,
                                   IODebugContext*) {
  return IOStatus::NotSupported("Metabypass WAL recycling");
}
IOStatus Backup::DeleteFile(const std::string& p, const IOOptions& o,
                            IODebugContext* d) {
  if (storage_->IsBlob(p)) {
    if (!options_.enable_blob_file_gc) return IOStatus::OK();
    Channel& channel = metadata_channel_;
    std::lock_guard<std::mutex> serial(channel.operations);
    uint64_t number;
    FileType type;
    if (!ParseFileName(Base(p), &number, &type))
      return IOStatus::Corruption("invalid retiring blob name");
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!retiring_blobs_.insert(number).second) return IOStatus::OK();
    }
    Event e{Kind::kRetireBlob, Base(p), {}, {}};
    e.size = number;
    e.charge = sizeof(e) + e.name.size();
    Status s = Reserve(channel, e.charge);
    if (!s.ok()) return AsIO(s);
    Finish(channel, std::move(e), Status::OK());
    return IOStatus::OK();
  }
  if (!Tracked(p)) return target()->DeleteFile(p, o, d);
  Channel& channel = ChannelFor(Base(p));
  std::lock_guard<std::mutex> serial(channel.operations);
  Event e{Kind::kDelete, Base(p), {}, {}};
  e.charge = sizeof(e) + e.name.size();
  Status s = Reserve(channel, e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->DeleteFile(p, o, d);
  Status mirrored = primary;
  if (!primary.ok()) {
    // Native obsolete-file cleanup tolerates a file already removed by a
    // prior purge. Preserve its filesystem result, but mirror the resulting
    // absence instead of turning that benign race into a sticky DB failure.
    IOStatus exists = target()->FileExists(p, o, d);
    if (exists.IsNotFound()) mirrored = Status::OK();
  }
  Finish(channel, std::move(e), mirrored);
  return primary;
}
IOStatus Backup::RenameFile(const std::string& a, const std::string& b,
                            const IOOptions& o, IODebugContext* d) {
  if (!Tracked(a) && !Tracked(b)) return target()->RenameFile(a, b, o, d);
  if (!Tracked(a) || !Tracked(b))
    return IOStatus::NotSupported("cross-boundary rename");
  Channel& source = ChannelFor(Base(a));
  Channel& destination = ChannelFor(Base(b));
  std::unique_lock<std::mutex> first(source.operations, std::defer_lock);
  std::unique_lock<std::mutex> second(destination.operations, std::defer_lock);
  if (&source == &destination) {
    first.lock();
  } else {
    std::lock(first, second);
  }
  // A cross-channel rename is one globally ordered control event. Holding
  // both operation locks prevents either file from being modified around it.
  Channel& channel = &source == &destination ? source : metadata_channel_;
  Event e{Kind::kRename, Base(a), Base(b), {}};
  e.charge = sizeof(e) + e.name.size() + e.other.size();
  Status s = Reserve(channel, e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->RenameFile(a, b, o, d);
  Finish(channel, std::move(e), primary);
  return primary;
}
IOStatus Backup::LinkFile(const std::string& a, const std::string& b,
                          const IOOptions& o, IODebugContext* d) {
  if (Tracked(a) || Tracked(b))
    return IOStatus::NotSupported("Metabypass external links");
  return target()->LinkFile(a, b, o, d);
}
Status Backup::Apply(const Event& e) {
  TEST_SYNC_POINT("MetaBypass::Apply");
#ifndef NDEBUG
  uint64_t sequence = e.seq;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::ApplySequence", &sequence);
#endif
  Status injected;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::ApplyStatus", &injected);
  if (!injected.ok()) return injected;
  if (e.kind == Kind::kRetireBlob) {
    uint64_t size = 0;
    Status s = storage_->GetFileSize(index_ + "/" + e.name, IOOptions(), &size,
                                     nullptr);
    // An obsolete orphan may consist only of tiered extent debris. Its bytes
    // are determined from the qualified physical inventory by the tier worker.
    if (s.IsNotFound() || s.IsPathNotFound()) s = Status::OK();
    if (!s.ok()) return s;
    std::lock_guard<std::mutex> lock(mutex_);
    pending_blobs_.emplace(e.size, RetiredBlob{e.seq, size});
    ++stats_.pending_blob_gc_files;
    stats_.pending_blob_gc_bytes += size;
    return Status::OK();
  }
  const std::string path = work_ + "/" + e.name;
  auto close = [&]() -> Status {
    auto it = writers_.find(e.name);
    if (it == writers_.end()) return Status::OK();
    Status s = it->second->Close(IOOptions(), nullptr);
    writers_.erase(it);
    return s;
  };
  if (e.kind == Kind::kCreate) {
    Status s = close();
    if (!s.ok()) return s;
    closed_.erase(e.name);
    epochs_[e.name] = ++next_epoch_;
    return disk_->NewWritableFile(path, FileOptions(), &writers_[e.name],
                                  nullptr);
  }
  if (e.kind == Kind::kAppend || e.kind == Kind::kTruncate) {
    if (e.kind == Kind::kTruncate) epochs_[e.name] = ++next_epoch_;
    auto& writer = writers_[e.name];
    if (!writer) {
      Status s =
          disk_->ReopenWritableFile(path, FileOptions(), &writer, nullptr);
      if (!s.ok()) return s;
    }
    closed_.erase(e.name);
    return e.kind == Kind::kAppend
               ? writer->Append(e.bytes, IOOptions(), nullptr)
               : writer->Truncate(e.size, IOOptions(), nullptr);
  }
  Status s = close();
  if (!s.ok()) return s;
  if (e.kind == Kind::kClose) {
    closed_.insert(e.name);
    return Status::OK();
  }
  closed_.erase(e.name);
  epochs_.erase(e.name);
  if (e.kind == Kind::kDelete) {
    s = disk_->DeleteFile(path, IOOptions(), nullptr);
    if (!s.ok()) {
      IOStatus exists = disk_->FileExists(path, IOOptions(), nullptr);
      if (exists.IsNotFound()) return Status::OK();
    }
    return s;
  }
  closed_.insert(e.other);
  epochs_[e.other] = ++next_epoch_;
  return disk_->RenameFile(path, work_ + "/" + e.other, IOOptions(), nullptr);
}
Status Backup::Capture(Candidate* candidate) {
  candidate->name = "point-" + std::to_string(++generation_);
  candidate->path = options_.backup_dir + "/" + candidate->name;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BeforeCaptureSnapshot", &closed_);
  candidate->closed = closed_;
  candidate->epochs = epochs_;
  for (const auto& writer : writers_) {
    if (!writer.second) continue;
    Status s = writer.second->Flush(IOOptions(), nullptr);
    if (!s.ok()) return s;
  }
  Status s = disk_->CreateDir(candidate->path, IOOptions(), nullptr);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::BeforeCaptureFiles");
  for (const auto& info : candidate->files) {
    const auto& file = info.relative_filename;
    if (!SafeName(file)) return Status::Corruption("invalid native file name");
    if (info.file_type == kBlobFile) {
      candidate->blobs.emplace(info.file_number, info.size);
      continue;
    }
    const std::string destination = candidate->path + "/" + file;
    if (!info.replacement_contents.empty()) {
      s = Write(disk_, destination, info.replacement_contents);
      candidate->epochs[file] = ++next_epoch_;
    } else {
      uint64_t size = 0;
      const std::string source = work_ + "/" + file;
      s = disk_->GetFileSize(source, IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size < info.size || (!info.trim_to_size && size != info.size))
        return Status::Corruption("native checkpoint size mismatch", file);
      if (!candidate->epochs.count(file))
        return Status::Corruption("missing mirrored file generation", file);
      const bool immutable = info.file_type == kTableFile;
      if (immutable) {
        candidate->state.tables.emplace(info.file_number, info.size);
        s = disk_->LinkFile(source, destination, IOOptions(), nullptr);
      }
      if (!immutable || !s.ok()) {
        s = Copy(disk_, source, destination, info.size);
        if (s.ok()) {
          std::lock_guard<std::mutex> lock(mutex_);
          stats_.candidate_copied_bytes += info.size;
        }
      }
    }
    if (!s.ok()) return s;
    candidate->work_bytes += info.size;
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateCaptured", &candidate->path);
  return Status::OK();
}
Status Backup::Publish(const Candidate& candidate) {
  TEST_SYNC_POINT("MetaBypass::ValidateCandidate");
  const std::string& point = candidate.path;
  const std::string& name = candidate.name;
  const NativeState& state = candidate.state;
  Status s;
  std::map<std::string, uint64_t> files;
  for (const auto& info : candidate.files)
    if (info.file_type != kBlobFile)
      files.emplace(info.relative_filename, info.size);
  const auto& blobs = candidate.blobs;
  s = storage_->Persist(blobs);
  if (!s.ok()) return s;
  for (const auto& blob : blobs) {
    auto& digest = blob_cache_[blob.first];
    if (digest.length != blob.second) {
      s = Digest(storage_->target(), storage_->BlobPath(blob.first),
                 blob.second, &digest.crc);
      if (!s.ok()) return s;
      digest.length = blob.second;
    }
  }
  for (auto it = digest_cache_.begin(); it != digest_cache_.end();) {
    if (!files.count(it->first))
      it = digest_cache_.erase(it);
    else
      ++it;
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::DependenciesSynced", &s);
  if (!s.ok()) return s;
  std::ostringstream inventory;
  inventory << "MBI2\n";
  uint64_t total = 0;
  for (const auto& f : files) {
    uint64_t number;
    FileType type;
    const bool table =
        ParseFileName(f.first, &number, &type) && type == kTableFile;
    if (!table) {
      std::unique_ptr<FSWritableFile> writer;
      s = disk_->ReopenWritableFile(point + "/" + f.first, FileOptions(),
                                    &writer, nullptr);
      if (!s.ok()) return s;
      s = writer->Truncate(f.second, IOOptions(), nullptr);
      if (s.ok()) s = writer->Fsync(IOOptions(), nullptr);
      s.UpdateIfOk(writer->Close(IOOptions(), nullptr));
    } else {
      s = disk_->SyncFile(point + "/" + f.first, FileOptions(), IOOptions(),
                          true, nullptr);
    }
    if (!s.ok()) return s;
    auto& digest = digest_cache_[f.first];
    const uint64_t epoch = candidate.epochs.at(f.first);
    if (digest.epoch != epoch || digest.length > f.second)
      digest = FileDigest();
    s = ExtendDigest(disk_, point + "/" + f.first, digest.length, f.second,
                     &digest.crc);
    if (!s.ok()) return s;
    digest.epoch = epoch;
    digest.length = f.second;
    inventory << "I " << f.first << ' ' << f.second << ' ' << digest.crc
              << '\n';
    total += f.second;
  }
  if (storage_->tier()) {
    s = storage_->tier()->SaveCheckpoint(point + "/BLOB-MAP", blobs);
    if (!s.ok()) return s;
    std::string map;
    s = Read(disk_, point + "/BLOB-MAP", &map);
    if (!s.ok()) return s;
    total += map.size();
    inventory << "D BLOB-MAP " << map.size() << ' '
              << crc32c::Value(map.data(), map.size()) << '\n';
  }
  for (const auto& b : blobs) {
    inventory << "B " << b.first << ' ' << b.second << ' '
              << blob_cache_.at(b.first).crc << '\n';
  }
  const std::string inventory_bytes = inventory.str();
  total += inventory_bytes.size();
  const uint32_t inventory_crc =
      crc32c::Value(inventory_bytes.data(), inventory_bytes.size());
  if (s.ok()) s = Write(disk_, point + "/INVENTORY", inventory_bytes);
  if (s.ok()) s = SyncDir(disk_, point);
  // Persist the candidate directory entry before LATEST can reference it.
  if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateSynced", &s);
  if (!s.ok()) return s;
  const std::string reference = name + " " + std::to_string(inventory_crc);
  std::string pointer = reference + "\n";
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!points_.empty()) pointer += points_.front().reference + "\n";
  }
  if (s.ok()) s = Write(disk_, options_.backup_dir + "/LATEST.tmp", pointer);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BeforePointerReplace", &s);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::BeforeGCSlotWait");
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // A retired directory occupies the only GC slot until deletion ends.
    // Closing still needs the final publication, so stopping_ cannot cancel
    // this wait. Fail() wakes it when cleanup or mirroring fails.
    gc_cv_.wait(lock, [&] { return !retired_pending_ || !stats_.error.ok(); });
    if (!stats_.error.ok()) return stats_.error;
  }
  {
    // A blocked pointer sync must not block successful queue producers.
    std::lock_guard<std::mutex> publish(publication_mutex_);
    s = Error();
    if (!s.ok()) return s;
    s = disk_->RenameFile(options_.backup_dir + "/LATEST.tmp",
                          options_.backup_dir + "/LATEST", IOOptions(),
                          nullptr);
    if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
    if (!s.ok()) return s;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      Point retired;
      if (points_.size() == 2) retired = points_.back();
      Point published{reference, total, {}};
      for (const auto& blob : blobs) published.blobs.insert(blob.first);
      points_.push_front(std::move(published));
      if (points_.size() > 2) points_.pop_back();
      uint64_t retained = candidate.work_bytes;
      for (const auto& retained_point : points_)
        retained += retained_point.bytes;
      if (!retired.reference.empty()) {
        assert(!retired_pending_);
        retired_ = std::move(retired);
        retired_pending_ = true;
        stats_.pending_gc_points = 1;
        stats_.pending_gc_bytes = retired_.bytes;
        gc_cv_.notify_one();
      }
      published_ = candidate.seq;
      published_writes_ = candidate.writes;
      published_request_ = candidate.request;
      ++stats_.recovery_points;
      stats_.last_build_micros = Now() - candidate.started;
      stats_.last_publish_micros = Now();
      stats_.last_point_lag_micros = Now() - candidate.oldest;
      stats_.retained_index_bytes = retained;
      sync_cv_.notify_all();
    }
  }
  // Only a fully verified and durably published point may authorize eviction.
  if (storage_->sst()) {
    s = storage_->sst()->Protect(point, state);
    if (!s.ok()) return s;
  }
  TEST_SYNC_POINT("MetaBypass::PointerSynced");
  return Status::OK();
}
bool Backup::MirrorReady() const {
  return stopping_ || !stats_.error.ok() || (candidate_ && !capture_done_) ||
         (!QueuesEmpty() &&
          (wal_channel_.waiting_charge != 0 ||
           metadata_channel_.waiting_charge != 0 || requested_ > published_ ||
           stats_.queued_bytes >= options_.batch_bytes));
}
void Backup::WakeMirror() {
  if (mirror_waiting_ && MirrorReady()) {
    TEST_SYNC_POINT("MetaBypass::MirrorNotified");
    mirror_cv_.notify_one();
  }
}
bool Backup::QueuesEmpty() const {
  return wal_channel_.queue.empty() && metadata_channel_.queue.empty();
}
Backup::Channel* Backup::NextChannel() {
  if (wal_channel_.queue.empty())
    return metadata_channel_.queue.empty() ? nullptr : &metadata_channel_;
  if (metadata_channel_.queue.empty()) return &wal_channel_;
  return wal_channel_.queue.front().seq < metadata_channel_.queue.front().seq
             ? &wal_channel_
             : &metadata_channel_;
}
void Backup::WakeCapacity() {
  for (auto* channel : {&wal_channel_, &metadata_channel_}) {
    if (channel->waiting_charge != 0 &&
        (stopping_ || !stats_.error.ok() ||
         stats_.queued_bytes <=
             options_.queue_capacity - channel->waiting_charge)) {
      channel->capacity.notify_one();
    }
  }
}
void Backup::Run() {
  std::unique_lock<std::mutex> lock(mutex_);
  auto fail = [&](const Status& status) {
    lock.unlock();
    Fail(status);
    lock.lock();
  };
  while (stats_.error.ok()) {
    mirror_waiting_ = true;
    TEST_SYNC_POINT("MetaBypass::BeforeMirrorWait");
    mirror_cv_.wait_for(lock, std::chrono::milliseconds(options_.interval_ms),
                        [&] { return MirrorReady(); });
    mirror_waiting_ = false;
    if (!stats_.error.ok()) break;
    const uint64_t boundary = accepted_;
    while (auto* channel = NextChannel()) {
      if (channel->queue.front().seq > boundary) break;
      Event e = std::move(channel->queue.front());
      channel->queue.pop_front();
      if (!oldest_unpublished_micros_)
        oldest_unpublished_micros_ = e.queued_micros;
      lock.unlock();
      Status s = Apply(e);
      lock.lock();
      assert(stats_.queued_bytes >= e.charge);
      stats_.queued_bytes -= e.charge;
      stats_.mirrored_bytes += e.bytes.size();
      if (channel == &wal_channel_ && e.kind == Kind::kAppend)
        stats_.mirrored_wal_bytes += e.bytes.size();
      assert(e.seq == applied_ + 1);
      applied_ = e.seq;
      if (!s.ok()) {
        fail(s);
        break;
      }
      WakeCapacity();
      if (!stats_.error.ok()) break;
    }
    if (!stats_.error.ok()) break;
    if (candidate_ && !capture_done_ && applied_ >= candidate_->seq) {
      Candidate* candidate = candidate_.get();
      lock.unlock();
      Status s = Capture(candidate);
      lock.lock();
      capture_status_ = s;
      capture_done_ = true;
      validator_cv_.notify_all();
    }
    validator_cv_.notify_one();
    if (stopping_ && QueuesEmpty()) break;
  }
  for (auto* channel : {&wal_channel_, &metadata_channel_}) {
    for (const auto& e : channel->queue) {
      assert(stats_.queued_bytes >= e.charge);
      stats_.queued_bytes -= e.charge;
    }
    channel->queue.clear();
  }
  // Outstanding primary operations retain their charges until Finish.
  TEST_SYNC_POINT("MetaBypass::QueuesDiscarded");
  mirror_done_ = true;
  validator_cv_.notify_one();
  lock.unlock();
  for (auto& w : writers_) {
    if (w.second) w.second->Close(IOOptions(), nullptr).PermitUncheckedError();
  }
  writers_.clear();
}
void Backup::ValidateLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_ && stats_.error.ok()) {
    validator_cv_.wait_for(
        lock, std::chrono::milliseconds(options_.interval_ms), [&] {
          return stopping_ || !stats_.error.ok() ||
                 (active_ && !blob_gc_busy_ &&
                  requested_point_ > published_request_);
        });
    if (stopping_ || !stats_.error.ok()) break;
    if (!active_ || blob_gc_busy_) continue;
    if (requested_point_ <= published_request_ && accepted_ <= captured_ &&
        completed_writes_ <= published_writes_ && !NeedsBlobCheckpoint())
      continue;
    candidate_busy_ = true;
    auto candidate = std::make_unique<Candidate>();
    candidate->started = Now();
    candidate->oldest = oldest_unpublished_micros_ ? oldest_unpublished_micros_
                                                   : candidate->started;
    oldest_unpublished_micros_ = 0;
    candidate->writes = completed_writes_;
    candidate->request = requested_point_;
    DB* db = db_;
    lock.unlock();
    Status s = db->DisableFileDeletions();
    const bool pinned = s.ok();
    if (s.ok()) {
      TEST_SYNC_POINT("MetaBypass::NativeFilesPinned");
      LiveFilesStorageInfoOptions options;
      options.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
      s = db->GetLiveFilesStorageInfo(options, &candidate->files);
      if (s.ok()) {
        // Native checkpoints do not preserve IDENTITY. Metabypass binds the
        // independently routed directories to this immutable database owner.
        LiveFileStorageInfo owner;
        owner.relative_filename = "IDENTITY";
        owner.file_type = kIdentityFile;
        owner.replacement_contents = identity_;
        owner.size = identity_.size();
        candidate->files.push_back(std::move(owner));
      }
      TEST_SYNC_POINT("MetaBypass::NativeFilesCaptured");
    }
    lock.lock();
    if (s.ok() && stats_.error.ok() && !stopping_) {
      candidate->seq = accepted_;
      captured_ = accepted_;
      requested_ = std::max(requested_, accepted_);
      capture_done_ = false;
      candidate_ = std::move(candidate);
      WakeMirror();
      validator_cv_.wait(lock, [&] { return capture_done_ || mirror_done_; });
      s = stats_.error;
      if (s.ok()) s = capture_status_;
      candidate = std::move(candidate_);
    } else if (s.ok()) {
      s = stats_.error.ok() ? Status::ShutdownInProgress() : stats_.error;
    }
    lock.unlock();
    // Candidate files now own their bytes and candidate_busy_ protects blobs.
    if (pinned) {
      s.UpdateIfOk(db->EnableFileDeletions());
      TEST_SYNC_POINT("MetaBypass::NativeFilesReleased");
    }
    if (s.ok()) s = Publish(*candidate);
    if (!s.ok()) Fail(s);
    lock.lock();
    candidate_busy_ = false;
    gc_cv_.notify_all();
    sync_cv_.notify_all();
  }
  candidate_busy_ = false;
  validator_done_ = true;
  gc_cv_.notify_all();
  sync_cv_.notify_all();
}
bool Backup::BlobEligible(uint64_t number, uint64_t seq) const {
  if (candidate_busy_ || points_.empty() || published_ < seq) return false;
  for (const auto& point : points_)
    if (point.blobs.count(number)) return false;
  if (retired_pending_ && retired_.blobs.count(number)) return false;
  return true;
}
bool Backup::NeedsBlobCheckpoint() const {
  if (points_.empty() || pending_blobs_.empty()) return false;
  for (const auto& blob : pending_blobs_) {
    if (published_ < blob.second.seq) return applied_ > captured_;
    if (!points_.front().blobs.count(blob.first) && points_.size() == 2 &&
        points_.back().blobs.count(blob.first))
      return true;
  }
  return false;
}
void Backup::GarbageCollectLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    gc_cv_.wait(lock, [&] {
      if (retired_pending_ || validator_done_ || !stats_.error.ok())
        return true;
      if (!stopping_)
        for (const auto& blob : pending_blobs_)
          if (BlobEligible(blob.first, blob.second.seq)) return true;
      return false;
    });
    if (!stats_.error.ok()) break;
    // Retirement cleanup must run even during candidate validation: Publish
    // waits for this slot. Never couple it to the blob capture/delete gate.
    if (retired_pending_) {
      const Point retired = retired_;
      lock.unlock();
      const uint64_t started = Now();
      TEST_SYNC_POINT("MetaBypass::GCStarted");
      Status s;
      TEST_SYNC_POINT_CALLBACK("MetaBypass::GCStatus", &s);
      if (s.ok())
        s = RemoveRetiredPoint(disk_,
                               options_.backup_dir + "/" + retired.Name());
      if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
      if (!s.ok()) {
        Fail(s);
        TEST_SYNC_POINT("MetaBypass::GCFailureRecorded");
      }
      lock.lock();
      stats_.gc_micros += Now() - started;
      if (!s.ok()) break;
      retired_ = Point();
      retired_pending_ = false;
      stats_.pending_gc_points = 0;
      stats_.pending_gc_bytes = 0;
      gc_cv_.notify_all();
      continue;
    }
    if (validator_done_) break;
    std::vector<uint64_t> batch;
    for (const auto& blob : pending_blobs_) {
      if (batch.size() == 16) break;
      if (BlobEligible(blob.first, blob.second.seq))
        batch.push_back(blob.first);
    }
    if (batch.empty()) continue;
    blob_gc_busy_ = true;
    lock.unlock();
    TEST_SYNC_POINT("MetaBypass::BlobGCClaimed");
    const uint64_t started = Now();
    Status s;
    for (uint64_t number : batch) {
      uint64_t bytes = 0;
      s = storage_->ReclaimBlob(number, &bytes);
      if (!s.ok()) break;
      // The capture gate also excludes the validator cache owner.
      blob_cache_.erase(number);
      lock.lock();
      stats_.pending_blob_gc_bytes -= pending_blobs_.at(number).bytes;
      --stats_.pending_blob_gc_files;
      pending_blobs_.erase(number);
      retiring_blobs_.erase(number);
      ++stats_.reclaimed_blob_files;
      stats_.reclaimed_blob_bytes += bytes;
      lock.unlock();
      TEST_SYNC_POINT("MetaBypass::BlobReclaimed");
    }
    if (!s.ok()) Fail(s);
    lock.lock();
    stats_.blob_gc_micros += Now() - started;
    blob_gc_busy_ = false;
    WakeMirror();
    gc_cv_.notify_all();
    if (!s.ok()) break;
  }
  gc_cv_.notify_all();
}
void Backup::WriteCompleted() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++completed_writes_;
}
Status Backup::Sync() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!stats_.error.ok()) return stats_.error;
  if (!active_ || stopping_) return Status::ShutdownInProgress();
  const uint64_t goal = ++requested_point_;
  const uint64_t writes = completed_writes_;
  const uint64_t start = Now();
  requested_ = std::max(requested_, accepted_);
  validator_cv_.notify_one();
  WakeMirror();
  TEST_SYNC_POINT("MetaBypass::SyncWaiting");
  sync_cv_.wait(lock, [&] {
    return !stats_.error.ok() || stopping_ ||
           (published_request_ >= goal && published_writes_ >= writes);
  });
  stats_.checkpoint_wait_micros += Now() - start;
  if (!stats_.error.ok()) return stats_.error;
  return stopping_ ? Status::ShutdownInProgress() : Status::OK();
}
void Backup::DetachDB() {
  std::unique_lock<std::mutex> lock(mutex_);
  active_ = false;
  sync_cv_.wait(lock, [&] { return !candidate_busy_; });
  db_ = nullptr;
}
Status Backup::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    validator_cv_.notify_all();
    gc_cv_.notify_all();
    WakeMirror();
    WakeCapacity();
    sync_cv_.notify_all();
  }
  if (thread_.joinable()) thread_.join();
  if (validator_.joinable()) validator_.join();
  TEST_SYNC_POINT("MetaBypass::BeforeGCJoin");
  if (garbage_collector_.joinable()) garbage_collector_.join();
  return Error();
}
Status Backup::Error() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_.error;
}
MetaBypassStats Backup::Stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}
Status Backup::CheckRestoreFormat(FileSystem* fs, const std::string& backup) {
  std::string point, inventory;
  return ReadPublishedInventory(fs, backup, &point, &inventory);
}
Status Backup::RestoreFiles(FileSystem* fs, const std::string& backup,
                            const std::string& destination,
                            const SeparatedStorage& storage) {
  std::string point, inventory;
  Status s = ReadPublishedInventory(fs, backup, &point, &inventory);
  if (!s.ok()) return s;
  if (storage.tier()) {
    std::istringstream maps(inventory.substr(5));
    std::string kind, file;
    uint64_t size;
    uint32_t crc;
    bool found = false;
    while (maps >> kind >> file >> size >> crc) {
      if (kind != "D") continue;
      if (found || file != "BLOB-MAP")
        return Status::Corruption("invalid blob map entry");
      std::string bytes;
      s = Read(fs, point + "/BLOB-MAP", &bytes);
      if (!s.ok()) return s;
      if (bytes.size() != size ||
          crc32c::Value(bytes.data(), bytes.size()) != crc)
        return Status::Corruption("blob map checksum");
      s = storage.tier()->LoadCheckpoint(point + "/BLOB-MAP");
      if (!s.ok()) return s;
      found = true;
    }
    if (!found) return Status::Corruption("missing tiered blob map");
  }
  std::istringstream in(inventory.substr(5));
  std::string kind, file;
  uint64_t length;
  uint32_t expected;
  struct Entry {
    std::string file;
    uint64_t length;
  };
  std::vector<Entry> entries;
  while (in >> kind >> file >> length >> expected) {
    if (!SafeName(file) ||
        (kind != "I" && kind != "B" &&
         !(storage.tier() && kind == "D" && file == "BLOB-MAP")))
      return Status::Corruption("invalid inventory");
    std::string path;
    if (kind == "B") {
      uint64_t number;
      std::istringstream n(file);
      if (!(n >> number) || !n.eof())
        return Status::Corruption("invalid blob number");
      path = storage.BlobPath(number);
    } else {
      path = point + "/" + file;
      uint64_t size;
      s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size != length)
        return Status::Corruption("published file size", path);
      entries.push_back({file, length});
    }
    uint32_t crc;
    s = Digest(fs, path, length, &crc);
    if (!s.ok()) return s;
    if (crc != expected) return Status::Corruption("published checksum", path);
  }
  if (!in.eof() || entries.empty())
    return Status::Corruption("invalid inventory");
  std::vector<SstStorage::RestoreFile> ssts;
  for (const auto& e : entries) {
    uint64_t number;
    FileType type;
    if (storage.sst() && ParseFileName(e.file, &number, &type) &&
        type == kTableFile) {
      ssts.push_back({point + "/" + e.file, number, e.length});
    } else {
      s = Copy(fs, point + "/" + e.file, destination + "/" + e.file, e.length);
      if (!s.ok()) return s;
    }
  }
  if (storage.sst()) {
    s = storage.sst()->RestoreTables(ssts);
    if (!s.ok()) return s;
  }
  s = SyncDir(fs, destination);
  return s;
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
