//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/backup.h"

#include <array>
#include <cassert>
#include <chrono>
#include <cstring>
#include <limits>
#include <sstream>

#include "file/filename.h"
#include "test_util/sync_point.h"
#include "utilities/metabypass/wal_breakdown.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
namespace wal_probe {
thread_local Trace* active = nullptr;
thread_local bool mirror_append = false;
}  // namespace wal_probe
#endif
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
  std::string extra;
  if (!(p >> name) || (p >> extra) || !SafeName(name) ||
      name.compare(0, 6, "point-") != 0)
    return Status::Corruption("invalid published pointer");
  *point = backup + "/" + name;
  std::string identity;
  s = Read(fs, *point + "/IDENTITY", &identity);
  if (!s.ok()) return s;
  if (identity != owner) return Status::Corruption("backup identity mismatch");
  s = Read(fs, *point + "/INVENTORY", inventory);
  if (!s.ok()) return s;
  if (inventory->compare(0, 5, "MBI6\n") != 0)
    return Status::NotSupported("Metabypass inventory format");
  return Status::OK();
}
Status ExtendWal(FileSystem* fs, const std::string& source,
                 const std::string& target, uint64_t size) {
  uint64_t prefix = 0;
  Status s = fs->FileExists(target, IOOptions(), nullptr);
  if (s.ok()) {
    s = fs->GetFileSize(target, IOOptions(), &prefix, nullptr);
    if (!s.ok()) return s;
  } else if (!s.IsNotFound())
    return s;
  if (prefix >= size) return Status::OK();
  // Reopen appends: a restart or failed append cannot destroy the previously
  // surviving prefix. The same rule preserves the checkpoint's trusted bytes.
  std::unique_ptr<FSRandomAccessFile> reader;
  std::unique_ptr<FSWritableFile> writer;
  s = fs->NewRandomAccessFile(source, FileOptions(), &reader, nullptr);
  if (s.ok())
    s = fs->ReopenWritableFile(target, FileOptions(), &writer, nullptr);
  std::string buffer(64 * 1024, '\0');
  while (s.ok() && prefix < size) {
    Slice bytes;
    size_t count = std::min<uint64_t>(buffer.size(), size - prefix);
    s = reader->Read(prefix, count, IOOptions(), &bytes, buffer.data(),
                     nullptr);
    if (s.ok() && bytes.size() != count)
      s = Status::Corruption("short incremental WAL");
    if (s.ok()) s = writer->Append(bytes, IOOptions(), nullptr);
    prefix += count;
  }
  if (writer) {
    if (s.ok()) s = writer->Fsync(IOOptions(), nullptr);
    s.UpdateIfOk(writer->Close(IOOptions(), nullptr));
  }
  return s;
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
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
    wal_probe::MirrorAppend append(wal_probe::active &&
                                   &channel_ == &backup_->wal_channel_);
#endif
    MB_WAL_SPAN(wait, operations_wait, wal_probe::mirror_append);
    std::lock_guard<std::mutex> serial(channel_.operations);
    MB_WAL_END(wait);
    MB_WAL_SPAN(build, event_build, wal_probe::mirror_append);
    Backup::Event e{Backup::Kind::kAppend, name_, {}};
    e.charge = sizeof(Backup::ChargedEventLayout) + name_.size() + bytes.size();
    MB_WAL_END(build);
    Status s = backup_->Reserve(channel_, e.charge);
    if (!s.ok()) return AsIO(s);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::PrimaryReserved", &e.name);
    MB_WAL_SPAN(copy, payload_copy, wal_probe::mirror_append);
    if (&channel_ == &backup_->wal_channel_ && bytes.size() >= 32 &&
        bytes.size() <= 1024) {
      auto& block = backup_->wal_block_;
      auto& used = backup_->wal_block_used_;
      if (!block || sizeof(block->data) - used < bytes.size()) {
        block = std::make_shared<Backup::WalBlock>();
        used = 0;
      }
      char* dest = block->data + used;
      std::memcpy(dest, bytes.data(), bytes.size());
      used += bytes.size();
      e.shared_bytes = std::shared_ptr<const char>(block, dest);
      e.size = bytes.size();
    } else {
      e.contents.assign(bytes.data(), bytes.size());
    }
    MB_WAL_END(copy);
    IOStatus primary = verification
                           ? target()->Append(bytes, o, *verification, d)
                           : target()->Append(bytes, o, d);
    backup_->Finish(channel_, std::move(e), primary);
    return primary;
  }
  IOStatus Control(Backup::Kind kind, uint64_t n, const IOOptions& o,
                   IODebugContext* d) {
    std::lock_guard<std::mutex> serial(channel_.operations);
    Backup::Event e{kind, name_, {}};
    e.size = n;
    e.charge = sizeof(Backup::ChargedEventLayout) + name_.size();
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
bool Backup::Tracked(const std::string& path, bool include_tables) const {
  if (path.compare(0, index_.size() + 1, index_ + "/") != 0) return false;
  uint64_t number;
  FileType type;
  if (!ParseFileName(Base(path), &number, &type)) return false;
  return type == kWalFile || (include_tables && type == kTableFile) ||
         type == kDescriptorFile || type == kCurrentFile ||
         type == kOptionsFile || type == kIdentityFile || type == kTempFile;
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
    while (in >> p) {
      if (!SafeName(p) || p.compare(0, 6, "point-") != 0)
        return Status::Corruption("invalid recovery point pointer");
      Point point{p, 0, {}};
      std::string inventory;
      s = Read(disk_, options_.backup_dir + "/" + p + "/INVENTORY", &inventory);
      if (!s.ok()) return s;
      if (inventory.compare(0, 5, "MBI6\n") != 0)
        return Status::NotSupported("Metabypass inventory format");
      std::istringstream entries(inventory.substr(5));
      std::string kind, file;
      uint64_t length;
      while (entries >> kind) {
        if (!(entries >> file >> length))
          return Status::Corruption("incomplete inventory entry");
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
      std::string recovery;
      s = Read(disk_, options_.backup_dir + "/" + p + "/RECOVERY", &recovery);
      if (s.ok()) {
        uint64_t sequence;
        std::istringstream metadata(recovery);
        if (!(metadata >> sequence >> point.blob_floor >> point.wal_floor))
          return Status::Corruption("checkpoint recovery boundary");
      } else if (!s.IsNotFound())
        return s;
      points_.push_back(std::move(point));
    }
    if (points_.empty() || points_.size() > 1)
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
  s = EnsureDir(disk_, work_);
  if (!s.ok()) return s;
  std::vector<std::string> previous;
  s = disk_->GetChildren(work_, IOOptions(), &previous, nullptr);
  if (!s.ok()) return s;
  for (const auto& file : previous) {
    if (file == "." || file == "..") continue;
    uint64_t number;
    FileType type;
    if (ParseFileName(file, &number, &type) && type == kWalFile) {
      continue;
    }
    s = disk_->DeleteFile(work_ + "/" + file, IOOptions(), nullptr);
    if (!s.ok()) return s;
  }
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
        if (ParseFileName(f, &number, &type) && type == kWalFile) {
          TEST_SYNC_POINT("MetaBypass::BeforeWalReseed");
          s = ExtendWal(disk_, index_ + "/" + f, work_ + "/" + f, size);
          TEST_SYNC_POINT("MetaBypass::AfterWalReseed");
        } else {
          s = Copy(disk_, index_ + "/" + f, work_ + "/" + f, size);
        }
      }
      if (!s.ok()) return s;
    }
  }
  garbage_collector_ = std::thread(&Backup::GarbageCollectLoop, this);
  validator_ = std::thread(&Backup::ValidateLoop, this);
  thread_ = std::thread(&Backup::Run, this);
  if (options_.enable_recovery_time_sampling)
    sampler_ = std::thread(&Backup::SampleLoop, this);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::SamplerStarted", &sampler_);
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
  MB_WAL_SPAN(wait, reserve_wait, wal_probe::mirror_append);
  std::unique_lock<std::mutex> lock(mutex_);
  MB_WAL_END(wait);
  MB_WAL_SPAN(bookkeeping, reserve_bookkeeping, wal_probe::mirror_append);
  if (charge > options_.queue_capacity)
    return Status::InvalidArgument(
        "file operation exceeds Metabypass queue capacity");
  const bool blocked = stats_.queued_bytes > options_.queue_capacity - charge;
  if (blocked) {
    const uint64_t start = Now();
    assert(channel.waiting_charge == 0);
    channel.waiting_charge = charge;
    // A reservation can run out of space before reaching batch_bytes.
    // Drain immediately rather than waiting for the batching timer.
    WakeMirror();
    TEST_SYNC_POINT("MetaBypass::Backpressure");
    MB_WAL_SPAN(capacity, capacity_wait, wal_probe::mirror_append);
    channel.capacity.wait(lock, [&] {
      return stopping_ || !stats_.error.ok() ||
             stats_.queued_bytes <= options_.queue_capacity - charge;
    });
    MB_WAL_END(capacity);
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
  if (stats_.error.ok()) {
    stats_.error = status;
    failed_.store(true, std::memory_order_release);
  }
  WakeMirror();
  validator_cv_.notify_one();
  gc_cv_.notify_all();
  WakeCapacity();
  sync_cv_.notify_all();
}
void Backup::Finish(Channel& channel, Event&& e, const Status& primary) {
  if (!primary.ok()) Fail(primary);
  MB_WAL_SPAN(wait, finish_wait, wal_probe::mirror_append);
  std::lock_guard<std::mutex> lock(mutex_);
  MB_WAL_END(wait);
  MB_WAL_SPAN(bookkeeping, finish_bookkeeping, wal_probe::mirror_append);
  if (primary.ok() && stats_.error.ok() && !stopping_) {
    e.seq = ++accepted_;
    if (e.kind == Kind::kRetireBlob) {
      ++native_changes_;
      MarkDirty();
    }
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
  Event e{Kind::kCreate, Base(p), {}};
  e.charge = sizeof(Backup::ChargedEventLayout) + e.name.size();
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
    Event e{Kind::kRetireBlob, Base(p), {}};
    e.size = number;
    e.charge = sizeof(Backup::ChargedEventLayout) + e.name.size();
    Status s = Reserve(channel, e.charge);
    if (!s.ok()) return AsIO(s);
    Finish(channel, std::move(e), Status::OK());
    return IOStatus::OK();
  }
  if (!Tracked(p)) return target()->DeleteFile(p, o, d);
  Channel& channel = ChannelFor(Base(p));
  std::lock_guard<std::mutex> serial(channel.operations);
  Event e{Kind::kDelete, Base(p), {}};
  e.charge = sizeof(Backup::ChargedEventLayout) + e.name.size();
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
  if (Tracked(a, true) != Tracked(b, true))
    return IOStatus::NotSupported("cross-boundary rename");
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
  Event e{Kind::kRename, Base(a), Base(b)};
  e.charge =
      sizeof(Backup::ChargedEventLayout) + e.name.size() + e.contents.size();
  Status s = Reserve(channel, e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->RenameFile(a, b, o, d);
  Finish(channel, std::move(e), primary);
  return primary;
}
IOStatus Backup::LinkFile(const std::string& a, const std::string& b,
                          const IOOptions& o, IODebugContext* d) {
  if (Tracked(a, true) || Tracked(b, true))
    return IOStatus::NotSupported("Metabypass external links");
  return target()->LinkFile(a, b, o, d);
}
Status Backup::BeforeApply([[maybe_unused]] const Event& e) {
  TEST_SYNC_POINT("MetaBypass::Apply");
#ifndef NDEBUG
  uint64_t sequence = e.seq;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::ApplySequence", &sequence);
#endif
  Status injected;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::ApplyStatus", &injected);
  return injected;
}
Status Backup::Append(const std::string& name, const Slice& bytes) {
  auto& writer = writers_[name];
  if (!writer) {
    Status s = disk_->ReopenWritableFile(work_ + "/" + name, FileOptions(),
                                         &writer, nullptr);
    if (!s.ok()) return s;
  }
  return writer->Append(bytes, IOOptions(), nullptr);
}
Status Backup::Apply(const Event& e) {
  Status injected = BeforeApply(e);
  if (!injected.ok()) return injected;
  if (e.kind == Kind::kAppend) return Append(e.name, e.Payload());
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
    return disk_->NewWritableFile(path, FileOptions(), &writers_[e.name],
                                  nullptr);
  }
  if (e.kind == Kind::kTruncate) {
    auto& writer = writers_[e.name];
    if (!writer) {
      Status s =
          disk_->ReopenWritableFile(path, FileOptions(), &writer, nullptr);
      if (!s.ok()) return s;
    }
    return writer->Truncate(e.size, IOOptions(), nullptr);
  }
  Status s = close();
  if (!s.ok()) return s;
  if (e.kind == Kind::kClose) {
    return Status::OK();
  }
  if (e.kind == Kind::kDelete) {
    uint64_t number;
    FileType type;
    if (ParseFileName(e.name, &number, &type) && type == kWalFile) {
      std::lock_guard<std::mutex> lock(mutex_);
      // Retain incremental logs until every published recovery root moves on.
      if (points_.empty()) return Status::OK();
      for (const auto& point : points_)
        if (!point.wal_floor || number >= point.wal_floor) return Status::OK();
      if (retired_pending_ &&
          (!retired_.wal_floor || number >= retired_.wal_floor))
        return Status::OK();
    }
    s = disk_->DeleteFile(path, IOOptions(), nullptr);
    if (!s.ok()) {
      IOStatus exists = disk_->FileExists(path, IOOptions(), nullptr);
      if (exists.IsNotFound()) return Status::OK();
    }
    return s;
  }
  return disk_->RenameFile(path, work_ + "/" + e.contents, IOOptions(),
                           nullptr);
}
Status Backup::Capture(Candidate* candidate) {
  candidate->name = "point-" + std::to_string(++generation_);
  candidate->path = options_.backup_dir + "/" + candidate->name;
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
    if (info.file_type == kTableFile) continue;
    const std::string destination = candidate->path + "/" + file;
    if (!info.replacement_contents.empty()) {
      // These bytes are private to this candidate. Publish synchronizes and
      // validates them before making the recovery point visible.
      std::unique_ptr<FSWritableFile> writer;
      s = disk_->NewWritableFile(destination, FileOptions(), &writer, nullptr);
      if (s.ok()) {
        s = writer->Append(info.replacement_contents, IOOptions(), nullptr);
        s.UpdateIfOk(writer->Close(IOOptions(), nullptr));
      }
    } else {
      uint64_t size = 0;
      const std::string source = work_ + "/" + file;
      s = disk_->GetFileSize(source, IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size < info.size || (!info.trim_to_size && size != info.size))
        return Status::Corruption("native checkpoint size mismatch", file);
      s = StageCopy(disk_, source, destination, info.size);
      if (s.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_.candidate_copied_bytes += info.size;
      }
    }
    if (!s.ok()) return s;
    candidate->work_bytes += info.size;
  }
  s = PruneWal(candidate->wal_floor);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::MutableFilesCaptured");
  return Status::OK();
}
Status Backup::CaptureTables(Candidate* candidate) {
  std::string published_point;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // The sole validator cannot retire this point before Publish completes.
    if (!points_.empty())
      published_point = options_.backup_dir + "/" + points_.back().Name();
  }
  TEST_SYNC_POINT("MetaBypass::BeforeCaptureTables");
  for (const auto& info : candidate->files) {
    if (info.file_type != kTableFile) continue;
    const auto& file = info.relative_filename;
    const std::string destination = candidate->path + "/" + file;
    candidate->state.tables.emplace(info.file_number, info.size);
    Status s;
    if (!published_point.empty()) {
      const std::string previous = published_point + "/" + file;
      uint64_t previous_size = 0;
      s = disk_->GetFileSize(previous, IOOptions(), &previous_size, nullptr);
      if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound()) return s;
      if (s.ok() && previous_size == info.size) {
        s = disk_->LinkFile(previous, destination, IOOptions(), nullptr);
        if (s.ok()) candidate->durable_tables.insert(info.file_number);
      }
    }
    if (!candidate->durable_tables.count(info.file_number)) {
      // Resolve through the logical filesystem, including cold SST routing.
      // Native deletions remain pinned until every table has been captured.
      const std::string source = info.directory + "/" + file;
      uint64_t size = 0;
      s = storage_->GetFileSize(source, IOOptions(), &size, nullptr);
      if (!s.ok()) return s;
      if (size != info.size)
        return Status::Corruption("native checkpoint size mismatch", file);
      s = storage_->LinkFile(source, destination, IOOptions(), nullptr);
      if (!s.ok()) {
        s = StageCopy(storage_.get(), source, destination, info.size);
        if (s.ok()) {
          std::lock_guard<std::mutex> lock(mutex_);
          stats_.candidate_copied_bytes += info.size;
        }
      }
      if (!s.ok()) return s;
    }
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateCaptured", &candidate->path);
  return Status::OK();
}
Status Backup::PruneWal(uint64_t floor) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (points_.empty()) return Status::OK();
    for (const auto& point : points_) floor = std::min(floor, point.wal_floor);
    if (retired_pending_) floor = std::min(floor, retired_.wal_floor);
    if (candidate_ && !capture_done_)
      floor = std::min(floor, candidate_->wal_floor);
  }
  if (!floor) return Status::OK();
  std::vector<std::string> names;
  Status s = disk_->GetChildren(work_, IOOptions(), &names, nullptr);
  if (!s.ok()) return s;
  for (const auto& name : names) {
    uint64_t number;
    FileType type;
    if (!ParseFileName(name, &number, &type) || type != kWalFile ||
        number >= floor || writers_.count(name))
      continue;
    s = disk_->DeleteFile(work_ + "/" + name, IOOptions(), nullptr);
    if (!s.ok()) return s;
  }
  TEST_SYNC_POINT("MetaBypass::WalPruned");
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
  const uint64_t persist_started = Now();
  s = storage_->Persist(blobs);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.checkpoint_persist_micros += Now() - persist_started;
  }
  if (!s.ok()) return s;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::DependenciesSynced", &s);
  if (!s.ok()) return s;
  std::ostringstream inventory;
  inventory << "MBI6\n";
  uint64_t total = 0;
  for (const auto& f : files) {
    uint64_t number;
    FileType type;
    const bool table =
        ParseFileName(f.first, &number, &type) && type == kTableFile;
    TEST_SYNC_POINT("MetaBypass::BeforeCandidateFileSync");
    if (!table) {
      std::unique_ptr<FSWritableFile> writer;
      s = disk_->ReopenWritableFile(point + "/" + f.first, FileOptions(),
                                    &writer, nullptr);
      if (!s.ok()) return s;
      s = writer->Truncate(f.second, IOOptions(), nullptr);
      if (s.ok()) s = writer->Fsync(IOOptions(), nullptr);
      if (s.ok()) {
        // Non-table candidates own independent copies. Their durable bytes are
        // cold until recovery; never invalidate shared SSTs or active mirrors.
        Status cold = writer->InvalidateCache(0, f.second);
        if (!cold.IsNotSupported()) s = cold;
      }
      s.UpdateIfOk(writer->Close(IOOptions(), nullptr));
    } else if (!candidate.durable_tables.count(number)) {
      s = disk_->SyncFile(point + "/" + f.first, FileOptions(), IOOptions(),
                          true, nullptr);
    }
    TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateFileSyncStatus", &s);
    if (!s.ok()) return s;
    inventory << "I " << f.first << ' ' << f.second << '\n';
    total += f.second;
  }
  if (storage_->tier()) {
    s = storage_->tier()->SaveCheckpoint(point + "/BLOB-MAP", blobs);
    if (!s.ok()) return s;
    std::string map;
    s = Read(disk_, point + "/BLOB-MAP", &map);
    if (!s.ok()) return s;
    total += map.size();
    inventory << "D BLOB-MAP " << map.size() << '\n';
  }
  for (const auto& b : blobs) {
    inventory << "B " << b.first << ' ' << b.second << '\n';
  }
  const std::string inventory_bytes = inventory.str();
  total += inventory_bytes.size();
  if (s.ok())
    s = Write(disk_, point + "/RECOVERY",
              std::to_string(candidate.native_sequence) + " " +
                  std::to_string(candidate.blob_floor) + " " +
                  std::to_string(candidate.wal_floor) + "\n");
  if (s.ok()) s = Write(disk_, point + "/INVENTORY", inventory_bytes);
  if (s.ok()) s = SyncDir(disk_, point);
  // Persist the candidate directory entry before LATEST can reference it.
  if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateSynced", &s);
  if (!s.ok()) return s;
  const std::string reference = name;
  const std::string pointer = reference + "\n";
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
    TEST_SYNC_POINT_CALLBACK("MetaBypass::PointerDirectorySyncStatus", &s);
    if (!s.ok()) return s;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      Point retired;
      if (points_.size() == 1) retired = points_.back();
      Point published{
          reference, total, {}, candidate.blob_floor, candidate.wal_floor};
      for (const auto& blob : blobs) published.blobs.insert(blob.first);
      points_.push_front(std::move(published));
      if (points_.size() > 1) points_.pop_back();
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
      published_native_changes_ = candidate.native_changes;
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
  return stopping_ || prune_wal_ || !stats_.error.ok() ||
         (candidate_ && !capture_done_) ||
         (!QueuesEmpty() &&
          (wal_channel_.waiting_charge != 0 ||
           metadata_channel_.waiting_charge != 0 || requested_ > applied_ ||
           stats_.queued_bytes >= options_.batch_bytes));
}
void Backup::WakeMirror() {
  TEST_SYNC_POINT_CALLBACK("MetaBypass::MirrorReadyProbe", this);
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
void Backup::TakeBatch(Channel* channel, uint64_t boundary,
                       std::vector<Event>* batch) {
  batch->clear();
  batch->push_back(std::move(channel->queue.front()));
  channel->queue.pop_front();
  const Event& first = batch->front();
  constexpr size_t kMaxBytes = 64 * 1024;
  constexpr size_t kMaxEvents = 64;
  if (channel != &wal_channel_ || first.kind != Kind::kAppend ||
      first.Payload().empty() || first.Payload().size() > kMaxBytes)
    return;
  // Control events delimit file lifetimes. Global adjacency also excludes
  // interleaved metadata operations. Capture still runs after the full drain.
  if (candidate_ && !capture_done_ && first.seq <= candidate_->seq)
    boundary = std::min(boundary, candidate_->seq);
  size_t bytes = first.Payload().size();
  while (batch->size() < kMaxEvents && NextChannel() == channel) {
    const Event& next = channel->queue.front();
    if (next.seq > boundary || next.seq != batch->back().seq + 1 ||
        next.kind != Kind::kAppend || next.name != batch->front().name ||
        next.Payload().empty() || next.Payload().size() > kMaxBytes - bytes)
      break;
    bytes += next.Payload().size();
    batch->push_back(std::move(channel->queue.front()));
    channel->queue.pop_front();
  }
}
Status Backup::ApplyBatch(const std::vector<Event>& batch, std::string* scratch,
                          size_t* completed) {
  *completed = 0;
  if (batch.size() == 1) {
    Status s = Apply(batch.front());
    if (s.ok()) *completed = 1;
    return s;
  }
  scratch->clear();
  Status injected;
  size_t ready = 0;
  for (const auto& event : batch) {
    // These are ordered logical preflight hooks. WalAppendBatch marks the
    // physical batch boundary; later logical hooks do not imply prior I/O.
    injected = BeforeApply(event);
    if (!injected.ok()) break;
    const Slice payload = event.Payload();
    scratch->append(payload.data(), payload.size());
    ++ready;
  }
  if (ready != 0) {
#ifndef NDEBUG
    std::array<uint64_t, 3> info{{batch.front().seq, ready, scratch->size()}};
    TEST_SYNC_POINT_CALLBACK("MetaBypass::WalAppendBatch", &info);
#endif
    Status s = Append(batch.front().name, *scratch);
    if (!s.ok()) return s;  // Partial writes are never retried or acknowledged.
    *completed = ready;
  }
  return injected;
}
void Backup::Run() {
  HddPurposeScope background(HddPurpose::kBulk);
  std::vector<Event> batch;
  batch.reserve(64);
  std::string scratch;
  scratch.reserve(64 * 1024);
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
    if (prune_wal_) {
      prune_wal_ = false;
      lock.unlock();
      Status s = PruneWal(std::numeric_limits<uint64_t>::max());
      lock.lock();
      if (!s.ok()) {
        fail(s);
        break;
      }
    }
    const uint64_t boundary = accepted_;
    const uint64_t before_applied = applied_;
    while (auto* channel = NextChannel()) {
      if (channel->queue.front().seq > boundary) break;
      TakeBatch(channel, boundary, &batch);
      const bool barrier = requested_point_ > published_request_;
      lock.unlock();
      HddPurposeScope purpose(barrier ? HddPurpose::kBarrier
                                      : HddPurpose::kBulk);
      size_t completed = 0;
      Status s = ApplyBatch(batch, &scratch, &completed);
      lock.lock();
      for (size_t i = 0; i < batch.size(); ++i) {
        const Event& e = batch[i];
        assert(stats_.queued_bytes >= e.charge);
        stats_.queued_bytes -= e.charge;
        if (i < completed) {
          if (e.kind == Kind::kAppend) {
            const size_t bytes = e.Payload().size();
            stats_.mirrored_bytes += bytes;
            if (channel == &wal_channel_) stats_.mirrored_wal_bytes += bytes;
          }
          assert(e.seq == applied_ + 1);
          applied_ = e.seq;
        }
      }
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
    if (applied_ != before_applied && NeedsBlobCheckpoint())
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
void Backup::SampleProgress() {
  auto clock = db_options_.env->GetSystemClock();
  uint64_t begin, steady_begin, sequence, end, steady_end;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!active_ || !db_) return;
    begin = clock->NowMicros();
    steady_begin = clock->NowNanos() / 1000;
    sequence = db_->GetLatestSequenceNumber();
    end = clock->NowMicros();
    steady_end = clock->NowNanos() / 1000;
  }
  TEST_SYNC_POINT("MetaBypass::ProgressSampled");
  std::string row = std::to_string(begin) + " " + std::to_string(steady_begin) +
                    " " + std::to_string(sequence) + " " + std::to_string(end) +
                    " " + std::to_string(steady_end) + "\n";
  std::unique_ptr<FSWritableFile> file;
  Status s = disk_->ReopenWritableFile(options_.backup_dir + "/PROGRESS",
                                       FileOptions(), &file, nullptr);
  if (s.ok()) {
    s = file->Append(row, IOOptions(), nullptr);
    if (s.ok()) {
      s = file->Fsync(IOOptions(), nullptr);
    }
    s.UpdateIfOk(file->Close(IOOptions(), nullptr));
  }
  // Samples carry no recovery correctness dependency. A gap widens the time
  // bracket; an interrupted last row is ignored by the estimator.
  s.PermitUncheckedError();
}
void Backup::SampleLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_) {
    sync_cv_.wait_for(lock, std::chrono::seconds(1), [&] { return stopping_; });
    if (stopping_) break;
    lock.unlock();
    SampleProgress();
    lock.lock();
  }
}
void Backup::EstimateRecoveryTime(const std::string& samples,
                                  MetaBypassRecoveryReport* report) {
  report->has_time_lower_bound = report->has_time_upper_bound = false;
  std::istringstream input(samples);
  std::string row;
  uint64_t prior_sequence = 0, prior_wall = 0, prior_steady = 0;
  bool first = true;
  while (std::getline(input, row)) {
    // A torn final append cannot supply a bound.
    if (input.eof()) break;
    uint64_t begin, steady_begin, sequence, end, steady_end;
    std::istringstream values(row);
    if (!(values >> begin >> steady_begin >> sequence >> end >> steady_end))
      continue;
    bool invalid = end < begin || steady_end < steady_begin;
    if (!invalid) {
      const uint64_t wall_duration = end - begin;
      const uint64_t steady_duration = steady_end - steady_begin;
      invalid |= wall_duration > steady_duration + 1000000 ||
                 steady_duration > wall_duration + 1000000;
    }
    if (!first) {
      invalid |= sequence < prior_sequence || begin < prior_wall ||
                 steady_begin < prior_steady;
      if (!invalid) {
        uint64_t wall_delta = begin - prior_wall;
        uint64_t steady_delta = steady_begin - prior_steady;
        // A wall clock step larger than one sampling period cannot support
        // an honest seconds-scale estimate. Queueing merely widens bounds.
        invalid |= wall_delta > steady_delta + 1000000 ||
                   steady_delta > wall_delta + 1000000;
      }
    }
    if (invalid) {
      report->has_time_lower_bound = report->has_time_upper_bound = false;
      return;
    }
    first = false;
    prior_sequence = sequence;
    prior_wall = end;
    prior_steady = steady_end;
    if (sequence <= report->recovered_sequence) {
      report->has_time_lower_bound = true;
      report->time_lower_bound_micros = begin;
    } else if (!report->has_time_upper_bound) {
      report->has_time_upper_bound = true;
      report->time_upper_bound_micros = end;
    }
  }
}
void Backup::ValidateLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_ && stats_.error.ok()) {
    if (!active_ || blob_gc_busy_) {
      validator_cv_.wait(lock);
      continue;
    }
    if (completed_writes_ > published_writes_ ||
        native_changes_ > published_native_changes_ || NeedsBlobCheckpoint())
      MarkDirty();
    const bool requested = requested_point_ > published_request_;
    if (!requested) {
      if (!dirty_since_micros_) {
        TEST_SYNC_POINT("MetaBypass::CheckpointIdle");
        validator_cv_.wait(lock);
        continue;
      }
      uint64_t deadline =
          dirty_since_micros_ + options_.checkpoint_interval_ms * 1000;
      uint64_t now = Now();
      TEST_SYNC_POINT_CALLBACK("MetaBypass::CheckpointNow", &now);
      if (now < deadline) {
        TEST_SYNC_POINT_CALLBACK("MetaBypass::CheckpointDeadline", &deadline);
        uint64_t wait_micros = deadline - now;
        TEST_SYNC_POINT_CALLBACK("MetaBypass::CheckpointWaitMicros",
                                 &wait_micros);
        validator_cv_.wait_for(lock, std::chrono::microseconds(wait_micros));
        continue;
      }
    }
    candidate_busy_ = true;
    auto candidate = std::make_unique<Candidate>();
    candidate->started = Now();
    candidate->oldest =
        dirty_since_micros_ ? dirty_since_micros_ : candidate->started;
    dirty_since_micros_ = 0;
    candidate->writes = completed_writes_;
    candidate->request = requested_point_;
    candidate->native_changes = native_changes_;
    DB* db = db_;
    lock.unlock();
    HddPurposeScope purpose(requested ? HddPurpose::kBarrier
                                      : HddPurpose::kBulk);
    TEST_SYNC_POINT("MetaBypass::CheckpointBoundary");
    // Never wait for application writes while holding the event queue lock.
    std::unique_lock<std::shared_mutex> fence(write_fence_);
    Status s = db->DisableFileDeletions();
    const bool pinned = s.ok();
    if (s.ok()) {
      TEST_SYNC_POINT("MetaBypass::NativeFilesPinned");
      LiveFilesStorageInfoOptions options;
      const bool direct = db_options_.enable_blob_direct_write;
      options.wal_size_for_flush =
          direct ? 0 : std::numeric_limits<uint64_t>::max();
      options.include_wal_files = !direct;
      const uint64_t capture_started = Now();
      s = db->GetLiveFilesStorageInfo(options, &candidate->files);
      TEST_SYNC_POINT_CALLBACK("MetaBypass::NativeCaptureStatus", &s);
      {
        std::lock_guard<std::mutex> stats_lock(mutex_);
        const uint64_t elapsed = Now() - capture_started;
        ++stats_.checkpoint_captures;
        stats_.checkpoint_capture_micros += elapsed;
        if (direct) stats_.checkpoint_flush_micros += elapsed;
      }
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
      if (s.ok()) {
        candidate->native_sequence = db->GetLatestSequenceNumber();
        candidate->blob_floor = 1;
        for (const auto& file : candidate->files) {
          if (file.file_type == kBlobFile)
            candidate->blob_floor =
                std::max(candidate->blob_floor, file.file_number + 1);
        }
        std::vector<std::string> allocated;
        s = storage_->GetChildren(index_, IOOptions(), &allocated, nullptr);
        if (s.ok()) {
          for (const auto& name : allocated) {
            uint64_t number;
            FileType type;
            if (ParseFileName(name, &number, &type))
              candidate->blob_floor =
                  std::max(candidate->blob_floor, number + 1);
          }
        }
        VectorLogPtr logs;
        if (s.ok()) s = db->GetSortedWalFiles(logs);
        if (s.ok() && !logs.empty())
          candidate->wal_floor = logs.front()->LogNumber();
      }
      // Freeze the mirror boundary with the native snapshot. Writes admitted
      // after releasing the fence belong to the next checkpoint.
      std::lock_guard<std::mutex> boundary_lock(mutex_);
      candidate->seq = accepted_;
      candidate->writes = completed_writes_;
    }
    fence.unlock();
    TEST_SYNC_POINT("MetaBypass::NativeFilesCaptured");
    lock.lock();
    if (s.ok() && stats_.error.ok() && !stopping_) {
      captured_ = candidate->seq;
      requested_ = std::max(requested_, candidate->seq);
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
    if (s.ok()) s = CaptureTables(candidate.get());
    // Candidate files now own their bytes and candidate_busy_ protects blobs.
    if (pinned) {
      s.UpdateIfOk(db->EnableFileDeletions());
      TEST_SYNC_POINT("MetaBypass::NativeFilesReleased");
    }
    if (s.ok()) {
      const uint64_t publish_started = Now();
      s = Publish(*candidate);
      std::lock_guard<std::mutex> stats_lock(mutex_);
      stats_.checkpoint_publish_micros += Now() - publish_started;
    }
    if (!s.ok()) Fail(s);
    lock.lock();
    candidate_busy_ = false;
    // Retirement events after capture require their own coverage boundary.
    // New writes and listener events already own their first-event deadline.
    if (s.ok() && NeedsBlobCheckpoint()) MarkDirty();
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
    if (point.blobs.count(number) || !point.blob_floor ||
        number >= point.blob_floor)
      return false;
  if (retired_pending_ &&
      (retired_.blobs.count(number) || !retired_.blob_floor ||
       number >= retired_.blob_floor))
    return false;
  return true;
}
bool Backup::NeedsBlobCheckpoint() const {
  if (points_.empty() || pending_blobs_.empty()) return false;
  for (const auto& blob : pending_blobs_) {
    if (published_ < blob.second.seq) return applied_ > captured_;
  }
  return false;
}
void Backup::GarbageCollectLoop() {
  HddPurposeScope background(HddPurpose::kBulk);
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
      prune_wal_ = true;
      WakeMirror();
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
    const auto account = [&](uint64_t number, uint64_t bytes) {
      lock.lock();
      stats_.pending_blob_gc_bytes -= pending_blobs_.at(number).bytes;
      --stats_.pending_blob_gc_files;
      pending_blobs_.erase(number);
      retiring_blobs_.erase(number);
      ++stats_.reclaimed_blob_files;
      stats_.reclaimed_blob_bytes += bytes;
      lock.unlock();
      TEST_SYNC_POINT("MetaBypass::BlobReclaimed");
    };
    if (storage_->tier()) {
      for (uint64_t number : batch) {
        uint64_t bytes = 0;
        s = storage_->ReclaimBlob(number, &bytes);
        if (!s.ok()) break;
        account(number, bytes);
      }
    } else {
      std::vector<std::pair<uint64_t, uint64_t>> durable;
      s = storage_->ReclaimBlobs(batch, &durable);
      for (const auto& blob : durable) account(blob.first, blob.second);
    }
    // Fail takes mutex_ itself. Publish the sticky error before releasing the
    // capture gate, including when an earlier prefix was durably reclaimed.
    if (!s.ok()) Fail(s);
    lock.lock();
    stats_.blob_gc_micros += Now() - started;
    blob_gc_busy_ = false;
    validator_cv_.notify_one();
    WakeMirror();
    gc_cv_.notify_all();
    if (!s.ok()) break;
  }
  gc_cv_.notify_all();
}
void Backup::WriteCompleted() {
  MB_WAL_SPAN(wait, completed_wait, true);
  std::lock_guard<std::mutex> lock(mutex_);
  MB_WAL_END(wait);
  MB_WAL_SPAN(bookkeeping, completed_bookkeeping, true);
  ++completed_writes_;
  MarkDirty();
}
void Backup::NativeFilesChanged() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++native_changes_;
  MarkDirty();
}
void Backup::MarkDirty() {
  if (!dirty_since_micros_) {
    dirty_since_micros_ = Now();
    validator_cv_.notify_one();
  }
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
  TEST_SYNC_POINT("MetaBypass::BeforeDetachDB");
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
  if (sampler_.joinable()) sampler_.join();
  if (thread_.joinable()) thread_.join();
  if (validator_.joinable()) validator_.join();
  TEST_SYNC_POINT("MetaBypass::BeforeGCJoin");
  if (garbage_collector_.joinable()) garbage_collector_.join();
  return Error();
}
Status Backup::Error() const {
  // Fail publishes the complete first Status before exposing the sticky flag.
  if (!failed_.load(std::memory_order_acquire)) return Status::OK();
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
Status Backup::PreflightCheckpoint(FileSystem* fs, const std::string& backup,
                                   const SeparatedStorage& storage) {
  for (unsigned attempt = 0; attempt < 3; ++attempt) {
    std::string before, after;
    Status first = Read(fs, backup + "/LATEST", &before);
    const bool missing = first.IsNotFound() || first.IsPathNotFound();
    if (!first.ok() && !missing) return first;
    Status validated =
        first.ok() ? PreflightCheckpointImpl(fs, backup, storage) : first;
    TEST_SYNC_POINT_CALLBACK("MetaBypass::RestorePreflightValidated",
                             &validated);
    Status last = Read(fs, backup + "/LATEST", &after);
    const bool now_missing = last.IsNotFound() || last.IsPathNotFound();
    if (!last.ok() && !now_missing) {
      validated.PermitUncheckedError();
      return last;
    }
    if ((missing && now_missing) ||
        (first.ok() && last.ok() && before == after))
      return validated;
    validated.PermitUncheckedError();
  }
  return Status::TryAgain("checkpoint changed during restore preflight");
}
Status Backup::PrepareRecovery(FileSystem* fs, const std::string& backup,
                               const std::string& destination,
                               std::string* point,
                               MetaBypassRecoveryReport* report,
                               bool* has_boundary, SstStorage* sst) {
  std::string inventory;
  Status s = ReadPublishedInventory(fs, backup, point, &inventory);
  if (!s.ok()) return s;
  std::string metadata;
  s = Read(fs, *point + "/RECOVERY", &metadata);
  *has_boundary = s.ok();
  if (s.ok()) {
    uint64_t blob_floor, wal_floor;
    std::istringstream in(metadata);
    if (!(in >> report->checkpoint_sequence >> blob_floor >> wal_floor))
      return Status::Corruption("checkpoint recovery boundary");
  } else if (!s.IsNotFound())
    return s;
  report->recovered_sequence = report->checkpoint_sequence;
  std::istringstream in(inventory.substr(5));
  std::string kind, file;
  uint64_t length;
  std::vector<SstStorage::RestoreFile> ssts;
  while (in >> kind) {
    if (!(in >> file >> length))
      return Status::Corruption("incomplete inventory entry");
    if (kind != "I") continue;
    if (!SafeName(file)) return Status::Corruption("checkpoint file name");
    uint64_t number;
    FileType type;
    if (sst && ParseFileName(file, &number, &type) && type == kTableFile) {
      ssts.push_back({*point + "/" + file, number, length});
      continue;
    }
    s = Copy(fs, *point + "/" + file, destination + "/" + file, length);
    if (!s.ok()) return s;
  }
  if (!in.eof()) return Status::Corruption("checkpoint inventory");
  if (sst) {
    s = sst->RestoreTables(ssts);
    if (!s.ok()) return s;
  }
  if (!*has_boundary) return SyncDir(fs, destination);
  std::vector<std::string> files;
  s = fs->GetChildren(backup + "/work", IOOptions(), &files, nullptr);
  if (s.IsNotFound()) return SyncDir(fs, destination);
  if (!s.ok()) return s;
  for (const auto& name : files) {
    uint64_t number;
    FileType type;
    if (!ParseFileName(name, &number, &type) || type != kWalFile) continue;
    const std::string source = backup + "/work/" + name;
    const std::string target = destination + "/" + name;
    uint64_t size;
    s = fs->GetFileSize(source, IOOptions(), &size, nullptr);
    if (s.ok()) s = ExtendWal(fs, source, target, size);
    if (!s.ok()) return s;
  }
  return SyncDir(fs, destination);
}
Status Backup::PreflightCheckpointImpl(FileSystem* fs,
                                       const std::string& backup,
                                       const SeparatedStorage& storage) {
  std::map<uint64_t, TieredStorage::Version> parsed;
  std::string point, inventory;
  Status s = ReadPublishedInventory(fs, backup, &point, &inventory);
  if (!s.ok()) return s;
  if (storage.tier()) {
    std::string map;
    s = Read(fs, point + "/BLOB-MAP", &map);
    if (s.ok())
      s = storage.tier()->ParseCheckpoint(point + "/BLOB-MAP", &parsed, false);
    if (s.ok())
      s = storage.tier()->BindCheckpointInventory(inventory, map, parsed);
    if (!s.ok()) return s;
    for (const auto& blob : parsed) {
      for (const auto& extent : blob.second.extents) {
        uint64_t size;
        s = fs->GetFileSize(storage.tier()->Path(extent), IOOptions(), &size,
                            nullptr);
        if (!s.ok()) return s;
        if (size != extent.length)
          return Status::Corruption("checkpoint extent length");
      }
    }
    if (!s.ok()) return s;
  }
  std::istringstream in(inventory.substr(5));
  std::string kind, file;
  uint64_t length;
  std::set<std::string> names;
  bool native = false;
  while (in >> kind) {
    if (!(in >> file >> length))
      return Status::Corruption("incomplete inventory entry");
    if (!SafeName(file) || !names.insert(kind + ":" + file).second ||
        (kind != "I" && kind != "B" &&
         !(storage.tier() && kind == "D" && file == "BLOB-MAP")))
      return Status::Corruption("invalid inventory");
    std::string path = point + "/" + file;
    if (kind == "B") {
      uint64_t number;
      std::istringstream n(file);
      if (!(n >> number) || !n.eof() || file != std::to_string(number))
        return Status::Corruption("invalid blob number");
      if (storage.tier()) continue;
      path = storage.BlobPath(number);
    }
    native |= kind == "I";
    uint64_t size;
    s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size != length) return Status::Corruption("published file size", path);
  }
  if (!in.eof() || !native) return Status::Corruption("invalid inventory");
  return Status::OK();
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
