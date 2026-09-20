//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/backup.h"

#include <chrono>
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
}  // namespace
class MirrorWriter : public FSWritableFileOwnerWrapper {
 public:
  MirrorWriter(std::unique_ptr<FSWritableFile> file, Backup* backup,
               std::string name)
      : FSWritableFileOwnerWrapper(std::move(file)),
        backup_(backup),
        name_(std::move(name)) {}
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
    std::lock_guard<std::mutex> serial(backup_->operations_);
    Backup::Event e{Backup::Kind::kAppend, name_, {}, {}};
    e.charge = sizeof(e) + name_.size() + bytes.size();
    Status s = backup_->Reserve(e.charge);
    if (!s.ok()) return AsIO(s);
    e.bytes.assign(bytes.data(), bytes.size());
    IOStatus primary = verification
                           ? target()->Append(bytes, o, *verification, d)
                           : target()->Append(bytes, o, d);
    backup_->Finish(std::move(e), primary);
    return primary;
  }
  IOStatus Control(Backup::Kind kind, uint64_t n, const IOOptions& o,
                   IODebugContext* d) {
    std::lock_guard<std::mutex> serial(backup_->operations_);
    Backup::Event e{kind, name_, {}, {}};
    e.size = n;
    e.charge = sizeof(e) + name_.size();
    Status s = backup_->Reserve(e.charge);
    if (!s.ok()) {
      if (kind == Backup::Kind::kClose)
        target()->Close(o, d).PermitUncheckedError();
      return AsIO(s);
    }
    IOStatus primary = kind == Backup::Kind::kClose
                           ? target()->Close(o, d)
                           : target()->Truncate(n, o, d);
    backup_->Finish(std::move(e), primary);
    return primary;
  }
  Backup* backup_;
  std::string name_;
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
Status Backup::Start(bool existing) {
  Status s = EnsureDir(disk_, options_.backup_dir);
  if (s.ok())
    s = disk_->LockFile(options_.backup_dir + "/LOCK", IOOptions(), &lock_,
                        nullptr);
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
      points_.push_back(p + " " + std::to_string(inventory_crc));
    }
    if (points_.empty() || points_.size() > 2)
      return Status::Corruption("invalid recovery point pointer");
  } else if (!s.IsNotFound())
    return s;
  for (const auto& f : files) {
    if (f.compare(0, 6, "point-") != 0) continue;
    bool retained = false;
    for (const auto& point : points_)
      if (point.substr(0, point.find(' ')) == f) retained = true;
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
  if (existing) {
    s = disk_->GetChildren(index_, IOOptions(), &files, nullptr);
    if (!s.ok()) return s;
    for (const auto& f : files) {
      if (!Tracked(index_ + "/" + f)) continue;
      uint64_t size;
      s = disk_->GetFileSize(index_ + "/" + f, IOOptions(), &size, nullptr);
      if (s.ok()) s = Copy(disk_, index_ + "/" + f, work_ + "/" + f, size);
      if (!s.ok()) return s;
      closed_.insert(f);
    }
  }
  thread_ = std::thread(&Backup::Run, this);
  return Status::OK();
}
Status Backup::Activate(const std::string& identity) {
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
  active_ = true;
  cv_.notify_all();
  return Status::OK();
}
Status Backup::Reserve(size_t charge) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (charge > options_.queue_capacity)
    return Status::InvalidArgument(
        "file operation exceeds Metabypass queue capacity");
  const uint64_t start = Now();
  const bool blocked = stats_.queued_bytes > options_.queue_capacity - charge;
  if (blocked) {
    TEST_SYNC_POINT("MetaBypass::Backpressure");
  }
  cv_.wait(lock, [&] {
    return stopping_ || !stats_.error.ok() ||
           stats_.queued_bytes <= options_.queue_capacity - charge;
  });
  if (blocked) stats_.backpressure_micros += Now() - start;
  if (!stats_.error.ok()) return stats_.error;
  if (stopping_) return Status::ShutdownInProgress();
  stats_.queued_bytes += charge;
  stats_.peak_queued_bytes =
      std::max(stats_.peak_queued_bytes, stats_.queued_bytes);
  return Status::OK();
}
void Backup::Finish(Event&& e, const Status& primary) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (primary.ok()) {
    e.seq = ++accepted_;
    e.queued_micros = Now();
    queue_.push_back(std::move(e));
  } else {
    stats_.queued_bytes -= e.charge;
    // A failed primary append can leave a partial file. Never publish across
    // it.
    if (stats_.error.ok()) stats_.error = primary;
  }
  cv_.notify_all();
}
IOStatus Backup::NewWritableFile(const std::string& p, const FileOptions& o,
                                 std::unique_ptr<FSWritableFile>* r,
                                 IODebugContext* d) {
  if (!Tracked(p)) return target()->NewWritableFile(p, o, r, d);
  std::lock_guard<std::mutex> serial(operations_);
  Event e{Kind::kCreate, Base(p), {}, {}};
  e.charge = sizeof(e) + e.name.size();
  Status s = Reserve(e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->NewWritableFile(p, o, r, d);
  Finish(std::move(e), primary);
  if (primary.ok()) r->reset(new MirrorWriter(std::move(*r), this, Base(p)));
  return primary;
}
IOStatus Backup::ReopenWritableFile(const std::string& p, const FileOptions& o,
                                    std::unique_ptr<FSWritableFile>* r,
                                    IODebugContext* d) {
  if (!Tracked(p)) return target()->ReopenWritableFile(p, o, r, d);
  Status s = Error();
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->ReopenWritableFile(p, o, r, d);
  if (primary.ok()) r->reset(new MirrorWriter(std::move(*r), this, Base(p)));
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
  if (!Tracked(p)) return target()->DeleteFile(p, o, d);
  std::lock_guard<std::mutex> serial(operations_);
  Event e{Kind::kDelete, Base(p), {}, {}};
  e.charge = sizeof(e) + e.name.size();
  Status s = Reserve(e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->DeleteFile(p, o, d);
  Finish(std::move(e), primary);
  return primary;
}
IOStatus Backup::RenameFile(const std::string& a, const std::string& b,
                            const IOOptions& o, IODebugContext* d) {
  if (!Tracked(a) && !Tracked(b)) return target()->RenameFile(a, b, o, d);
  if (!Tracked(a) || !Tracked(b))
    return IOStatus::NotSupported("cross-boundary rename");
  std::lock_guard<std::mutex> serial(operations_);
  Event e{Kind::kRename, Base(a), Base(b), {}};
  e.charge = sizeof(e) + e.name.size() + e.other.size();
  Status s = Reserve(e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->RenameFile(a, b, o, d);
  Finish(std::move(e), primary);
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
  Status injected;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::ApplyStatus", &injected);
  if (!injected.ok()) return injected;
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
    return disk_->NewWritableFile(path, FileOptions(), &writers_[e.name],
                                  nullptr);
  }
  if (e.kind == Kind::kAppend || e.kind == Kind::kTruncate) {
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
  if (e.kind == Kind::kDelete)
    return disk_->DeleteFile(path, IOOptions(), nullptr);
  closed_.insert(e.other);
  return disk_->RenameFile(path, work_ + "/" + e.other, IOOptions(), nullptr);
}
Status Backup::Publish(uint64_t seq) {
  const uint64_t start = Now();
  for (const auto& w : writers_) {
    if (!w.second) continue;
    Status s = w.second->Flush(IOOptions(), nullptr);
    if (!s.ok()) return s;
  }
  NativeState state;
  Status s = Inspect(disk_, work_, &state);
  if (!s.ok())
    return s.IsNotFound() ? Status::Incomplete("baseline pending") : s;
  if (state.manifest_end == 0 || !closed_.count("IDENTITY"))
    return Status::Incomplete("baseline pending");
  std::map<std::string, uint64_t> files;
  files[state.manifest] = state.manifest_end;
  for (const auto& t : state.tables) {
    const std::string f = MakeTableFileName(t.first);
    uint64_t size = 0;
    s = disk_->GetFileSize(work_ + "/" + f, IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size != t.second || !closed_.count(f))
      return Status::Incomplete("SST not complete");
    files[f] = size;
  }
  std::vector<std::string> children;
  s = disk_->GetChildren(work_, IOOptions(), &children, nullptr);
  if (!s.ok()) return s;
  for (const auto& f : children) {
    uint64_t number;
    FileType type;
    if (!ParseFileName(f, &number, &type)) continue;
    if (type == kWalFile && number >= state.log_number) {
      uint64_t end;
      s = ReadLog(
          disk_, work_ + "/" + f, number,
          [](const Slice&) { return Status::OK(); }, &end);
      if (!s.ok()) return s;
      files[f] = end;
    } else if (type == kIdentityFile || type == kOptionsFile) {
      if (!closed_.count(f)) continue;
      s = disk_->GetFileSize(work_ + "/" + f, IOOptions(), &files[f], nullptr);
      if (!s.ok()) return s;
    }
  }
  std::map<uint64_t, uint64_t> blobs;
  s = storage_->Dependencies(work_, state, &blobs);
  if (!s.ok()) return s;
  for (const auto& table : state.tables) {
    s = storage_->ValidateTable(work_ + "/" + MakeTableFileName(table.first),
                                db_options_, &blobs);
    if (!s.ok()) return s;
  }
  s = storage_->Persist(blobs);
  if (!s.ok()) return s;
  TEST_SYNC_POINT_CALLBACK("MetaBypass::DependenciesSynced", &s);
  if (!s.ok()) return s;
  const std::string name = "point-" + std::to_string(++generation_);
  const std::string point = options_.backup_dir + "/" + name;
  s = disk_->CreateDir(point, IOOptions(), nullptr);
  if (!s.ok()) return s;
  std::ostringstream inventory;
  uint64_t total = 0;
  for (const auto& f : files) {
    uint64_t number;
    FileType type;
    const bool table =
        ParseFileName(f.first, &number, &type) && type == kTableFile;
    if (table) {
      s = disk_->SyncFile(work_ + "/" + f.first, FileOptions(), IOOptions(),
                          true, nullptr);
      if (s.ok()) {
        s = disk_->LinkFile(work_ + "/" + f.first, point + "/" + f.first,
                            IOOptions(), nullptr);
        if (!s.ok())
          s = Copy(disk_, work_ + "/" + f.first, point + "/" + f.first,
                   f.second);
      }
    } else
      s = Copy(disk_, work_ + "/" + f.first, point + "/" + f.first, f.second);
    if (!s.ok()) return s;
    uint32_t crc;
    s = Digest(disk_, point + "/" + f.first, f.second, &crc);
    if (!s.ok()) return s;
    inventory << "I " << f.first << ' ' << f.second << ' ' << crc << '\n';
    total += f.second;
  }
  for (const auto& b : blobs) {
    uint32_t crc;
    s = Digest(disk_, storage_->BlobPath(b.first), b.second, &crc);
    if (!s.ok()) return s;
    inventory << "B " << b.first << ' ' << b.second << ' ' << crc << '\n';
  }
  s = Write(disk_, point + "/CURRENT", state.manifest + "\n");
  const std::string current = state.manifest + "\n";
  inventory << "I CURRENT " << current.size() << ' '
            << crc32c::Value(current.data(), current.size()) << '\n';
  const std::string inventory_bytes = inventory.str();
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
  if (!points_.empty()) pointer += points_.front() + "\n";
  if (s.ok()) s = Write(disk_, options_.backup_dir + "/LATEST.tmp", pointer);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BeforePointerReplace", &s);
  if (s.ok())
    s = disk_->RenameFile(options_.backup_dir + "/LATEST.tmp",
                          options_.backup_dir + "/LATEST", IOOptions(),
                          nullptr);
  if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::PointerSynced");
  points_.push_front(reference);
  while (points_.size() > 2) {
    s = RemoveDir(disk_,
                  options_.backup_dir + "/" +
                      points_.back().substr(0, points_.back().find(' ')));
    if (!s.ok()) return s;
    points_.pop_back();
  }
  total = 0;
  std::vector<std::string> retained_dirs{work_};
  for (const auto& p : points_)
    retained_dirs.push_back(options_.backup_dir + "/" +
                            p.substr(0, p.find(' ')));
  for (const auto& dir : retained_dirs) {
    std::vector<FileAttributes> attributes;
    s = disk_->GetChildrenFileAttributes(dir, IOOptions(), &attributes,
                                         nullptr);
    if (!s.ok()) return s;
    for (const auto& file : attributes) total += file.size_bytes;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    published_ = seq;
    ++stats_.recovery_points;
    stats_.last_build_micros = Now() - start;
    stats_.last_publish_micros = Now();
    stats_.last_point_lag_micros = Now() - oldest_unpublished_micros_;
    oldest_unpublished_micros_ = 0;
    stats_.retained_index_bytes = total;
    cv_.notify_all();
  }
  return Status::OK();
}
void Backup::Run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (stats_.error.ok()) {
    cv_.wait_for(lock, std::chrono::milliseconds(options_.interval_ms), [&] {
      // Reservations include producers still executing primary I/O. Waking
      // on reserved bytes alone can spin with mutex_ held and prevent Finish
      // from ever committing its event.
      return stopping_ || !stats_.error.ok() ||
             (!queue_.empty() && (requested_ > published_ ||
                                  stats_.queued_bytes >= options_.batch_bytes));
    });
    if (!stats_.error.ok()) break;
    const uint64_t boundary = accepted_;
    while (!queue_.empty() && queue_.front().seq <= boundary) {
      Event e = std::move(queue_.front());
      queue_.pop_front();
      if (!oldest_unpublished_micros_)
        oldest_unpublished_micros_ = e.queued_micros;
      lock.unlock();
      Status s = Apply(e);
      lock.lock();
      stats_.queued_bytes -= e.charge;
      stats_.mirrored_bytes += e.bytes.size();
      applied_ = e.seq;
      cv_.notify_all();
      if (!s.ok()) {
        stats_.error = s;
        break;
      }
    }
    if (!stats_.error.ok()) break;
    if (active_ && applied_ > published_) {
      const uint64_t seq = applied_;
      lock.unlock();
      Status s = Publish(seq);
      lock.lock();
      if (!s.ok() && (!s.IsIncomplete() || stopping_)) stats_.error = s;
      // A fragmented log record or pending atomic group needs more events.
      if (s.IsIncomplete()) requested_ = published_;
    }
    if (stopping_ && queue_.empty()) break;
  }
  queue_.clear();
  stats_.queued_bytes = 0;
  cv_.notify_all();
  lock.unlock();
  for (auto& w : writers_) {
    if (w.second) w.second->Close(IOOptions(), nullptr).PermitUncheckedError();
  }
  writers_.clear();
}
Status Backup::Sync() {
  std::unique_lock<std::mutex> lock(mutex_);
  const uint64_t goal = accepted_;
  requested_ = std::max(requested_, goal);
  cv_.notify_all();
  cv_.wait(lock, [&] {
    return !stats_.error.ok() || published_ >= goal || stopping_;
  });
  return stats_.error;
}
Status Backup::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    cv_.notify_all();
  }
  if (thread_.joinable()) thread_.join();
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
Status Backup::RestoreFiles(FileSystem* fs, const std::string& backup,
                            const std::string& destination,
                            const SeparatedStorage& storage) {
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
  const std::string point = backup + "/" + name;
  std::string identity;
  s = Read(fs, point + "/IDENTITY", &identity);
  if (!s.ok()) return s;
  if (identity != owner) return Status::Corruption("backup identity mismatch");
  std::string inventory;
  s = Read(fs, point + "/INVENTORY", &inventory);
  if (!s.ok()) return s;
  if (crc32c::Value(inventory.data(), inventory.size()) != inventory_crc)
    return Status::Corruption("published inventory checksum");
  std::istringstream in(inventory);
  std::string kind, file;
  uint64_t length;
  uint32_t expected;
  struct Entry {
    std::string file;
    uint64_t length;
  };
  std::vector<Entry> entries;
  while (in >> kind >> file >> length >> expected) {
    if (!SafeName(file) || (kind != "I" && kind != "B"))
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
  NativeState state;
  s = Inspect(fs, point, &state);
  if (!s.ok()) return s;
  for (const auto& e : entries) {
    s = Copy(fs, point + "/" + e.file, destination + "/" + e.file, e.length);
    if (!s.ok()) return s;
  }
  s = Write(fs, destination + "/CURRENT", state.manifest + "\n");
  if (s.ok()) s = SyncDir(fs, destination);
  return s;
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
