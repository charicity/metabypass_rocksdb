//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/sst_storage.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <limits>
#include <sstream>

#include "file/filename.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
namespace {
IOStatus AsIO(const Status& s) {
  if (s.ok()) return IOStatus::OK();
  if (s.IsNotFound()) return IOStatus::NotFound(s.ToString());
  if (s.IsCorruption()) return IOStatus::Corruption(s.ToString());
  if (s.IsNotSupported()) return IOStatus::NotSupported(s.ToString());
  return IOStatus::IOError(s.ToString());
}
// POSIX DeleteFile reports ENOENT as IOError on some FileSystem versions.
// Treat a delete as idempotent only when FileExists proves the path is absent;
// never mask permissions, media errors, or an inaccessible parent directory.
Status RemoveIfExists(FileSystem* fs, const std::string& path) {
  Status exists = fs->FileExists(path, IOOptions(), nullptr);
  if (exists.IsNotFound()) return Status::OK();
  if (!exists.ok()) return exists;
  Status s = fs->DeleteFile(path, IOOptions(), nullptr);
  if (!s.ok()) {
    exists = fs->FileExists(path, IOOptions(), nullptr);
    if (exists.IsNotFound()) return Status::OK();
    exists.PermitUncheckedError();
  }
  return s;
}
}  // namespace
class SstLogicalReader : public FSRandomAccessFile {
 public:
  SstLogicalReader(SstStorage* storage,
                   std::shared_ptr<SstStorage::Entry> entry)
      : storage_(storage), entry_(std::move(entry)) {}
  ~SstLogicalReader() override {
    std::lock_guard<std::mutex> lock(entry_->mutex);
    if (--entry_->readers == 0) entry_->handle.reset();
  }
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    std::shared_ptr<SstStorage::Handle> handle;
    {
      std::lock_guard<std::mutex> lock(entry_->mutex);
      handle = entry_->handle;
    }
    if (!handle) return IOStatus::Corruption("SST reader lost its handle");
    TEST_SYNC_POINT("MetaBypassSst::ReadPinned");
    auto s = handle->file->Read(offset, n, options, result, scratch, dbg);
    if (s.ok()) {
      // Never return memory owned by a replaced physical handle.
      if (result->data() != scratch && !result->empty()) {
        std::memmove(scratch, result->data(), result->size());
        *result = Slice(scratch, result->size());
      }
      storage_->sampler_.Record(entry_->number, options.io_activity);
    }
    return s;
  }

 private:
  SstStorage* storage_;
  const std::shared_ptr<SstStorage::Entry> entry_;
};
// Sequential readers use the same route pins; offset belongs to this reader.
class SstSequentialReader : public FSSequentialFile {
 public:
  explicit SstSequentialReader(std::unique_ptr<FSRandomAccessFile> reader)
      : reader_(std::move(reader)) {}
  IOStatus Read(size_t n, const IOOptions& o, Slice* r, char* scratch,
                IODebugContext* d) override {
    auto s = reader_->Read(offset_, n, o, r, scratch, d);
    if (s.ok()) offset_ += r->size();
    return s;
  }
  IOStatus Skip(uint64_t n) override {
    offset_ += n;
    return IOStatus::OK();
  }

 private:
  std::unique_ptr<FSRandomAccessFile> reader_;
  uint64_t offset_ = 0;
};
SstStorage::SstStorage(std::shared_ptr<FileSystem> fs, std::string index,
                       const MetaBypassOptions& options, SstClock clock)
    : FileSystemWrapper(std::move(fs)),
      index_(std::move(index)),
      backup_(options.backup_dir),
      store_(backup_ + "/sst-store"),
      options_(options.sst_tiering),
      clock_(std::move(clock)),
      sampler_(options_, clock_) {}
SstStorage::~SstStorage() { Stop(); }
std::string SstStorage::Local(uint64_t number) const {
  return MakeTableFileName(index_, number);
}
bool SstStorage::IsTable(const std::string& path, uint64_t* number) const {
  if (path.compare(0, index_.size() + 1, index_ + "/") != 0) return false;
  uint64_t n;
  FileType type;
  if (!ParseFileName(path.substr(index_.size() + 1), &n, &type) ||
      type != kTableFile)
    return false;
  if (number) *number = n;
  return true;
}
std::shared_ptr<SstStorage::Entry> SstStorage::Find(uint64_t number) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = entries_.find(number);
  return it == entries_.end() ? nullptr : it->second;
}
std::string SstStorage::Path(const std::shared_ptr<Entry>& entry) const {
  std::lock_guard<std::mutex> lock(entry->mutex);
  return entry->hot ? Local(entry->number) : store_ + "/" + entry->object;
}
Status SstStorage::Initialize(const std::string& identity, bool restore) {
  identity_ = identity;
  while (!identity_.empty() &&
         (identity_.back() == '\n' || identity_.back() == '\r'))
    identity_.pop_back();
  Status s = EnsureDir(target(), store_);
  if (!s.ok()) return s;
  std::string epoch;
  s = Read(target(), backup_ + "/SST-EPOCH", &epoch);
  if (s.ok()) {
    std::istringstream in(epoch);
    std::string owner;
    if (!(in >> epoch_ >> owner) || owner != identity_ ||
        epoch_ == std::numeric_limits<uint64_t>::max())
      return Status::Corruption("SST epoch identity or format");
  } else if (!s.IsNotFound())
    return s;
  ++epoch_;
  s = Write(target(), backup_ + "/SST-EPOCH.tmp",
            std::to_string(epoch_) + " " + identity_ + "\n");
  if (s.ok())
    s = target()->RenameFile(backup_ + "/SST-EPOCH.tmp", backup_ + "/SST-EPOCH",
                             IOOptions(), nullptr);
  if (s.ok()) s = SyncDir(target(), backup_);
  if (!s.ok()) return s;
  if (!restore) {
    s = LoadPlacement();
    if (!s.ok()) return s;
  }
  s = Discover();
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!restore) {
    std::vector<std::string> local_files;
    s = target()->GetChildren(index_, IOOptions(), &local_files, nullptr);
    if (!s.ok()) return s;
    const std::string suffix = ".sst-tier-tmp";
    for (const auto& file : local_files) {
      if (file.size() >= suffix.size() &&
          file.compare(file.size() - suffix.size(), suffix.size(), suffix) ==
              0) {
        s = target()->DeleteFile(index_ + "/" + file, IOOptions(), nullptr);
        if (!s.ok()) return s;
      }
    }
    std::set<std::string> retained;
    for (const auto& item : entries_) retained.insert(item.second->object);
    std::vector<std::string> objects;
    s = target()->GetChildren(store_, IOOptions(), &objects, nullptr);
    if (!s.ok()) return s;
    for (const auto& object : objects) {
      if (object == "." || object == ".." || retained.count(object)) continue;
      s = target()->DeleteFile(store_ + "/" + object, IOOptions(), nullptr);
      if (!s.ok()) return s;
    }
  }
  return Status::OK();
}
Status SstStorage::Discover() {
  std::vector<std::string> files;
  Status s = target()->GetChildren(index_, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  for (const auto& file : files) {
    uint64_t number;
    if (!IsTable(index_ + "/" + file, &number)) continue;
    auto entry = Find(number);
    if (entry) {
      std::lock_guard<std::mutex> lock(entry->mutex);
      if (!entry->hot || !entry->object.empty()) continue;
    }
    uint64_t size;
    s = target()->GetFileSize(index_ + "/" + file, IOOptions(), &size, nullptr);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return s;
    if (entry) {
      std::lock_guard<std::mutex> lock(entry->mutex);
      entry->size = size;
      continue;
    }
    entry = std::make_shared<Entry>();
    entry->number = number;
    entry->size = size;
    entry->changed = clock_();
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.emplace(number, std::move(entry));
  }
  return Status::OK();
}
Status SstStorage::SavePlacement(uint64_t changed_number, int hot) {
  std::vector<std::shared_ptr<Entry>> entries;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& item : entries_) entries.push_back(item.second);
  }
  std::ostringstream out;
  out << "MBS1 " << identity_ << '\n';
  for (const auto& entry : entries) {
    std::lock_guard<std::mutex> lock(entry->mutex);
    if (entry->object.empty() || !entry->live) continue;
    out << entry->number << ' ' << entry->size << ' ' << entry->crc << ' '
        << (entry->number == changed_number ? hot : int(entry->hot)) << ' '
        << entry->object << '\n';
  }
  const std::string data = out.str();
  const std::string bytes =
      data + "CRC " + std::to_string(crc32c::Value(data.data(), data.size())) +
      "\n";
  Status s = Write(target(), index_ + "/SST-PLACEMENT.tmp", bytes);
  TEST_SYNC_POINT_CALLBACK("MetaBypassSst::PlacementWritten", &s);
  if (s.ok())
    s = target()->RenameFile(index_ + "/SST-PLACEMENT.tmp",
                             index_ + "/SST-PLACEMENT", IOOptions(), nullptr);
  TEST_SYNC_POINT_CALLBACK("MetaBypassSst::PlacementRenamed", &s);
  if (s.ok()) s = SyncDir(target(), index_);
  TEST_SYNC_POINT_CALLBACK("MetaBypassSst::PlacementSynced", &s);
  return s;
}
Status SstStorage::LoadPlacement() {
  std::string bytes;
  Status s = Read(target(), index_ + "/SST-PLACEMENT", &bytes);
  if (s.IsNotFound()) return Status::OK();
  if (!s.ok()) return s;
  const auto at = bytes.rfind("CRC ");
  if (at == std::string::npos)
    return Status::Corruption("SST placement checksum");
  uint32_t expected;
  std::istringstream checksum(bytes.substr(at + 4));
  if (!(checksum >> expected) || crc32c::Value(bytes.data(), at) != expected)
    return Status::Corruption("SST placement checksum");
  std::istringstream in(bytes.substr(0, at));
  std::string version, owner;
  if (!(in >> version >> owner) || version != "MBS1" || owner != identity_)
    return Status::Corruption("SST placement identity or version");
  uint64_t number, size;
  uint32_t crc;
  int hot;
  std::string object;
  while (in >> number >> size >> crc >> hot >> object) {
    if ((hot != 0 && hot != 1) || object.find('/') != std::string::npos ||
        object.find("..") != std::string::npos || object.empty() ||
        entries_.count(number))
      return Status::Corruption("SST placement entry");
    const auto dash = object.find('-');
    uint64_t object_epoch = 0;
    std::istringstream object_id(object.substr(0, dash));
    if (dash == std::string::npos || !(object_id >> object_epoch) ||
        !object_id.eof() || !object_epoch || object_epoch > epoch_ ||
        object.substr(dash) !=
            "-" + std::to_string(number) + "-" + std::to_string(crc) + ".sst")
      return Status::Corruption("SST object identity");
    auto entry = std::make_shared<Entry>();
    entry->number = number;
    entry->size = size;
    entry->crc = crc;
    entry->hot = hot != 0;
    entry->object = object;
    entry->changed = clock_();
    uint64_t actual;
    s = target()->GetFileSize(store_ + "/" + object, IOOptions(), &actual,
                              nullptr);
    if (!s.ok()) return s;
    if (actual != size) return Status::Corruption("SST object size");
    uint32_t actual_crc;
    s = Digest(target(), store_ + "/" + object, size, &actual_crc);
    if (!s.ok()) return s;
    if (actual_crc != crc) return Status::Corruption("SST object checksum");
    if (entry->hot) {
      s = target()->GetFileSize(Local(number), IOOptions(), &actual, nullptr);
      // The durable cold map always precedes SSD deletion. Missing hot data
      // means loss outside the migration protocol; require explicit Restore.
      if (!s.ok()) return s;
      if (actual != size) return Status::Corruption("hot SST size");
    } else {
      // A crash after publishing the cold map may leave a disposable SSD copy.
      s = RemoveIfExists(target(), Local(number));
      if (!s.ok() && !s.IsNotFound()) return s;
    }
    entries_.emplace(number, std::move(entry));
  }
  return in.eof() ? Status::OK() : Status::Corruption("SST placement parse");
}
Status SstStorage::MakeObject(const std::string& source,
                              const std::shared_ptr<Entry>& e) {
  uint64_t size;
  {
    std::lock_guard<std::mutex> lock(e->mutex);
    size = e->size;
  }
  uint32_t crc;
  Status s = Digest(target(), source, size, &crc);
  if (!s.ok()) return s;
  const std::string object = std::to_string(epoch_) + "-" +
                             std::to_string(e->number) + "-" +
                             std::to_string(crc) + ".sst";
  const std::string dest = store_ + "/" + object;
  s = target()->FileExists(dest, IOOptions(), nullptr);
  if (s.ok()) {
    uint64_t actual_size;
    s = target()->GetFileSize(dest, IOOptions(), &actual_size, nullptr);
    if (!s.ok()) return s;
    uint32_t actual;
    s = Digest(target(), dest, actual_size, &actual);
    if (!s.ok()) return s;
    if (actual_size != size || actual != crc)
      return Status::Corruption("SST immutable object identity conflict");
  } else if (s.IsNotFound()) {
    s = target()->LinkFile(source, dest, IOOptions(), nullptr);
    if (s.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      ++stats_.reused_links;
    } else {
      s = CopyLimited(source, dest + ".tmp", size, crc);
      if (s.ok())
        s = target()->RenameFile(dest + ".tmp", dest, IOOptions(), nullptr);
      if (!s.ok()) {
        target()
            ->DeleteFile(dest + ".tmp", IOOptions(), nullptr)
            .PermitUncheckedError();
        return s;
      }
    }
  } else
    return s;
  if (s.ok()) s = SyncDir(target(), store_);
  TEST_SYNC_POINT_CALLBACK("MetaBypassSst::ObjectSynced", &s);
  if (s.ok()) {
    std::lock_guard<std::mutex> mutation(mutation_);
    {
      std::lock_guard<std::mutex> lock(e->mutex);
      if (!e->live) {
        target()->DeleteFile(dest, IOOptions(), nullptr).PermitUncheckedError();
        return Status::OK();
      }
      e->object = object;
      e->crc = crc;
    }
    s = SavePlacement();
  }
  return s;
}
Status SstStorage::Protect(const std::string& point, const NativeState& state) {
  Status s;
  {
    std::lock_guard<std::mutex> mutation(mutation_);
    s = Discover();
  }
  if (!s.ok()) return s;
  for (const auto& table : state.tables) {
    auto entry = Find(table.first);
    if (!entry) continue;  // Already logically deleted by native RocksDB.
    {
      std::lock_guard<std::mutex> lock(entry->mutex);
      if (!entry->object.empty()) continue;
      if (entry->size != table.second) continue;
    }  // An output is still growing.
    s = MakeObject(MakeTableFileName(point, table.first), entry);
    if (!s.ok()) return s;
  }
  return Status::OK();
}
Status SstStorage::RestoreTable(const std::string& source, uint64_t number,
                                uint64_t size) {
  auto entry = std::make_shared<Entry>();
  entry->number = number;
  entry->size = size;
  entry->hot = false;
  entry->changed = clock_();
  Status s = MakeObject(source, entry);
  if (!s.ok()) return s;
  std::lock_guard<std::mutex> mutation(mutation_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_[number] = entry;
  }
  return SavePlacement();
}
Status SstStorage::OpenHandle(const std::shared_ptr<Entry>& e, bool hot,
                              std::shared_ptr<Handle>* result) {
  auto handle = std::make_shared<Handle>();
  FileOptions options;
  options.use_mmap_reads = false;
  options.use_direct_reads = false;
  Status s = target()->NewRandomAccessFile(
      hot ? Local(e->number) : store_ + "/" + e->object, options, &handle->file,
      nullptr);
  if (s.ok()) *result = std::move(handle);
  return s;
}
IOStatus SstStorage::NewRandomAccessFile(
    const std::string& path, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  uint64_t number;
  if (!IsTable(path, &number))
    return target()->NewRandomAccessFile(path, options, result, dbg);
  if (options.use_mmap_reads || options.use_direct_reads)
    return IOStatus::NotSupported("SST tiering requires buffered reads");
  // A newly created SST can be opened before the next controller discovery.
  std::lock_guard<std::mutex> mutation(mutation_);
  auto e = Find(number);
  if (!e) {
    uint64_t size;
    auto s = target()->GetFileSize(path, IOOptions(), &size, dbg);
    if (!s.ok()) return s;
    e = std::make_shared<Entry>();
    e->number = number;
    e->size = size;
    e->changed = clock_();
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.emplace(number, e);
  }
  // mutation_ excludes route replacement, but normal reads need neither it nor
  // the global registry lock. The cached handle exists only while readers do.
  std::shared_ptr<Handle> handle;
  {
    std::lock_guard<std::mutex> lock(e->mutex);
    handle = e->handle;
  }
  if (!handle) {
    Status s = OpenHandle(e, e->hot, &handle);
    if (!s.ok()) return AsIO(s);
  }
  {
    std::lock_guard<std::mutex> lock(e->mutex);
    e->handle = std::move(handle);
    ++e->readers;
  }
  result->reset(new SstLogicalReader(this, std::move(e)));
  return IOStatus::OK();
}
IOStatus SstStorage::NewSequentialFile(
    const std::string& path, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  if (!IsTable(path))
    return target()->NewSequentialFile(path, options, result, dbg);
  std::unique_ptr<FSRandomAccessFile> reader;
  auto s = NewRandomAccessFile(path, options, &reader, dbg);
  if (s.ok()) result->reset(new SstSequentialReader(std::move(reader)));
  return s;
}
IOStatus SstStorage::GetChildren(const std::string& path,
                                 const IOOptions& options,
                                 std::vector<std::string>* result,
                                 IODebugContext* dbg) {
  auto s = target()->GetChildren(path, options, result, dbg);
  if (!s.ok() || path != index_) return s;
  std::set<std::string> names(result->begin(), result->end());
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& item : entries_) {
    const auto name = Local(item.first).substr(index_.size() + 1);
    if (names.insert(name).second) result->push_back(name);
  }
  return s;
}
IOStatus SstStorage::GetChildrenFileAttributes(
    const std::string& path, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  if (path != index_)
    return target()->GetChildrenFileAttributes(path, options, result, dbg);
  std::vector<std::string> children;
  auto s = GetChildren(path, options, &children, dbg);
  if (!s.ok()) return s;
  result->clear();
  for (const auto& name : children) {
    FileAttributes attr;
    attr.name = name;
    s = GetFileSize(path + "/" + name, options, &attr.size_bytes, dbg);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return s;
    result->push_back(std::move(attr));
  }
  return IOStatus::OK();
}
IOStatus SstStorage::FileExists(const std::string& path,
                                const IOOptions& options, IODebugContext* dbg) {
  uint64_t number;
  if (IsTable(path, &number)) {
    auto e = Find(number);
    if (e) return IOStatus::OK();
  }
  return target()->FileExists(path, options, dbg);
}
IOStatus SstStorage::GetFileSize(const std::string& path,
                                 const IOOptions& options, uint64_t* size,
                                 IODebugContext* dbg) {
  uint64_t number;
  if (IsTable(path, &number)) {
    auto e = Find(number);
    if (e) {
      std::lock_guard<std::mutex> lock(e->mutex);
      if (!e->object.empty()) {
        *size = e->size;
        return IOStatus::OK();
      }
    }
  }
  return target()->GetFileSize(path, options, size, dbg);
}
IOStatus SstStorage::LinkFile(const std::string& from, const std::string& to,
                              const IOOptions& options, IODebugContext* dbg) {
  uint64_t number;
  if (IsTable(from, &number)) {
    // Backup startup shares the durable HDD object, even for hot SSTs.
    std::lock_guard<std::mutex> mutation(mutation_);
    auto e = Find(number);
    if (e && !e->object.empty())
      return target()->LinkFile(store_ + "/" + e->object, to, options, dbg);
  }
  return target()->LinkFile(from, to, options, dbg);
}
IOStatus SstStorage::SyncFile(const std::string& path, const FileOptions& f,
                              const IOOptions& options, bool full,
                              IODebugContext* dbg) {
  uint64_t number;
  if (IsTable(path, &number)) {
    auto e = Find(number);
    if (e) return target()->SyncFile(Path(e), f, options, full, dbg);
  }
  return target()->SyncFile(path, f, options, full, dbg);
}
IOStatus SstStorage::DeleteFile(const std::string& path,
                                const IOOptions& options, IODebugContext* dbg) {
  uint64_t number;
  if (!IsTable(path, &number)) return target()->DeleteFile(path, options, dbg);
  std::lock_guard<std::mutex> mutation(mutation_);
  auto e = Find(number);
  if (!e) return target()->DeleteFile(path, options, dbg);
  {
    std::lock_guard<std::mutex> lock(e->mutex);
    e->live = false;
  }
  Status s = SavePlacement();
  if (!s.ok()) {
    std::lock_guard<std::mutex> lock(e->mutex);
    e->live = true;
    return AsIO(s);
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.erase(number);
  }
  // Existing logical readers retain a physical handle, just as native POSIX
  // readers retain unlinked files. Points retain independent hardlink roots.
  if (!e->object.empty()) {
    s = RemoveIfExists(target(), store_ + "/" + e->object);
    if (!s.ok() && !s.IsNotFound()) return AsIO(s);
  }
  s = RemoveIfExists(target(), path);
  if (s.ok() || s.IsNotFound()) {
    std::shared_ptr<Handle> handle;
    {
      std::lock_guard<std::mutex> lock(e->mutex);
      if (e->hot && e->readers) handle = e->handle;
    }
    if (handle) {
      std::lock_guard<std::mutex> lock(mutex_);
      retired_.push_back({std::move(handle), "", e->size});
      stats_.pending_delete_bytes += e->size;
    }
  }
  return s.IsNotFound() ? IOStatus::OK() : AsIO(s);
}
Status SstStorage::CopyLimited(const std::string& from, const std::string& to,
                               uint64_t size, uint32_t expected,
                               uint64_t promotion) {
  std::unique_ptr<FSSequentialFile> input;
  std::unique_ptr<FSWritableFile> output;
  Status s = target()->NewSequentialFile(from, FileOptions(), &input, nullptr);
  if (s.ok())
    s = target()->NewWritableFile(to, FileOptions(), &output, nullptr);
  if (!s.ok()) return s;
  std::array<char, 65536> buffer;
  uint64_t done = 0;
  uint32_t crc = 0;
  const auto start = std::chrono::steady_clock::now();
  while (s.ok() && done < size) {
    if (promotion) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (stopping_ || !desired_.count(promotion)) {
        s = Status::Aborted("SST promotion superseded");
        break;
      }
    }
    Slice data;
    s = input->Read(std::min<uint64_t>(size - done, buffer.size()), IOOptions(),
                    &data, buffer.data(), nullptr);
    if (!s.ok()) break;
    if (data.empty()) {
      s = Status::Corruption("short SST copy", from);
      break;
    }
    crc = crc32c::Extend(crc, data.data(), data.size());
    s = output->Append(data, IOOptions(), nullptr);
    done += data.size();
    TEST_SYNC_POINT("MetaBypassSst::CopyChunk");
    if (s.ok() && options_.migration_bytes_per_sec) {
      const auto due =
          start + std::chrono::microseconds(
                      done / options_.migration_bytes_per_sec * 1000000 +
                      done % options_.migration_bytes_per_sec * 1000000 /
                          options_.migration_bytes_per_sec);
      std::unique_lock<std::mutex> lock(mutex_);
      if (wake_.wait_until(lock, due, [&] { return promotion && stopping_; }))
        s = Status::ShutdownInProgress();
    }
  }
  if (s.ok() && crc != expected)
    s = Status::Corruption("SST copy checksum", from);
  if (s.ok()) s = output->Fsync(IOOptions(), nullptr);
  s.UpdateIfOk(output->Close(IOOptions(), nullptr));
  if (s.ok()) {
    uint32_t actual;
    uint64_t actual_size;
    s = target()->GetFileSize(to, IOOptions(), &actual_size, nullptr);
    if (s.ok() && actual_size != size)
      s = Status::Corruption("SST copy size", to);
    if (s.ok()) s = Digest(target(), to, size, &actual);
    if (s.ok() && actual != expected)
      s = Status::Corruption("SST destination checksum", to);
  }
  if (s.ok()) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.copied_bytes += done;
  }
  return s;
}
Status SstStorage::Migrate(const SstMigrationIntent& intent) {
  std::unique_lock<std::mutex> mutation(mutation_);
  auto e = Find(intent.number);
  if (!e || e->hot == intent.hot || e->object.empty()) return Status::OK();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if ((desired_.count(intent.number) != 0) != intent.hot) return Status::OK();
  }
  Status s;
  const std::string local = Local(e->number);
  std::shared_ptr<Handle> replacement;
  auto abandon = [&] {
    RemoveIfExists(target(), local + ".sst-tier-tmp").PermitUncheckedError();
    Status installed = target()->FileExists(local, IOOptions(), nullptr);
    Status temporary =
        target()->FileExists(local + ".sst-tier-tmp", IOOptions(), nullptr);
    const bool absent = installed.IsNotFound() && temporary.IsNotFound();
    installed.PermitUncheckedError();
    temporary.PermitUncheckedError();
    std::lock_guard<std::mutex> lock(mutex_);
    if (absent)
      stats_.reserved_bytes -= e->size;
    else
      orphaned_[e->number] = e->size;
  };
  if (intent.hot) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto& retired : retired_)
        if (retired.path == local) return s;
      if (orphaned_.count(e->number)) return s;
      const uint64_t budget =
          options_.ssd_capacity_bytes -
          options_.ssd_capacity_bytes / 100 * options_.reserve_percent -
          options_.ssd_capacity_bytes % 100 * options_.reserve_percent / 100;
      uint64_t charged = stats_.pending_delete_bytes + stats_.reserved_bytes;
      for (const auto& item : entries_) {
        std::lock_guard<std::mutex> entry(item.second->mutex);
        if (item.second->hot) charged += item.second->size;
      }
      if (e->size > budget || charged > budget - e->size) return s;
      stats_.reserved_bytes += e->size;
    }
    const auto object = e->object;
    const auto size = e->size;
    const auto crc = e->crc;
    mutation.unlock();
    s = CopyLimited(store_ + "/" + object, local + ".sst-tier-tmp", size, crc,
                    e->number);
    mutation.lock();
    if (s.ok() && Find(e->number) != e)
      s = Status::Aborted("SST deleted during promotion");
    if (s.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!desired_.count(e->number))
        s = Status::Aborted("SST promotion superseded");
    }
    TEST_SYNC_POINT_CALLBACK("MetaBypassSst::PromotionCopied", &s);
    if (s.ok())
      s = target()->RenameFile(local + ".sst-tier-tmp", local, IOOptions(),
                               nullptr);
    if (s.ok()) s = SyncDir(target(), index_);
    TEST_SYNC_POINT_CALLBACK("MetaBypassSst::PromotionInstalled", &s);
    if (!s.ok()) {
      abandon();
      return s;
    }
  }
  if (!intent.hot) {
    const auto object = e->object;
    const auto size = e->size;
    const auto expected = e->crc;
    // Verify the only surviving copy before discarding the SSD copy. This
    // potentially long read neither holds the placement lock nor records heat.
    mutation.unlock();
    uint64_t actual_size = 0;
    uint32_t actual_crc = 0;
    const std::string path = store_ + "/" + object;
    s = target()->GetFileSize(path, IOOptions(), &actual_size, nullptr);
    if (s.ok() && actual_size != size)
      s = Status::Corruption("SST protection size before demotion", path);
    if (s.ok()) s = Digest(target(), path, size, &actual_crc);
    if (s.ok() && actual_crc != expected)
      s = Status::Corruption("SST protection checksum before demotion", path);
    mutation.lock();
    if (Find(e->number) != e) {
      s.PermitUncheckedError();
      return Status::OK();
    }
    if (!s.ok()) return s;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (desired_.count(e->number)) return s;
    }
  }
  // Open the replacement before committing the map; a route is never switched
  // to a missing or inaccessible object.
  bool readers;
  {
    std::lock_guard<std::mutex> lock(e->mutex);
    readers = e->readers != 0;
  }
  if (readers) s = OpenHandle(e, intent.hot, &replacement);
  if (s.ok()) s = SavePlacement(e->number, intent.hot ? 1 : 0);
  if (!s.ok()) {
    if (intent.hot) abandon();
    return s;
  }
  std::shared_ptr<Handle> old;
  {
    std::lock_guard<std::mutex> lock(e->mutex);
    old = std::move(e->handle);
    if (e->readers) e->handle = std::move(replacement);
    e->hot = intent.hot;
    e->changed = clock_();
    e->in_rounds = e->out_rounds = 0;
  }
  TEST_SYNC_POINT("MetaBypassSst::RouteSwitched");
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (intent.hot) {
      stats_.reserved_bytes -= e->size;
      ++stats_.promotions;
      stats_.promoted_bytes += e->size;
    } else {
      ++stats_.demotions;
      stats_.demoted_bytes += e->size;
      stats_.pending_delete_bytes += e->size;
      retired_.push_back({std::move(old), local, e->size});
    }
  }
  return Status::OK();
}
void SstStorage::Reap() {
  {
    std::lock_guard<std::mutex> mutation(mutation_);
    std::map<uint64_t, uint64_t> orphaned;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      orphaned = orphaned_;
    }
    if (!orphaned.empty()) {
      // A failed map fsync/rename may have committed either route. Re-publish
      // the currently selected routes before deleting any installed SSD copy.
      Status s = SavePlacement();
      if (s.ok()) {
        for (const auto& item : orphaned) {
          s = RemoveIfExists(target(), Local(item.first));
          if (s.ok())
            s = RemoveIfExists(target(), Local(item.first) + ".sst-tier-tmp");
          std::lock_guard<std::mutex> lock(mutex_);
          if (s.ok()) {
            stats_.reserved_bytes -= item.second;
            orphaned_.erase(item.first);
          } else {
            ++stats_.migration_errors;
            stats_.error = s.ToString();
          }
        }
      } else {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.migration_errors;
        stats_.error = s.ToString();
      }
    }
  }
  std::vector<Retired> ready;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = retired_.begin(); it != retired_.end();) {
      if (it->handle && it->handle.use_count() != 1) {
        ++it;
        continue;
      }
      ready.push_back(std::move(*it));
      it = retired_.erase(it);
    }
  }
  for (auto& retired : ready) {
    retired.handle.reset();
    Status s;
    if (!retired.path.empty()) s = RemoveIfExists(target(), retired.path);
    TEST_SYNC_POINT_CALLBACK("MetaBypassSst::LocalDeleted", &s);
    std::lock_guard<std::mutex> lock(mutex_);
    if (!s.ok() && !s.IsNotFound()) {
      ++stats_.migration_errors;
      stats_.error = s.ToString();
      retired_.push_back(std::move(retired));
    } else {
      stats_.pending_delete_bytes -= retired.size;
    }
  }
}
void SstStorage::Tick() {
  std::vector<SstFileState> files;
  std::set<uint64_t> live;
  uint64_t charged = 0;
  {
    std::lock_guard<std::mutex> mutation(mutation_);
    Status s = Discover();
    if (!s.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      ++stats_.migration_errors;
      stats_.error = s.ToString();
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.hdd_bytes = stats_.protected_bytes = stats_.unprotected_bytes = 0;
    for (const auto& item : entries_) {
      const auto& e = item.second;
      std::lock_guard<std::mutex> entry(e->mutex);
      live.insert(e->number);
      files.push_back({e->number, e->size, e->changed, e->hot,
                       !e->object.empty(), 0, e->in_rounds, e->out_rounds});
      if (e->hot) charged += e->size;
      if (!e->object.empty()) {
        stats_.hdd_bytes += e->size;
        stats_.protected_bytes += e->size;
      } else
        stats_.unprotected_bytes += e->size;
    }
    charged += stats_.reserved_bytes + stats_.pending_delete_bytes;
    stats_.ssd_bytes = charged;
    stats_.peak_ssd_bytes = std::max(stats_.peak_ssd_bytes, charged);
    if (charged > options_.ssd_capacity_bytes)
      stats_.over_budget_micros += options_.interval_ms * 1000;
  }
  auto heat = sampler_.HeatSnapshot(live);
  for (auto& file : files) file.heat = heat[file.number];
  auto plan = PlanSstPlacement(options_, files, charged, clock_());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.oversized_files = plan.oversized;
    stats_.cutoff_score = 0;
    for (const auto& file : files) {
      auto it = entries_.find(file.number);
      if (it == entries_.end()) continue;
      std::lock_guard<std::mutex> entry(it->second->mutex);
      auto& e = *it->second;
      if (e.hot != file.hot || e.changed != file.changed_micros) continue;
      if (plan.target.count(file.number)) {
        e.in_rounds = std::min(e.in_rounds, options_.promote_rounds - 1) + 1;
        e.out_rounds = 0;
        const double score = file.heat / std::max<uint64_t>(1, file.size);
        if (!stats_.cutoff_score || score < stats_.cutoff_score)
          stats_.cutoff_score = score;
      } else {
        e.out_rounds = std::min(e.out_rounds, options_.demote_rounds - 1) + 1;
        e.in_rounds = 0;
      }
    }
    // Rebuild rather than append: intents deduplicate by file number and
    // superseded decisions cannot survive into the next policy window.
    queue_.clear();
    desired_ = plan.target;
    for (const auto& intent : plan.migrations) {
      if (options_.mode == SstTieringMode::kObserveOnly) {
        if (intent.hot)
          ++stats_.observed_promotions;
        else
          ++stats_.observed_demotions;
      } else if (intent.number != migrating_ &&
                 queue_.size() < options_.migration_queue_capacity) {
        queue_.push_back(intent);
      }
    }
    stats_.queued_migrations = queue_.size();
  }
  wake_.notify_all();
  TEST_SYNC_POINT("MetaBypassSst::TickComplete");
}
void SstStorage::Execute() {
  while (true) {
    SstMigrationIntent intent;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      wake_.wait_for(lock, std::chrono::milliseconds(options_.interval_ms),
                     [&] { return stopping_ || !queue_.empty(); });
      if (stopping_) break;
      if (queue_.empty()) {
        lock.unlock();
        Reap();
        continue;
      }
      intent = queue_.front();
      queue_.pop_front();
      migrating_ = intent.number;
      stats_.queued_migrations = queue_.size();
    }
    Status s = Migrate(intent);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      migrating_ = 0;
      if (!s.ok() && !s.IsAborted() && !s.IsShutdownInProgress()) {
        ++stats_.migration_errors;
        stats_.error = s.ToString();
        queue_.clear();
        stats_.queued_migrations = 0;
      }
    }
    if (s.IsCorruption()) {
      std::function<void(const Status&)> failure;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        failure = failure_;
      }
      if (failure) failure(s);
    }
    Reap();
    TEST_SYNC_POINT("MetaBypassSst::MigrationComplete");
  }
  Reap();
}
void SstStorage::Run() {
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      if (wake_.wait_for(lock, std::chrono::milliseconds(options_.interval_ms),
                         [&] { return stopping_; }))
        break;
    }
    Tick();
  }
}
void SstStorage::SetFailureHandler(std::function<void(const Status&)> handler) {
  std::lock_guard<std::mutex> lock(mutex_);
  failure_ = std::move(handler);
}
void SstStorage::Start() {
  executor_ = std::thread(&SstStorage::Execute, this);
  worker_ = std::thread(&SstStorage::Run, this);
}
void SstStorage::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    wake_.notify_all();
  }
  if (worker_.joinable()) worker_.join();
  if (executor_.joinable()) executor_.join();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.clear();
    desired_.clear();
    stats_.queued_migrations = 0;
  }
  Reap();
}
void SstStorage::AddStats(MetaBypassStats* stats) const {
  std::lock_guard<std::mutex> lock(mutex_);
  stats->sst_tiering = stats_;
  sampler_.AddStats(&stats->sst_tiering);
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
