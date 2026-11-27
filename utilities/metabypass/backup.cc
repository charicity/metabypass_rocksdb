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

#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/metabypass/sst_storage.h"
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
constexpr size_t kMaxWalBatchEvents = 256;
constexpr size_t kSstCopyQuantum = 1024 * 1024;
constexpr size_t kSstCopyBufferSize = 64 * 1024;
constexpr size_t kMaxStagedSsts = 1024;
constexpr uint64_t kMaxStagedSstBytes = uint64_t{2} * 1024 * 1024 * 1024;

uint64_t Now() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
IOStatus AsIO(const Status& s) {
  return s.ok() ? IOStatus::OK() : IOStatus::IOError(s.ToString());
}
bool IsCheckpointFile(const std::string& name, FileType* type) {
  if (name.find_first_of("/\\ \t\r\n\v\f") != std::string::npos ||
      name.find('\0') != std::string::npos)
    return false;
  uint64_t number;
  WalFileType wal_type;
  if (!ParseFileName(name, &number, type, &wal_type)) return false;
  switch (*type) {
    case kCurrentFile:
    case kDescriptorFile:
    case kIdentityFile:
    case kTableFile:
      return true;
    case kOptionsFile:
      return name.find_first_not_of("0123456789",
                                    kOptionsFileNamePrefix.size()) ==
             std::string::npos;
    case kWalFile:
      return wal_type == kAliveLogFile;
    default:
      return false;
  }
}
bool ReadDecimal(std::istream& input, uint64_t* value) {
  std::string token;
  if (!(input >> token)) return false;
  Slice digits(token);
  return ConsumeDecimalNumber(&digits, value) && digits.empty();
}
Status ReadRecoveryBoundary(FileSystem* fs, const std::string& point,
                            Backup::RecoveryPoint* recovery) {
  std::string metadata;
  Status s = Read(fs, point + "/RECOVERY", &metadata);
  if (s.IsNotFound() || s.IsPathNotFound())
    return Status::Corruption("missing checkpoint recovery boundary", point);
  if (!s.ok()) return s;

  std::istringstream fields(metadata);
  std::string extra;
  if (!ReadDecimal(fields, &recovery->sequence) ||
      !ReadDecimal(fields, &recovery->wal_floor) || (fields >> extra)) {
    return Status::Corruption("invalid checkpoint recovery boundary", point);
  }
  return Status::OK();
}
Status ReadPublishedPoint(FileSystem* fs, const std::string& backup,
                          const std::string& latest,
                          Backup::RecoveryPoint* recovery) {
  Status s = ParseLatestPoint(latest, &recovery->name);
  if (!s.ok()) return s;
  const std::string point = backup + "/" + recovery->name;
  s = Read(fs, backup + "/METABYPASS-IDENTITY", &recovery->identity);
  if (!s.ok()) return s;
  std::string identity;
  s = Read(fs, point + "/IDENTITY", &identity);
  if (!s.ok()) return s;
  if (identity != recovery->identity)
    return Status::Corruption("backup identity mismatch");
  std::string inventory;
  s = Read(fs, point + "/INVENTORY", &inventory);
  if (!s.ok()) return s;
  if (inventory.compare(0, 5, "MBI7\n") != 0)
    return Status::NotSupported("Metabypass inventory format");
  s = ReadRecoveryBoundary(fs, point, recovery);
  if (!s.ok()) return s;
  std::istringstream entries(inventory.substr(5));
  std::string line;
  while (std::getline(entries, line)) {
    std::istringstream row(line);
    std::string file, extra;
    uint64_t length;
    FileType type;
    if (!(row >> file) || !ReadDecimal(row, &length) || (row >> extra) ||
        !IsCheckpointFile(file, &type) ||
        !recovery->files.emplace(file, length).second)
      return Status::Corruption("invalid checkpoint inventory entry", line);
  }
  if (recovery->files.empty())
    return Status::Corruption("empty checkpoint inventory");
  for (const auto& file : recovery->files) {
    const std::string path = point + "/" + file.first;
    uint64_t size;
    s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size != file.second)
      return Status::Corruption("published file size", path);
  }
  return Status::OK();
}
Status ExtendWal(FileSystem* fs, const std::string& source,
                 const std::string& target, uint64_t size) {
  uint64_t prefix = 0;
  Status s = fs->FileExists(target, IOOptions(), nullptr);
  if (s.ok()) {
    s = fs->GetFileSize(target, IOOptions(), &prefix, nullptr);
    if (!s.ok()) return s;
  } else if (!s.IsNotFound() && !s.IsPathNotFound())
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

struct Backup::StagedSst {
  StagedSst(uint64_t file_number, uint64_t file_size, std::string file_source,
            std::string temp_path, bool ordinary_task, uint64_t not_before)
      : number(file_number),
        size(file_size),
        source(std::move(file_source)),
        path(std::move(temp_path)),
        eligible_at(not_before),
        ordinary(ordinary_task) {}
  ~StagedSst() { status.PermitUncheckedError(); }

  const uint64_t number, size;
  const std::string source;
  std::string path;
  const uint64_t eligible_at;
  const bool ordinary;
  // Only the worker uses these handles. It may pause one ordinary file while
  // a candidate requests another file.
  std::unique_ptr<FSSequentialFile> reader;
  std::unique_ptr<FSWritableFile> writer;
  uint64_t copied = 0, ordinary_bytes = 0, candidate_bytes = 0;
  bool linked = false, temp_created = false;
  // Completion is transferred to a candidate under Backup::mutex_.
  bool done = false;
  uint64_t ready_epoch = 0;
  Status status;
};

struct Backup::SstStaging {
  // Scheduler state is protected by Backup::mutex_. Database and filesystem
  // calls are made with the mutex released.
  std::condition_variable cv;
  std::thread worker;
  std::map<uint64_t, std::unique_ptr<StagedSst>> jobs;
  std::map<uint64_t, uint64_t> output_hints;  // First observed eligible time.
  std::unordered_map<uint64_t, int> published_tables;
  uint64_t revision = 0, reconciled_revision = 0;
  uint64_t capture_epoch = 0;
  uint64_t next_temp_id = 0;
  StagedSst* ordinary_in_progress = nullptr;
  StagedSst* urgent = nullptr;
  bool urgent_barrier = false;
  bool ordinary_paused = false;
  bool rescan_requested = false;
  bool mirror_busy = false, borrowing_db = false, detaching = false;

  std::string NewTempPath(const std::string& work, uint64_t number) {
    return work + "/sst-async-" + std::to_string(number) + "-" +
           std::to_string(++next_temp_id) + ".tmp";
  }
};

void WriteGate::Admit() {
  uint64_t state = state_.load(std::memory_order_relaxed);
  for (;;) {
    if (state & kClosed) {
      TEST_SYNC_POINT("MetaBypass::WriteGateAdmissionBlocked");
      Wait(state, std::memory_order_relaxed);
      state = state_.load(std::memory_order_relaxed);
      continue;
    }
    assert(state < kClosed - 1);
    if (state_.compare_exchange_weak(state, state + 1,
                                     std::memory_order_acquire,
                                     std::memory_order_relaxed)) {
      TEST_SYNC_POINT("MetaBypass::WriteGateAdmitted");
      return;
    }
  }
}

void WriteGate::Leave() {
  if (state_.fetch_sub(1, std::memory_order_release) == kClosed + 1) Notify();
}

void WriteGate::CloseAndDrain() {
  uint64_t state = state_.fetch_or(kClosed, std::memory_order_acquire);
  assert(!(state & kClosed));
  state |= kClosed;
  TEST_SYNC_POINT("MetaBypass::WriteGateClosed");
  while (state != kClosed) {
    Wait(state, std::memory_order_acquire);
    state = state_.load(std::memory_order_acquire);
  }
}

void WriteGate::Reopen() {
  assert(state_.load(std::memory_order_relaxed) == kClosed);
  state_.store(0, std::memory_order_release);
  Notify();
  TEST_SYNC_POINT("MetaBypass::WriteGateReopened");
}

void WriteGate::Wait(uint64_t observed, std::memory_order order) {
#if ROCKSDB_MB_WRITE_GATE_ATOMIC_WAIT
  state_.wait(observed, order);
#else
  std::unique_lock<std::mutex> lock(wait_mutex_);
  wait_cv_.wait(lock, [&] { return state_.load(order) != observed; });
#endif
}

void WriteGate::Notify() {
#if ROCKSDB_MB_WRITE_GATE_ATOMIC_WAIT
  state_.notify_all();
#else
  // Synchronize with the predicate check and transition to waiting.
  std::lock_guard<std::mutex> lock(wait_mutex_);
  wait_cv_.notify_all();
#endif
}

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
    const bool wal = backup_->IsWalChannel(channel_);
#ifdef ROCKSDB_MB_WAL_BREAKDOWN
    wal_probe::MirrorAppend append(wal_probe::active && wal);
#endif
    MB_WAL_SPAN(build, event_build, wal_probe::mirror_append);
    Backup::Event e{Backup::Kind::kAppend, name_, {}};
    e.charge = sizeof(Backup::ChargedEventLayout) + name_.size() + bytes.size();
    MB_WAL_END(build);
    MB_WAL_SPAN(wait, operations_wait, wal_probe::mirror_append);
    std::unique_lock<std::mutex> serial(channel_.operations);
    MB_WAL_END(wait);
    Status s = backup_->Reserve(channel_, e.charge);
    if (!s.ok()) return AsIO(s);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::PrimaryReserved", &e.name);
    MB_WAL_SPAN(copy, payload_copy, wal_probe::mirror_append);
    if (wal && bytes.size() >= 32 && bytes.size() <= 1024) {
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
    if (wal) {
      const bool wake = backup_->FinishWal(std::move(e), primary);
      serial.unlock();
      if (wake) backup_->SignalMirror();
    } else {
      backup_->Finish(channel_, std::move(e), primary);
    }
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
Backup::WalQueue::WalQueue()
    : tail(nullptr), head(std::make_unique<Segment>()) {
  tail = head.get();
}

void Backup::WalQueue::Prepare() {
  if (tail_slot == kSlots) {
    tail->next = std::make_unique<Segment>();
    tail = tail->next.get();
    tail_slot = 0;
  }
}

bool Backup::WalQueue::Publish(Event&& event) {
  auto& slot = tail->events[tail_slot];
  slot.emplace(std::move(event));
  uint64_t expected = tail_count;
  // Also pairs with the metadata wake threshold check. A failed CAS can
  // only mean that shutdown or failure sealed the queue.
  if (!published.compare_exchange_strong(expected, tail_count + 1)) {
    assert(expected & kClosed);
    slot.reset();
    return false;
  }
  ++tail_count;
  ++tail_slot;
  return true;
}

const Backup::Event& Backup::WalQueue::Front() const {
  return head_slot == kSlots ? *head->next->events[0]
                             : *head->events[head_slot];
}

Backup::Event Backup::WalQueue::Pop() {
  if (head_slot == kSlots) {
    auto next = std::move(head->next);
    head = std::move(next);
    head_slot = 0;
  }
  auto& slot = head->events[head_slot++];
  Event event = std::move(*slot);
  slot.reset();
  return event;
}

uint64_t Backup::WalQueue::ChargedBytes() const {
  uint64_t returned_before;
  uint64_t reserved_snapshot;
  // Refunds run outside the queue mutex. Bracket the reservation read with
  // the same returned total to avoid mixing counts from different batches.
  do {
    returned_before = returned.load(std::memory_order_acquire);
    reserved_snapshot = reserved.load(std::memory_order_acquire);
  } while (returned_before != returned.load(std::memory_order_acquire));
  return reserved_snapshot - returned_before;
}

Backup::Backup(std::shared_ptr<FileSystem> fs, std::string index,
               MetaBypassOptions options, std::shared_ptr<SstStorage> sst)
    : FileSystemWrapper(sst ? std::shared_ptr<FileSystem>(sst) : fs),
      fs_(std::move(fs)),
      sst_(sst.get()),
      disk_(fs_.get()),
      index_(std::move(index)),
      options_(std::move(options)),
      work_(options_.backup_dir + "/work"),
      sst_staging_(options_.sst_async_min_level == -1
                       ? nullptr
                       : std::make_unique<SstStaging>()),
      wal_wake_charge_(options_.batch_bytes) {}
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
  assert(!lock_);
  Status s = EnsureDir(disk_, options_.backup_dir);
  if (s.ok())
    s = disk_->LockFile(options_.backup_dir + "/LOCK", IOOptions(), &lock_,
                        nullptr);
  return s;
}
Status Backup::LoadLatest() {
  assert(lock_);
  std::string pointer;
  Status s = Read(disk_, options_.backup_dir + "/LATEST", &pointer);
  if (s.IsNotFound() || s.IsPathNotFound()) return Status::OK();
  if (!s.ok()) return s;
  RecoveryPoint recovery;
  s = ReadPublishedPoint(disk_, options_.backup_dir, pointer, &recovery);
  if (!s.ok()) return s;
  Point point{std::move(recovery.name), 0, recovery.wal_floor};
  std::vector<FileAttributes> attributes;
  s = disk_->GetChildrenFileAttributes(options_.backup_dir + "/" + point.name,
                                       IOOptions(), &attributes, nullptr);
  if (!s.ok()) return s;
  for (const auto& attribute : attributes) point.bytes += attribute.size_bytes;
  published_point_ = std::move(point);
  return Status::OK();
}
Status Backup::Start(bool existing) {
  assert(lock_);
  std::vector<std::string> files;
  Status s =
      disk_->GetChildren(options_.backup_dir, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  // Generation names are monotonic even after interrupted publication.
  for (const auto& f : files) {
    if (f.compare(0, 6, "point-") != 0) continue;
    Slice digits(f.data() + 6, f.size() - 6);
    uint64_t n;
    if (!digits.empty() && digits[0] != '0' &&
        ConsumeDecimalNumber(&digits, &n) && digits.empty())
      generation_ = std::max(generation_, n);
  }
  for (const auto& f : files) {
    if (f.compare(0, 6, "point-") != 0) continue;
    if (published_point_ && published_point_->name == f) continue;
    s = RemoveDir(disk_, options_.backup_dir + "/" + f);
    if (!s.ok()) return s;
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
  if (sst_staging_)
    sst_staging_->worker = std::thread(&Backup::StageSstLoop, this);
  if (options_.enable_recovery_time_sampling)
    sampler_ = std::thread(&Backup::SampleLoop, this);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::SamplerStarted", &sampler_);
  return Status::OK();
}
Status Backup::Activate(const std::string& identity, DB* db,
                        std::shared_ptr<SystemClock> clock) {
  const std::string path = options_.backup_dir + "/METABYPASS-IDENTITY";
  std::string owner;
  Status s = Read(disk_, path, &owner);
  if (s.IsNotFound() || s.IsPathNotFound()) {
    s = Write(disk_, path, identity);
    if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
  } else if (s.ok() && owner != identity) {
    return Status::Corruption("Metabypass directory identity mismatch");
  }
  if (!s.ok()) return s;
  std::lock_guard<std::mutex> lock(mutex_);
  db_ = db;
  clock_ = std::move(clock);
  identity_ = identity;
  active_ = true;
  if (sst_staging_) sst_staging_->cv.notify_all();
  validator_cv_.notify_one();
  WakeMirror();
  return Status::OK();
}
bool Backup::IsWalChannel(const Channel& channel) const {
  return &channel == &wal_channel_;
}

Status Backup::ReserveWal(size_t charge) {
  MB_WAL_SPAN(bookkeeping, reserve_bookkeeping, wal_probe::mirror_append);
  auto& queue = wal_queue_;
  const size_t capacity = options_.wal_queue_capacity;
  if (charge > capacity)
    return Status::InvalidArgument(
        "file operation exceeds Metabypass WAL queue capacity");
  auto closed = [&] {
    return queue.published.load(std::memory_order_relaxed) & WalQueue::kClosed;
  };
  auto has_space = [&] {
    if (queue.reserved_total - queue.cached_returned <= capacity - charge)
      return true;
    queue.cached_returned = queue.returned.load(std::memory_order_acquire);
    return queue.reserved_total - queue.cached_returned <= capacity - charge;
  };
  if (!closed() && !has_space()) {
    const uint64_t start = Now();
    std::unique_lock<std::mutex> lock(queue.space_mutex);
    queue.space_return_target = queue.reserved_total - (capacity - charge);
    queue.waiting_for_space.store(true);
    SignalMirror();
    TEST_SYNC_POINT("MetaBypass::Backpressure");
    MB_WAL_SPAN(capacity_wait_span, capacity_wait, wal_probe::mirror_append);
    queue.space.wait(lock, [&] { return closed() || has_space(); });
    MB_WAL_END(capacity_wait_span);
    queue.waiting_for_space.store(false);
    queue.backpressure_micros.fetch_add(Now() - start,
                                        std::memory_order_relaxed);
  }
  if (closed()) {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_.error.ok() ? Status::ShutdownInProgress() : stats_.error;
  }
  queue.Prepare();
  const uint64_t peak = queue.peak.load(std::memory_order_relaxed);
  if (queue.reserved_total - queue.cached_returned + charge > peak) {
    queue.cached_returned = queue.returned.load(std::memory_order_acquire);
  }
  queue.reserved_total += charge;
  const uint64_t used = queue.reserved_total - queue.cached_returned;
  if (used > peak) queue.peak.store(used, std::memory_order_relaxed);
  // Stats acquires the reservation before reading the corresponding peak.
  queue.reserved.store(queue.reserved_total, std::memory_order_release);
  return Status::OK();
}

void Backup::CloseWalQueue() {
  bool was_closed;
  {
    std::lock_guard<std::mutex> lock(wal_queue_.space_mutex);
    was_closed =
        wal_queue_.published.fetch_or(WalQueue::kClosed) & WalQueue::kClosed;
    wal_queue_.space.notify_one();
  }
  if (!was_closed) {
    TEST_SYNC_POINT("MetaBypass::WalQueueSealed");
  }
}

void Backup::ReturnWalBytes(uint64_t charge, uint64_t mirrored_bytes) {
  auto& queue = wal_queue_;
  std::lock_guard<std::mutex> lock(queue.space_mutex);
  const uint64_t returned = queue.returned.load(std::memory_order_relaxed);
  if (mirrored_bytes != 0) {
    queue.mirrored_bytes.store(
        queue.mirrored_bytes.load(std::memory_order_relaxed) + mirrored_bytes,
        std::memory_order_relaxed);
  }
  // Stats acquires returned before loading the mirrored byte count.
  queue.returned.store(returned + charge, std::memory_order_release);
  if (queue.waiting_for_space.load(std::memory_order_relaxed)) {
    // Compare the bytes returned by this batch with the unsigned distance to
    // the target, including when the cumulative counters wrap.
    const uint64_t needed = queue.space_return_target - returned;
    if (needed != 0 && needed <= charge) queue.space.notify_one();
  }
}

Status Backup::Reserve(Channel& channel, size_t charge) {
  if (IsWalChannel(channel)) return ReserveWal(charge);
  std::unique_lock<std::mutex> lock(mutex_);
  const size_t capacity = options_.metadata_queue_capacity;
  if (charge > capacity)
    return Status::InvalidArgument(
        "file operation exceeds Metabypass metadata queue capacity");
  const bool blocked = stats_.queued_bytes > capacity - charge;
  if (blocked) {
    const uint64_t start = Now();
    assert(metadata_channel_.waiting_charge == 0);
    metadata_channel_.waiting_charge = charge;
    // A reservation can run out of space before reaching batch_bytes.
    // Drain immediately rather than waiting for the batching timer.
    WakeMirror();
    TEST_SYNC_POINT("MetaBypass::Backpressure");
    metadata_channel_.capacity.wait(lock, [&] {
      return stopping_ || !stats_.error.ok() ||
             stats_.queued_bytes <= capacity - charge;
    });
    metadata_channel_.waiting_charge = 0;
    stats_.backpressure_micros += Now() - start;
  }
  if (!stats_.error.ok()) return stats_.error;
  if (stopping_) return Status::ShutdownInProgress();
  stats_.queued_bytes += charge;
  stats_.peak_queued_bytes =
      std::max(stats_.peak_queued_bytes, stats_.queued_bytes);
  UpdateWalWakeCharge();
  WakeMirror();
  return Status::OK();
}
void Backup::Fail(const Status& status) {
  std::lock_guard<std::mutex> publish(publication_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  if (stats_.error.ok()) {
    stats_.error = status;
    CloseWalQueue();
    failed_.store(true, std::memory_order_release);
  }
  WakeMirror();
  validator_cv_.notify_one();
  gc_cv_.notify_all();
  if (sst_staging_) sst_staging_->cv.notify_all();
  WakeMetadataCapacity();
  sync_cv_.notify_all();
}
bool Backup::FinishWal(Event&& e, const Status& primary) {
  if (!primary.ok()) Fail(primary);
  MB_WAL_SPAN(bookkeeping, finish_bookkeeping, wal_probe::mirror_append);
  auto& queue = wal_queue_;
  const auto charge = e.charge;
  if (!primary.ok() || !queue.Publish(std::move(e))) {
    queue.reserved_total -= charge;
    queue.reserved.store(queue.reserved_total, std::memory_order_release);
    return false;
  }
  const uint64_t threshold = wal_wake_charge_.load();
  if (queue.reserved_total - queue.cached_returned < threshold) return false;
  queue.cached_returned = queue.returned.load(std::memory_order_acquire);
  return queue.reserved_total - queue.cached_returned >= threshold;
}

void Backup::Finish(Channel& channel, Event&& e, const Status& primary) {
  if (IsWalChannel(channel)) {
    if (FinishWal(std::move(e), primary)) SignalMirror();
    return;
  }
  if (!primary.ok()) Fail(primary);
  std::lock_guard<std::mutex> lock(mutex_);
  if (primary.ok() && stats_.error.ok() && !stopping_) {
    ++accepted_;
    // The published WAL tail is the ordering point for this metadata event.
    e.seq = Accepted();
    TEST_SYNC_POINT("MetaBypass::MetadataSequenceAssigned");
    metadata_channel_.queue.push_back(std::move(e));
  } else {
    // Failure cleanup owns queued events, but a producer still owns its
    // reservation until Finish. Do not enqueue after either worker has failed.
    assert(stats_.queued_bytes >= e.charge);
    stats_.queued_bytes -= e.charge;
    UpdateWalWakeCharge();
    WakeMetadataCapacity();
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
  if (!Tracked(p)) return target()->DeleteFile(p, o, d);
  Channel& channel = ChannelFor(Base(p));
  std::lock_guard<std::mutex> serial(channel.operations);
  Event e{Kind::kDelete, Base(p), {}};
  e.charge = sizeof(Backup::ChargedEventLayout) + e.name.size();
  Status s = Reserve(channel, e.charge);
  if (!s.ok()) return AsIO(s);
  IOStatus primary = target()->DeleteFile(p, o, d);
  // Mirror the absence left by an earlier purge while preserving the primary
  // filesystem result for its caller.
  Status mirrored =
      primary.IsNotFound() || primary.IsPathNotFound() ? Status::OK() : primary;
  Finish(channel, std::move(e), mirrored);
  return primary;
}
IOStatus Backup::RenameFile(const std::string& a, const std::string& b,
                            const IOOptions& o, IODebugContext* d) {
  if (Tracked(a, true) != Tracked(b, true))
    return IOStatus::NotSupported("cross-boundary rename");
  const bool tracked_a = Tracked(a);
  const bool tracked_b = Tracked(b);
  if (!tracked_a && !tracked_b) return target()->RenameFile(a, b, o, d);
  if (!tracked_a || !tracked_b)
    return IOStatus::NotSupported("cross-boundary rename");
  Event e{Kind::kRename, Base(a), Base(b)};
  if (IsWalChannel(ChannelFor(e.name)) || IsWalChannel(ChannelFor(e.contents)))
    return IOStatus::NotSupported("Metabypass WAL rename");
  Channel& channel = metadata_channel_;
  std::lock_guard<std::mutex> serial(channel.operations);
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
      if (!published_point_ || number >= published_point_->wal_floor ||
          (retired_point_ && number >= retired_point_->wal_floor))
        return Status::OK();
    }
    s = disk_->DeleteFile(path, IOOptions(), nullptr);
    if (s.IsNotFound() || s.IsPathNotFound()) return Status::OK();
    return s;
  }
  return disk_->RenameFile(path, work_ + "/" + e.contents, IOOptions(),
                           nullptr);
}
Status Backup::Capture(Candidate* candidate) {
  if (generation_ == std::numeric_limits<uint64_t>::max())
    return Status::InvalidArgument(
        "Metabypass checkpoint generation exhausted");
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
    FileType type;
    if (!IsCheckpointFile(file, &type) || type != info.file_type)
      return Status::Corruption("invalid native file name", file);
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
std::unique_ptr<Backup::StagedSst> Backup::TakeStagedSst(uint64_t number) {
  auto& staging = *sst_staging_;
  auto node = staging.jobs.extract(number);
  auto table = std::move(node.mapped());
  if (staging.ordinary_in_progress == table.get())
    staging.ordinary_in_progress = nullptr;
  if (staging.urgent == table.get()) staging.urgent = nullptr;
  if (table->ordinary) stats_.sst_async_staged_bytes -= table->size;
  return table;
}

void Backup::DeleteStagedSst(StagedSst& table) {
  table.reader.reset();
  if (table.writer) {
    table.writer->Close(IOOptions(), nullptr).PermitUncheckedError();
    table.writer.reset();
  }
  if (table.temp_created) {
    Status s = disk_->DeleteFile(table.path, IOOptions(), nullptr);
    if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound()) {
      // The next Open removes orphaned work files. Bound optional staging for
      // this session while a filesystem failure prevents freeing its space.
      std::lock_guard<std::mutex> lock(mutex_);
      sst_staging_->ordinary_paused = true;
    }
    table.temp_created = false;
  }
  table.linked = false;
}

void Backup::ReconcileStagedSsts(std::unique_lock<std::mutex>& lock) {
  auto& staging = *sst_staging_;
  const uint64_t revision = staging.revision;
  auto* db = static_cast_with_check<DBImpl>(db_);
  staging.borrowing_db = true;
  lock.unlock();
  std::unordered_map<uint64_t, uint64_t> live;
  db->GetLiveTableFileSizes(options_.sst_async_min_level, &live);
  lock.lock();
  staging.borrowing_db = false;
  sync_cv_.notify_all();
  // A later native change needs a new snapshot. Leave staged files in place
  // while the validator owns a separately pinned Version.
  if (!active_ || candidate_busy_ || staging.revision != revision ||
      stopping_ || !stats_.error.ok()) {
    return;
  }
  staging.reconciled_revision = revision;
  std::vector<std::unique_ptr<StagedSst>> obsolete;
  for (auto it = staging.jobs.begin(); it != staging.jobs.end();) {
    const uint64_t number = (it++)->first;
    const bool still_eligible = live.count(number) != 0;
    if (still_eligible && !staging.published_tables.count(number)) continue;
    auto table = TakeStagedSst(number);
    stats_.sst_async_discarded_bytes += table->ordinary_bytes;
    if (!still_eligible && !table->temp_created)
      stats_.sst_async_cancelled_bytes += table->size;
    obsolete.push_back(std::move(table));
  }
  auto enqueue = [&](uint64_t number, uint64_t size, uint64_t observed) {
    if (staging.ordinary_paused || staging.jobs.size() >= kMaxStagedSsts ||
        size > kMaxStagedSstBytes - stats_.sst_async_staged_bytes ||
        staging.jobs.count(number) || staging.published_tables.count(number)) {
      return;
    }
    staging.jobs.emplace(
        number, std::make_unique<StagedSst>(
                    number, size, index_ + "/" + MakeTableFileName(number),
                    staging.NewTempPath(work_, number),
                    true, observed + options_.sst_async_delay_ms * 1000));
    stats_.sst_async_staged_bytes += size;
  };
  for (const auto& [number, observed] : staging.output_hints) {
    auto it = live.find(number);
    if (it != live.end()) enqueue(number, it->second, observed);
  }
  staging.output_hints.clear();
  if (staging.rescan_requested) {
    // An explicit refit reports no per-file install time. Start the delay
    // from the first live observation of its newly eligible files.
    const uint64_t observed = Now();
    for (const auto& [number, size] : live) {
      if (staging.jobs.size() >= kMaxStagedSsts) break;
      enqueue(number, size, observed);
    }
    staging.rescan_requested = false;
  }
  lock.unlock();
  for (const auto& table : obsolete) {
    DeleteStagedSst(*table);
#ifndef NDEBUG
    uint64_t number = table->number;
    TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncObsolete", &number);
#endif
  }
  lock.lock();
}

Status Backup::CopySstQuantum(StagedSst& table, std::string* scratch,
                              uint64_t* appended, bool* ready) {
  const IOOptions io;
  Status s;
  if (!table.reader && !table.linked) {
#ifndef NDEBUG
    uint64_t number = table.number;
    TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncBeforeCopy", &number);
#endif
    uint64_t size;
    s = target()->GetFileSize(table.source, io, &size, nullptr);
    if (s.ok() && size != table.size)
      s = Status::Corruption("native checkpoint size mismatch", table.source);
    if (!s.ok()) return s;
    s = target()->LinkFile(table.source, table.path, io, nullptr);
    if (s.ok()) {
      table.temp_created = table.linked = true;
      return s;
    }
    if (!s.IsNotSupported()) return s;
    s = target()->NewSequentialFile(table.source, FileOptions(), &table.reader,
                                    nullptr);
    if (s.ok()) {
      table.temp_created = true;
      s = disk_->NewWritableFile(table.path, FileOptions(), &table.writer,
                                 nullptr);
    }
    if (!s.ok()) return s;
  }
  if (table.linked || table.copied == table.size) {
    s = table.linked
            ? disk_->SyncFile(table.path, FileOptions(), io, true, nullptr)
            : table.writer->Fsync(io, nullptr);
    TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncSyncStatus", &s);
    if (table.writer) {
      s.UpdateIfOk(table.writer->Close(io, nullptr));
      table.writer.reset();
    }
    table.reader.reset();
    uint64_t size;
    if (s.ok()) s = disk_->GetFileSize(table.path, io, &size, nullptr);
    if (s.ok() && size != table.size)
      s = Status::Corruption("staged checkpoint size mismatch", table.path);
    *ready = s.ok();
    return s;
  }
  if (scratch->empty()) scratch->resize(kSstCopyBufferSize);
  const uint64_t end =
      table.copied +
      std::min<uint64_t>(table.size - table.copied, kSstCopyQuantum);
  while (s.ok() && table.copied < end) {
    const size_t length =
        std::min<uint64_t>(end - table.copied, scratch->size());
    Slice data;
    s = table.reader->Read(length, io, &data, scratch->data(), nullptr);
    if (!s.ok()) return s;
    if (data.empty() || data.size() > length)
      return Status::Corruption("invalid staged SST read length", table.source);
    s = table.writer->Append(data, io, nullptr);
    if (s.ok()) {
      table.copied += data.size();
      *appended += data.size();
    }
    TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncAppendStatus", &s);
  }
  return s;
}

void Backup::StageSstLoop() {
  auto& staging = *sst_staging_;
  std::string scratch;
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_ && stats_.error.ok() &&
         !(staging.detaching && !candidate_busy_)) {
    StagedSst* table = staging.urgent;
    const bool urgent = table != nullptr;
    if (!urgent) {
      if (!active_ || candidate_busy_ || staging.mirror_busy || MirrorReady()) {
        staging.cv.wait(lock);
        continue;
      }
      if (staging.reconciled_revision != staging.revision) {
        ReconcileStagedSsts(lock);
        continue;
      }
      if (staging.ordinary_paused) {
        staging.cv.wait(lock);
        continue;
      }
      table = staging.ordinary_in_progress;
      if (!table) {
        for (const auto& [number, job] : staging.jobs) {
          if (!job->done && (!table || job->eligible_at < table->eligible_at))
            table = job.get();
        }
        if (!table) {
          staging.cv.wait(lock);
          continue;
        }
        uint64_t now = Now();
        TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncNow", &now);
        if (now < table->eligible_at) {
          staging.cv.wait_for(
              lock, std::chrono::microseconds(table->eligible_at - now));
          continue;
        }
        staging.ordinary_in_progress = table;
      }
    } else if (table->done) {
      staging.cv.wait(lock);
      continue;
    }

    const bool barrier = urgent && staging.urgent_barrier;
    lock.unlock();
    HddPurposeScope purpose(barrier ? HddPurpose::kBarrier : HddPurpose::kBulk);
    uint64_t appended = 0;
    bool ready = false;
    Status s = CopySstQuantum(*table, &scratch, &appended, &ready);
    lock.lock();
    if (urgent) {
      table->candidate_bytes += appended;
    } else {
      table->ordinary_bytes += appended;
      stats_.sst_async_copy_bytes += appended;
    }
    if (!s.ok() && !urgent) {
      stats_.sst_async_discarded_bytes += table->ordinary_bytes;
      if (staging.urgent == table) {
        // The optional quantum was already in flight when a candidate
        // requested this file. Retry its pinned source as required work.
        table->copied = table->ordinary_bytes = 0;
        lock.unlock();
        DeleteStagedSst(*table);
        lock.lock();
        table->path = staging.NewTempPath(work_, table->number);
      } else {
        auto discarded = TakeStagedSst(table->number);
        lock.unlock();
        DeleteStagedSst(*discarded);
        lock.lock();
      }
      continue;
    }
    if (ready || !s.ok()) {
      table->done = true;
      table->status = s;
      table->ready_epoch = staging.capture_epoch;
      if (staging.ordinary_in_progress == table)
        staging.ordinary_in_progress = nullptr;
      staging.cv.notify_all();
#ifndef NDEBUG
      if (ready) {
        uint64_t number = table->number;
        lock.unlock();
        TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncReady", &number);
        lock.lock();
      }
#endif
    }
  }
  staging.cv.notify_all();
  while (!staging.jobs.empty()) {
    auto table = TakeStagedSst(staging.jobs.begin()->first);
    stats_.sst_async_discarded_bytes += table->ordinary_bytes;
    lock.unlock();
    DeleteStagedSst(*table);
    lock.lock();
  }
}

Status Backup::CaptureStagedSst(const LiveFileStorageInfo& info,
                                const std::string& destination,
                                Candidate* candidate) {
  auto& staging = *sst_staging_;
  uint64_t number = info.file_number;
  std::unique_lock<std::mutex> lock(mutex_);
  if (!stats_.error.ok()) return stats_.error;
  if (stopping_) return Status::ShutdownInProgress();
  auto [it, inserted] = staging.jobs.try_emplace(number);
  if (inserted) {
    it->second = std::make_unique<StagedSst>(
        number, info.size, info.directory + "/" + info.relative_filename,
        staging.NewTempPath(work_, number), false, 0);
  }
  StagedSst* pending = it->second.get();
  if (pending->size != info.size)
    return Status::Corruption("staged SST metadata mismatch",
                              info.relative_filename);
  if (!pending->done) {
    const uint64_t wait_start = Now();
    staging.urgent = pending;
    staging.urgent_barrier = HddPurposeScope::Current() == HddPurpose::kBarrier;
    staging.cv.notify_all();
    lock.unlock();
    TEST_SYNC_POINT_CALLBACK("MetaBypass::SstAsyncCandidateWait", &number);
    lock.lock();
    staging.cv.wait(
        lock, [&] { return !stats_.error.ok() || stopping_ || pending->done; });
    candidate->sst_wait_micros += Now() - wait_start;
    if (!stats_.error.ok()) return stats_.error;
    if (stopping_) return Status::ShutdownInProgress();
  }
  auto table = TakeStagedSst(number);
  staging.cv.notify_all();
  lock.unlock();

  Status s = table->status;
  if (s.ok()) {
    s = disk_->RenameFile(table->path, destination, IOOptions(), nullptr);
    if (s.ok()) {
      table->temp_created = false;
      candidate->durable_tables.insert(number);
    } else if (s.IsNotSupported()) {
      s = StageCopy(disk_, table->path, destination, table->size);
      if (s.ok()) candidate->sst_copy_bytes += table->size;
    }
  }
  DeleteStagedSst(*table);
  if (s.ok()) {
    candidate->sst_copy_bytes += table->candidate_bytes;
    candidate->sst_adopted_ordinary_bytes += table->ordinary_bytes;
    if (table->ready_epoch < candidate->sst_epoch)
      candidate->sst_ready_bytes += table->size;
    std::lock_guard<std::mutex> stats_lock(mutex_);
    stats_.candidate_copied_bytes += table->candidate_bytes;
    if (!candidate->durable_tables.count(number))
      stats_.candidate_copied_bytes += table->size;
  } else {
    std::lock_guard<std::mutex> stats_lock(mutex_);
    stats_.sst_async_discarded_bytes += table->ordinary_bytes;
  }
  return s;
}

Status Backup::CaptureTables(Candidate* candidate) {
  std::string published_point;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // The sole validator cannot retire this point before Publish completes.
    if (published_point_)
      published_point = options_.backup_dir + "/" + published_point_->name;
  }
  TEST_SYNC_POINT("MetaBypass::BeforeCaptureTables");
  for (const auto& info : candidate->files) {
    if (info.file_type != kTableFile) continue;
    const auto& file = info.relative_filename;
    const std::string destination = candidate->path + "/" + file;
    if (!published_point.empty()) {
      const std::string previous = published_point + "/" + file;
      uint64_t previous_size = 0;
      Status s =
          disk_->GetFileSize(previous, IOOptions(), &previous_size, nullptr);
      if (s.ok() && previous_size == info.size) {
        s = disk_->LinkFile(previous, destination, IOOptions(), nullptr);
        if (s.ok()) {
          candidate->durable_tables.insert(info.file_number);
          continue;
        }
        if (!s.IsNotSupported()) return s;
      } else if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound()) {
        return s;
      }
    }
    if (sst_staging_) {
      const auto level = candidate->table_levels.find(info.file_number);
      if (level == candidate->table_levels.end())
        return Status::Corruption("missing SST level", file);
      if (level->second >= options_.sst_async_min_level) {
        Status s = CaptureStagedSst(info, destination, candidate);
        if (!s.ok()) return s;
        continue;
      }
    }
    // Resolve through the logical filesystem, including cold SST routing.
    // Native deletions remain pinned until every table has been captured.
    const std::string source = info.directory + "/" + file;
    uint64_t size = 0;
    Status s = target()->GetFileSize(source, IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size != info.size)
      return Status::Corruption("native checkpoint size mismatch", file);
    s = target()->LinkFile(source, destination, IOOptions(), nullptr);
    if (s.IsNotSupported()) {
      s = StageCopy(target(), source, destination, info.size);
      if (!s.ok()) return s;
      candidate->sst_copy_bytes += info.size;
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.candidate_copied_bytes += info.size;
    }
    if (!s.ok()) return s;
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateCaptured", &candidate->path);
  return Status::OK();
}
Status Backup::PruneWal(uint64_t floor) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!published_point_) return Status::OK();
    floor = std::min(floor, published_point_->wal_floor);
    if (retired_point_) floor = std::min(floor, retired_point_->wal_floor);
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
Status Backup::Publish(Candidate& candidate) {
  TEST_SYNC_POINT("MetaBypass::ValidateCandidate");
  const std::string& point = candidate.path;
  const std::string& name = candidate.name;
  Status s;
  std::map<std::string, uint64_t> files;
  for (const auto& info : candidate.files) {
    if (!files.emplace(info.relative_filename, info.size).second)
      return Status::Corruption("duplicate native checkpoint file",
                                info.relative_filename);
  }
  std::ostringstream inventory;
  inventory << "MBI7\n";
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
      const uint64_t sync_started = Now();
      s = disk_->SyncFile(point + "/" + f.first, FileOptions(), IOOptions(),
                          true, nullptr);
      candidate.sst_micros += Now() - sync_started;
    }
    TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateFileSyncStatus", &s);
    if (!s.ok()) return s;
    inventory << f.first << ' ' << f.second << '\n';
    total += f.second;
  }
  const std::string inventory_bytes = inventory.str();
  total += inventory_bytes.size();
  if (s.ok())
    s = Write(disk_, point + "/RECOVERY",
              std::to_string(candidate.native_sequence) + " " +
                  std::to_string(candidate.wal_floor) + "\n");
  if (s.ok()) s = Write(disk_, point + "/INVENTORY", inventory_bytes);
  if (s.ok()) s = SyncDir(disk_, point);
  // Persist the candidate directory entry before LATEST can reference it.
  if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
  TEST_SYNC_POINT_CALLBACK("MetaBypass::CandidateSynced", &s);
  if (!s.ok()) return s;
  s = Write(disk_, options_.backup_dir + "/LATEST.tmp", name + "\n");
  TEST_SYNC_POINT_CALLBACK("MetaBypass::BeforePointerReplace", &s);
  if (!s.ok()) return s;
  TEST_SYNC_POINT("MetaBypass::BeforeGCSlotWait");
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // A retired directory occupies the only GC slot until deletion ends.
    // Closing still needs the final publication, so stopping_ cannot cancel
    // this wait. Fail() wakes it when cleanup or mirroring fails.
    gc_cv_.wait(lock, [&] { return !retired_point_ || !stats_.error.ok(); });
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
      Point published{name, total, candidate.wal_floor};
      assert(!retired_point_);
      retired_point_ = std::move(published_point_);
      published_point_ = std::move(published);
      if (retired_point_) {
        stats_.pending_gc_points = 1;
        stats_.pending_gc_bytes = retired_point_->bytes;
        gc_cv_.notify_one();
      }
      published_ = candidate.seq;
      published_writes_ = candidate.writes;
      published_request_ = candidate.request;
      published_native_changes_ = candidate.native_changes;
      if (sst_staging_) {
        sst_staging_->published_tables.swap(candidate.table_levels);
        if (!sst_staging_->jobs.empty()) ++sst_staging_->revision;
        candidate.sst_adopted_ordinary_bytes = 0;
      }
      ++stats_.recovery_points;
      stats_.last_checkpoint_sst_copy_bytes = candidate.sst_copy_bytes;
      stats_.last_checkpoint_sst_ready_bytes = candidate.sst_ready_bytes;
      stats_.last_checkpoint_sst_wait_micros = candidate.sst_wait_micros;
      stats_.last_checkpoint_sst_micros = candidate.sst_micros;
      stats_.last_build_micros = Now() - candidate.started;
      stats_.last_publish_micros = Now();
      stats_.last_point_lag_micros = Now() - candidate.oldest;
      stats_.retained_index_bytes =
          candidate.work_bytes + published_point_->bytes;
      sync_cv_.notify_all();
    }
  }
  // Only a fully verified and durably published point may authorize eviction.
  if (sst_) {
    s = sst_->Protect(point, candidate.files);
    if (!s.ok()) return s;
  }
  TEST_SYNC_POINT("MetaBypass::PointerSynced");
  return Status::OK();
}
bool Backup::MirrorReady() const {
  return stopping_ || prune_wal_ || !stats_.error.ok() ||
         (candidate_ && !capture_done_) ||
         (!QueuesEmpty() &&
          (wal_queue_.waiting_for_space.load() ||
           metadata_channel_.waiting_charge != 0 || requested_ > applied_ ||
           QueuedBytes() >= options_.batch_bytes));
}

uint64_t Backup::Accepted() const { return accepted_ + wal_queue_.Published(); }

uint64_t Backup::QueuedBytes() const {
  return stats_.queued_bytes + wal_queue_.ChargedBytes();
}

void Backup::WaitForMirror(std::unique_lock<std::mutex>& lock) {
  const auto interval = std::chrono::milliseconds(options_.interval_ms);
  const auto deadline = std::chrono::steady_clock::now() + interval;
  for (;;) {
    // Consume any previous signal before testing the current work. A signal
    // sent after this check stays pending until the next iteration.
    mirror_signal_.exchange(false);
    if (MirrorReady()) return;
    TEST_SYNC_POINT("MetaBypass::BeforeSpscMirrorSleep");
    lock.unlock();
    bool signaled;
    {
      std::unique_lock<std::mutex> sleep_lock(mirror_sleep_mutex_);
      signaled = mirror_cv_.wait_until(sleep_lock, deadline,
                                       [&] { return mirror_signal_.load(); });
    }
    lock.lock();
    if (!signaled) return;
  }
}

void Backup::SignalMirror() {
  // Work is made visible before checking the bit. If a signal was already
  // pending, the mirror clears it before its next readiness check.
  if (mirror_signal_.load() || mirror_signal_.exchange(true)) return;
  std::lock_guard<std::mutex> lock(mirror_sleep_mutex_);
  TEST_SYNC_POINT("MetaBypass::MirrorNotified");
  mirror_cv_.notify_one();
}

void Backup::UpdateWalWakeCharge() {
  wal_wake_charge_.store(
      options_.batch_bytes -
      std::min<uint64_t>(stats_.queued_bytes, options_.batch_bytes));
}

void Backup::WakeMirror() {
  TEST_SYNC_POINT_CALLBACK("MetaBypass::MirrorReadyProbe", this);
  if (mirror_waiting_ && MirrorReady()) SignalMirror();
}
bool Backup::QueuesEmpty() const {
  // Read the WAL tail even with pending metadata. Together with the producer
  // reading wal_wake_charge_ after publishing, this covers a simultaneous
  // WAL publication and metadata threshold update.
  const bool wal_empty = wal_taken_ == wal_queue_.Published();
  return wal_empty && metadata_channel_.queue.empty();
}
Backup::Channel* Backup::NextChannel(uint64_t wal_tail) {
  if (wal_taken_ == wal_tail)
    return metadata_channel_.queue.empty() ? nullptr : &metadata_channel_;
  if (metadata_channel_.queue.empty()) return &wal_channel_;
  return FrontSequence(wal_channel_) < FrontSequence(metadata_channel_)
             ? &wal_channel_
             : &metadata_channel_;
}

uint64_t Backup::FrontSequence(const Channel& channel) const {
  if (!IsWalChannel(channel)) return metadata_channel_.queue.front().seq;
  // Metadata sequences incorporate the WAL tail at publication. All entries
  // at the current WAL head belong before the next WAL event, including ties.
  // The picker holds mutex_, so it cannot pass metadata assigning its sequence.
  const uint64_t metadata_taken = accepted_ - metadata_channel_.queue.size();
  return wal_taken_ + metadata_taken + 1;
}

uint64_t Backup::WalBatchLimit(uint64_t first, uint64_t boundary) const {
  // The first metadata event ends the globally adjacent WAL prefix. A known
  // capture cut also ends a batch that starts at or before that cut.
  if (!metadata_channel_.queue.empty())
    boundary = std::min(boundary, metadata_channel_.queue.front().seq - 1);
  if (candidate_ && !capture_done_ && first <= candidate_->seq)
    boundary = std::min(boundary, candidate_->seq);
  return boundary;
}

Backup::Event Backup::TakeMetadataEvent() {
  Event event = std::move(metadata_channel_.queue.front());
  metadata_channel_.queue.pop_front();
  return event;
}

void Backup::WakeMetadataCapacity() {
  const size_t waiting = metadata_channel_.waiting_charge;
  if (waiting != 0 &&
      (stopping_ || !stats_.error.ok() ||
       stats_.queued_bytes <= options_.metadata_queue_capacity - waiting)) {
    metadata_channel_.capacity.notify_one();
  }
}
void Backup::TakeWalBatch(uint64_t first_seq, uint64_t last,
                          std::vector<Event>* batch) {
  assert(batch->empty() && first_seq <= last);
  batch->push_back(wal_queue_.Pop());
  batch->back().seq = first_seq;
  const Event& first = batch->front();
  constexpr size_t kMaxBytes = 64 * 1024;
  if (first.kind != Kind::kAppend || first.Payload().empty() ||
      first.Payload().size() > kMaxBytes)
    return;
  size_t bytes = first.Payload().size();
  for (uint64_t seq = first_seq + 1;
       seq <= last && batch->size() < kMaxWalBatchEvents; ++seq) {
    const Event& next = wal_queue_.Front();
    if (next.kind != Kind::kAppend || next.name != batch->front().name ||
        next.Payload().empty() || next.Payload().size() > kMaxBytes - bytes)
      break;
    bytes += next.Payload().size();
    batch->push_back(wal_queue_.Pop());
    batch->back().seq = seq;
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
  batch.reserve(kMaxWalBatchEvents);
  std::string scratch;
  scratch.reserve(64 * 1024);
  std::unique_lock<std::mutex> lock(mutex_);
  auto fail = [&](const Status& status) {
    lock.unlock();
    Fail(status);
    lock.lock();
  };
  while (stats_.error.ok()) {
    if (sst_staging_ && sst_staging_->mirror_busy) {
      sst_staging_->mirror_busy = false;
      sst_staging_->cv.notify_all();
    }
    mirror_waiting_ = true;
    TEST_SYNC_POINT("MetaBypass::BeforeMirrorWait");
    WaitForMirror(lock);
    mirror_waiting_ = false;
    if (!stats_.error.ok()) break;
    if (sst_staging_) sst_staging_->mirror_busy = true;
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
    // The metadata tail and the WAL publication snapshot define this round.
    const uint64_t wal_tail = wal_queue_.Published();
    const uint64_t boundary = accepted_ + wal_tail;
    while (true) {
      MB_WAL_BG_SPAN(selection, mirror_selection_hold);
      auto* channel = NextChannel(wal_tail);
      if (!channel) break;
      const uint64_t first = FrontSequence(*channel);
      if (first > boundary) break;
      const bool wal = IsWalChannel(*channel);
      const uint64_t last = wal ? WalBatchLimit(first, boundary) : first;
      if (!wal) batch.push_back(TakeMetadataEvent());
      const bool barrier = requested_point_ > published_request_;
      MB_WAL_END(selection);
      lock.unlock();
      if (wal) {
#ifndef NDEBUG
        std::array<uint64_t, 2> selected{{first, last}};
        TEST_SYNC_POINT_CALLBACK("MetaBypass::WalBatchSelected", &selected);
#endif
        TakeWalBatch(first, last, &batch);
      }
      HddPurposeScope purpose(barrier ? HddPurpose::kBarrier
                                      : HddPurpose::kBulk);
      size_t completed = 0;
      Status s = ApplyBatch(batch, &scratch, &completed);
      uint64_t charge = 0;
      uint64_t mirrored_bytes = 0;
      for (size_t i = 0; i < batch.size(); ++i) {
        const Event& e = batch[i];
        charge += e.charge;
        if (i < completed && e.kind == Kind::kAppend)
          mirrored_bytes += e.Payload().size();
      }
      const size_t taken = batch.size();
      batch.clear();
      const bool success = s.ok();
      if (wal && success) ReturnWalBytes(charge, mirrored_bytes);
      lock.lock();
      MB_WAL_BG_SPAN(completion, mirror_completion_hold);
      assert(first == applied_ + 1);
      applied_ += completed;
      if (wal) {
        wal_taken_ += taken;
      } else {
        stats_.mirrored_bytes += mirrored_bytes;
        assert(stats_.queued_bytes >= charge);
        stats_.queued_bytes -= charge;
        UpdateWalWakeCharge();
      }
      if (!success) {
        MB_WAL_END(completion);
        lock.unlock();
        Fail(s);
        if (wal) ReturnWalBytes(charge, mirrored_bytes);
        lock.lock();
        break;
      }
      if (stats_.error.ok() && !wal) WakeMetadataCapacity();
      MB_WAL_END(completion);
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
    if (stopping_ && QueuesEmpty()) break;
  }
  for (const auto& e : metadata_channel_.queue) {
    assert(stats_.queued_bytes >= e.charge);
    stats_.queued_bytes -= e.charge;
  }
  metadata_channel_.queue.clear();
  const uint64_t tail = wal_queue_.Published();
  const uint64_t discarded = tail - wal_taken_;
  lock.unlock();
  uint64_t charge = 0;
  for (uint64_t i = 0; i < discarded; ++i) charge += wal_queue_.Pop().charge;
  if (charge != 0) ReturnWalBytes(charge);
  lock.lock();
  wal_taken_ = tail;
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
  uint64_t begin, steady_begin, sequence, end, steady_end;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!active_ || !db_) return;
    begin = clock_->NowMicros();
    steady_begin = clock_->NowNanos() / 1000;
    sequence = db_->GetLatestSequenceNumber();
    end = clock_->NowMicros();
    steady_end = clock_->NowNanos() / 1000;
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
    if (!active_) {
      validator_cv_.wait(lock);
      continue;
    }
    if (completed_writes_.load(std::memory_order_relaxed) > published_writes_ ||
        native_changes_ > published_native_changes_)
      MarkDirty();
    const bool requested = requested_point_ > published_request_;
    const uint64_t dirty_since =
        dirty_since_micros_.load(std::memory_order_relaxed);
    if (!requested) {
      if (!dirty_since) {
        TEST_SYNC_POINT("MetaBypass::CheckpointIdle");
        validator_cv_.wait(lock);
        continue;
      }
      uint64_t deadline = dirty_since + options_.checkpoint_interval_ms * 1000;
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
    if (sst_staging_) candidate->sst_epoch = ++sst_staging_->capture_epoch;
    candidate->started = Now();
    candidate->oldest = dirty_since ? dirty_since : candidate->started;
    dirty_since_micros_.store(0, std::memory_order_relaxed);
    candidate->writes = completed_writes_.load(std::memory_order_relaxed);
    candidate->request = requested_point_;
    candidate->native_changes = native_changes_;
    DB* db = db_;
    lock.unlock();
    HddPurposeScope purpose(requested ? HddPurpose::kBarrier
                                      : HddPurpose::kBulk);
    TEST_SYNC_POINT("MetaBypass::CheckpointBoundary");
    Status s;
    bool pinned;
    {
      LiveFilesStorageInfoOptions options;
      options.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
      uint64_t elapsed = 0;
      VectorLogPtr logs;
      MB_WAL_BG_SPAN(closed_gate, checkpoint_gate_hold);
      {
        // Keep obsolete WAL pruning enabled while admitted writes drain.
        WriteGate::Closure admission(write_gate_);
        s = db->DisableFileDeletions();
        pinned = s.ok();
        if (pinned) {
          TEST_SYNC_POINT("MetaBypass::NativeFilesPinned");
          const uint64_t capture_started = Now();
          s = sst_staging_
                  ? static_cast_with_check<DBImpl>(db)->GetLiveFilesStorageInfo(
                        options, &candidate->files, &candidate->table_levels)
                  : db->GetLiveFilesStorageInfo(options, &candidate->files);
          TEST_SYNC_POINT_CALLBACK("MetaBypass::NativeCaptureStatus", &s);
          elapsed = Now() - capture_started;
          if (s.ok()) {
            candidate->native_sequence = db->GetLatestSequenceNumber();
            s = db->GetSortedWalFiles(logs);
          }
          // Freeze the mirror boundary with the native snapshot. Writes
          // admitted after reopening belong to the next checkpoint.
          std::lock_guard<std::mutex> boundary_lock(mutex_);
          candidate->seq = Accepted();
          candidate->writes = completed_writes_.load(std::memory_order_relaxed);
          // Drained writes may have set a deadline after this candidate began.
          // This snapshot covers them, but native changes can need another
          // point.
          if (s.ok() && native_changes_ == candidate->native_changes)
            dirty_since_micros_.store(0, std::memory_order_relaxed);
        }
      }
      MB_WAL_END(closed_gate);
      if (pinned) {
        {
          std::lock_guard<std::mutex> stats_lock(mutex_);
          ++stats_.checkpoint_captures;
          stats_.checkpoint_capture_micros += elapsed;
        }
        if (s.ok()) {
          // Native checkpoints omit IDENTITY. Include it to bind each point to
          // its database owner.
          LiveFileStorageInfo owner;
          owner.relative_filename = "IDENTITY";
          owner.file_type = kIdentityFile;
          owner.replacement_contents = identity_;
          owner.size = identity_.size();
          candidate->files.push_back(std::move(owner));
          if (!logs.empty()) candidate->wal_floor = logs.front()->LogNumber();
        }
      }
    }
    TEST_SYNC_POINT("MetaBypass::NativeFilesCaptured");
    lock.lock();
    if (s.ok() && stats_.error.ok() && !stopping_) {
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
    if (s.ok()) {
      const uint64_t sst_started = Now();
      s = CaptureTables(candidate.get());
      candidate->sst_micros = Now() - sst_started;
    }
    // Candidate files now own their bytes.
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
    if (!s.ok()) {
      if (candidate->sst_adopted_ordinary_bytes != 0) {
        std::lock_guard<std::mutex> stats_lock(mutex_);
        stats_.sst_async_discarded_bytes +=
            candidate->sst_adopted_ordinary_bytes;
      }
      Fail(s);
    }
    lock.lock();
    candidate_busy_ = false;
    if (sst_staging_) sst_staging_->cv.notify_all();
    sync_cv_.notify_all();
  }
  candidate_busy_ = false;
  if (sst_staging_) sst_staging_->cv.notify_all();
  validator_done_ = true;
  gc_cv_.notify_all();
  sync_cv_.notify_all();
}
void Backup::GarbageCollectLoop() {
  HddPurposeScope background(HddPurpose::kBulk);
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    gc_cv_.wait(lock, [&] {
      return retired_point_ || validator_done_ || !stats_.error.ok();
    });
    if (!stats_.error.ok() || !retired_point_) break;
    const std::string path = options_.backup_dir + "/" + retired_point_->name;
    lock.unlock();
    const uint64_t started = Now();
    TEST_SYNC_POINT("MetaBypass::GCStarted");
    Status s;
    TEST_SYNC_POINT_CALLBACK("MetaBypass::GCStatus", &s);
    if (s.ok()) s = RemoveRetiredPoint(disk_, path);
    if (s.ok()) s = SyncDir(disk_, options_.backup_dir);
    if (!s.ok()) {
      Fail(s);
      TEST_SYNC_POINT("MetaBypass::GCFailureRecorded");
    }
    lock.lock();
    stats_.gc_micros += Now() - started;
    if (!s.ok()) break;
    retired_point_.reset();
    prune_wal_ = true;
    WakeMirror();
    stats_.pending_gc_points = 0;
    stats_.pending_gc_bytes = 0;
    gc_cv_.notify_all();
  }
  gc_cv_.notify_all();
}
void Backup::WriteCompleted() {
  // If capture clears the observed deadline concurrently, it still drains
  // this writer before taking a snapshot. Admission after reopening observes
  // the cleared deadline or one already installed for the next checkpoint.
  MB_WAL_SPAN(wait, completed_wait, true);
  std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
  if (dirty_since_micros_.load(std::memory_order_relaxed) == 0) lock.lock();
  MB_WAL_END(wait);
  MB_WAL_SPAN(bookkeeping, completed_bookkeeping, true);
  completed_writes_.fetch_add(1, std::memory_order_relaxed);
  if (lock.owns_lock()) MarkDirty();
}
void Backup::NativeFilesChanged(const CompactionJobInfo* compaction) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++native_changes_;
  MarkDirty();
  if (!sst_staging_ || !compaction || stopping_ || !stats_.error.ok()) return;
  auto& staging = *sst_staging_;
  const uint64_t now = Now();
  for (const auto& output : compaction->output_file_infos) {
    if (staging.output_hints.size() >= kMaxStagedSsts) break;
    if (output.level >= options_.sst_async_min_level)
      staging.output_hints.try_emplace(output.file_number, now);
  }
  // A file can be both input and output of a trivial move. The worker
  // reconciles inputs and outputs against one current native Version.
  if (staging.output_hints.empty() && staging.jobs.empty() &&
      !staging.rescan_requested)
    return;
  ++staging.revision;
  staging.cv.notify_all();
}
void Backup::NativeSstLevelsChanged() {
  if (!sst_staging_) return;
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopping_ || !stats_.error.ok()) return;
  ++native_changes_;
  MarkDirty();
  auto& staging = *sst_staging_;
  staging.rescan_requested = true;
  ++staging.revision;
  staging.cv.notify_all();
}
void Backup::MarkDirty() {
  if (dirty_since_micros_.load(std::memory_order_relaxed) == 0) {
    dirty_since_micros_.store(Now(), std::memory_order_relaxed);
    validator_cv_.notify_one();
  }
}
Status Backup::Sync() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!stats_.error.ok()) return stats_.error;
  if (!active_ || stopping_) return Status::ShutdownInProgress();
  const uint64_t goal = ++requested_point_;
  const uint64_t writes = completed_writes_.load(std::memory_order_relaxed);
  const uint64_t start = Now();
  requested_ = std::max(requested_, Accepted());
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
  if (sst_staging_) {
    sst_staging_->detaching = true;
    sst_staging_->cv.notify_all();
  }
  TEST_SYNC_POINT("MetaBypass::BeforeDetachDB");
  sync_cv_.wait(lock, [&] {
    return !candidate_busy_ && (!sst_staging_ || !sst_staging_->borrowing_db);
  });
  db_ = nullptr;
  lock.unlock();
  if (sst_staging_ && sst_staging_->worker.joinable())
    sst_staging_->worker.join();
}
Status Backup::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    CloseWalQueue();
    validator_cv_.notify_all();
    gc_cv_.notify_all();
    if (sst_staging_) sst_staging_->cv.notify_all();
    WakeMirror();
    WakeMetadataCapacity();
    sync_cv_.notify_all();
  }
  if (sampler_.joinable()) sampler_.join();
  if (thread_.joinable()) thread_.join();
  if (validator_.joinable()) validator_.join();
  if (sst_staging_ && sst_staging_->worker.joinable())
    sst_staging_->worker.join();
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
  MetaBypassStats result = stats_;
  result.wal_queued_bytes = wal_queue_.ChargedBytes();
  result.queued_bytes += result.wal_queued_bytes;
  result.mirrored_wal_bytes =
      wal_queue_.mirrored_bytes.load(std::memory_order_relaxed);
  result.mirrored_bytes += result.mirrored_wal_bytes;
  result.peak_queued_bytes += wal_queue_.peak.load(std::memory_order_relaxed);
  result.wal_backpressure_micros =
      wal_queue_.backpressure_micros.load(std::memory_order_relaxed);
  result.backpressure_micros += result.wal_backpressure_micros;
  return result;
}
Status Backup::ReadRecoveryPoint(FileSystem* fs, const std::string& backup,
                                 RecoveryPoint* recovery) {
  std::string pointer;
  Status s = Read(fs, backup + "/LATEST", &pointer);
  return s.ok() ? ReadPublishedPoint(fs, backup, pointer, recovery) : s;
}
Status Backup::PrepareRecovery(FileSystem* fs, const std::string& backup,
                               const RecoveryPoint& recovery,
                               const std::string& destination,
                               MetaBypassRecoveryReport* report,
                               SstStorage* sst) {
  const std::string point = backup + "/" + recovery.name;
  report->checkpoint_sequence = recovery.sequence;
  report->recovered_sequence = recovery.sequence;
  Status s;
  std::vector<SstStorage::RestoreFile> ssts;
  for (const auto& [file, length] : recovery.files) {
    uint64_t number;
    FileType type;
    if (sst && ParseFileName(file, &number, &type) && type == kTableFile) {
      ssts.push_back({point + "/" + file, number, length});
      continue;
    }
    s = Copy(fs, point + "/" + file, destination + "/" + file, length);
    if (!s.ok()) return s;
  }
  if (sst) {
    s = sst->RestoreTables(ssts);
    if (!s.ok()) return s;
  }
  std::vector<std::string> files;
  s = fs->GetChildren(backup + "/work", IOOptions(), &files, nullptr);
  if (s.IsNotFound() || s.IsPathNotFound()) return SyncDir(fs, destination);
  if (!s.ok()) return s;
  for (const auto& wal_name : files) {
    uint64_t number;
    FileType type;
    if (!ParseFileName(wal_name, &number, &type) || type != kWalFile) continue;
    const std::string source = backup + "/work/" + wal_name;
    const std::string target = destination + "/" + wal_name;
    uint64_t size;
    s = fs->GetFileSize(source, IOOptions(), &size, nullptr);
    if (s.ok()) s = ExtendWal(fs, source, target, size);
    if (!s.ok()) return s;
  }
  return SyncDir(fs, destination);
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
