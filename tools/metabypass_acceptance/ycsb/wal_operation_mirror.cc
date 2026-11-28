// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "wal_operation_mirror.h"

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"

namespace mb_ycsb {
namespace {
namespace fs = std::filesystem;
namespace rdb = ROCKSDB_NAMESPACE;
using Clock = std::chrono::steady_clock;

constexpr size_t kMaxBatchEvents = 256;
constexpr size_t kMaxBatchBytes = 64 * 1024;

enum class Kind { kCreate, kAppend, kTruncate, kClose };

// Account capacity as in the reference MetaBypass WAL queue, independent of
// the representation used for captured bytes.
struct ChargedEventLayout {
  Kind kind;
  std::string name, other, bytes;
  uint64_t size, sequence;
  size_t charge;
};

struct Event {
  Event(Kind kind, uint64_t number, std::string_view name, uint64_t size = 0)
      : kind(kind), number(number), name(name), size(size) {}

  rdb::Slice Payload() const {
    return shared_bytes ? rdb::Slice(shared_bytes.get(), size)
                        : rdb::Slice(bytes);
  }

  Kind kind;
  uint64_t number;
  std::string name;
  uint64_t size;
  uint64_t charge = 0;
  std::string bytes;
  std::shared_ptr<const char> shared_bytes;
};

struct WalBlock {
  WalBlock() {}
  char bytes[64 * 1024];
};

// The producer side is serialized by the source operation mutex. Only the
// HDD worker consumes and frees segments. The tail publication makes a slot
// and any newly linked segment visible to that worker.
class WalQueue {
 public:
  static constexpr uint64_t kClosed = uint64_t{1} << 63;
  static constexpr size_t kSlots = 256;

  uint64_t Published() const { return published_.load() & ~kClosed; }
  bool Closed() const { return (published_.load() & kClosed) != 0; }
  void Seal() { published_.fetch_or(kClosed); }

  void Prepare() {
    if (tail_slot_ == kSlots) {
      tail_->next = std::make_unique<Segment>();
      tail_ = tail_->next.get();
      tail_slot_ = 0;
    }
  }

  bool Publish(Event&& event) {
    auto& slot = tail_->events[tail_slot_];
    slot.emplace(std::move(event));
    uint64_t expected = tail_count_;
    if (!published_.compare_exchange_strong(expected, tail_count_ + 1)) {
      slot.reset();
      return false;
    }
    ++tail_slot_;
    ++tail_count_;
    return true;
  }

  const Event& Front() const {
    return head_slot_ == kSlots ? *head_->next->events[0]
                                : *head_->events[head_slot_];
  }

  Event Pop() {
    if (head_slot_ == kSlots) {
      auto next = std::move(head_->next);
      head_ = std::move(next);
      head_slot_ = 0;
    }
    auto& slot = head_->events[head_slot_++];
    Event event = std::move(*slot);
    slot.reset();
    return event;
  }

  std::atomic<uint64_t> reserved{0};
  std::atomic<uint64_t> returned{0};

  uint64_t ChargedBytes() const {
    uint64_t returned_snapshot;
    uint64_t reserved_snapshot;
    do {
      returned_snapshot = returned.load(std::memory_order_acquire);
      reserved_snapshot = reserved.load(std::memory_order_acquire);
    } while (returned_snapshot != returned.load(std::memory_order_acquire));
    return reserved_snapshot - returned_snapshot;
  }

 private:
  struct Segment {
    std::array<std::optional<Event>, kSlots> events;
    std::unique_ptr<Segment> next;
  };

  std::atomic<uint64_t> published_{0};
  std::unique_ptr<Segment> head_ = std::make_unique<Segment>();
  size_t head_slot_ = 0;
  Segment* tail_ = head_.get();
  size_t tail_slot_ = 0;
  uint64_t tail_count_ = 0;
};

struct WalPath {
  uint64_t number = 0;
  std::string name;
};

WalPath ParseWalName(std::string name) {
  constexpr std::string_view suffix = ".log";
  if (!name.ends_with(suffix)) {
    return {};
  }
  const auto end = name.data() + name.size() - suffix.size();
  uint64_t number;
  const auto result = std::from_chars(name.data(), end, number);
  if (result.ec != std::errc{} || result.ptr != end || number == 0) {
    return {};
  }
  return {number, std::move(name)};
}

std::string DirectoryPrefix(const fs::path& path) {
  auto normalized = fs::absolute(path).lexically_normal().string();
  if (!normalized.ends_with('/')) {
    normalized += '/';
  }
  return normalized;
}

bool Missing(const rdb::IOStatus& status) {
  return status.IsNotFound() || status.IsPathNotFound();
}
}  // namespace

class WalOperationMirror::State {
 public:
  class CaptureFS;
  class CaptureFile;

  State(std::shared_ptr<rdb::FileSystem> mirror_fs, const fs::path& mirror_dir,
        WalOperationMirrorOptions options)
      : options_(options), mirror_fs_(std::move(mirror_fs)) {
    if (!mirror_fs_ || options_.queue_capacity_bytes == 0 ||
        options_.batch_bytes == 0 || options_.interval_ms == 0 ||
        options_.interval_ms >
            static_cast<uint64_t>(std::chrono::milliseconds::max().count())) {
      throw std::invalid_argument("invalid WAL operation mirror configuration");
    }
    const auto directory = fs::absolute(mirror_dir).lexically_normal().string();
    mirror_prefix_ = DirectoryPrefix(directory);
    auto status = mirror_fs_->FileExists(directory, {}, nullptr);
    if (status.ok()) {
      throw std::runtime_error("WAL mirror already exists: " + directory);
    }
    if (!Missing(status)) {
      throw std::runtime_error("check WAL mirror directory: " +
                               status.ToString());
    }
    status = mirror_fs_->CreateDir(directory, {}, nullptr);
    if (!status.ok()) {
      throw std::runtime_error("create WAL mirror directory: " +
                               status.ToString());
    }
  }

  void Start() {
    worker_ = std::thread([this] { Run(); });
  }

  void AdvanceFloor(uint64_t floor) {
    {
      std::lock_guard lock(state_mutex_);
      if (queue_.Closed() || floor <= requested_floor_) {
        return;
      }
      requested_floor_ = floor;
    }
    SignalMirror();
  }

  rdb::Status Close() {
    std::lock_guard close_lock(close_mutex_);
    {
      std::lock_guard lock(state_mutex_);
      SealQueue();
    }
    SignalMirror();
    if (worker_.joinable()) {
      worker_.join();
    }
    return GetError();
  }

  rdb::Status GetError() const try {
    if (!failed_.load(std::memory_order_acquire)) {
      return rdb::Status::OK();
    }
    std::lock_guard lock(state_mutex_);
    return *error_;
  } catch (const std::bad_alloc&) {
    return rdb::Status::IOError(rdb::Status::kMemoryLimit);
  }

 private:
  rdb::IOStatus ClosedError() const {
    std::lock_guard lock(state_mutex_);
    if (error_) {
      return rdb::status_to_io_status(rdb::Status(*error_));
    }
    return rdb::status_to_io_status(
        rdb::Status::Aborted("WAL operation mirror closed"));
  }

  void Fail(rdb::IOStatus status) {
    {
      std::lock_guard lock(state_mutex_);
      if (!error_) {
        error_.emplace(std::move(status));
        error_->PermitUncheckedError();
        SealQueue();
        failed_.store(true, std::memory_order_release);
      } else {
        status.PermitUncheckedError();
      }
    }
    SignalMirror();
  }

  // The caller holds state_mutex_. Waiters release space_mutex_ before
  // acquiring state_mutex_ to retrieve the first error.
  void SealQueue() {
    std::lock_guard lock(space_mutex_);
    queue_.Seal();
    capacity_.notify_one();
  }

  rdb::IOStatus Observe(rdb::IOStatus status) {
    if (!status.ok()) {
      try {
        // Copy the base status so allocation failure cannot leave an
        // unchecked IOStatus base subobject during unwinding.
        Fail(rdb::status_to_io_status(rdb::Status(status)));
      } catch (const std::bad_alloc&) {
        // Copying a status with a message can allocate.
        status = rdb::IOStatus::IOError(rdb::IOStatus::kMemoryLimit);
        Fail(status);
      }
    }
    return status;
  }

  rdb::IOStatus AllocationFailure() {
    return Observe(rdb::IOStatus::IOError(rdb::IOStatus::kMemoryLimit));
  }

  bool HasSpace(uint64_t charge) {
    if (reserved_total_ - cached_returned_ <=
        options_.queue_capacity_bytes - charge) {
      return true;
    }
    cached_returned_ = queue_.returned.load(std::memory_order_acquire);
    return reserved_total_ - cached_returned_ <=
           options_.queue_capacity_bytes - charge;
  }

  rdb::IOStatus Reserve(Event& event, uint64_t payload = 0) {
    const uint64_t overhead = sizeof(ChargedEventLayout) + event.name.size();
    if (overhead > options_.queue_capacity_bytes ||
        payload > options_.queue_capacity_bytes - overhead) {
      return rdb::status_to_io_status(rdb::Status::InvalidArgument(
          "operation exceeds WAL mirror capacity"));
    }
    event.charge = overhead + payload;
    if (!queue_.Closed() && !HasSpace(event.charge)) {
      std::unique_lock lock(space_mutex_);
      space_return_target_ =
          reserved_total_ - (options_.queue_capacity_bytes - event.charge);
      waiting_for_space_.store(true);
      lock.unlock();
      SignalMirror();
      lock.lock();
      capacity_.wait(lock,
                     [&] { return queue_.Closed() || HasSpace(event.charge); });
      waiting_for_space_.store(false);
    }
    if (queue_.Closed()) {
      return ClosedError();
    }
    try {
      queue_.Prepare();
    } catch (const std::bad_alloc&) {
      return AllocationFailure();
    }
    reserved_total_ += event.charge;
    queue_.reserved.store(reserved_total_, std::memory_order_release);
    return rdb::IOStatus::OK();
  }

  void Cancel(uint64_t charge) {
    reserved_total_ -= charge;
    queue_.reserved.store(reserved_total_, std::memory_order_release);
  }

  void CopyPayload(Event& event, const rdb::Slice& payload) {
    if (payload.size() >= 32 && payload.size() <= 1024) {
      if (!wal_block_ ||
          sizeof(wal_block_->bytes) - wal_block_used_ < payload.size()) {
        wal_block_ = std::make_shared<WalBlock>();
        wal_block_used_ = 0;
      }
      char* destination = wal_block_->bytes + wal_block_used_;
      std::memcpy(destination, payload.data(), payload.size());
      wal_block_used_ += payload.size();
      event.shared_bytes = std::shared_ptr<const char>(wal_block_, destination);
      event.size = payload.size();
    } else {
      event.bytes.assign(payload.data(), payload.size());
    }
  }

  rdb::IOStatus Finish(Event&& event, rdb::IOStatus primary, bool& wake) {
    const auto charge = event.charge;
    if (!primary.ok()) {
      Cancel(charge);
      return Observe(std::move(primary));
    }
    if (!queue_.Publish(std::move(event))) {
      Cancel(charge);
      return ClosedError();
    }
    if (reserved_total_ - cached_returned_ >= options_.batch_bytes) {
      cached_returned_ = queue_.returned.load(std::memory_order_acquire);
      wake = reserved_total_ - cached_returned_ >= options_.batch_bytes;
    }
    return primary;
  }

  void SignalMirror() {
    // The worker clears the notification before checking the published tail.
    // A producer that saw a pending notification is covered by that check.
    if (mirror_signal_.load() || mirror_signal_.exchange(true)) {
      return;
    }
    std::lock_guard lock(sleep_mutex_);
    mirror_wake_.notify_one();
  }

  rdb::IOStatus FinishAndSignal(Event&& event, rdb::IOStatus primary) {
    bool wake = false;
    auto status = Finish(std::move(event), std::move(primary), wake);
    if (wake) {
      SignalMirror();
    }
    return status;
  }

  struct MirrorFile {
    std::string path;
    std::unique_ptr<rdb::FSWritableFile> writer;
  };

  rdb::IOStatus Apply(const Event& event, const rdb::Slice& data) {
    if (event.number < applied_floor_) {
      return rdb::IOStatus::OK();
    }
    auto file = files_.find(event.number);
    if (event.kind == Kind::kCreate) {
      if (file != files_.end()) {
        return rdb::status_to_io_status(
            rdb::Status::Corruption("duplicate mirrored WAL", event.name));
      }
      MirrorFile created{mirror_prefix_ + event.name, nullptr};
      auto status = mirror_fs_->NewWritableFile(created.path, {},
                                                &created.writer, nullptr);
      if (status.ok()) {
        files_.emplace(event.number, std::move(created));
      }
      return status;
    }
    if (file == files_.end()) {
      return rdb::status_to_io_status(
          rdb::Status::Corruption("unknown mirrored WAL", event.name));
    }
    auto& writer = file->second.writer;
    if (event.kind == Kind::kClose) {
      if (!writer) {
        return rdb::IOStatus::OK();
      }
      auto status = writer->Close({}, nullptr);
      writer.reset();
      return status;
    }
    if (!writer) {
      auto status = mirror_fs_->ReopenWritableFile(file->second.path, {},
                                                   &writer, nullptr);
      if (!status.ok()) {
        return status;
      }
    }
    if (event.kind == Kind::kTruncate) {
      return writer->Truncate(event.size, {}, nullptr);
    }
    return writer->Append(data, {}, nullptr);
  }

  rdb::IOStatus Prune(uint64_t floor) {
    auto file = files_.begin();
    while (file != files_.end() && file->first < floor) {
      auto& entry = file->second;
      if (entry.writer) {
        auto status = entry.writer->Close({}, nullptr);
        entry.writer.reset();
        if (!status.ok()) {
          return status;
        }
      }
      auto status = mirror_fs_->DeleteFile(entry.path, {}, nullptr);
      if (!status.ok() && !Missing(status)) {
        return status;
      }
      file = files_.erase(file);
    }
    return rdb::IOStatus::OK();
  }

  void TakeBatch(uint64_t boundary, std::vector<Event>& batch) {
    batch.push_back(queue_.Pop());
    ++taken_;
    const auto& first = batch.front();
    if (first.kind != Kind::kAppend) {
      return;
    }
    size_t bytes = first.Payload().size();
    if (bytes == 0 || bytes > kMaxBatchBytes) {
      return;
    }
    while (taken_ < boundary && batch.size() < kMaxBatchEvents) {
      const auto& next = queue_.Front();
      if (next.kind != Kind::kAppend || next.number != first.number ||
          next.Payload().empty() ||
          next.Payload().size() > kMaxBatchBytes - bytes) {
        break;
      }
      bytes += next.Payload().size();
      batch.push_back(queue_.Pop());
      ++taken_;
    }
  }

  bool Ready() {
    return queue_.Closed() || requested_floor_ > applied_floor_ ||
           (queue_.Published() != taken_ &&
            (waiting_for_space_.load() ||
             queue_.ChargedBytes() >= options_.batch_bytes));
  }

  void WaitForMirror(std::unique_lock<std::mutex>& lock,
                     std::chrono::milliseconds interval) {
    const auto deadline = Clock::now() + interval;
    for (;;) {
      mirror_signal_.exchange(false);
      if (Ready()) {
        return;
      }
      lock.unlock();
      bool signaled;
      {
        std::unique_lock sleep_lock(sleep_mutex_);
        signaled = mirror_wake_.wait_until(
            sleep_lock, deadline, [&] { return mirror_signal_.load(); });
      }
      lock.lock();
      if (!signaled) {
        return;
      }
    }
  }

  uint64_t ClearBatch(std::vector<Event>& batch) {
    uint64_t charge = 0;
    for (const auto& event : batch) {
      charge += event.charge;
    }
    batch.clear();
    return charge;
  }

  void ReturnWalBytes(uint64_t charge) {
    std::lock_guard lock(space_mutex_);
    const auto returned = queue_.returned.load(std::memory_order_relaxed);
    queue_.returned.store(returned + charge, std::memory_order_release);
    if (waiting_for_space_.load(std::memory_order_relaxed)) {
      // Use unsigned distance so cumulative byte counters may wrap.
      const auto needed = space_return_target_ - returned;
      if (needed != 0 && needed <= charge) {
        capacity_.notify_one();
      }
    }
  }

  void RunLoop(std::vector<Event>& batch) {
    batch.reserve(kMaxBatchEvents);
    std::string scratch;
    scratch.reserve(kMaxBatchBytes);
    const auto interval = std::chrono::milliseconds(options_.interval_ms);
    for (;;) {
      uint64_t floor, boundary;
      {
        std::unique_lock lock(state_mutex_);
        WaitForMirror(lock, interval);
        if (error_) {
          return;
        }
        floor = requested_floor_;
        boundary = queue_.Published();
        if (queue_.Closed() && taken_ == boundary && floor == applied_floor_) {
          return;
        }
      }
      if (floor > applied_floor_) {
        auto status = Prune(floor);
        if (!status.ok()) {
          Fail(std::move(status));
          return;
        }
        applied_floor_ = floor;
      }
      while (taken_ < boundary && !failed_.load(std::memory_order_acquire)) {
        TakeBatch(boundary, batch);
        const auto& first = batch.front();
        const bool append = first.kind == Kind::kAppend;
        rdb::Slice data;
        if (append) {
          if (batch.size() == 1) {
            data = first.Payload();
          } else {
            scratch.clear();
            for (const auto& event : batch) {
              const auto payload = event.Payload();
              scratch.append(payload.data(), payload.size());
            }
            data = scratch;
          }
        }
        auto status = Apply(first, data);
        if (!status.ok()) {
          // A failed Append may have made a partial write. Stop here so
          // later operations cannot turn that tail into a hole.
          Fail(std::move(status));
          return;
        }
        ReturnWalBytes(ClearBatch(batch));
      }
    }
  }

  void Run() {
    std::vector<Event> batch;
    try {
      RunLoop(batch);
    } catch (const std::bad_alloc&) {
      Fail(rdb::IOStatus::IOError(rdb::IOStatus::kMemoryLimit));
    }

    // The queue has been sealed. Release data from accepted but unapplied
    // events after a failure.
    uint64_t charge = ClearBatch(batch);
    const auto published = queue_.Published();
    while (taken_ < published) {
      charge += queue_.Pop().charge;
      ++taken_;
    }
    if (charge != 0) {
      ReturnWalBytes(charge);
    }
    for (auto& [number, file] : files_) {
      if (file.writer) {
        auto status = file.writer->Close({}, nullptr);
        if (!status.ok()) {
          Fail(std::move(status));
        }
      }
    }
    files_.clear();
  }

  const WalOperationMirrorOptions options_;
  const std::shared_ptr<rdb::FileSystem> mirror_fs_;
  std::string mirror_prefix_;

  // All producers serialize source mutation and subsequent queue publication.
  // The HDD thread never acquires this mutex or calls the source filesystem.
  std::mutex operations_;
  WalQueue queue_;
  uint64_t reserved_total_ = 0;
  uint64_t cached_returned_ = 0;
  std::shared_ptr<WalBlock> wal_block_;
  size_t wal_block_used_ = 0;

  mutable std::mutex state_mutex_;
  std::optional<rdb::IOStatus> error_;
  uint64_t requested_floor_ = 0;

  std::mutex space_mutex_;
  std::condition_variable capacity_;
  uint64_t space_return_target_ = 0;
  std::atomic<bool> waiting_for_space_{false};

  std::mutex sleep_mutex_;
  std::condition_variable mirror_wake_;
  std::atomic<bool> mirror_signal_{false};

  std::atomic<bool> failed_{false};

  // Owned exclusively by the HDD thread.
  uint64_t taken_ = 0;
  uint64_t applied_floor_ = 0;
  std::map<uint64_t, MirrorFile> files_;
  std::mutex close_mutex_;
  std::thread worker_;
};

class WalOperationMirror::State::CaptureFile final
    : public rdb::FSWritableFileOwnerWrapper {
 public:
  CaptureFile(std::unique_ptr<rdb::FSWritableFile> file,
              std::shared_ptr<State> state, WalPath wal)
      : rdb::FSWritableFileOwnerWrapper(std::move(file)),
        state_(std::move(state)),
        number_(wal.number),
        name_(std::move(wal.name)) {}

  rdb::IOStatus Append(const rdb::Slice& data, const rdb::IOOptions& opts,
                       rdb::IODebugContext* dbg) override {
    return CaptureAppend(data, opts, nullptr, dbg);
  }

  rdb::IOStatus Append(const rdb::Slice& data, const rdb::IOOptions& opts,
                       const rdb::DataVerificationInfo& verification,
                       rdb::IODebugContext* dbg) override {
    return CaptureAppend(data, opts, &verification, dbg);
  }

  rdb::IOStatus PositionedAppend(const rdb::Slice&, uint64_t,
                                 const rdb::IOOptions&,
                                 rdb::IODebugContext*) override try {
    return rdb::status_to_io_status(
        rdb::Status::NotSupported("WAL mirror requires append I/O"));
  } catch (const std::bad_alloc&) {
    return state_->AllocationFailure();
  }

  rdb::IOStatus PositionedAppend(const rdb::Slice& data, uint64_t offset,
                                 const rdb::IOOptions& opts,
                                 const rdb::DataVerificationInfo&,
                                 rdb::IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }

  rdb::IOStatus Truncate(uint64_t size, const rdb::IOOptions& opts,
                         rdb::IODebugContext* dbg) override {
    return CaptureControl(Kind::kTruncate, size, opts, dbg);
  }

  rdb::IOStatus Close(const rdb::IOOptions& opts,
                      rdb::IODebugContext* dbg) override {
    return CaptureControl(Kind::kClose, 0, opts, dbg);
  }

  rdb::IOStatus Flush(const rdb::IOOptions& opts,
                      rdb::IODebugContext* dbg) override {
    return state_->Observe(target()->Flush(opts, dbg));
  }

  rdb::IOStatus Sync(const rdb::IOOptions& opts,
                     rdb::IODebugContext* dbg) override {
    return state_->Observe(target()->Sync(opts, dbg));
  }

  rdb::IOStatus Fsync(const rdb::IOOptions& opts,
                      rdb::IODebugContext* dbg) override {
    return state_->Observe(target()->Fsync(opts, dbg));
  }

  rdb::IOStatus RangeSync(uint64_t offset, uint64_t length,
                          const rdb::IOOptions& opts,
                          rdb::IODebugContext* dbg) override {
    return state_->Observe(target()->RangeSync(offset, length, opts, dbg));
  }

 private:
  rdb::IOStatus CaptureAppend(const rdb::Slice& data,
                              const rdb::IOOptions& opts,
                              const rdb::DataVerificationInfo* verification,
                              rdb::IODebugContext* dbg) try {
    Event event(Kind::kAppend, number_, name_);
    std::unique_lock lock(state_->operations_);
    auto status = state_->Reserve(event, data.size());
    if (!status.ok()) {
      return status;
    }
    try {
      state_->CopyPayload(event, data);
    } catch (const std::bad_alloc&) {
      state_->Cancel(event.charge);
      return state_->AllocationFailure();
    }
    bool wake = false;
    status = state_->Finish(
        std::move(event),
        verification ? target()->Append(data, opts, *verification, dbg)
                     : target()->Append(data, opts, dbg),
        wake);
    lock.unlock();
    if (wake) {
      state_->SignalMirror();
    }
    return status;
  } catch (const std::bad_alloc&) {
    return state_->AllocationFailure();
  }

  rdb::IOStatus CaptureControl(Kind kind, uint64_t size,
                               const rdb::IOOptions& opts,
                               rdb::IODebugContext* dbg) {
    bool close_pending = kind == Kind::kClose;
    try {
      Event event(kind, number_, name_, size);
      std::lock_guard lock(state_->operations_);
      auto status = state_->Reserve(event);
      if (!status.ok()) {
        if (close_pending) {
          close_pending = false;
          target()->Close(opts, dbg).PermitUncheckedError();
        }
        return status;
      }
      close_pending = false;
      return state_->FinishAndSignal(std::move(event),
                                     kind == Kind::kClose
                                         ? target()->Close(opts, dbg)
                                         : target()->Truncate(size, opts, dbg));
    } catch (const std::bad_alloc&) {
      if (close_pending) {
        target()->Close(opts, dbg).PermitUncheckedError();
      }
      return state_->AllocationFailure();
    }
  }

  const std::shared_ptr<State> state_;
  const uint64_t number_;
  const std::string name_;
};

class WalOperationMirror::State::CaptureFS final
    : public rdb::FileSystemWrapper {
 public:
  CaptureFS(std::shared_ptr<rdb::FileSystem> source,
            std::shared_ptr<State> state, const fs::path& source_wal_dir)
      : rdb::FileSystemWrapper(std::move(source)),
        state_(std::move(state)),
        working_dir_(fs::current_path()),
        source_dir_(Normalize(source_wal_dir / ".").parent_path()),
        archive_dir_(source_dir_ / "archive") {}

  const char* Name() const override { return "BackupEnginePlusPlusWalCapture"; }

  rdb::IOStatus NewWritableFile(const std::string& path,
                                const rdb::FileOptions& opts,
                                std::unique_ptr<rdb::FSWritableFile>* output,
                                rdb::IODebugContext* dbg) override try {
    auto wal = Match(path);
    if (wal.number == 0) {
      return target()->NewWritableFile(path, opts, output, dbg);
    }
    if (opts.use_direct_writes || opts.use_mmap_writes) {
      return rdb::status_to_io_status(
          rdb::Status::NotSupported("WAL mirror requires buffered file I/O"));
    }
    Event event(Kind::kCreate, wal.number, wal.name);
    std::lock_guard lock(state_->operations_);
    auto status = state_->Reserve(event);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<rdb::FSWritableFile> file;
    status = state_->FinishAndSignal(
        std::move(event), target()->NewWritableFile(path, opts, &file, dbg));
    if (status.ok()) {
      WrapFile(file, std::move(wal), output, opts.io_options, dbg);
    }
    return status;
  } catch (const std::bad_alloc&) {
    return state_->AllocationFailure();
  }

  rdb::IOStatus ReopenWritableFile(const std::string& path,
                                   const rdb::FileOptions& opts,
                                   std::unique_ptr<rdb::FSWritableFile>* output,
                                   rdb::IODebugContext* dbg) override try {
    auto wal = Match(path);
    if (wal.number == 0) {
      return target()->ReopenWritableFile(path, opts, output, dbg);
    }
    if (opts.use_direct_writes || opts.use_mmap_writes) {
      return rdb::status_to_io_status(
          rdb::Status::NotSupported("WAL mirror requires buffered file I/O"));
    }
    std::lock_guard lock(state_->operations_);
    if (state_->queue_.Closed()) {
      return state_->ClosedError();
    }
    std::unique_ptr<rdb::FSWritableFile> file;
    auto status = target()->FileExists(path, opts.io_options, dbg);
    if (status.ok()) {
      status =
          state_->Observe(target()->ReopenWritableFile(path, opts, &file, dbg));
    } else if (Missing(status)) {
      // The source filesystem can create an absent file on reopen.
      Event event(Kind::kCreate, wal.number, wal.name);
      status = state_->Reserve(event);
      if (!status.ok()) {
        return status;
      }
      status = state_->FinishAndSignal(
          std::move(event),
          target()->ReopenWritableFile(path, opts, &file, dbg));
    } else {
      return state_->Observe(std::move(status));
    }
    if (status.ok()) {
      WrapFile(file, std::move(wal), output, opts.io_options, dbg);
    }
    return status;
  } catch (const std::bad_alloc&) {
    return state_->AllocationFailure();
  }

  rdb::IOStatus ReuseWritableFile(const std::string& path,
                                  const std::string& old_path,
                                  const rdb::FileOptions& opts,
                                  std::unique_ptr<rdb::FSWritableFile>* output,
                                  rdb::IODebugContext* dbg) override try {
    if (Match(path).number != 0 || Match(old_path).number != 0) {
      return rdb::status_to_io_status(
          rdb::Status::NotSupported("WAL recycling with operation mirroring"));
    }
    return target()->ReuseWritableFile(path, old_path, opts, output, dbg);
  } catch (const std::bad_alloc&) {
    return state_->AllocationFailure();
  }

  rdb::IOStatus SyncFile(const std::string& path,
                         const rdb::FileOptions& file_opts,
                         const rdb::IOOptions& opts, bool use_fsync,
                         rdb::IODebugContext* dbg) override try {
    if (Match(path).number != 0) {
      return state_->Observe(
          target()->SyncFile(path, file_opts, opts, use_fsync, dbg));
    }
    return target()->SyncFile(path, file_opts, opts, use_fsync, dbg);
  } catch (const std::bad_alloc&) {
    return state_->AllocationFailure();
  }

 private:
  void WrapFile(std::unique_ptr<rdb::FSWritableFile>& file, WalPath&& wal,
                std::unique_ptr<rdb::FSWritableFile>* output,
                const rdb::IOOptions& opts, rdb::IODebugContext* dbg) {
    try {
      *output = std::make_unique<CaptureFile>(std::move(file), state_,
                                              std::move(wal));
    } catch (const std::bad_alloc&) {
      if (file) {
        file->Close(opts, dbg).PermitUncheckedError();
      }
      throw;
    }
  }

  fs::path Normalize(const fs::path& path) const {
    return (path.is_absolute() ? path : working_dir_ / path).lexically_normal();
  }

  WalPath Match(std::string_view path) const {
    if (!path.ends_with(".log")) {
      return {};
    }
    const auto normalized = Normalize(fs::path(path));
    const auto parent = normalized.parent_path();
    if (parent != source_dir_ && parent != archive_dir_) {
      return {};
    }
    return ParseWalName(normalized.filename().string());
  }

  const std::shared_ptr<State> state_;
  const fs::path working_dir_;
  const fs::path source_dir_;
  const fs::path archive_dir_;
};

WalOperationMirror::WalOperationMirror(
    std::shared_ptr<rdb::FileSystem> source_fs,
    std::shared_ptr<rdb::FileSystem> mirror_fs, fs::path source_wal_dir,
    fs::path mirror_dir, WalOperationMirrorOptions options) {
  if (!source_fs) {
    throw std::invalid_argument(
        "WAL operation mirror requires a source filesystem");
  }
  state_ = std::make_shared<State>(std::move(mirror_fs), mirror_dir, options);
  capture_fs_ = std::make_shared<State::CaptureFS>(std::move(source_fs), state_,
                                                   source_wal_dir);
  state_->Start();
}

WalOperationMirror::~WalOperationMirror() { Close().PermitUncheckedError(); }

std::shared_ptr<rdb::FileSystem> WalOperationMirror::CaptureFileSystem() const {
  return capture_fs_;
}

void WalOperationMirror::AdvanceFloor(uint64_t floor) {
  state_->AdvanceFloor(floor);
}

rdb::Status WalOperationMirror::Close() { return state_->Close(); }

rdb::Status WalOperationMirror::GetError() const { return state_->GetError(); }

}  // namespace mb_ycsb
