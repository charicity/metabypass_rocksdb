//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <version>

#include "rocksdb/metadata.h"
#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/native_files.h"

// Some libc++ versions advertise atomic waiting even when the deployment
// target predates the required runtime symbols.
#if defined(__cpp_lib_atomic_wait) &&                             \
    (!defined(__ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__) ||   \
     __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ >= 110000) &&  \
    (!defined(__ENVIRONMENT_IPHONE_OS_VERSION_MIN_REQUIRED__) ||  \
     __ENVIRONMENT_IPHONE_OS_VERSION_MIN_REQUIRED__ >= 140000) && \
    (!defined(__ENVIRONMENT_TV_OS_VERSION_MIN_REQUIRED__) ||      \
     __ENVIRONMENT_TV_OS_VERSION_MIN_REQUIRED__ >= 140000) &&     \
    (!defined(__ENVIRONMENT_WATCH_OS_VERSION_MIN_REQUIRED__) ||   \
     __ENVIRONMENT_WATCH_OS_VERSION_MIN_REQUIRED__ >= 70000)
#define ROCKSDB_MB_WRITE_GATE_ATOMIC_WAIT 1
#else
#define ROCKSDB_MB_WRITE_GATE_ATOMIC_WAIT 0
#endif

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
class SstStorage;
// A single validator closes writer admission and waits for admitted writers.
class WriteGate {
 public:
  class Writer {
   public:
    explicit Writer(WriteGate& gate) : gate_(gate) { gate_.Admit(); }
    ~Writer() { gate_.Leave(); }
    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

   private:
    WriteGate& gate_;
  };

  class Closure {
   public:
    explicit Closure(WriteGate& gate) : gate_(gate) { gate_.CloseAndDrain(); }
    ~Closure() { gate_.Reopen(); }
    Closure(const Closure&) = delete;
    Closure& operator=(const Closure&) = delete;

   private:
    WriteGate& gate_;
  };

 private:
  void Admit();
  void Leave();
  void CloseAndDrain();
  void Reopen();
  void Wait(uint64_t observed, std::memory_order order);
  void Notify();

  static constexpr uint64_t kClosed = uint64_t{1} << 63;
  std::atomic<uint64_t> state_{0};  // Closed bit and admitted writer count.
#if !ROCKSDB_MB_WRITE_GATE_ATOMIC_WAIT
  std::mutex wait_mutex_;
  std::condition_variable wait_cv_;
#endif
};

class Backup : public FileSystemWrapper {
 public:
  Backup(std::shared_ptr<FileSystem> fs, std::string index,
         MetaBypassOptions options, std::shared_ptr<SstStorage> sst = nullptr);
  ~Backup() override;
  const char* Name() const override { return "MetaBypassBackup"; }
  Status Lock();
  Status LoadLatest();
  Status Start(bool existing);
  Status Activate(const std::string& identity, DB* db,
                  std::shared_ptr<SystemClock> clock);
  void WriteCompleted();
  WriteGate::Writer EnterWrite() { return WriteGate::Writer(write_gate_); }
  // Listener notification only; never performs I/O or waits for a checkpoint.
  void NativeFilesChanged();
  void DetachDB();
  Status Sync();
  Status Stop();
  MetaBypassStats Stats() const;
  Status Error() const;
  void StorageFailed(const Status& s) { Fail(s); }
  struct RecoveryPoint {
    std::string name;
    std::string identity;
    std::map<std::string, uint64_t> files;
    uint64_t sequence = 0, wal_floor = 0;
  };
  // Read and verify the published point while holding the backup lock.
  static Status ReadRecoveryPoint(FileSystem*, const std::string& backup,
                                  RecoveryPoint*);
  static Status PrepareRecovery(FileSystem*, const std::string& backup,
                                const RecoveryPoint& point,
                                const std::string& destination,
                                MetaBypassRecoveryReport* report,
                                SstStorage* sst);
  // Estimation only: malformed, nonmonotone or clock-discontinuous samples
  // leave the report bounds unknown and never change the recovered prefix.
  static void EstimateRecoveryTime(const std::string& samples,
                                   MetaBypassRecoveryReport* report);
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
  friend class BackupTestAccess;
  enum class Kind { kCreate, kAppend, kTruncate, kClose, kDelete, kRename };
  // Preserve the queue capacity contract across changes to payload storage.
  struct ChargedEventLayout {
    Kind kind;
    std::string name, other, bytes;
    uint64_t size = 0;
    uint64_t seq = 0;
    size_t charge = 0;
  };
  struct Event {
    Kind kind;
    std::string name;
    std::string contents;  // Rename target or unpooled append bytes.
    std::shared_ptr<const char> shared_bytes{};
    // Shared payload length or truncate offset.
    uint64_t size = 0;
    uint64_t seq = 0;
    size_t charge = 0;
    Slice Payload() const {
      assert(kind == Kind::kAppend);
      return shared_bytes ? Slice(shared_bytes.get(), size) : Slice(contents);
    }
  };
  struct WalBlock {
    // Each slice is fully initialized before its event is enqueued.
    WalBlock() {}
    char data[64 * 1024];
  };
  struct Channel {
    // Serializes primary operations through enqueue. Never acquired by workers.
    std::mutex operations;
  };
  struct MetadataChannel : Channel {
    // Protected by Backup::mutex_.
    std::deque<Event> queue;
    std::condition_variable capacity;
    size_t waiting_charge = 0;
  };
  struct WalQueue {
    static constexpr size_t kSlots = 256;
    static constexpr uint64_t kClosed = uint64_t{1} << 63;
    struct Segment {
      std::array<std::optional<Event>, kSlots> events;
      std::unique_ptr<Segment> next;
    };
    WalQueue();

    uint64_t Published() const { return published.load() & ~kClosed; }
    void Prepare();
    bool Publish(Event&& event);
    const Event& Front() const;
    Event Pop();
    uint64_t ChargedBytes() const;

    // Shared publication state. Closing seals the last published slot.
    std::atomic<uint64_t> published{0};
    std::atomic<uint64_t> reserved{0};
    std::atomic<uint64_t> returned{0};
    std::atomic<uint64_t> mirrored_bytes{0};
    std::atomic<uint64_t> peak{0};
    std::atomic<uint64_t> backpressure_micros{0};

    // Producer state is serialized by wal_channel_.operations.
    Segment* tail;
    size_t tail_slot = 0;
    uint64_t tail_count = 0;
    uint64_t cached_returned = 0;
    uint64_t reserved_total = 0;

    // Only the mirror advances or frees the consumer side.
    std::unique_ptr<Segment> head;
    size_t head_slot = 0;

    std::mutex space_mutex;
    std::condition_variable space;
    uint64_t space_return_target = 0;  // Protected by space_mutex.
    std::atomic<bool> waiting_for_space{false};
  };
  Channel& ChannelFor(const std::string& name);
  bool Tracked(const std::string& path, bool include_tables = false) const;
  std::string Base(const std::string& path) const;
  Status Reserve(Channel& channel, size_t charge);
  bool IsWalChannel(const Channel& channel) const;
  Status ReserveWal(size_t charge);
  void CloseWalQueue();
  void ReturnWalBytes(uint64_t charge, uint64_t mirrored_bytes = 0);
  void Fail(const Status& status);
  void Finish(Channel& channel, Event&& event, const Status& primary);
  // Caller still holds wal_channel_.operations. A true return requests a wake.
  bool FinishWal(Event&& event, const Status& primary);
  Status Apply(const Event& event);
  Status BeforeApply(const Event& event);
  Status Append(const std::string& name, const Slice& bytes);
  // Only the mirror touches the WAL consumer side, using the bounds selected
  // under mutex_.
  void TakeWalBatch(uint64_t first, uint64_t last, std::vector<Event>* batch);
  // No queue lock. completed counts only successfully written logical events.
  Status ApplyBatch(const std::vector<Event>& batch, std::string* scratch,
                    size_t* completed);
  struct Candidate {
    uint64_t seq = 0, started = 0, oldest = 0, work_bytes = 0;
    uint64_t writes = 0, request = 0, native_changes = 0;
    uint64_t native_sequence = 0, wal_floor = 0;
    std::vector<LiveFileStorageInfo> files;
    std::string name, path;
    // Hard links to immutable tables already synced by the published point.
    std::set<uint64_t> durable_tables;
  };
  Status Capture(Candidate* candidate);
  Status CaptureTables(Candidate* candidate);
  Status PruneWal(uint64_t floor);
  Status Publish(const Candidate& candidate);
  // Called with mutex_ held. Each wait channel has a distinct predicate.
  bool MirrorReady() const;
  uint64_t Accepted() const;
  uint64_t QueuedBytes() const;
  void WaitForMirror(std::unique_lock<std::mutex>& lock);
  void SignalMirror();
  void UpdateWalWakeCharge();
  void WakeMirror();
  void WakeMetadataCapacity();
  Channel* NextChannel(uint64_t wal_tail);
  uint64_t FrontSequence(const Channel& channel) const;
  uint64_t WalBatchLimit(uint64_t first, uint64_t boundary) const;
  Event TakeMetadataEvent();
  bool QueuesEmpty() const;
  void Run();
  void ValidateLoop();
  void SampleLoop();
  void SampleProgress();
  void GarbageCollectLoop();
  void MarkDirty();  // Requires mutex_. Keeps the first pending deadline.
  struct Point {
    std::string name;
    uint64_t bytes = 0;
    uint64_t wal_floor = 0;
  };
  const std::shared_ptr<FileSystem> fs_;
  SstStorage* const sst_;
  FileSystem* disk_;
  const std::string index_;
  const MetaBypassOptions options_;
  const std::string work_;
  // Only WAL producers access the current block and cursor, under operations.
  std::shared_ptr<WalBlock> wal_block_;
  size_t wal_block_used_ = 0;
  Channel wal_channel_;
  MetadataChannel metadata_channel_;
  WalQueue wal_queue_;
  // Notifications persist across the mirror checking and going to sleep.
  std::mutex mirror_sleep_mutex_;
  std::condition_variable mirror_cv_;
  std::atomic<bool> mirror_signal_{false};
  std::atomic<uint64_t> wal_wake_charge_{0};
  // Orders failure recording with pointer commit, without holding the queue
  // mutex across filesystem I/O. Never acquired by successful producers.
  std::mutex publication_mutex_;
  WriteGate write_gate_;
  mutable std::mutex mutex_;
  std::condition_variable validator_cv_, sync_cv_, gc_cv_;
  bool mirror_waiting_ = false;
  bool prune_wal_ = false;
  // Queue charges, backpressure and mirrored bytes here account for metadata.
  // Stats combines them with the counters in wal_queue_.
  MetaBypassStats stats_;
  std::atomic<bool> failed_{false};
  // accepted_ counts metadata events. Accepted() includes the WAL tail.
  uint64_t accepted_ = 0, applied_ = 0, published_ = 0, requested_ = 0;
  uint64_t wal_taken_ = 0;  // Protected by mutex_, including failed events.
  uint64_t generation_ = 0;
  DB* db_ = nullptr;
  std::shared_ptr<SystemClock> clock_;
  std::string identity_;
  // Writers increment while admitted. Capture closes admission and drains
  // all admitted writers before reading the count and the native sequence.
  std::atomic<uint64_t> completed_writes_{0};
  uint64_t published_writes_ = 0;
  uint64_t native_changes_ = 0, published_native_changes_ = 0;
  uint64_t requested_point_ = 0, published_request_ = 0;
  bool capture_done_ = false;
  Status capture_status_;
  // Only modified under mutex_. Writers can check for a pending deadline
  // without taking the queue lock.
  std::atomic<uint64_t> dirty_since_micros_{0};
  bool stopping_ = false;
  bool active_ = false;
  bool candidate_busy_ = false, mirror_done_ = false;
  bool validator_done_ = false;
  std::unique_ptr<Candidate> candidate_;
  std::thread thread_, validator_, garbage_collector_, sampler_;
  std::map<std::string, std::unique_ptr<FSWritableFile>> writers_;
  std::optional<Point> published_point_;
  std::optional<Point> retired_point_;
  FileLock* lock_ = nullptr;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
