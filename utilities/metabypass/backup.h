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
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <thread>

#include "rocksdb/metadata.h"
#include "rocksdb/utilities/metabypass.h"
#include "utilities/metabypass/native_files.h"
#include "utilities/metabypass/separated_storage.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
class Backup : public FileSystemWrapper {
 public:
  Backup(std::shared_ptr<SeparatedStorage> storage, std::string index,
         MetaBypassOptions options, Options db_options = Options());
  ~Backup() override;
  const char* Name() const override { return "MetaBypassBackup"; }
  Status Lock();
  Status Start(bool existing);
  Status Activate(const std::string& identity, DB* db);
  void WriteCompleted();
  std::shared_mutex& WriteFence() { return write_fence_; }
  // Listener notification only; never performs I/O or waits for a checkpoint.
  void NativeFilesChanged();
  void DetachDB();
  Status Sync();
  Status Stop();
  MetaBypassStats Stats() const;
  Status Error() const;
  void StorageFailed(const Status& s) { Fail(s); }
  // Read-only protocol validation before Restore creates any directories.
  static Status CheckRestoreFormat(FileSystem*, const std::string& backup);
  static Status PreflightCheckpoint(FileSystem*, const std::string& backup,
                                    const SeparatedStorage& storage);
  static Status PrepareRecovery(FileSystem*, const std::string& backup,
                                const std::string& destination,
                                std::string* point,
                                MetaBypassRecoveryReport* report,
                                bool* has_boundary, SstStorage* sst = nullptr);
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
  static Status PreflightCheckpointImpl(FileSystem*, const std::string&,
                                        const SeparatedStorage&);
  friend class MirrorWriter;
  friend class BackupTestAccess;
  enum class Kind {
    kCreate,
    kAppend,
    kTruncate,
    kClose,
    kDelete,
    kRename,
    kRetireBlob
  };
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
    // Shared payload length, truncate offset, or retired blob number.
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
    uint64_t head_count = 0;

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
  void ReturnWalBytes(uint64_t charge);
  void Fail(const Status& status);
  void Finish(Channel& channel, Event&& event, const Status& primary);
  Status Apply(const Event& event);
  Status BeforeApply(const Event& event);
  Status Append(const std::string& name, const Slice& bytes);
  // mutex_ held; consumes only events within the existing drain window.
  void TakeBatch(Channel* channel, uint64_t boundary,
                 std::vector<Event>* batch);
  // No queue lock. completed counts only successfully written logical events.
  Status ApplyBatch(const std::vector<Event>& batch, std::string* scratch,
                    size_t* completed);
  struct Candidate {
    uint64_t seq = 0, started = 0, oldest = 0, work_bytes = 0;
    uint64_t writes = 0, request = 0, native_changes = 0;
    uint64_t native_sequence = 0, blob_floor = 0, wal_floor = 0;
    std::vector<LiveFileStorageInfo> files;
    std::map<uint64_t, uint64_t> blobs;
    std::string name, path;
    NativeState state;
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
  Channel* NextChannel();
  uint64_t FrontSequence(const Channel& channel) const;
  Event TakeEvent(Channel& channel, uint64_t seq);
  bool QueuesEmpty() const;
  void Run();
  void ValidateLoop();
  void SampleLoop();
  void SampleProgress();
  void GarbageCollectLoop();
  bool BlobEligible(uint64_t number, uint64_t seq) const;
  bool NeedsBlobCheckpoint() const;
  void MarkDirty();  // Requires mutex_. Keeps the first pending deadline.
  struct Point {
    std::string reference;
    uint64_t bytes = 0;
    std::set<uint64_t> blobs;
    uint64_t blob_floor = 0, wal_floor = 0;
    std::string Name() const {
      return reference.substr(0, reference.find(' '));
    }
  };
  std::shared_ptr<SeparatedStorage> storage_;
  FileSystem* disk_;
  const std::string index_;
  const MetaBypassOptions options_;
  const Options db_options_;
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
  std::shared_mutex write_fence_;
  mutable std::mutex mutex_;
  std::condition_variable validator_cv_, sync_cv_, gc_cv_;
  bool mirror_waiting_ = false;
  bool prune_wal_ = false;
  // Queue charges and backpressure here account for metadata. Stats combines
  // them with the counters in wal_queue_.
  MetaBypassStats stats_;
  std::atomic<bool> failed_{false};
  // accepted_ counts metadata events. Accepted() includes the WAL tail.
  uint64_t accepted_ = 0, applied_ = 0, published_ = 0, requested_ = 0;
  uint64_t generation_ = 0;
  DB* db_ = nullptr;
  std::string identity_;
  // Writers increment under the shared write fence. Capture reads the final
  // count under the exclusive fence, alongside the native sequence.
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
  bool validator_done_ = false, retired_pending_ = false;
  uint64_t captured_ = 0;
  std::unique_ptr<Candidate> candidate_;
  std::thread thread_, validator_, garbage_collector_, sampler_;
  std::map<std::string, std::unique_ptr<FSWritableFile>> writers_;
  std::deque<Point> points_;
  Point retired_;
  struct RetiredBlob {
    uint64_t seq = 0, bytes = 0;
  };
  // All recovery roots, pending retirements and the capture/delete gate use
  // mutex_. Membership includes queued and in-flight retirement requests.
  std::set<uint64_t> retiring_blobs_;
  std::map<uint64_t, RetiredBlob> pending_blobs_;
  bool blob_gc_busy_ = false;
  FileLock* lock_ = nullptr;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
