//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "rocksdb/utilities/metabypass.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/wal_filter.h"
#include "rocksdb/write_batch.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/metabypass/backup.h"
#include "utilities/metabypass/tiered_storage.h"

#ifndef OS_WIN
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
// Internal protocol tests use the real selector, apply path, and drain loop.
class BackupTestAccess {
 public:
  static void SeedBlobGC(Backup* backup, uint64_t count) {
    backup->points_.push_back({"point", 0, {}, count + 1, 1});
    backup->published_ = 1;
    for (uint64_t number = 1; number <= count; ++number) {
      backup->pending_blobs_[number] = {1, 4};
      backup->retiring_blobs_.insert(number);
    }
    backup->stats_.pending_blob_gc_files = count;
    backup->stats_.pending_blob_gc_bytes = count * 4;
  }
  static void FinishBlobGC(Backup* backup) {
    std::lock_guard<std::mutex> lock(backup->mutex_);
    backup->validator_done_ = true;
    backup->gc_cv_.notify_all();
  }
  static void RunBlobGC(Backup* backup) { backup->GarbageCollectLoop(); }
  static void CheckIdlePrune(Backup* backup) {
    ASSERT_OK(EnsureDir(backup->disk_, backup->options_.backup_dir));
    ASSERT_OK(EnsureDir(backup->disk_, backup->work_));
    ASSERT_OK(
        EnsureDir(backup->disk_, backup->options_.backup_dir + "/point-old"));
    ASSERT_OK(Write(backup->disk_, backup->work_ + "/000001.log", "obsolete"));
    ASSERT_OK(Write(backup->disk_, backup->work_ + "/000002.log", "live"));
    backup->points_.push_back({"point-new", 0, {}, 0, 2});
    backup->retired_ = {"point-old", 0, {}, 0, 1};
    backup->retired_pending_ = true;
    backup->validator_done_ = true;
    ASSERT_OK(backup->PruneWal(2));
    ASSERT_OK(backup->disk_->FileExists(backup->work_ + "/000001.log",
                                        IOOptions(), nullptr));
    backup->GarbageCollectLoop();
    ASSERT_TRUE(backup->prune_wal_);
    ASSERT_TRUE(backup->MirrorReady());
    backup->stopping_ = true;
    backup->Run();
    ASSERT_TRUE(
        backup->disk_
            ->FileExists(backup->work_ + "/000001.log", IOOptions(), nullptr)
            .IsNotFound());
    ASSERT_OK(backup->disk_->FileExists(backup->work_ + "/000002.log",
                                        IOOptions(), nullptr));
  }
  static std::weak_ptr<void> CurrentWalBlock(Backup* backup) {
    std::lock_guard<std::mutex> lock(backup->wal_channel_.operations);
    return backup->wal_block_;
  }
  static size_t EventHeaderCharge() {
    return sizeof(Backup::ChargedEventLayout);
  }
  static std::unique_lock<std::mutex> LockQueueMutex(Backup* backup) {
    return std::unique_lock<std::mutex>(backup->mutex_);
  }
  struct Input {
    std::string name = "000001.log";
    std::string bytes = "x";
    char operation = 'a';
  };
  struct RecordingFile : FSWritableFile {
    std::vector<std::string>* calls;
    std::string* content;
    bool partial;
    RecordingFile(std::vector<std::string>* c, std::string* b, bool p)
        : calls(c), content(b), partial(p) {}
    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions&,
                    IODebugContext*) override {
      calls->push_back(data.ToString());
      content->append(data.data(), partial ? data.size() / 2 : data.size());
      return partial ? IOStatus::IOError("partial batch write")
                     : IOStatus::OK();
    }
    uint64_t GetFileSize(const IOOptions&, IODebugContext*) override {
      return content->size();
    }
    IOStatus Close(const IOOptions&, IODebugContext*) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions&, IODebugContext*) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions&, IODebugContext*) override {
      return IOStatus::OK();
    }
  };
  static void Load(Backup* backup, const std::vector<Input>& input) {
    for (const auto& item : input) {
      Backup::Kind kind =
          item.operation == 'a' ? Backup::Kind::kAppend : Backup::Kind::kClose;
      Backup::Event event{kind, item.name, item.bytes};
      event.charge = sizeof(Backup::ChargedEventLayout) + event.name.size() +
                     item.bytes.size();
      auto& channel = backup->ChannelFor(event.name);
      ASSERT_OK(backup->Reserve(channel, event.charge));
      backup->Finish(channel, std::move(event), Status::OK());
    }
  }
  static std::vector<size_t> Select(Backup* backup,
                                    const std::vector<Input>& input,
                                    uint64_t boundary, uint64_t capture) {
    Load(backup, input);
    if (capture != 0) {
      backup->candidate_ = std::make_unique<Backup::Candidate>();
      backup->candidate_->seq = capture;
    }
    std::vector<Backup::Event> batch;
    std::vector<size_t> sizes;
    const auto wal_tail = backup->wal_queue_.Published();
    boundary = std::min(boundary, backup->Accepted());
    while (auto* channel = backup->NextChannel(wal_tail)) {
      const auto first = backup->FrontSequence(*channel);
      if (first > boundary) break;
      if (backup->IsWalChannel(*channel)) {
        batch.clear();
        const auto last = backup->WalBatchLimit(first, boundary);
        backup->TakeWalBatch(first, last, &batch);
        backup->wal_taken_ += batch.size();
        sizes.push_back(batch.size());
      } else {
        backup->TakeMetadataEvent();
        sizes.push_back(1);
      }
    }
    return sizes;
  }
  static void Drain(Backup* backup, const std::vector<Input>& input,
                    bool partial, size_t successful,
                    const std::vector<std::string>& expected_calls) {
    ASSERT_NO_FATAL_FAILURE(Load(backup, input));
    std::vector<std::string> calls;
    std::string bytes;
    backup->writers_["000001.log"] =
        std::make_unique<RecordingFile>(&calls, &bytes, partial);
    // Stop drains the queued work through the same production Run loop.
    backup->stopping_ = true;
    backup->Run();
    EXPECT_EQ(backup->applied_, successful);
    const auto stats = backup->Stats();
    EXPECT_EQ(stats.queued_bytes, 0U);
    uint64_t mirrored_bytes = 0;
    for (size_t i = 0; i < successful; ++i) {
      mirrored_bytes += input[i].bytes.size();
    }
    EXPECT_EQ(stats.mirrored_wal_bytes, mirrored_bytes);
    EXPECT_EQ(calls, expected_calls);
    if (partial) {
      EXPECT_EQ(stats.error.ToString(),
                Status::IOError("partial batch write").ToString());
      ASSERT_EQ(calls.size(), 1U);
      EXPECT_EQ(bytes, calls.front().substr(0, calls.front().size() / 2));
    } else if (successful != input.size()) {
      EXPECT_TRUE(stats.error.IsIOError());
    } else {
      EXPECT_OK(stats.error);
      std::string expected;
      for (const auto& item : input) expected += item.bytes;
      EXPECT_EQ(bytes, expected);
    }
  }

  struct Readiness {
    uint64_t published, requested, applied, accepted;
    bool ready, capture, capacity, size, empty, stopping, error;
  };
  static void CheckDirtyWriteCompletions(Backup* backup) {
    auto finish_write = [backup] {
      auto writer = backup->EnterWrite();
      backup->WriteCompleted();
    };
    finish_write();
    std::unique_lock<std::mutex> queue(backup->mutex_);
    const auto first_deadline = backup->dirty_since_micros_.load();
    ASSERT_NE(first_deadline, 0U);
    // While the event queue is locked, already-dirty writers must still
    // complete and retain the first deadline.
    auto a = std::async(std::launch::async, finish_write);
    auto b = std::async(std::launch::async, finish_write);
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    EXPECT_EQ(a.wait_until(until), std::future_status::ready);
    EXPECT_EQ(b.wait_until(until), std::future_status::ready);
    queue.unlock();
    a.get();
    b.get();
    WriteGate::Closure closure(backup->write_gate_);
    EXPECT_EQ(backup->completed_writes_.load(), 3U);
    EXPECT_EQ(backup->dirty_since_micros_.load(), first_deadline);
  }
  static void CheckIndependentReasons(Backup* backup) {
    EXPECT_FALSE(backup->MirrorReady());
    backup->requested_ = 1;
    // A request without events must not spin; Capture is a separate trigger.
    EXPECT_FALSE(backup->MirrorReady());
    ASSERT_NO_FATAL_FAILURE(Load(backup, {Input{}}));
    EXPECT_TRUE(backup->MirrorReady());
    backup->applied_ = 1;
    // Directly distinguish the old and new request predicates.
    EXPECT_GT(backup->requested_, backup->published_);
    EXPECT_FALSE(backup->requested_ > backup->applied_);
    EXPECT_FALSE(backup->MirrorReady());
    backup->published_ = 1;
    EXPECT_FALSE(backup->MirrorReady());
    backup->wal_queue_.waiting_for_space.store(true);
    EXPECT_TRUE(backup->MirrorReady());
    backup->wal_queue_.waiting_for_space.store(false);
    backup->metadata_channel_.waiting_charge = 1;
    EXPECT_TRUE(backup->MirrorReady());
    backup->metadata_channel_.waiting_charge = 0;
    const auto charge = backup->QueuedBytes();
    ASSERT_LT(charge, backup->options_.batch_bytes);
    backup->stats_.queued_bytes = backup->options_.batch_bytes - charge;
    EXPECT_TRUE(backup->MirrorReady());
    backup->stats_.queued_bytes = 0;
    backup->wal_queue_.Pop();
    ++backup->wal_taken_;
    backup->ReturnWalBytes(charge);
    backup->candidate_ = std::make_unique<Backup::Candidate>();
    EXPECT_TRUE(backup->MirrorReady());
    backup->capture_done_ = true;
    EXPECT_FALSE(backup->MirrorReady());
    backup->candidate_.reset();
    backup->stopping_ = true;
    EXPECT_TRUE(backup->MirrorReady());
    backup->stopping_ = false;
    backup->stats_.error = Status::IOError("injected");
    EXPECT_TRUE(backup->MirrorReady());
    backup->stats_.error = Status::OK();
  }
  // Called by MirrorReadyProbe with the queue mutex held. Never waits or does
  // I/O.
  static Readiness Observe(Backup* backup) {
    return {backup->published_,
            backup->requested_,
            backup->applied_,
            backup->Accepted(),
            backup->MirrorReady(),
            backup->candidate_ && !backup->capture_done_,
            backup->wal_queue_.waiting_for_space.load() ||
                backup->metadata_channel_.waiting_charge != 0,
            backup->QueuedBytes() >= backup->options_.batch_bytes,
            backup->QueuesEmpty(),
            backup->stopping_,
            !backup->stats_.error.ok()};
  }
};
}  // namespace metabypass
namespace {
std::string executable;
MetaBypassOptions TestMetaBypassOptions() {
  MetaBypassOptions options;
  options.wal_queue_capacity = options.metadata_queue_capacity =
      64 * 1024 * 1024;
  options.batch_bytes = 256 * 1024;
  options.interval_ms = 1000;
  options.checkpoint_interval_ms = 30000;
  return options;
}

size_t QueueBudget(const MetaBypassOptions& options) {
  return options.wal_queue_capacity + options.metadata_queue_capacity;
}

// Fail after truncation to expose destructive rewrites of protocol markers.
class MarkerFailureFileSystem : public FileSystemWrapper {
 public:
  explicit MarkerFailureFileSystem(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}
  const char* Name() const override { return "MarkerFailureFileSystem"; }
  std::string fail_path;
  int failures = 0;
  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* dbg) override {
    IOStatus s = target()->NewWritableFile(path, options, file, dbg);
    if (s.ok() && path == fail_path) {
      (*file)->Close(IOOptions(), dbg).PermitUncheckedError();
      file->reset();
      ++failures;
      return IOStatus::IOError("injected failure after truncation");
    }
    return s;
  }
};
class CandidateNoLinkFileSystem : public FileSystemWrapper {
 public:
  explicit CandidateNoLinkFileSystem(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}
  const char* Name() const override { return "CandidateNoLinkFileSystem"; }
  std::atomic<uint64_t> rejected_links{0}, copied_tables{0};
  std::atomic<uint64_t> reused_tables{0}, synced_tables{0};
  std::atomic<bool> reject_reuse{false}, reject_native{true}, fail_sync{false};
  IOStatus LinkFile(const std::string& from, const std::string& to,
                    const IOOptions& options, IODebugContext* dbg) override {
    const bool reuse = from.find("/point-") != std::string::npos;
    if (((reject_native.load() && !reuse) || (reuse && reject_reuse.load())) &&
        to.find("/point-") != std::string::npos) {
      ++rejected_links;
      return IOStatus::NotSupported("injected candidate link failure");
    }
    IOStatus s = target()->LinkFile(from, to, options, dbg);
    if (s.ok() && reuse) ++reused_tables;
    return s;
  }
  IOStatus SyncFile(const std::string& path, const FileOptions& file_options,
                    const IOOptions& options, bool fsync,
                    IODebugContext* dbg) override {
    if (path.find("/point-") != std::string::npos && path.size() >= 4 &&
        path.substr(path.size() - 4) == ".sst") {
      ++synced_tables;
      if (fail_sync.load()) return IOStatus::IOError("candidate table sync");
    }
    return target()->SyncFile(path, file_options, options, fsync, dbg);
  }
  IOStatus NewSequentialFile(const std::string& path,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    if (path.find("/point-") == std::string::npos && path.size() >= 4 &&
        path.substr(path.size() - 4) == ".sst")
      ++copied_tables;
    return target()->NewSequentialFile(path, options, result, dbg);
  }
};
// Observe real replacement-file operations, distinguishing capture creation
// from publication reopening. The hook runs outside the counting mutex.
class ReplacementFileSystem : public FileSystemWrapper {
 public:
  explicit ReplacementFileSystem(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}
  const char* Name() const override { return "ReplacementFileSystem"; }
  std::function<IOStatus(const std::string&)> hook;
  uint64_t Count(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return counts_[key];
  }
  IOStatus Operation(const std::string& key) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++counts_[key];
    }
    return hook ? hook(key) : IOStatus::OK();
  }
  IOStatus GetFileSize(const std::string& path, const IOOptions& o,
                       uint64_t* size, IODebugContext* d) override {
    IOStatus s = Operation("size." + path);
    return s.ok() ? target()->GetFileSize(path, o, size, d) : s;
  }
  IOStatus DeleteFile(const std::string& path, const IOOptions& o,
                      IODebugContext* d) override {
    IOStatus s = Operation("delete." + path);
    return s.ok() ? target()->DeleteFile(path, o, d) : s;
  }
  IOStatus SyncFile(const std::string& path, const FileOptions& f,
                    const IOOptions& o, bool full, IODebugContext* d) override {
    IOStatus s = Operation("sync." + path);
    return s.ok() ? target()->SyncFile(path, f, o, full, d) : s;
  }
  IOStatus NewDirectory(const std::string& path, const IOOptions& o,
                        std::unique_ptr<FSDirectory>* out,
                        IODebugContext* d) override {
    class Directory : public FSDirectoryWrapper {
     public:
      Directory(std::unique_ptr<FSDirectory> dir, ReplacementFileSystem* fs,
                std::string path)
          : FSDirectoryWrapper(std::move(dir)),
            fs_(fs),
            path_(std::move(path)) {}
      IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
        IOStatus s = fs_->Operation("dirsync." + path_);
        return s.ok() ? FSDirectoryWrapper::Fsync(options, dbg) : s;
      }

     private:
      ReplacementFileSystem* fs_;
      std::string path_;
    };
    IOStatus s = target()->NewDirectory(path, o, out, d);
    if (s.ok()) out->reset(new Directory(std::move(*out), this, path));
    return s;
  }
  class File : public FSWritableFileWrapper {
   public:
    using FSWritableFileWrapper::Append;
    File(std::unique_ptr<FSWritableFile> file, ReplacementFileSystem* fs,
         std::string prefix, bool candidate)
        : FSWritableFileWrapper(file.get()),
          owned_(std::move(file)),
          fs_(fs),
          prefix_(std::move(prefix)),
          candidate_(candidate) {}
    IOStatus Append(const Slice& data, const IOOptions& o,
                    IODebugContext* d) override {
      IOStatus s = fs_->Operation(prefix_ + ".append");
      return s.ok() ? target()->Append(data, o, d) : s;
    }
    IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
      IOStatus s = fs_->Operation(prefix_ + ".fsync");
      if (s.ok()) s = target()->Fsync(o, d);
      synced_ = s.ok();
      return s;
    }
    IOStatus InvalidateCache(size_t offset, size_t length) override {
      EXPECT_TRUE(candidate_);
      EXPECT_TRUE(synced_);
      EXPECT_EQ(prefix_.compare(0, 8, "publish."), 0);
      EXPECT_EQ(offset, 0U);
      EXPECT_EQ(length, target()->GetFileSize(IOOptions(), nullptr));
      uint64_t number;
      FileType type;
      if (ParseFileName(prefix_.substr(8), &number, &type)) {
        EXPECT_NE(type, kTableFile);
      }
      IOStatus s = fs_->Operation(prefix_ + ".invalidate");
      return s.ok() ? target()->InvalidateCache(offset, length) : s;
    }
    IOStatus Close(const IOOptions& o, IODebugContext* d) override {
      IOStatus injected = fs_->Operation(prefix_ + ".close");
      IOStatus s = target()->Close(o, d);
      s.UpdateIfOk(injected);
      return s;
    }

   private:
    std::unique_ptr<FSWritableFile> owned_;
    ReplacementFileSystem* fs_;
    std::string prefix_;
    bool candidate_, synced_ = false;
  };
  IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* d) override {
    IOStatus s = target()->NewWritableFile(path, o, file, d);
    if (s.ok()) Wrap(path, "capture", file);
    return s;
  }
  IOStatus ReopenWritableFile(const std::string& path, const FileOptions& o,
                              std::unique_ptr<FSWritableFile>* file,
                              IODebugContext* d) override {
    IOStatus s = target()->ReopenWritableFile(path, o, file, d);
    if (s.ok()) Wrap(path, "publish", file);
    return s;
  }

 private:
  void Wrap(const std::string& path, const std::string& phase,
            std::unique_ptr<FSWritableFile>* file) {
    const auto slash = path.find_last_of('/');
    const std::string name =
        path.substr(slash == std::string::npos ? 0 : slash + 1);
    const bool candidate = path.find("/point-") != std::string::npos;
    const bool work = path.find("/work/") != std::string::npos;
    if (candidate || work || name == "ordinary-write")
      file->reset(new File(std::move(*file), this,
                           (work ? "work" : phase) + "." + name, candidate));
  }
  std::mutex mutex_;
  std::map<std::string, uint64_t> counts_;
};
class PartialWalFileSystem : public FileSystemWrapper {
 public:
  explicit PartialWalFileSystem(std::shared_ptr<FileSystem> fs)
      : FileSystemWrapper(std::move(fs)) {}
  const char* Name() const override { return "PartialWalFileSystem"; }
  std::atomic<bool> fail{false};
  std::atomic<size_t> partial_calls{0};
  class File : public FSWritableFileOwnerWrapper {
   public:
    File(std::unique_ptr<FSWritableFile> file, PartialWalFileSystem* owner)
        : FSWritableFileOwnerWrapper(std::move(file)), owner_(owner) {}
    using FSWritableFileOwnerWrapper::Append;
    IOStatus Append(const Slice& data, const IOOptions& options,
                    IODebugContext* dbg) override {
      if (!owner_->fail) return target()->Append(data, options, dbg);
      ++owner_->partial_calls;
      IOStatus s =
          target()->Append(Slice(data.data(), data.size() / 2), options, dbg);
      if (!s.ok()) return s;
      return IOStatus::IOError("partial mirror WAL write");
    }

   private:
    PartialWalFileSystem* owner_;
  };
  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* dbg) override {
    IOStatus s = target()->NewWritableFile(path, options, file, dbg);
    if (s.ok()) Wrap(path, file);
    return s;
  }
  IOStatus ReopenWritableFile(const std::string& path,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* file,
                              IODebugContext* dbg) override {
    IOStatus s = target()->ReopenWritableFile(path, options, file, dbg);
    if (s.ok()) Wrap(path, file);
    return s;
  }

 private:
  void Wrap(const std::string& path, std::unique_ptr<FSWritableFile>* file) {
    if (path.find("/work/") != std::string::npos && path.size() >= 4 &&
        path.substr(path.size() - 4) == ".log")
      file->reset(new File(std::move(*file), this));
  }
};
class UnsupportedCompactionService : public CompactionService {
 public:
  const char* Name() const override { return "UnsupportedCompactionService"; }
};
class TestGate {
 public:
  void Block() {
    std::unique_lock<std::mutex> lock(mutex_);
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [&] { return released_; });
  }
  void WaitUntilBlocked() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return entered_; });
  }
  bool WaitUntilBlockedFor(std::chrono::seconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&] { return entered_; });
  }
  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool entered_ = false, released_ = false;
};
class MetaBypassTest : public testing::Test {
 public:
  MetaBypassTest() {
    root_ = test::PerThreadDBPath("metabypass");
    index_ = root_ + "/index";
    m_.data_dir = root_ + "/data";
    m_.backup_dir = root_ + "/backup";
    o_.create_if_missing = true;
    o_.enable_blob_files = true;
    o_.write_buffer_size = 64 * 1024;
    o_.max_manifest_file_size = 4096;
    fs_ = o_.env->GetFileSystem();
    Clean(root_);
    EXPECT_OK(fs_->CreateDirIfMissing(root_, IOOptions(), nullptr));
  }
  ~MetaBypassTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    db_.reset();
    Clean(root_);
  }
  void Clean(const std::string& path) {
    std::vector<std::string> children;
    Status s = fs_->GetChildren(path, IOOptions(), &children, nullptr);
    if (s.IsNotFound()) return;
    ASSERT_OK(s);
    for (const auto& f : children) {
      if (f == "." || f == "..") continue;
      bool dir;
      ASSERT_OK(fs_->IsDirectory(path + "/" + f, IOOptions(), &dir, nullptr));
      if (dir)
        Clean(path + "/" + f);
      else
        ASSERT_OK(fs_->DeleteFile(path + "/" + f, IOOptions(), nullptr));
    }
    ASSERT_OK(fs_->DeleteDir(path, IOOptions(), nullptr));
  }
  void EnableTier(uint64_t capacity = 1024 * 1024) {
    m_.staging_dir = root_ + "/staging";
    m_.staging_capacity = capacity;
  }
  void EnableSst(SstTieringMode mode = SstTieringMode::kAdaptive,
                 uint64_t capacity = 1) {
    m_.sst_tiering.mode = mode;
    m_.sst_tiering.ssd_capacity_bytes = capacity;
    m_.sst_tiering.interval_ms = 1;
    m_.sst_tiering.min_residency_ms = 0;
    m_.sst_tiering.sample_one_in = 1;
    m_.sst_tiering.migration_bytes_per_sec = 0;
  }
  void AwaitSst(const std::function<bool(const SstTieringStats&)>& ready) {
    std::mutex mutex;
    std::condition_variable cv;
    auto notify = [&](void*) {
      std::lock_guard<std::mutex> lock(mutex);
      cv.notify_all();
    };
    SyncPoint::GetInstance()->SetCallBack("MetaBypassSst::TickComplete",
                                          notify);
    SyncPoint::GetInstance()->SetCallBack("MetaBypassSst::MigrationComplete",
                                          notify);
    SyncPoint::GetInstance()->EnableProcessing();
    {
      std::unique_lock<std::mutex> lock(mutex);
      EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(20), [&] {
        auto stats = db_->GetBackupStats();
        EXPECT_OK(stats.error);
        return ready(stats.sst_tiering);
      }));
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  void BlockBlobGCDirSync(const std::shared_ptr<ReplacementFileSystem>& fs,
                          TestGate* gate) {
    struct State {
      std::mutex mutex;
      std::thread::id owner;
    };
    auto state = std::make_shared<State>();
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::BlobGCClaimed", [state](void*) {
          std::lock_guard<std::mutex> lock(state->mutex);
          state->owner = std::this_thread::get_id();
        });
    fs->hook = [state, gate, dir = m_.data_dir](const std::string& key) {
      bool gc;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        gc = state->owner == std::this_thread::get_id();
      }
      if (gc && key == "dirsync." + dir) gate->Block();
      return IOStatus::OK();
    };
  }
  void Open() { ASSERT_OK(MetaBypassDB::Open(o_, m_, index_, &db_)); }
  std::set<std::string> BlobFiles() {
    std::vector<std::string> children;
    EXPECT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &children, nullptr));
    std::set<std::string> result;
    for (const auto& name : children) {
      uint64_t number;
      FileType type;
      if (ParseFileName(name, &number, &type) && type == kBlobFile)
        result.insert(name);
    }
    return result;
  }
  std::map<std::string, std::string> SnapshotTree(const std::string& path) {
    std::map<std::string, std::string> result;
    std::vector<std::string> children;
    Status status = fs_->GetChildren(path, IOOptions(), &children, nullptr);
    if (status.IsNotFound()) return result;
    EXPECT_OK(status);
    result[path + "/"] = "directory";
    for (const auto& child : children) {
      if (child == "." || child == "..") continue;
      const std::string file = path + "/" + child;
      bool directory = false;
      EXPECT_OK(fs_->IsDirectory(file, IOOptions(), &directory, nullptr));
      if (directory) {
        auto nested = SnapshotTree(file);
        result.insert(nested.begin(), nested.end());
      } else {
        EXPECT_OK(metabypass::Read(fs_.get(), file, &result[file]));
      }
    }
    return result;
  }
  std::vector<std::string> FilesOfType(const std::string& path,
                                       FileType wanted) {
    std::vector<std::string> files, result;
    EXPECT_OK(fs_->GetChildren(path, IOOptions(), &files, nullptr));
    for (const auto& file : files) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type) && type == wanted)
        result.push_back(file);
    }
    return result;
  }
  std::string PublishedPoint() {
    std::string pointer;
    EXPECT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
    return m_.backup_dir + "/" + pointer.substr(0, pointer.find('\n'));
  }
  void AwaitBlobGC(uint64_t minimum) {
    std::mutex mutex;
    std::condition_variable cv;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::BlobReclaimed", [&](void*) {
          std::lock_guard<std::mutex> lock(mutex);
          cv.notify_all();
        });
    SyncPoint::GetInstance()->EnableProcessing();
    {
      std::unique_lock<std::mutex> lock(mutex);
      EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(20), [&] {
        auto stats = db_->GetBackupStats();
        EXPECT_OK(stats.error);
        return stats.reclaimed_blob_files >= minimum || !stats.error.ok();
      }));
      auto stats = db_->GetBackupStats();
      EXPECT_OK(stats.error);
      EXPECT_GE(stats.reclaimed_blob_files, minimum)
          << "pending blobs=" << stats.pending_blob_gc_files
          << " points=" << stats.recovery_points
          << " retiring points=" << stats.pending_gc_points;
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  void CheckNoIdleCaptures() {
    std::mutex mutex;
    std::condition_variable cv;
    unsigned waits = 0;
    std::atomic<unsigned> captures{0};
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::BeforeMirrorWait", [&](void*) {
          std::lock_guard<std::mutex> lock(mutex);
          ++waits;
          cv.notify_all();
        });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureFiles",
                                          [&](void*) { ++captures; });
    SyncPoint::GetInstance()->EnableProcessing();
    {
      std::unique_lock<std::mutex> lock(mutex);
      EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(20),
                              [&] { return waits >= 4; }));
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(captures.load(), 0U);
  }
  void BlobGCScenario(bool tier, bool sst, bool enabled) {
    if (tier) EnableTier();
    if (sst) EnableSst();
    m_.enable_blob_file_gc = enabled;
    m_.interval_ms = 1;
    m_.checkpoint_interval_ms = m_.interval_ms;
    o_.blob_file_size = 128;
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_OK(db_->Put(WriteOptions(), "old", std::string(256, 'a')));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->SyncBackup());
    auto old = BlobFiles();
    ASSERT_FALSE(old.empty());
    ASSERT_OK(db_->Delete(WriteOptions(), "old"));
    ASSERT_OK(db_->Put(WriteOptions(), "live", std::string(256, 'b')));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(CompactBlobFiles());
    ASSERT_OK(db_->SyncBackup());
    if (enabled) AwaitBlobGC(1);
    auto remaining = BlobFiles();
    for (const auto& name : old)
      ASSERT_EQ(remaining.count(name), enabled ? 0U : 1U);
    ASSERT_OK(db_->Close());
    db_.reset();
    // Compatibility: a database that has reclaimed files can reopen/restore
    // with GC disabled and never needs obsolete historical MANIFEST additions.
    m_.enable_blob_file_gc = false;
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("live", std::string(256, 'b'));
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "old", &value).IsNotFound());
  }

  Status CompactBlobFiles() {
    CompactRangeOptions options;
    // Default manual compaction can trivial-move files and skip bottommost
    // rewrites. These scenarios require actual removal of obsolete SST data.
    options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    return db_->CompactRange(options, nullptr, nullptr);
  }
  void Check(const std::string& key, const std::string& expected) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, expected);
  }
  void EnableDirectWrite() {
    o_.enable_blob_direct_write = true;
    o_.allow_concurrent_memtable_write = false;
    o_.blob_direct_write_partitions = 1;
    o_.write_buffer_size = 4 * 1024 * 1024;
    o_.blob_file_size = 1024 * 1024;
    m_.checkpoint_interval_ms = 60000;
  }
  void CheckPointValue(const std::string& point, const std::string& key,
                       const std::string* expected) {
    auto storage =
        std::make_shared<metabypass::SeparatedStorage>(fs_, point, m_.data_dir);
    auto env = NewCompositeEnv(storage);
    Options options = o_;
    options.env = env.get();
    options.create_if_missing = false;
    options.enable_blob_files = true;
    options.enable_blob_direct_write = false;
    std::unique_ptr<DB> snapshot;
    // The Options overload may select CompactedDBImpl for a single SST. Its
    // optimized Get does not support ordinary blob indexes. Use the standard
    // read-only implementation, which also verifies recovery without a WAL.
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(DB::OpenForReadOnly(
        DBOptions(options), point,
        {{kDefaultColumnFamilyName, ColumnFamilyOptions(options)}}, &handles,
        &snapshot, /*error_if_wal_file_exists=*/true));
    std::vector<LiveFileMetaData> files;
    snapshot->GetLiveFilesMetaData(&files);
    for (const auto& file : files) {
      EXPECT_GE(snapshot->GetLatestSequenceNumber(), file.largest_seqno);
    }
    std::string value;
    Status s = snapshot->Get(ReadOptions(), key, &value);
    for (auto* handle : handles) {
      ASSERT_OK(snapshot->DestroyColumnFamilyHandle(handle));
    }
    if (expected) {
      ASSERT_OK(s);
      ASSERT_EQ(value, *expected);
    } else {
      ASSERT_TRUE(s.IsNotFound());
    }
    ASSERT_OK(snapshot->Close());
  }
  void AlwaysFullSuffixDbBarrier(const std::string& barrier) {
    EnableDirectWrite();
    EnableTier(8192);
    o_.blob_file_size = 128 * 1024 * 1024;
    o_.write_buffer_size = 128 * 1024 * 1024;
    ASSERT_NO_FATAL_FAILURE(Open());
    TestGate seal;
    std::atomic<bool> first{true}, done{false};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierSealBeforeRequeue",
                                          [&](void*) {
                                            if (first.exchange(false))
                                              seal.Block();
                                          });
    SyncPoint::GetInstance()->LoadDependency(
        {{"MetaBypass::TierBarrierWaiting", "AlwaysDb::BarrierWaiting"}});
    SyncPoint::GetInstance()->EnableProcessing();
    const std::string value(32 * 1024 * 1024, 'v');
    // DW1 Put must complete its per-record Flush across the full HDD extent.
    ASSERT_OK(db_->Put(WriteOptions(), "large", value));
    seal.WaitUntilBlocked();
    auto stats = db_->GetBackupStats();
    EXPECT_OK(stats.error);
    EXPECT_GT(stats.staging_spill_files, 0);
    EXPECT_GT(stats.adaptive_pending_seals, 0);
    EXPECT_OK(db_->Put(WriteOptions(), "next", "visible"));
    Check("next", "visible");
    Status result;
    std::thread waiter([&] {
      if (barrier == "flush")
        result = db_->Flush(FlushOptions());
      else if (barrier == "sync") {
        WriteOptions sync;
        sync.sync = true;
        result = db_->Put(sync, "synced", "durable");
      } else if (barrier == "backup")
        result = db_->SyncBackup();
      else
        result = db_->Close();
      done = true;
    });
    TEST_SYNC_POINT("AlwaysDb::BarrierWaiting");
    EXPECT_FALSE(done.load());
    seal.Release();
    waiter.join();
    ASSERT_OK(result);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (barrier != "close") {
      ASSERT_OK(db_->Close());
    }
    db_.reset();
    Clean(m_.staging_dir);
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("large", value);
    Check("next", "visible");
    if (barrier == "sync") Check("synced", "durable");
  }
  void DirectWriteStorageScenario(uint64_t capacity) {
    EnableDirectWrite();
    if (capacity) EnableTier(capacity);
    ASSERT_NO_FATAL_FAILURE(Open());
    const std::string value(16 * 1024, 'v');
    WriteBatch batch;
    ASSERT_OK(batch.Put("a", value));
    ASSERT_OK(batch.Put("b", value));
    ASSERT_OK(batch.Put("empty", ""));
    ASSERT_OK(batch.Delete("absent"));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    Check("a", value);
    Check("b", value);
    Check("empty", "");
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "a");
      ASSERT_EQ(iter->value(), value);
      ASSERT_OK(iter->status());
    }
    auto stats = db_->GetBackupStats();
    ASSERT_OK(stats.error);
    if (capacity == 8192) {
      ASSERT_GT(stats.staging_spill_files, 0U);
    }
    if (capacity > value.size() * 3) {
      ASSERT_EQ(stats.staging_spill_files, 0U);
    }
    ASSERT_OK(db_->SyncBackup());
    ASSERT_TRUE(FilesOfType(PublishedPoint(), kWalFile).empty());
    ASSERT_FALSE(FilesOfType(PublishedPoint(), kTableFile).empty());
    // Close must checkpoint the final unflushed direct-write generation.
    ASSERT_OK(db_->Put(WriteOptions(), "close", "last"));
    ASSERT_OK(db_->Close());
    db_.reset();
    if (capacity) Clean(m_.staging_dir);
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("a", value);
    Check("b", value);
    Check("empty", "");
    Check("close", "last");
    WriteOptions sync;
    sync.sync = true;
    ASSERT_OK(db_->Put(sync, "after", "restored"));
    ASSERT_OK(db_->Close());
    db_.reset();
    ASSERT_NO_FATAL_FAILURE(Open());
    Check("after", "restored");
  }
  enum class DirectWriteFailurePhase { kBackup, kBeforeWal };
  void DirectWriteFailureScenario(
      const std::string& point, bool tier,
      DirectWriteFailurePhase phase = DirectWriteFailurePhase::kBackup) {
    EnableDirectWrite();
    if (tier) EnableTier();
    ASSERT_NO_FATAL_FAILURE(Open());
    WriteOptions sync;
    sync.sync = true;
    ASSERT_OK(db_->Put(sync, "key", "old"));
    const std::string published = PublishedPoint();
    const Status failure = Status::IOError("direct checkpoint failure");
    TestGate before_wal, migration_failed;
    Status result;
    std::thread writer;
    Defer cleanup([&] {
      before_wal.Release();
      migration_failed.Release();
      if (phase == DirectWriteFailurePhase::kBeforeWal)
        TEST_SYNC_POINT("DirectWriteFailure::BeforeWal");
      if (writer.joinable()) writer.join();
      result.PermitUncheckedError();
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->LoadDependency({});
    });
    SyncPoint::GetInstance()->SetCallBack(
        point, [&](void* arg) { *static_cast<Status*>(arg) = failure; });
    if (phase == DirectWriteFailurePhase::kBeforeWal) {
      ASSERT_TRUE(tier);
      SyncPoint::GetInstance()->LoadDependency(
          {{"DirectWriteFailure::BeforeWal", "MetaBypass::TierMigration"}});
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::WriteImpl:AfterBlobDirectWrite", [&](void* arg) {
            EXPECT_OK(*static_cast<Status*>(arg));
            before_wal.Block();
          });
      // This point follows both tier failure and its backup failure handler.
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::TierMigrationComplete",
          [&](void*) { migration_failed.Block(); });
    } else if (tier) {
      // Blob Sync can freeze an extent before the native WAL write. Delay
      // migration until the completed native write enters its backup barrier.
      SyncPoint::GetInstance()->LoadDependency(
          {{"MetaBypass::SyncWaiting", "MetaBypass::TierMigration"}});
    }
    SyncPoint::GetInstance()->EnableProcessing();
    if (phase == DirectWriteFailurePhase::kBeforeWal) {
      writer = std::thread([&] { result = db_->Put(sync, "key", "new"); });
      before_wal.WaitUntilBlocked();
      Check("key", "old");
      EXPECT_EQ(PublishedPoint(), published);
      TEST_SYNC_POINT("DirectWriteFailure::BeforeWal");
      migration_failed.WaitUntilBlocked();
      auto stats = db_->GetBackupStats();
      EXPECT_EQ(stats.error.ToString(), failure.ToString());
      Check("key", "old");
      EXPECT_EQ(PublishedPoint(), published);
      migration_failed.Release();
      before_wal.Release();
      writer.join();
    } else {
      result = db_->Put(sync, "key", "new");
    }
    ASSERT_TRUE(result.IsIOError());
    ASSERT_NE(result.ToString().find("direct checkpoint failure"),
              std::string::npos);
    Check("key", phase == DirectWriteFailurePhase::kBeforeWal ? "old" : "new");
    ASSERT_EQ(PublishedPoint(), published);
    ASSERT_EQ(db_->SyncBackup().ToString(), failure.ToString());
    ASSERT_EQ(db_->Put(WriteOptions(), "rejected", "x").ToString(),
              failure.ToString());
    ASSERT_EQ(db_->Close().ToString(), failure.ToString());
    ASSERT_EQ(db_->Close().ToString(), failure.ToString());
    db_.reset();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->LoadDependency({});
    if (tier) Clean(m_.staging_dir);
    const uint64_t recovered = tier ? 1 : 2;
    ASSERT_NO_FATAL_FAILURE(RestorePrefix(1, recovered));
    CheckContents({{"key", recovered == 1 ? "old" : "new"}});
    ASSERT_OK(db_->Put(sync, "after", "recovered"));
  }
  void Restore(MetaBypassRecoveryReport* report = nullptr) {
    Clean(index_);
    MetaBypassRecoveryReport local;
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_, report ? report : &local));
    ASSERT_NO_FATAL_FAILURE(Open());
  }
  void RestorePrefix(uint64_t checkpoint, uint64_t recovered) {
    MetaBypassRecoveryReport report;
    ASSERT_NO_FATAL_FAILURE(Restore(&report));
    ASSERT_EQ(report.checkpoint_sequence, checkpoint);
    ASSERT_EQ(report.recovered_sequence, recovered);
  }
  void CheckContents(const std::map<std::string, std::string>& expected) {
    std::map<std::string, std::string> actual;
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
      actual.emplace(iterator->key().ToString(), iterator->value().ToString());
    ASSERT_OK(iterator->status());
    ASSERT_EQ(actual, expected);
  }
#ifndef OS_WIN
  void DirectWriteCrashScenario(const std::string& stage, bool tier) {
    EnableDirectWrite();
    if (tier) EnableTier(8192);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(),
            "--direct-write-crash-writer", index_.c_str(), m_.data_dir.c_str(),
            m_.backup_dir.c_str(), m_.staging_dir.c_str(), stage.c_str(),
            nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    // Crash recovery intentionally starts from the published native point.
    Clean(index_);
    if (tier) Clean(m_.staging_dir);
    MetaBypassRecoveryReport report;
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_, &report));
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(report.recovered_sequence == 2 ||
                report.recovered_sequence == 4);
    const std::string expected(16 * 1024,
                               report.recovered_sequence == 4 ? 'n' : 'o');
    Check("a", expected);
    Check("b", expected);
    WriteOptions sync;
    sync.sync = true;
    ASSERT_OK(db_->Put(sync, "after", "recovery"));
  }
  void TieredCrashScenario(const std::string& stage) {
    EnableTier(1024 * 1024);
    SCOPED_TRACE(stage);
    db_.reset();
    Clean(index_);
    Clean(m_.staging_dir);
    Clean(m_.data_dir);
    Clean(m_.backup_dir);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(), "--tiered-crash-writer",
            index_.c_str(), m_.data_dir.c_str(), m_.backup_dir.c_str(),
            m_.staging_dir.c_str(), stage.c_str(), nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    Clean(index_);
    Clean(m_.staging_dir);
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
    Open();
    std::string a, b;
    ASSERT_OK(db_->Get(ReadOptions(), "a", &a));
    ASSERT_OK(db_->Get(ReadOptions(), "b", &b));
    ASSERT_EQ(a, b);
    ASSERT_TRUE(a == "old" || a == "new");
    if (stage.empty()) {
      ASSERT_EQ(a, "new");
    }
    WriteOptions sync;
    sync.sync = true;
    ASSERT_OK(db_->Put(sync, "after", "restore"));
    ASSERT_OK(db_->Close());
    db_.reset();
    Open();
    Check("after", "restore");
  }
  void RunCrashWriter(const std::string& stage = "") {
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      if (stage.empty()) {
        execl(executable.c_str(), executable.c_str(),
              "--metabypass-crash-writer", index_.c_str(), m_.data_dir.c_str(),
              m_.backup_dir.c_str(), nullptr);
      } else {
        execl(executable.c_str(), executable.c_str(),
              "--metabypass-crash-writer", index_.c_str(), m_.data_dir.c_str(),
              m_.backup_dir.c_str(), stage.c_str(), nullptr);
      }
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
  }
#endif
  void StartBackup(std::unique_ptr<metabypass::Backup>* backup) {
    auto storage = std::make_shared<metabypass::SeparatedStorage>(fs_, index_,
                                                                  m_.data_dir);
    ASSERT_OK(storage->Lock());
    ASSERT_OK(fs_->CreateDir(index_, IOOptions(), nullptr));
    *backup = std::make_unique<metabypass::Backup>(storage, index_, m_);
    ASSERT_OK((*backup)->Start(false));
  }
  void CoalescePressureScenario(bool fail) {
    m_.wal_queue_capacity = m_.metadata_queue_capacity = 32768;
    m_.batch_bytes = 1;
    m_.checkpoint_interval_ms = 60000;
    o_.write_buffer_size = 4 * 1024 * 1024;
    auto partial = std::make_shared<PartialWalFileSystem>(fs_);
    auto env = NewCompositeEnv(partial);
    Options options = o_;
    options.env = env.get();
    ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
    ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
    ASSERT_OK(db_->SyncBackup());
    const std::string old_point = PublishedPoint();
    TestGate first, batch, pressure, sync_waiters;
    int waiting_syncs = 0;
    BlockFirstApply(&first);
    bool first_batch = true;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::WalAppendBatch", [&](void* arg) {
          if (!first_batch) return;
          first_batch = false;
          const auto& info = *static_cast<std::array<uint64_t, 3>*>(arg);
          EXPECT_GT(info[1], 1U);
          auto stats = db_->GetBackupStats();
          EXPECT_OK(stats.error);
          EXPECT_GE(stats.queued_bytes, info[2]);
          batch.Block();
          partial->fail = fail;
        });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure",
                                          [&](void*) { pressure.Block(); });
    SyncPoint::GetInstance()->EnableProcessing();
    EXPECT_OK(db_->Put(WriteOptions(), "first", "value"));
    first.WaitUntilBlocked();
    for (int i = 0; i < 8; ++i)
      EXPECT_OK(db_->Put(WriteOptions(), "queued" + std::to_string(i),
                         std::string(1024, 'q')));
    first.Release();
    batch.WaitUntilBlocked();
    Status produced;
    std::thread producer([&] {
      for (int i = 0; i < 128; ++i) {
        produced = db_->Put(WriteOptions(), "tail" + std::to_string(i),
                            std::string(1024, 't'));
        if (!produced.ok()) break;
      }
    });
    pressure.WaitUntilBlocked();
    pressure.Release();
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                          [&](void*) {
                                            if (++waiting_syncs == 2)
                                              sync_waiters.Block();
                                          });
    std::array<Status, 2> synced;
    std::thread sync1([&] { synced[0] = db_->SyncBackup(); });
    std::thread sync2([&] { synced[1] = db_->SyncBackup(); });
    sync_waiters.WaitUntilBlocked();
    sync_waiters.Release();
    batch.Release();
    producer.join();
    sync1.join();
    sync2.join();
    if (fail) {
      EXPECT_TRUE(produced.IsIOError());
      for (const auto& s : synced) EXPECT_TRUE(s.IsIOError());
      EXPECT_EQ(partial->partial_calls.load(), 1U);
      EXPECT_EQ(PublishedPoint(), old_point);
      EXPECT_TRUE(db_->Close().IsIOError());
    } else {
      EXPECT_OK(produced);
      for (const auto& s : synced) EXPECT_OK(s);
      EXPECT_OK(db_->SyncBackup());
      EXPECT_OK(db_->Close());
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    db_.reset();
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("old", "retained");
    if (!fail) Check("tail127", std::string(1024, 't'));
  }
  void CoalesceSelector(
      const std::vector<metabypass::BackupTestAccess::Input>& input,
      const std::vector<size_t>& expected, uint64_t boundary = UINT64_MAX,
      uint64_t capture = 0) {
    auto storage = std::make_shared<metabypass::SeparatedStorage>(fs_, index_,
                                                                  m_.data_dir);
    metabypass::Backup backup(storage, index_, m_);
    EXPECT_EQ(
        metabypass::BackupTestAccess::Select(&backup, input, boundary, capture),
        expected);
  }
  void CoalesceDrain(
      const std::vector<metabypass::BackupTestAccess::Input>& input,
      bool partial, size_t successful, const std::vector<std::string>& calls) {
    auto storage = std::make_shared<metabypass::SeparatedStorage>(fs_, index_,
                                                                  m_.data_dir);
    metabypass::Backup backup(storage, index_, m_);
    const size_t header = metabypass::BackupTestAccess::EventHeaderCharge();
    std::vector<size_t> remaining_charge(input.size() + 1);
    for (size_t i = input.size(); i > 0; --i) {
      remaining_charge[i - 1] = remaining_charge[i] + header +
                                input[i - 1].name.size() +
                                input[i - 1].bytes.size();
    }
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::WalAppendBatch", [&](void* arg) {
          const auto first = (*static_cast<std::array<uint64_t, 3>*>(arg))[0];
          ASSERT_GT(first, 0U);
          ASSERT_LE(first, input.size());
          // Stats takes the queue lock. Selected but uncommitted events must
          // remain charged through the physical Append.
          auto stats = backup.Stats();
          EXPECT_OK(stats.error);
          EXPECT_EQ(stats.queued_bytes, remaining_charge[first - 1]);
        });
    metabypass::BackupTestAccess::Drain(&backup, input, partial, successful,
                                        calls);
    const auto stats = backup.Stats();
    EXPECT_LE(stats.peak_queued_bytes, m_.wal_queue_capacity);
    EXPECT_EQ(stats.peak_queued_bytes, remaining_charge.front());
    stats.error.PermitUncheckedError();
  }
  void BlockFirstApply(TestGate* gate) {
    // Only the single mirror thread invokes this callback.
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply",
                                          [gate, first = true](void*) mutable {
                                            if (first) {
                                              first = false;
                                              gate->Block();
                                            }
                                          });
  }
  void CandidateValidationBlockedScenario(const std::string& point) {
    m_.wal_queue_capacity = m_.metadata_queue_capacity = 32768;
    m_.batch_bytes = 1;
    m_.interval_ms = m_.checkpoint_interval_ms = 60000;
    o_.write_buffer_size = 4 * 1024 * 1024;
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
    ASSERT_OK(db_->SyncBackup());
    std::string before;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &before));
    TestGate validating;
    std::atomic<bool> returned{false};
    SyncPoint::GetInstance()->SetCallBack(point,
                                          [&](void*) { validating.Block(); });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->Put(WriteOptions(), "candidate", "value"));
    Status synced;
    std::thread sync([&] {
      synced = db_->SyncBackup();
      returned = true;
    });
    validating.WaitUntilBlocked();
    // Completion requires the mirror to consume substantially more than the
    // queue capacity while the validator holds the candidate.
    for (int i = 0; i < 200; ++i)
      EXPECT_OK(db_->Put(WriteOptions(), "tail" + std::to_string(i),
                         std::string(1024, 'v')));
    EXPECT_FALSE(returned.load());
    std::string pending;
    EXPECT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pending));
    EXPECT_EQ(pending, before);
    validating.Release();
    sync.join();
    ASSERT_OK(synced);
    SyncPoint::GetInstance()->DisableProcessing();
    ASSERT_OK(db_->SyncBackup());
    ASSERT_OK(db_->Close());
    db_.reset();
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("old", "retained");
    Check("tail199", std::string(1024, 'v'));
  }
  void CandidateFailureScenario(const std::string& point) {
    m_.wal_queue_capacity = m_.metadata_queue_capacity = 32768;
    m_.batch_bytes = m_.wal_queue_capacity;
    m_.interval_ms = m_.checkpoint_interval_ms = 60000;
    o_.write_buffer_size = 4 * 1024 * 1024;
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_OK(db_->Put(WriteOptions(), "key", "old"));
    ASSERT_OK(db_->SyncBackup());
    std::string before;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &before));
    TestGate failing, waiters, applying, capacity;
    std::atomic<int> waiting{0};
    SyncPoint::GetInstance()->SetCallBack(point, [&](void* arg) {
      failing.Block();
      *static_cast<Status*>(arg) = Status::IOError("candidate fault");
    });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                          [&](void*) {
                                            if (++waiting == 4) waiters.Block();
                                          });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->Put(WriteOptions(), "key", "new"));
    std::array<Status, 4> results;
    std::vector<std::thread> threads;
    threads.emplace_back([&] { results[0] = db_->SyncBackup(); });
    failing.WaitUntilBlocked();
    for (size_t i = 1; i < results.size(); ++i)
      threads.emplace_back([&, i] { results[i] = db_->SyncBackup(); });
    waiters.WaitUntilBlocked();
    waiters.Release();
    // Also park a WAL producer on capacity. For a validator failure, keep the
    // mirror from draining; for a staging failure it is already blocked above.
    BlockFirstApply(&applying);
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure",
                                          [&](void*) { capacity.Block(); });
    Status produced;
    uint64_t completed_tail = 0;
    std::thread producer([&] {
      for (int i = 0; i < 200; ++i) {
        produced = db_->Put(WriteOptions(), "tail" + std::to_string(i),
                            std::string(1024, 'v'));
        if (!produced.ok()) break;
        ++completed_tail;
      }
    });
    capacity.WaitUntilBlocked();
    capacity.Release();
    failing.Release();
    for (auto& thread : threads) thread.join();
    producer.join();
    applying.Release();
    EXPECT_TRUE(produced.IsIOError()) << produced.ToString();
    for (const auto& result : results)
      EXPECT_TRUE(result.IsIOError()) << result.ToString();
    EXPECT_EQ(db_->GetBackupStats().error, results[0]);
    EXPECT_EQ(db_->Put(WriteOptions(), "rejected", "value"), results[0]);
    EXPECT_EQ(db_->Close(), results[0]);
    db_.reset();
    SyncPoint::GetInstance()->DisableProcessing();
    std::string after;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &after));
    EXPECT_EQ(after, before);
    MetaBypassRecoveryReport report;
    ASSERT_NO_FATAL_FAILURE(Restore(&report));
    ASSERT_EQ(report.checkpoint_sequence, 1U);
    ASSERT_GE(report.recovered_sequence, 2U);
    ASSERT_LE(report.recovered_sequence, 2 + completed_tail);
    // A mirror batch already in flight may finish after the injected failure.
    // Project the independent, single-producer operation order onto exact X.
    std::map<std::string, std::string> expected{{"key", "new"}};
    for (uint64_t i = 0; i < report.recovered_sequence - 2; ++i)
      expected.emplace("tail" + std::to_string(i), std::string(1024, 'v'));
    CheckContents(expected);
  }
  void StartBackupFiles(std::unique_ptr<metabypass::Backup>* backup,
                        std::unique_ptr<FSWritableFile>* wal,
                        std::unique_ptr<FSWritableFile>* metadata) {
    ASSERT_NO_FATAL_FAILURE(StartBackup(backup));
    ASSERT_OK((*backup)->NewWritableFile(index_ + "/000001.log", FileOptions(),
                                         wal, nullptr));
    ASSERT_OK((*backup)->NewWritableFile(index_ + "/MANIFEST-000002",
                                         FileOptions(), metadata, nullptr));
  }
  void NotificationScenario(bool pressure, bool fail, bool timer,
                            bool metadata = false) {
    const std::string file_name = metadata ? "MANIFEST-000001" : "000001.log";
    m_.wal_queue_capacity = m_.metadata_queue_capacity = m_.batch_bytes = 4096;
    m_.interval_ms = timer ? 1 : 60000;
    m_.checkpoint_interval_ms = m_.interval_ms;
    TestGate waiting, applying;
    std::atomic<bool> first{true};
    std::atomic<int> notifications{0}, blocked{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeMirrorWait",
                                          [&](void*) {
                                            if (first.exchange(false))
                                              waiting.Block();
                                          });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::MirrorNotified",
                                          [&](void*) { ++notifications; });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure",
                                          [&](void*) { ++blocked; });
    if (timer) {
      SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply",
                                            [&](void*) { applying.Block(); });
    }
    if (fail) {
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::ApplyStatus", [](void* arg) {
            *static_cast<Status*>(arg) = Status::IOError("mirror failure");
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    std::unique_ptr<metabypass::Backup> backup;
    ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
    waiting.WaitUntilBlocked();
    waiting.Release();
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(backup->NewWritableFile(index_ + "/" + file_name, FileOptions(),
                                      &file, nullptr));
    ASSERT_OK(file->Append(std::string(2000, 'a'), IOOptions(), nullptr));
    EXPECT_EQ(notifications.load(), 0);
    if (timer) {
      // Only the timed wait can drain these below-threshold events.
      applying.WaitUntilBlocked();
      applying.Release();
    }
    if (pressure) {
      Status s = file->Append(std::string(3000, 'b'), IOOptions(), nullptr);
      if (fail)
        EXPECT_TRUE(s.IsIOError());
      else
        EXPECT_OK(s);
      EXPECT_GT(blocked.load(), 0);
      EXPECT_GT(notifications.load(), 0);
    }
    if (fail) {
      EXPECT_TRUE(file->Close(IOOptions(), nullptr).IsIOError());
      EXPECT_TRUE(backup->Sync().IsIOError());
      EXPECT_TRUE(backup->Stop().IsIOError());
    } else {
      EXPECT_OK(file->Close(IOOptions(), nullptr));
      EXPECT_OK(backup->Stop());
      std::string bytes;
      ASSERT_OK(metabypass::Read(fs_.get(),
                                 m_.backup_dir + "/work/" + file_name, &bytes));
      EXPECT_EQ(bytes, std::string(2000, 'a') +
                           (pressure ? std::string(3000, 'b') : ""));
      auto stats = backup->Stats();
      EXPECT_OK(stats.error);
      EXPECT_LE(stats.peak_queued_bytes, m_.wal_queue_capacity);
    }
    SyncPoint::GetInstance()->DisableProcessing();
  }
  void QueueScenario(bool fail) {
    m_.wal_queue_capacity = m_.metadata_queue_capacity = 4096;
    m_.batch_bytes = 1;
    auto storage = std::make_shared<metabypass::SeparatedStorage>(fs_, index_,
                                                                  m_.data_dir);
    ASSERT_OK(storage->Lock());
    ASSERT_OK(fs_->CreateDir(index_, IOOptions(), nullptr));
    metabypass::Backup backup(storage, index_, m_);
    ASSERT_OK(backup.Start(false));
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, release = false, blocked = false;
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply", [&](void*) {
      std::unique_lock<std::mutex> lock(mutex);
      entered = true;
      cv.notify_all();
      cv.wait(lock, [&] { return release; });
    });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::Backpressure", [&](void*) {
          std::lock_guard<std::mutex> lock(mutex);
          blocked = true;
          cv.notify_all();
        });
    if (fail)
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::ApplyStatus", [](void* arg) {
            *static_cast<Status*>(arg) =
                Status::IOError("injected mirror failure");
          });
    SyncPoint::GetInstance()->EnableProcessing();
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(backup.NewWritableFile(index_ + "/000001.log", FileOptions(),
                                     &file, nullptr));
    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&] { return entered; });
    }
    ASSERT_OK(file->Append(std::string(2000, 'a'), IOOptions(), nullptr));
    std::thread writer([&] {
      Status written =
          file->Append(std::string(3000, 'b'), IOOptions(), nullptr);
      if (fail)
        EXPECT_TRUE(written.IsIOError());
      else
        EXPECT_OK(written);
    });
    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&] { return blocked; });
      release = true;
      cv.notify_all();
    }
    writer.join();
    SyncPoint::GetInstance()->DisableProcessing();
    if (fail) {
      ASSERT_TRUE(backup.Sync().IsIOError());
      ASSERT_TRUE(file->Close(IOOptions(), nullptr).IsIOError());
      file.reset();
      ASSERT_TRUE(backup.Stop().IsIOError());
      return;
    }
    ASSERT_OK(file->Close(IOOptions(), nullptr));
    file.reset();
    ASSERT_OK(backup.Stop());
    auto stats = backup.Stats();
    ASSERT_OK(stats.error);
    ASSERT_LE(stats.peak_queued_bytes, m_.wal_queue_capacity);
    ASSERT_GT(stats.backpressure_micros, 0);
    std::string bytes;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log",
                               &bytes));
    ASSERT_EQ(bytes, std::string(2000, 'a') + std::string(3000, 'b'));
  }
  void OutstandingReservationScenario(bool wal_append) {
    m_.batch_bytes = 1;
    m_.wal_queue_capacity = m_.metadata_queue_capacity = 4096;
    const std::string filename = wal_append ? "000001.log" : "MANIFEST-000002";
    const Status failure =
        Status::IOError("mirror failure with primary in flight");
    TestGate applying, primary, discarded;
    std::unique_ptr<metabypass::Backup> backup;
    std::unique_ptr<FSWritableFile> wal, metadata;
    auto* sync = SyncPoint::GetInstance();
    Defer cleanup([&] {
      applying.Release();
      primary.Release();
      discarded.Release();
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    BlockFirstApply(&applying);
    sync->SetCallBack("MetaBypass::PrimaryReserved", [&](void* arg) {
      if (*static_cast<std::string*>(arg) == filename) primary.Block();
    });
    sync->SetCallBack("MetaBypass::ApplyStatus",
                      [&](void* arg) { *static_cast<Status*>(arg) = failure; });
    sync->SetCallBack("MetaBypass::QueuesDiscarded",
                      [&](void*) { discarded.Block(); });
    sync->EnableProcessing();
    ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
    ASSERT_TRUE(applying.WaitUntilBlockedFor(std::chrono::seconds(10)));
    auto& file = wal_append ? wal : metadata;
    const std::string value(1024, 'x');
    auto writer = std::async(std::launch::async, [&] {
      return file->Append(value, IOOptions(), nullptr);
    });
    EXPECT_TRUE(primary.WaitUntilBlockedFor(std::chrono::seconds(10)));
    applying.Release();
    EXPECT_TRUE(discarded.WaitUntilBlockedFor(std::chrono::seconds(10)));
    discarded.Release();
    EXPECT_EQ(backup->Stop().ToString(), failure.ToString());
    const auto before = backup->Stats();
    EXPECT_EQ(before.error.ToString(), failure.ToString());
    EXPECT_EQ(before.queued_bytes,
              metabypass::BackupTestAccess::EventHeaderCharge() +
                  filename.size() + value.size());
    primary.Release();
    EXPECT_OK(writer.get());
    const auto after = backup->Stats();
    EXPECT_EQ(after.error.ToString(), failure.ToString());
    EXPECT_EQ(after.queued_bytes, 0U);
    EXPECT_EQ(after.mirrored_wal_bytes, 0U);
    EXPECT_TRUE(metadata->Close(IOOptions(), nullptr).IsIOError());
    EXPECT_TRUE(wal->Close(IOOptions(), nullptr).IsIOError());
    std::string bytes;
    ASSERT_OK(metabypass::Read(fs_.get(), index_ + "/" + filename, &bytes));
    EXPECT_EQ(bytes, value);
  }
  std::string root_, index_;
  Options o_;
  MetaBypassOptions m_ = TestMetaBypassOptions();
  std::shared_ptr<FileSystem> fs_;
  std::unique_ptr<MetaBypassDB> db_;
};

class MetaBypassNonBlobTest : public MetaBypassTest {
 public:
  MetaBypassNonBlobTest() {
    o_ = Options();
    o_.create_if_missing = true;
  }
  void CheckNoBlobs(const std::string& path) {
    std::vector<std::string> names;
    Status status = fs_->GetChildren(path, IOOptions(), &names, nullptr);
    if (status.IsNotFound()) return;
    ASSERT_OK(status);
    for (const auto& name : names) {
      if (name == "." || name == "..") continue;
      uint64_t number;
      FileType type;
      ASSERT_FALSE(ParseFileName(name, &number, &type) && type == kBlobFile);
      const std::string child = path + "/" + name;
      bool directory = false;
      status = fs_->IsDirectory(child, IOOptions(), &directory, nullptr);
      // IsDirectory may report a removed path as IOError on some filesystems.
      // Skip only confirmed removals while retired recovery points are
      // reclaimed.
      if (!status.ok()) {
        Status exists = fs_->FileExists(child, IOOptions(), nullptr);
        if (exists.IsNotFound()) continue;
        ASSERT_OK(exists);
      }
      ASSERT_OK(status);
      if (directory) {
        ASSERT_NO_FATAL_FAILURE(CheckNoBlobs(child));
      }
    }
  }
  void CheckContents(const std::map<std::string, std::string>& expected) {
    for (const auto& entry : expected) Check(entry.first, entry.second);
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    iterator->SeekToFirst();
    for (const auto& entry : expected) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(iterator->key(), entry.first);
      ASSERT_EQ(iterator->value(), entry.second);
      iterator->Next();
    }
    ASSERT_FALSE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->SeekToLast();
    for (auto entry = expected.rbegin(); entry != expected.rend(); ++entry) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(iterator->key(), entry->first);
      ASSERT_EQ(iterator->value(), entry->second);
      iterator->Prev();
    }
    ASSERT_FALSE(iterator->Valid());
    ASSERT_OK(iterator->status());
  }
  void ReopenContents(const std::map<std::string, std::string>& expected,
                      bool restore) {
    ASSERT_OK(db_->Close());
    db_.reset();
    if (restore) {
      ASSERT_NO_FATAL_FAILURE(Restore());
    } else {
      ASSERT_NO_FATAL_FAILURE(Open());
    }
    ASSERT_NO_FATAL_FAILURE(CheckContents(expected));
    ASSERT_NO_FATAL_FAILURE(CheckNoBlobs(root_));
  }
  void CrudScenario() {
    ASSERT_FALSE(o_.enable_blob_files);
    ASSERT_NO_FATAL_FAILURE(Open());
    const std::string value(4096, 'v');
    ASSERT_OK(db_->Put(WriteOptions(), "a", "old"));
    ASSERT_OK(db_->Put(WriteOptions(), "b", "deleted"));
    ASSERT_OK(db_->Delete(WriteOptions(), "b"));
    WriteBatch batch;
    ASSERT_OK(batch.Put("a", value));
    ASSERT_OK(batch.Put("b", "temporary"));
    ASSERT_OK(batch.Delete("b"));
    ASSERT_OK(batch.Put("c", "last"));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    const std::map<std::string, std::string> expected{{"a", value},
                                                      {"c", "last"}};
    ASSERT_NO_FATAL_FAILURE(CheckContents(expected));
    std::string absent;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b", &absent).IsNotFound());
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_OK(db_->SyncBackup());
    ASSERT_NO_FATAL_FAILURE(CheckNoBlobs(root_));
    ASSERT_NO_FATAL_FAILURE(ReopenContents(expected, false));
  }
  void RecoveryScenario(bool flush) {
    o_.avoid_flush_during_recovery = true;
    m_.checkpoint_interval_ms = 60000;
    ASSERT_NO_FATAL_FAILURE(Open());
    const std::map<std::string, std::string> expected{
        {"a", std::string(4096, 'a')}, {"b", std::string(4096, 'b')}};
    for (const auto& entry : expected)
      ASSERT_OK(db_->Put(WriteOptions(), entry.first, entry.second));
    ASSERT_OK(db_->Put(WriteOptions(), "removed", "value"));
    ASSERT_OK(db_->Delete(WriteOptions(), "removed"));
    if (flush) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
    ASSERT_OK(db_->SyncBackup());
    ASSERT_EQ(FilesOfType(index_, kTableFile).empty(), !flush);
    ASSERT_EQ(FilesOfType(PublishedPoint(), kTableFile).empty(), !flush);
    if (!flush) {
      ASSERT_FALSE(FilesOfType(PublishedPoint(), kWalFile).empty());
      auto stats = db_->GetBackupStats();
      ASSERT_OK(stats.error);
      ASSERT_EQ(stats.checkpoint_flush_micros, 0U);
      ASSERT_GT(stats.mirrored_wal_bytes, 8192U);
    }
    ASSERT_NO_FATAL_FAILURE(ReopenContents(expected, true));
    // Restore now completes native replay and persists its WAL-only baseline.
    ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
    std::string absent;
    ASSERT_TRUE(db_->Get(ReadOptions(), "removed", &absent).IsNotFound());
  }
  void SstScenario(SstTieringMode mode) {
    EnableSst(mode);
    o_.compression = kNoCompression;
    ASSERT_NO_FATAL_FAILURE(Open());
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 16; ++i) {
      const std::string key = "key" + std::to_string(i);
      expected[key] = std::string(2048, 'a' + i);
      ASSERT_OK(db_->Put(WriteOptions(), key, expected[key]));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->SyncBackup());
    if (mode == SstTieringMode::kAdaptive) {
      AwaitSst([](const auto& stats) {
        return stats.demotions > 0 && stats.ssd_bytes == 0;
      });
      ASSERT_TRUE(FilesOfType(index_, kTableFile).empty());
      auto stats = db_->GetBackupStats();
      ASSERT_OK(stats.error);
      ASSERT_GT(stats.sst_tiering.hdd_bytes, 32768U);
    } else {
      if (mode == SstTieringMode::kObserveOnly)
        AwaitSst(
            [](const auto& stats) { return stats.observed_demotions > 0; });
      ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
    }
    ASSERT_NO_FATAL_FAILURE(CheckContents(expected));
    ASSERT_NO_FATAL_FAILURE(ReopenContents(expected, false));
    ASSERT_NO_FATAL_FAILURE(ReopenContents(expected, true));
  }
};
TEST_F(MetaBypassNonBlobTest, RecoveryOrdinaryBatchKeepsOriginalBytes) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Close());
  db_.reset();
  unsigned checked = 0, rebuilt = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::RecoveryBatchRebuilt",
                                        [&](void*) { ++rebuilt; });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::RecoveryFilterCreated", [&](void* arg) {
        auto* filter = static_cast<WalFilter*>(arg);
        WriteBatch batch, output;
        EXPECT_OK(batch.Put("put", "value"));
        EXPECT_OK(batch.Delete("deleted"));
        EXPECT_OK(batch.SingleDelete("single"));
        EXPECT_OK(batch.DeleteRange("begin", "end"));
        EXPECT_OK(batch.PutLogData("metadata"));
        WriteBatchInternal::SetSequence(&batch, 1);
        bool changed = true;
        EXPECT_EQ(filter->LogRecordFound(1, "test", batch, &output, &changed),
                  WalFilter::WalProcessingOption::kContinueProcessing);
        EXPECT_FALSE(changed);
        EXPECT_EQ(output.Count(), 0U);
        // Default Merge handlers may silently succeed, but it still counts as
        // an update in the header and must not advance the recovery frontier.
        WriteBatch merge;
        EXPECT_OK(merge.Merge("merge", "value"));
        WriteBatchInternal::SetSequence(&merge, 5);
        EXPECT_EQ(filter->LogRecordFound(1, "test", merge, &output, &changed),
                  WalFilter::WalProcessingOption::kStopReplay);
        EXPECT_FALSE(changed);
        WriteBatch next;
        EXPECT_OK(next.Put("next", "value"));
        WriteBatchInternal::SetSequence(&next, 5);
        EXPECT_EQ(filter->LogRecordFound(1, "test", next, &output, &changed),
                  WalFilter::WalProcessingOption::kContinueProcessing);
        EXPECT_FALSE(changed);
        ++checked;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(Restore());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(checked, 1U);
  ASSERT_EQ(rebuilt, 0U);
}
TEST_F(MetaBypassNonBlobTest, RecoveryMixedMissingBlobStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Close());
  db_.reset();
  unsigned checked = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::RecoveryFilterCreated", [&](void* arg) {
        auto* filter = static_cast<WalFilter*>(arg);
        WriteBatch batch, output;
        EXPECT_OK(batch.Put("ordinary", "must-not-be-applied"));
        std::string index;
        BlobIndex::EncodeBlob(&index, 999999, 100, 8, kNoCompression);
        EXPECT_OK(
            WriteBatchInternal::PutBlobIndex(&batch, 0, "missing", index));
        EXPECT_OK(batch.Delete("after-missing"));
        WriteBatchInternal::SetSequence(&batch, 1);
        bool changed = true;
        EXPECT_EQ(filter->LogRecordFound(1, "test", batch, &output, &changed),
                  WalFilter::WalProcessingOption::kStopReplay);
        EXPECT_FALSE(changed);
        // No frontier advance, even though rebuilding copied the first Put.
        output.Clear();
        WriteBatch next;
        EXPECT_OK(next.Put("first", "valid"));
        WriteBatchInternal::SetSequence(&next, 1);
        EXPECT_EQ(filter->LogRecordFound(1, "test", next, &output, &changed),
                  WalFilter::WalProcessingOption::kContinueProcessing);
        EXPECT_FALSE(changed);
        EXPECT_EQ(output.Count(), 0U);
        ++checked;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(Restore());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(checked, 1U);
}
TEST_F(MetaBypassNonBlobTest, RecoverySingle512MiBBatchDoesNotRebuild) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Close());
  db_.reset();
  unsigned checked = 0, rebuilt = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::RecoveryBatchRebuilt",
                                        [&](void*) { ++rebuilt; });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::RecoveryFilterCreated", [&](void* arg) {
        auto* filter = static_cast<WalFilter*>(arg);
        constexpr size_t value_size = 4 * 1024 * 1024;
        WriteBatch batch(128 * (value_size + 32));
        const std::string value(value_size, 'v');
        for (unsigned i = 0; i < 128; ++i)
          EXPECT_OK(batch.Put(std::to_string(i), value));
        ASSERT_EQ(batch.Count(), 128U);
        ASSERT_GE(batch.GetDataSize(), 512ULL * 1024 * 1024);
        WriteBatchInternal::SetSequence(&batch, 1);
        WriteBatch output;
        bool changed = true;
        EXPECT_EQ(filter->LogRecordFound(1, "test", batch, &output, &changed),
                  WalFilter::WalProcessingOption::kContinueProcessing);
        EXPECT_FALSE(changed);
        EXPECT_EQ(output.Count(), 0U);
        EXPECT_EQ(output.GetDataSize(), WriteBatch().GetDataSize());
        ++checked;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(Restore());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(checked, 1U);
  ASSERT_EQ(rebuilt, 0U);
}
TEST_F(MetaBypassNonBlobTest, DefaultOptionsCrudAndReopen) {
  MetaBypassOptions defaults;
  defaults.data_dir = m_.data_dir;
  defaults.backup_dir = m_.backup_dir;
  m_ = std::move(defaults);
  ASSERT_NO_FATAL_FAILURE(CrudScenario());
}
TEST_F(MetaBypassNonBlobTest, ExplicitFalseCrudAndReopen) {
  o_.enable_blob_files = false;
  ASSERT_NO_FATAL_FAILURE(CrudScenario());
}
#ifndef OS_WIN
class MetaBypassIncrementalTest : public MetaBypassTest {
 public:
  size_t tail_size_ = 4096;
  void Crash(int mode, bool rotate = false, bool keep_index = false) {
    const auto mode_text = std::to_string(mode);
    const bool combined = mode == 9;
    if (combined) mode = 3;
    tail_size_ = mode >= 7 ? 8 * 1024 * 1024 : 4096;
    o_.enable_blob_files = mode != 0 && mode != 5;
    if (mode >= 2 && mode != 5) EnableDirectWrite();
    o_.blob_file_size = 4096;
    o_.write_buffer_size = 64 * 1024 * 1024;
    if (mode == 3 || mode == 4 || mode >= 7) {
      EnableTier(8192);
      if (mode == 4 || mode == 8)
        m_.staging_policy = MetaBypassStagingPolicy::kAdaptive;
    }
    if (mode == 5 || combined) EnableSst();
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(),
            "--incremental-crash-writer", index_.c_str(), m_.data_dir.c_str(),
            m_.backup_dir.c_str(), m_.staging_dir.c_str(), mode_text.c_str(),
            rotate ? "1" : "0", nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    if (!keep_index) Clean(index_);
    if (!keep_index && !m_.staging_dir.empty()) Clean(m_.staging_dir);
  }
  void Verify(uint64_t expected_x, MetaBypassRecoveryReport* result = nullptr) {
    MetaBypassRecoveryReport report;
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_, &report));
    if (result) *result = report;
    ASSERT_EQ(report.checkpoint_sequence, 3U);
    ASSERT_EQ(report.recovered_sequence, expected_x) << report.stop_reason;
    ASSERT_NO_FATAL_FAILURE(Open());
    // Independent ordered-operation oracle. No WAL or recovered metadata is
    // used to derive the expected user-visible state.
    struct Operation {
      const char* key;
      char value;
    };
    const Operation operations[] = {
        {"base", 'a'},      {"gone", 'b'}, {"overwrite", 'c'},
        {"overwrite", 'd'}, {"gone", 0},   {"first", 'e'},
        {"second", 'f'},    {"first", 0},  {"last", 'g'}};
    std::map<std::string, std::string> expected;
    for (uint64_t i = 0; i < expected_x; ++i) {
      const auto& operation = operations[i];
      if (operation.value)
        expected[operation.key] =
            std::string(i < 3 ? 4096 : tail_size_, operation.value);
      else
        expected.erase(operation.key);
    }
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    for (const auto& entry : expected) {
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(it->key().ToString(), entry.first);
      ASSERT_EQ(it->value().ToString(), entry.second);
      it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());
    it.reset();
    ASSERT_OK(db_->Put(WriteOptions(), "after", "new allocation"));
    ASSERT_OK(db_->Close());
    db_.reset();
    ASSERT_NO_FATAL_FAILURE(Open());
    Check("after", "new allocation");
  }
  std::vector<std::string> Logs() {
    std::vector<std::string> files, logs;
    EXPECT_OK(fs_->GetChildren(m_.backup_dir + "/work", IOOptions(), &files,
                               nullptr));
    for (const auto& file : files) {
      uint64_t n;
      FileType type;
      if (ParseFileName(file, &n, &type) && type == kWalFile)
        logs.push_back(m_.backup_dir + "/work/" + file);
    }
    std::sort(logs.begin(), logs.end());
    return logs;
  }
};
TEST_F(MetaBypassIncrementalTest, RecoverySamplingIgnoresExistingProgress) {
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  ASSERT_OK(metabypass::Write(fs_.get(), m_.backup_dir + "/PROGRESS",
                              "100 100 3 101 101\n200 200 10 201 201\n"));
  m_.enable_recovery_time_sampling = false;
  MetaBypassRecoveryReport report;
  ASSERT_NO_FATAL_FAILURE(Verify(9, &report));
  ASSERT_FALSE(report.has_time_lower_bound);
  ASSERT_FALSE(report.has_time_upper_bound);
  ASSERT_EQ(report.time_lower_bound_micros, 0U);
  ASSERT_EQ(report.time_upper_bound_micros, 0U);
  std::string progress;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/PROGRESS", &progress));
  ASSERT_TRUE(progress.empty());
}
TEST_F(MetaBypassIncrementalTest, RecoverySamplingClearsResumedBounds) {
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  ASSERT_OK(metabypass::Write(fs_.get(), m_.backup_dir + "/PROGRESS",
                              "100 100 3 101 101\n200 200 10 201 201\n"));
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl(executable.c_str(), executable.c_str(), "--incremental-restore",
          index_.c_str(), m_.data_dir.c_str(), m_.backup_dir.c_str(),
          m_.staging_dir.c_str(), "MetaBypass::RecoveryPersisted", nullptr);
    _exit(127);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
  std::string persisted, point;
  ASSERT_OK(metabypass::Read(fs_.get(), index_ + "/METABYPASS-RECOVERED",
                             &persisted));
  std::istringstream marker(persisted);
  std::getline(marker, point);
  uint64_t checkpoint, sequence, lower, upper;
  bool has_lower, has_upper;
  ASSERT_TRUE(bool(marker >> checkpoint >> sequence >> has_lower >> lower >>
                   has_upper >> upper));
  ASSERT_TRUE(has_lower);
  ASSERT_TRUE(has_upper);
  ASSERT_EQ(lower, 100U);
  ASSERT_EQ(upper, 201U);
  m_.enable_recovery_time_sampling = false;
  MetaBypassRecoveryReport report;
  ASSERT_NO_FATAL_FAILURE(Verify(9, &report));
  ASSERT_FALSE(report.has_time_lower_bound);
  ASSERT_FALSE(report.has_time_upper_bound);
  ASSERT_EQ(report.time_lower_bound_micros, 0U);
  ASSERT_EQ(report.time_upper_bound_micros, 0U);
}
TEST_F(MetaBypassIncrementalTest, WalOnlyContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, OrdinaryBlobContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(1));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, DirectWriteContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(2));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, AlwaysStagingContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(3));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, AdaptiveStagingContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(4));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, SstTieringContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(5));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, CombinedBlobAndSstTieringContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(9));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, PartialActiveBlobKeepsEarlierRecords) {
  ASSERT_NO_FATAL_FAILURE(Crash(7));
  ASSERT_NO_FATAL_FAILURE(Verify(7));
}
TEST_F(MetaBypassIncrementalTest,
       AdaptivePartialActiveBlobKeepsEarlierRecords) {
  ASSERT_NO_FATAL_FAILURE(Crash(8));
  ASSERT_NO_FATAL_FAILURE(Verify(7));
}
TEST_F(MetaBypassIncrementalTest, ContinuousPrefixAfterNormalRestart) {
  ASSERT_NO_FATAL_FAILURE(Crash(6));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, IncrementalBlobSurvivesObsoleteFileGC) {
  ASSERT_NO_FATAL_FAILURE(Crash(2, true));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, MissingBlobStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Crash(2));
  auto blobs = BlobFiles();
  ASSERT_EQ(blobs.size(), 7U);
  auto second_value = blobs.begin();
  std::advance(second_value, 5);
  ASSERT_OK(
      fs_->DeleteFile(m_.data_dir + "/" + *second_value, IOOptions(), nullptr));
  ASSERT_NO_FATAL_FAILURE(Verify(5));
}
TEST_F(MetaBypassIncrementalTest, MissingWholeWalStopsAtGap) {
  ASSERT_NO_FATAL_FAILURE(Crash(0, true));
  auto logs = Logs();
  ASSERT_GE(logs.size(), 3U);
  ASSERT_OK(fs_->DeleteFile(logs[1], IOOptions(), nullptr));
  ASSERT_NO_FATAL_FAILURE(Verify(5));
}
TEST_F(MetaBypassIncrementalTest, TruncatedWalStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Crash(0, true));
  auto logs = Logs();
  ASSERT_GE(logs.size(), 3U);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(fs_->ReopenWritableFile(logs[1], FileOptions(), &writer, nullptr));
  ASSERT_OK(writer->Truncate(30, IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_NO_FATAL_FAILURE(Verify(5));
}
TEST_F(MetaBypassIncrementalTest, CorruptWalStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Crash(0, true));
  auto logs = Logs();
  ASSERT_GE(logs.size(), 3U);
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), logs[1], &bytes));
  ASSERT_GT(bytes.size(), 40U);
  bytes[40] ^= 1;
  ASSERT_OK(metabypass::Write(fs_.get(), logs[1], bytes));
  ASSERT_NO_FATAL_FAILURE(Verify(5));
}
TEST_F(MetaBypassIncrementalTest, TrustedSameWalPrefixWins) {
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  auto logs = Logs();
  ASSERT_FALSE(logs.empty());
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), logs.front(), &bytes));
  ASSERT_GT(bytes.size(), 40U);
  bytes[40] ^= 1;
  ASSERT_OK(metabypass::Write(fs_.get(), logs.front(), bytes));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, MissingExtentStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Crash(3));
  auto blobs = BlobFiles();
  ASSERT_GE(blobs.size(), 7U);
  auto second = blobs.begin();
  std::advance(second, 5);
  uint64_t number;
  FileType type;
  ASSERT_TRUE(ParseFileName(*second, &number, &type));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &files, nullptr));
  bool removed = false;
  for (const auto& file : files) {
    if (file.find("segment-" + std::to_string(number) + "-") != 0) continue;
    ASSERT_OK(fs_->DeleteFile(m_.data_dir + "/" + file, IOOptions(), nullptr));
    removed = true;
  }
  ASSERT_TRUE(removed);
  ASSERT_NO_FATAL_FAILURE(Verify(5));
}
TEST_F(MetaBypassIncrementalTest, PartialExtentStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Crash(4));
  auto blobs = BlobFiles();
  ASSERT_GE(blobs.size(), 7U);
  auto second = blobs.begin();
  std::advance(second, 5);
  uint64_t number;
  FileType type;
  ASSERT_TRUE(ParseFileName(*second, &number, &type));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &files, nullptr));
  bool truncated = false;
  for (const auto& file : files) {
    if (file.find("segment-" + std::to_string(number) + "-") != 0) continue;
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(fs_->ReopenWritableFile(m_.data_dir + "/" + file, FileOptions(),
                                      &writer, nullptr));
    ASSERT_OK(writer->Truncate(40, IOOptions(), nullptr));
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    truncated = true;
  }
  ASSERT_TRUE(truncated);
  ASSERT_NO_FATAL_FAILURE(Verify(5));
}
class MetaBypassRecoveryStageTest
    : public MetaBypassIncrementalTest,
      public testing::WithParamInterface<const char*> {};
TEST_P(MetaBypassRecoveryStageTest, ResumeWithoutLosingPrefix) {
  const char* stage = GetParam();
  ASSERT_NO_FATAL_FAILURE(Crash(3));
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl(executable.c_str(), executable.c_str(), "--incremental-restore",
          index_.c_str(), m_.data_dir.c_str(), m_.backup_dir.c_str(),
          m_.staging_dir.c_str(), stage, nullptr);
    _exit(127);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
INSTANTIATE_TEST_CASE_P(
    Stages, MetaBypassRecoveryStageTest,
    testing::Values("MetaBypass::RecoveryPrepared",
                    "MetaBypass::RecoveryNativeClosed",
                    "MetaBypass::RecoveryPersisted",
                    "MetaBypass::TierRestoreDescriptorPublished",
                    "MetaBypass::TierRestoreDescriptorRemoved",
                    "MetaBypass::RecoveryFinalized"));
TEST_F(MetaBypassIncrementalTest, PersistedStageSupportsBackupPathWithSpaces) {
  m_.backup_dir += " with spaces";
  ASSERT_NO_FATAL_FAILURE(Crash(3));
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl(executable.c_str(), executable.c_str(), "--incremental-restore",
          index_.c_str(), m_.data_dir.c_str(), m_.backup_dir.c_str(),
          m_.staging_dir.c_str(), "MetaBypass::RecoveryPersisted", nullptr);
    _exit(127);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, WalReseedNeverTruncatesSurvivingPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(0, false, true));
  auto logs = Logs();
  ASSERT_FALSE(logs.empty());
  std::string before, after;
  ASSERT_OK(metabypass::Read(fs_.get(), logs.front(), &before));
  auto failing = std::make_shared<MarkerFailureFileSystem>(fs_);
  failing->fail_path = logs.front();
  {
    auto storage = std::make_shared<metabypass::SeparatedStorage>(
        failing, index_, m_.data_dir);
    metabypass::Backup backup(storage, index_, m_, o_);
    ASSERT_OK(backup.Lock());
    ASSERT_OK(backup.Start(true));
    ASSERT_OK(backup.Stop());
  }
  ASSERT_EQ(failing->failures, 0);
  ASSERT_OK(metabypass::Read(fs_.get(), logs.front(), &after));
  ASSERT_EQ(before, after);
  Clean(index_);
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, LegacyCheckpointDoesNotInventFrontier) {
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  const auto name = pointer.substr(0, pointer.find('\n'));
  ASSERT_OK(fs_->DeleteFile(m_.backup_dir + "/" + name + "/RECOVERY",
                            IOOptions(), nullptr));
  ASSERT_NO_FATAL_FAILURE(Verify(3));
}
#endif
TEST_F(MetaBypassTest, SyncWriteReleasesCheckpointCaptureFence) {
  EnableDirectWrite();
  ASSERT_NO_FATAL_FAILURE(Open());
  std::mutex mutex;
  std::condition_variable cv;
  bool barrier = false, release = false, checkpoint_done = false;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeSyncWriteBarrier", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        barrier = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status write_status, checkpoint_status;
  std::thread writer([&] {
    WriteOptions sync;
    sync.sync = true;
    write_status = db_->Put(sync, "fenced", "value");
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return barrier; }));
  }
  std::thread checkpoint([&] {
    checkpoint_status = db_->SyncBackup();
    std::lock_guard<std::mutex> lock(mutex);
    checkpoint_done = true;
    cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(10),
                            [&] { return checkpoint_done; }));
    release = true;
    cv.notify_all();
  }
  checkpoint.join();
  writer.join();
  ASSERT_OK(write_status);
  ASSERT_OK(checkpoint_status);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  MetaBypassRecoveryReport report;
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_, &report));
  ASSERT_EQ(report.checkpoint_sequence, 1U);
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("fenced", "value");
}

#ifndef NDEBUG
TEST_F(MetaBypassTest,
       WriteGateCoversPreCloseWriteAndClearsCheckpointDeadline) {
  o_.enable_blob_files = false;
  m_.checkpoint_interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  const auto before = db_->GetBackupStats();
  ASSERT_OK(before.error);
  TestGate boundary;
  std::atomic<bool> observing{false}, reported{false};
  std::promise<bool> idle;
  auto scheduling = idle.get_future();
  auto* sync = SyncPoint::GetInstance();
  auto report = [&](bool is_idle) {
    if (observing.load() && !reported.exchange(true)) idle.set_value(is_idle);
  };
  Defer cleanup([&] {
    boundary.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("MetaBypass::CheckpointIdle", [&](void*) { report(true); });
  sync->SetCallBack("MetaBypass::CheckpointDeadline",
                    [&](void*) { report(false); });
  sync->SetCallBack("MetaBypass::CheckpointBoundary",
                    [&](void*) { boundary.Block(); });
  sync->EnableProcessing();
  auto checkpoint =
      std::async(std::launch::async, [&] { return db_->SyncBackup(); });
  EXPECT_TRUE(boundary.WaitUntilBlockedFor(std::chrono::seconds(10)));
  auto writer = std::async(std::launch::async, [&] {
    return db_->Put(WriteOptions(), "before-close", "value");
  });
  const auto completed = writer.wait_for(std::chrono::seconds(10));
  observing = true;
  boundary.Release();
  EXPECT_EQ(completed, std::future_status::ready);
  ASSERT_OK(writer.get());
  ASSERT_OK(checkpoint.get());
  ASSERT_EQ(scheduling.wait_for(std::chrono::seconds(10)),
            std::future_status::ready);
  EXPECT_TRUE(scheduling.get()) << "covered write left a checkpoint deadline";
  const auto after = db_->GetBackupStats();
  ASSERT_OK(after.error);
  EXPECT_EQ(after.checkpoint_captures, before.checkpoint_captures + 1);
  std::string recovery;
  ASSERT_OK(
      metabypass::Read(fs_.get(), PublishedPoint() + "/RECOVERY", &recovery));
  std::istringstream input(recovery);
  uint64_t sequence = 0;
  ASSERT_TRUE(bool(input >> sequence));
  EXPECT_EQ(sequence, 1U);
}

TEST_F(MetaBypassTest, WriteGateDrainsAdmittedWritersBeforeCheckpoint) {
  EnableDirectWrite();
  ASSERT_NO_FATAL_FAILURE(Open());
  std::array<TestGate, 2> admitted;
  TestGate closed, waiting, capturing;
  std::atomic<size_t> admissions{0}, released_writers{0};
  std::atomic<bool> pinned{false};
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    for (auto& gate : admitted) gate.Release();
    closed.Release();
    waiting.Release();
    capturing.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("MetaBypass::WriteGateAdmitted", [&](void*) {
    const size_t index = admissions.fetch_add(1);
    if (index < admitted.size()) {
      admitted[index].Block();
      released_writers.fetch_add(1);
    }
  });
  sync->SetCallBack("MetaBypass::WriteGateClosed",
                    [&](void*) { closed.Block(); });
  sync->SetCallBack("MetaBypass::WriteGateAdmissionBlocked",
                    [&](void*) { waiting.Block(); });
  sync->SetCallBack("MetaBypass::NativeFilesPinned", [&](void*) {
    EXPECT_EQ(released_writers.load(), admitted.size());
    pinned = true;
  });
  sync->SetCallBack("MetaBypass::NativeCaptureStatus", [&](void* arg) {
    EXPECT_OK(*static_cast<Status*>(arg));
    capturing.Block();
  });
  sync->EnableProcessing();
  auto first = std::async(std::launch::async, [&] {
    return db_->Put(WriteOptions(), "first", "one");
  });
  EXPECT_TRUE(admitted[0].WaitUntilBlockedFor(std::chrono::seconds(10)));
  auto second = std::async(std::launch::async, [&] {
    return db_->Put(WriteOptions(), "second", "two");
  });
  EXPECT_TRUE(admitted[1].WaitUntilBlockedFor(std::chrono::seconds(10)));
  auto checkpoint =
      std::async(std::launch::async, [&] { return db_->SyncBackup(); });
  EXPECT_TRUE(closed.WaitUntilBlockedFor(std::chrono::seconds(10)));
  EXPECT_FALSE(pinned.load());
  auto late = std::async(std::launch::async, [&] {
    return db_->Put(WriteOptions(), "late", "three");
  });
  EXPECT_TRUE(waiting.WaitUntilBlockedFor(std::chrono::seconds(10)));
  waiting.Release();
  closed.Release();
  admitted[0].Release();
  EXPECT_OK(first.get());
  EXPECT_FALSE(pinned.load());
  EXPECT_FALSE(capturing.WaitUntilBlockedFor(std::chrono::seconds(0)));
  EXPECT_EQ(admissions.load(), 2U);
  admitted[1].Release();
  EXPECT_OK(second.get());
  EXPECT_TRUE(capturing.WaitUntilBlockedFor(std::chrono::seconds(10)));
  EXPECT_TRUE(pinned.load());
  EXPECT_EQ(admissions.load(), 2U);
  EXPECT_EQ(late.wait_for(std::chrono::seconds(0)),
            std::future_status::timeout);
  capturing.Release();
  EXPECT_OK(late.get());
  EXPECT_OK(checkpoint.get());
  EXPECT_EQ(admissions.load(), 3U);
  sync->DisableProcessing();

  const std::string point = PublishedPoint();
  const std::string one = "one", two = "two", three = "three";
  CheckPointValue(point, "first", &one);
  CheckPointValue(point, "second", &two);
  CheckPointValue(point, "late", nullptr);
  ASSERT_OK(db_->SyncBackup());
  CheckPointValue(PublishedPoint(), "late", &three);
}

TEST_F(MetaBypassTest, WriteGateReopensAfterCheckpointCaptureFailure) {
  EnableDirectWrite();
  ASSERT_NO_FATAL_FAILURE(Open());
  WriteOptions durable;
  durable.sync = true;
  ASSERT_OK(db_->Put(durable, "base", "value"));
  const std::string old_point = PublishedPoint();
  const Status failure = Status::IOError("injected checkpoint capture failure");
  TestGate capturing, waiting, reopened;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    capturing.Release();
    waiting.Release();
    reopened.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("MetaBypass::NativeCaptureStatus", [&](void* arg) {
    capturing.Block();
    *static_cast<Status*>(arg) = failure;
  });
  sync->SetCallBack("MetaBypass::WriteGateAdmissionBlocked",
                    [&](void*) { waiting.Block(); });
  sync->SetCallBack("MetaBypass::WriteGateReopened",
                    [&](void*) { reopened.Block(); });
  sync->EnableProcessing();
  auto checkpoint =
      std::async(std::launch::async, [&] { return db_->SyncBackup(); });
  EXPECT_TRUE(capturing.WaitUntilBlockedFor(std::chrono::seconds(10)));
  auto late = std::async(std::launch::async, [&] {
    return db_->Put(WriteOptions(), "late", "admitted after capture");
  });
  EXPECT_TRUE(waiting.WaitUntilBlockedFor(std::chrono::seconds(10)));
  EXPECT_EQ(late.wait_for(std::chrono::seconds(0)),
            std::future_status::timeout);
  waiting.Release();
  capturing.Release();
  EXPECT_TRUE(reopened.WaitUntilBlockedFor(std::chrono::seconds(10)));
  const auto completed = late.wait_for(std::chrono::seconds(10));
  EXPECT_EQ(completed, std::future_status::ready);
  // The waiter can make progress while the validator is still unwinding.
  if (completed == std::future_status::ready) {
    EXPECT_OK(late.get());
  }
  reopened.Release();
  if (late.valid()) late.get().PermitUncheckedError();
  EXPECT_EQ(checkpoint.get().ToString(), failure.ToString());
  sync->DisableProcessing();
  Check("late", "admitted after capture");
  EXPECT_EQ(PublishedPoint(), old_point);
  EXPECT_EQ(db_->Close().ToString(), failure.ToString());
}
#endif

TEST_F(MetaBypassTest, RecoverySamplingThreadOption) {
  ASSERT_TRUE(m_.enable_recovery_time_sampling);
  for (bool enabled : {true, false}) {
    SCOPED_TRACE(enabled);
    m_.enable_recovery_time_sampling = enabled;
    bool observed = false;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::SamplerStarted", [&](void* arg) {
          observed = true;
          EXPECT_EQ(static_cast<std::thread*>(arg)->joinable(), enabled);
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_TRUE(observed);
    ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
    ASSERT_OK(db_->SyncBackup());
    ASSERT_OK(db_->Close());
    db_.reset();
    if (!enabled) {
      ASSERT_TRUE(
          fs_->FileExists(m_.backup_dir + "/PROGRESS", IOOptions(), nullptr)
              .IsNotFound());
    }
    SyncPoint::GetInstance()->DisableProcessing();
    Clean(root_);
    ASSERT_OK(fs_->CreateDirIfMissing(root_, IOOptions(), nullptr));
  }
}
TEST(MetaBypassRecoveryTimeTest, MockClockVisibilityBrackets) {
  MockSystemClock clock(Env::Default()->GetSystemClock());
  clock.SetCurrentTime(1);
  std::string samples;
  const auto sample = [&](uint64_t sequence, int query_micros) {
    const uint64_t begin = clock.NowMicros();
    const uint64_t steady_begin = clock.NowNanos() / 1000;
    clock.SleepForMicroseconds(query_micros);
    samples += std::to_string(begin) + " " + std::to_string(steady_begin) +
               " " + std::to_string(sequence) + " " +
               std::to_string(clock.NowMicros()) + " " +
               std::to_string(clock.NowNanos() / 1000) + "\n";
  };
  sample(3, 10);
  clock.MockSleepForSeconds(1);
  sample(7, 20);
  clock.MockSleepForSeconds(4);
  sample(9, 30);
  MetaBypassRecoveryReport report;
  report.recovered_sequence = 7;
  metabypass::Backup::EstimateRecoveryTime(samples, &report);
  ASSERT_TRUE(report.has_time_lower_bound);
  ASSERT_TRUE(report.has_time_upper_bound);
  ASSERT_EQ(report.time_lower_bound_micros, 2000010U);
  ASSERT_EQ(report.time_upper_bound_micros, 6000060U);
}
TEST(MetaBypassRecoveryTimeTest, BoundsGapsAndClockDiscontinuity) {
  MetaBypassRecoveryReport report;
  report.recovered_sequence = 7;
  metabypass::Backup::EstimateRecoveryTime(
      "1000000 1000000 3 1000010 1000010\n"
      "2000000 2000000 7 2000020 2000020\n"
      "6000000 6000000 9 6000030 6000030\n",
      &report);
  ASSERT_TRUE(report.has_time_lower_bound);
  ASSERT_TRUE(report.has_time_upper_bound);
  ASSERT_EQ(report.time_lower_bound_micros, 2000000U);
  ASSERT_EQ(report.time_upper_bound_micros, 6000030U);
  metabypass::Backup::EstimateRecoveryTime(
      "1000000 1000000 3 1000010 1000010\n"
      "9000000 2000000 9 9000020 2000020\n",
      &report);
  ASSERT_FALSE(report.has_time_lower_bound);
  ASSERT_FALSE(report.has_time_upper_bound);
  metabypass::Backup::EstimateRecoveryTime(
      "1000000 1000000 3 1000010 1000010\n2000000 2000000", &report);
  ASSERT_TRUE(report.has_time_lower_bound);
  ASSERT_FALSE(report.has_time_upper_bound);
}
TEST_F(MetaBypassNonBlobTest, WalOnlyBackupDoesNotFlushAndRestores) {
  m_.wal_queue_capacity = 256 * 1024;
  m_.batch_bytes = QueueBudget(m_);
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(RecoveryScenario(false));
}
TEST_F(MetaBypassNonBlobTest, SstBackupRestores) {
  ASSERT_NO_FATAL_FAILURE(RecoveryScenario(true));
}
TEST_F(MetaBypassNonBlobTest, SstDisabledReadReopenAndRestore) {
  ASSERT_NO_FATAL_FAILURE(SstScenario(SstTieringMode::kDisabled));
}
TEST_F(MetaBypassNonBlobTest, SstObserveReadReopenAndRestore) {
  ASSERT_NO_FATAL_FAILURE(SstScenario(SstTieringMode::kObserveOnly));
}
TEST_F(MetaBypassNonBlobTest, SstAdaptiveColdReadReopenAndRestore) {
  ASSERT_NO_FATAL_FAILURE(SstScenario(SstTieringMode::kAdaptive));
}
TEST_F(MetaBypassNonBlobTest, RejectBlobFeaturesBeforeDirectoryChanges) {
  for (bool existing : {false, true}) {
    SCOPED_TRACE(existing);
    if (existing) {
      ASSERT_NO_FATAL_FAILURE(Open());
      ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
      ASSERT_OK(db_->Close());
      db_.reset();
    }
    for (int conflict = 1; conflict <= 3; ++conflict) {
      SCOPED_TRACE(conflict);
      Options invalid = o_;
      invalid.enable_blob_direct_write = (conflict & 1) != 0;
      invalid.allow_concurrent_memtable_write = false;
      MetaBypassOptions bypass = m_;
      if (conflict & 2) {
        bypass.staging_dir = root_ + "/staging";
        bypass.staging_capacity = 8192;
      }
      const auto before = SnapshotTree(root_);
      ASSERT_TRUE(
          MetaBypassDB::Open(invalid, bypass, index_, &db_).IsNotSupported());
      ASSERT_FALSE(db_);
      ASSERT_EQ(SnapshotTree(root_), before);
      ASSERT_TRUE(
          MetaBypassDB::Restore(invalid, bypass, index_).IsNotSupported());
      ASSERT_EQ(SnapshotTree(root_), before);
    }
  }
}
TEST_F(MetaBypassNonBlobTest, RandomizedRoundTrip) {
  const uint32_t seed = o_.env->NowMicros() & 0xffffffffU;
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random random(seed);
  std::map<std::string, std::string> expected;
  ASSERT_NO_FATAL_FAILURE(Open());
  for (int step = 0; step < 100; ++step) {
    WriteBatch batch;
    for (int operation = 0; operation < 2; ++operation) {
      const std::string key = std::to_string(random.Uniform(30));
      if (random.OneIn(4)) {
        ASSERT_OK(batch.Delete(key));
        expected.erase(key);
      } else {
        expected[key] =
            std::string(random.Uniform(2048), 'a' + random.Uniform(26));
        ASSERT_OK(batch.Put(key, expected[key]));
      }
    }
    WriteOptions write;
    write.sync = step % 31 == 0;
    ASSERT_OK(db_->Write(write, &batch));
    if (step % 23 == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_NO_FATAL_FAILURE(ReopenContents(expected, true));
}
TEST_F(MetaBypassNonBlobTest, ConcurrentBackupAllowsWritesAndFlush) {
  m_.checkpoint_interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
  ASSERT_OK(db_->SyncBackup());
  TestGate validating;
  std::atomic<bool> first{true}, returned{false};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) {
                                          if (first.exchange(false))
                                            validating.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "candidate", "value"));
  Status synced;
  std::thread backup([&] {
    synced = db_->SyncBackup();
    returned = true;
  });
  validating.WaitUntilBlocked();
  std::map<std::string, std::string> expected{{"old", "retained"},
                                              {"candidate", "value"}};
  for (int i = 0; i < 32; ++i) {
    const std::string key = "tail" + std::to_string(i);
    expected[key] = std::string(2048, 'v');
    EXPECT_OK(db_->Put(WriteOptions(), key, expected[key]));
  }
  EXPECT_OK(db_->Flush(FlushOptions()));
  EXPECT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_FALSE(returned.load());
  validating.Release();
  backup.join();
  ASSERT_OK(synced);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(db_->SyncBackup());
  ASSERT_NO_FATAL_FAILURE(ReopenContents(expected, true));
}

TEST_F(MetaBypassTest, RecoveryProgressIdentityAndClosedState) {
  ASSERT_NO_FATAL_FAILURE(Open());
  MetaBypassRecoveryProgress before, after;
  ASSERT_TRUE(db_->GetRecoveryProgress(nullptr).IsInvalidArgument());
  ASSERT_OK(db_->GetRecoveryProgress(&before));
  ASSERT_FALSE(before.db_identity.empty());
  ASSERT_FALSE(before.open_session_id.empty());
  ASSERT_OK(db_->Put(WriteOptions(), "progress", "value"));
  ASSERT_OK(db_->GetRecoveryProgress(&after));
  ASSERT_EQ(before.db_identity, after.db_identity);
  ASSERT_EQ(before.open_session_id, after.open_session_id);
  ASSERT_EQ(before.visible_sequence + 1, after.visible_sequence);
  ASSERT_OK(db_->Close());
  ASSERT_TRUE(db_->GetRecoveryProgress(&after).IsShutdownInProgress());
  ASSERT_EQ(before.open_session_id, after.open_session_id);
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->GetRecoveryProgress(&after));
  ASSERT_EQ(before.db_identity, after.db_identity);
  ASSERT_NE(before.open_session_id, after.open_session_id);
}
TEST_F(MetaBypassTest, RecoveryProgressConcurrentWriters) {
  ASSERT_NO_FATAL_FAILURE(Open());
  MetaBypassRecoveryProgress initial;
  ASSERT_OK(db_->GetRecoveryProgress(&initial));
  std::atomic<bool> start{false};
  std::atomic<unsigned> failures{0};
  std::vector<std::thread> writers;
  for (unsigned writer = 0; writer < 4; ++writer) {
    writers.emplace_back([&, writer] {
      while (!start.load()) std::this_thread::yield();
      for (unsigned operation = 0; operation < 32; ++operation) {
        auto status = db_->Put(
            WriteOptions(),
            std::to_string(writer) + "/" + std::to_string(operation), "value");
        if (!status.ok()) ++failures;
      }
    });
  }
  start.store(true);
  uint64_t last = initial.visible_sequence;
  for (unsigned observation = 0; observation < 128; ++observation) {
    MetaBypassRecoveryProgress progress;
    ASSERT_OK(db_->GetRecoveryProgress(&progress));
    ASSERT_EQ(initial.open_session_id, progress.open_session_id);
    ASSERT_GE(progress.visible_sequence, last);
    last = progress.visible_sequence;
  }
  for (auto& writer : writers) writer.join();
  ASSERT_EQ(failures.load(), 0U);
  MetaBypassRecoveryProgress final;
  ASSERT_OK(db_->GetRecoveryProgress(&final));
  ASSERT_EQ(final.visible_sequence, initial.visible_sequence + 128);
}
TEST_F(MetaBypassTest, RecoveryProgressCloseSerialization) {
  ASSERT_NO_FATAL_FAILURE(Open());
  std::mutex mutex;
  std::condition_variable cv;
  bool start = false;
  std::atomic<unsigned> failures{0};
  std::thread observer([&] {
    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&] { return start; });
    }
    for (unsigned i = 0; i < 1000; ++i) {
      MetaBypassRecoveryProgress progress;
      auto status = db_->GetRecoveryProgress(&progress);
      if (!status.ok() && !status.IsShutdownInProgress()) ++failures;
    }
  });
  {
    std::lock_guard<std::mutex> lock(mutex);
    start = true;
    cv.notify_all();
  }
  auto closed = db_->Close();
  observer.join();
  ASSERT_OK(closed);
  ASSERT_EQ(failures.load(), 0U);
}
TEST_F(MetaBypassTest, BlobGCBatchesAtMost16DirectorySyncs) {
  for (uint64_t count : {16, 17}) {
    SCOPED_TRACE(count);
    auto observed = std::make_shared<ReplacementFileSystem>(fs_);
    auto storage = std::make_shared<metabypass::SeparatedStorage>(
        observed, index_, m_.data_dir);
    ASSERT_OK(storage->Lock());
    for (uint64_t number = 1; number <= count; ++number)
      ASSERT_OK(
          metabypass::Write(fs_.get(), storage->BlobPath(number), "blob"));
    metabypass::Backup backup(storage, index_, m_, o_);
    metabypass::BackupTestAccess::SeedBlobGC(&backup, count);
    uint64_t reclaimed = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::BlobReclaimed", [&](void*) {
          if (++reclaimed == count)
            metabypass::BackupTestAccess::FinishBlobGC(&backup);
        });
    SyncPoint::GetInstance()->EnableProcessing();
    metabypass::BackupTestAccess::RunBlobGC(&backup);
    ASSERT_OK(backup.Error());
    ASSERT_EQ(observed->Count("dirsync." + m_.data_dir), (count + 15) / 16);
    auto stats = backup.Stats();
    ASSERT_OK(stats.error);
    ASSERT_EQ(stats.reclaimed_blob_files, count);
    ASSERT_EQ(stats.reclaimed_blob_bytes, count * 4);
    ASSERT_EQ(stats.pending_blob_gc_files, 0U);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
}
TEST_F(MetaBypassTest, BlobGCBatchFailureAccountsOnlyDurablePrefix) {
  for (bool sync_failure : {false, true}) {
    SCOPED_TRACE(sync_failure);
    auto observed = std::make_shared<ReplacementFileSystem>(fs_);
    auto storage = std::make_shared<metabypass::SeparatedStorage>(
        observed, index_, m_.data_dir);
    ASSERT_OK(storage->Lock());
    for (uint64_t number = 1; number <= 3; ++number)
      ASSERT_OK(
          metabypass::Write(fs_.get(), storage->BlobPath(number), "blob"));
    observed->hook = [&](const std::string& key) {
      if (key == "delete." + storage->BlobPath(3))
        return IOStatus::IOError("first deletion failure");
      if (sync_failure && key == "dirsync." + m_.data_dir)
        return IOStatus::IOError("later directory failure");
      return IOStatus::OK();
    };
    metabypass::Backup backup(storage, index_, m_, o_);
    metabypass::BackupTestAccess::SeedBlobGC(&backup, 3);
    metabypass::BackupTestAccess::RunBlobGC(&backup);
    const Status error = backup.Error();
    ASSERT_TRUE(error.IsIOError());
    ASSERT_NE(error.ToString().find("first deletion failure"),
              std::string::npos);
    ASSERT_EQ(observed->Count("dirsync." + m_.data_dir), 1U);
    auto stats = backup.Stats();
    ASSERT_TRUE(stats.error.IsIOError());
    ASSERT_EQ(stats.reclaimed_blob_files, sync_failure ? 0U : 2U);
    ASSERT_EQ(stats.reclaimed_blob_bytes, sync_failure ? 0U : 8U);
    ASSERT_EQ(stats.pending_blob_gc_files, sync_failure ? 3U : 1U);
    ASSERT_EQ(stats.pending_blob_gc_bytes, sync_failure ? 12U : 4U);
    ASSERT_OK(fs_->FileExists(storage->BlobPath(3), IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, BlobGCBatchMissingStillSyncsAndPropagatesErrors) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  metabypass::SeparatedStorage storage(observed, index_, m_.data_dir);
  ASSERT_OK(storage.Lock());
  std::vector<std::pair<uint64_t, uint64_t>> durable;
  ASSERT_OK(storage.ReclaimBlobs({1, 2}, &durable));
  ASSERT_EQ(durable.size(), 2U);
  ASSERT_EQ(durable[0].second, 0U);
  ASSERT_EQ(observed->Count("dirsync." + m_.data_dir), 1U);
  observed->hook = [&](const std::string& key) {
    return key == "dirsync." + m_.data_dir
               ? IOStatus::IOError("missing directory sync")
               : IOStatus::OK();
  };
  ASSERT_TRUE(storage.ReclaimBlobs({1}, &durable).IsIOError());
  ASSERT_TRUE(durable.empty());
  observed->hook = [&](const std::string& key) {
    return key == "size." + storage.BlobPath(2)
               ? IOStatus::IOError("size failure")
               : IOStatus::OK();
  };
  ASSERT_TRUE(storage.ReclaimBlobs({1, 2}, &durable).IsIOError());
  ASSERT_EQ(durable.size(), 1U);
  ASSERT_EQ(durable.front().first, 1U);
  ASSERT_EQ(observed->Count("dirsync." + m_.data_dir), 3U);
}
TEST_F(MetaBypassTest, BlobGCWholeFileAndRestore) {
  BlobGCScenario(false, false, true);
}
TEST_F(MetaBypassTest, BlobGCStagingAndRestore) {
  BlobGCScenario(true, false, true);
}
TEST_F(MetaBypassTest, BlobGCSstTierAndRestore) {
  BlobGCScenario(true, true, true);
}
TEST_F(MetaBypassTest, BlobGCDisabledRetainsFiles) {
  BlobGCScenario(false, false, false);
}
TEST_F(MetaBypassTest, BlobGCRejectsForcedRelocation) {
  Open();
  CompactRangeOptions options;
  options.blob_garbage_collection_policy = BlobGarbageCollectionPolicy::kForce;
  ASSERT_TRUE(db_->CompactRange(options, nullptr, nullptr).IsNotSupported());
}
TEST_F(MetaBypassTest, RejectLegacyDatabaseFormatWithoutMutation) {
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), index_));
  const std::string marker = index_ + "/METABYPASS-FORMAT";
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "MBN5\n"));
  ASSERT_OK(metabypass::Write(fs_.get(), index_ + "/sentinel", "untouched"));
  const auto before = SnapshotTree(root_);
  ASSERT_FALSE(MetaBypassDB::Open(o_, m_, index_, &db_).ok());
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &bytes));
  ASSERT_EQ(bytes, "MBN5\n");
  ASSERT_OK(metabypass::Read(fs_.get(), index_ + "/sentinel", &bytes));
  ASSERT_EQ(bytes, "untouched");
  ASSERT_TRUE(fs_->FileExists(m_.data_dir, IOOptions(), nullptr).IsNotFound());
  ASSERT_TRUE(
      fs_->FileExists(m_.backup_dir, IOOptions(), nullptr).IsNotFound());
  ASSERT_EQ(SnapshotTree(root_), before);
}
TEST_F(MetaBypassTest, RejectLegacyBackupFormatWithoutMutation) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  const std::string marker = m_.backup_dir + "/METABYPASS-FORMAT";
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "MBN5\n"));
  const std::string point = PublishedPoint();
  std::string inventory;
  ASSERT_OK(metabypass::Read(fs_.get(), point + "/INVENTORY", &inventory));
  Clean(index_);
  const auto before = SnapshotTree(root_);
  ASSERT_FALSE(MetaBypassDB::Restore(o_, m_, index_).ok());
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &after));
  ASSERT_EQ(after, "MBN5\n");
  ASSERT_OK(metabypass::Read(fs_.get(), point + "/INVENTORY", &after));
  ASSERT_EQ(after, inventory);
  ASSERT_TRUE(fs_->FileExists(index_, IOOptions(), nullptr).IsNotFound());
  ASSERT_EQ(SnapshotTree(root_), before);
}
TEST_F(MetaBypassTest, BlobGCNewestRootPreventsIdleCaptures) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "live", "root"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  const auto blobs = BlobFiles();
  ASSERT_EQ(blobs.size(), 1U);
  auto storage =
      std::make_shared<metabypass::SeparatedStorage>(fs_, index_, m_.data_dir);
  ASSERT_OK(storage->Lock());
  metabypass::Backup backup(storage, index_, m_, o_);
  ASSERT_OK(backup.Start(true));
  // Feed the retirement protocol directly while the latest physical snapshot
  // still references this blob. This exercises the backup root check itself.
  ASSERT_OK(
      backup.DeleteFile(index_ + "/" + *blobs.begin(), IOOptions(), nullptr));
  // This low-level test drains a retirement event against loaded roots. It
  // does not activate native capture without an owning DB.
  CheckNoIdleCaptures();
  ASSERT_OK(backup.Stop());
  auto stats = backup.Stats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.pending_blob_gc_files, 1U);
  ASSERT_EQ(BlobFiles(), blobs);
}
TEST_F(MetaBypassTest,
       BlobGCMemtableLazyIteratorRetainsFullValueWithoutBlobPin) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "a", "memtable"));
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  ASSERT_TRUE(BlobFiles().empty());
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto old_files = BlobFiles();
  ASSERT_EQ(old_files.size(), 1U);
  ASSERT_OK(db_->Put(WriteOptions(), "a", "replacement"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  // This iterator predates the flush and owns a full-value memtable entry.
  // It has no BlobIndex dependency on the blob created by that flush, so the
  // obsolete file can be reclaimed before the lazy iterator seeks.
  AwaitBlobGC(1);
  for (const auto& file : old_files) ASSERT_FALSE(BlobFiles().count(file));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key().ToString(), "a");
  ASSERT_EQ(iterator->value().ToString(), "memtable");
  iterator->Next();
  ASSERT_FALSE(iterator->Valid());
  ASSERT_OK(iterator->status());
  iterator.reset();
  Check("a", "replacement");
  ASSERT_OK(db_->Close());
  db_.reset();
  Restore();
  Check("a", "replacement");
}
#ifndef OS_WIN
TEST_F(MetaBypassTest, WalOnlyRecoveryHonorsAvoidFlushAndManifestReuse) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter());
  ASSERT_TRUE(BlobFiles().empty());
  o_.avoid_flush_during_recovery = true;
  o_.reuse_manifest_on_open = true;
  ASSERT_TRUE(FilesOfType(PublishedPoint(), kTableFile).empty());
  ASSERT_NO_FATAL_FAILURE(RestorePrefix(5, 5));
  // Restore persists native replay even when the subsequent normal Open avoids
  // recovery flushes. Checkpoint capture above remains WAL-only.
  ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
  ASSERT_TRUE(BlobFiles().empty());
  Check("a", "last");
  Check("b", "batch");
  ASSERT_OK(db_->Put(WriteOptions(), "a", "replacement"));
  ASSERT_OK(db_->Delete(WriteOptions(), "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
  ASSERT_FALSE(BlobFiles().empty());
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "replacement");
}
TEST_F(MetaBypassTest, BlobGCRecoveredSstLazyIteratorAndIdleRoots) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  // Native recovery rebuilds BlobDB files only when the memtable flushes.
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter());
  ASSERT_TRUE(BlobFiles().empty());
  Restore();
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  const auto old_files = BlobFiles();
  ASSERT_FALSE(old_files.empty());
  ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
  // The iterator has not sought yet: a pin per physical read is insufficient.
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  auto pending = db_->GetBackupStats();
  ASSERT_OK(pending.error);
  ASSERT_EQ(pending.pending_blob_gc_files, 0U);
  CheckNoIdleCaptures();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "replacement"));
  ASSERT_OK(db_->Delete(WriteOptions(), "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  for (const auto& file : old_files) ASSERT_TRUE(BlobFiles().count(file));
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.pending_blob_gc_files, 0U);
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key().ToString(), "a");
  ASSERT_EQ(iterator->value().ToString(), "last");
  ASSERT_OK(iterator->status());
  iterator.reset();
  AwaitBlobGC(1);
  for (const auto& file : old_files) ASSERT_FALSE(BlobFiles().count(file));
  ASSERT_OK(db_->SyncBackup());
  CheckNoIdleCaptures();
  Check("a", "replacement");
}
#endif
TEST_F(MetaBypassTest, BlobGCPartialFileAndIteratorRemainReadable) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  o_.blob_file_size = 1024 * 1024;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "old"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "keep"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  auto files = BlobFiles();
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid());
  ASSERT_OK(db_->Delete(WriteOptions(), "a"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  ASSERT_EQ(iterator->key().ToString(), "a");
  ASSERT_EQ(iterator->value().ToString(), "old");
  ASSERT_OK(iterator->status());
  iterator.reset();
  for (const auto& name : files) ASSERT_TRUE(BlobFiles().count(name));
  Check("b", "keep");
  ASSERT_OK(db_->Close());
  db_.reset();
  Restore();
  Check("b", "keep");
}
TEST_F(MetaBypassTest, BlobGCCaptureWaitsForPhysicalDeletion) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  o_.blob_file_size = 128;
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  TestGate claimed, applied;
  BlockBlobGCDirSync(observed, &claimed);
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "old", std::string(256, 'a')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Delete(WriteOptions(), "old"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  claimed.WaitUntilBlocked();
  auto waiting_stats = db_->GetBackupStats();
  EXPECT_OK(waiting_stats.error);
  EXPECT_EQ(waiting_stats.reclaimed_blob_files, 0U);
  EXPECT_GT(waiting_stats.pending_blob_gc_files, 0U);
  std::atomic<int> captures{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureFiles",
                                        [&](void*) { ++captures; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply",
                                        [&](void*) { applied.Block(); });
  ASSERT_OK(db_->Put(WriteOptions(), "new", "after retirement"));
  Check("new", "after retirement");
  Status synced;
  std::thread sync([&] { synced = db_->SyncBackup(); });
  applied.WaitUntilBlocked();
  EXPECT_EQ(captures.load(), 0);
  applied.Release();
  claimed.Release();
  sync.join();
  ASSERT_OK(synced);
  ASSERT_GT(captures.load(), 0);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(db_->Close());
  db_.reset();
  Restore();
  Check("new", "after retirement");
}
TEST_F(MetaBypassTest, BlobGCCandidateAndRetiredRootsProtectFiles) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  o_.blob_file_size = 128;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "old", std::string(256, 'a')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  auto blobs = BlobFiles();
  TestGate capture, validation;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureFiles",
                                        [&](void*) { capture.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::CandidateSynced",
                                        [&](void*) { validation.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "anchor", "value"));
  Status synced;
  std::thread sync([&] { synced = db_->SyncBackup(); });
  capture.WaitUntilBlocked();
  EXPECT_OK(db_->Delete(WriteOptions(), "old"));
  EXPECT_OK(db_->Flush(FlushOptions()));
  EXPECT_OK(CompactBlobFiles());
  for (const auto& name : blobs) EXPECT_TRUE(BlobFiles().count(name));
  capture.Release();
  validation.WaitUntilBlocked();
  // The validator has moved candidate_ into a local unique_ptr. The busy
  // lifetime, not that nullable pointer, still protects all candidate reads.
  for (const auto& name : blobs) EXPECT_TRUE(BlobFiles().count(name));
  validation.Release();
  sync.join();
  ASSERT_OK(synced);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  AwaitBlobGC(1);
  Check("anchor", "value");
}
TEST_F(MetaBypassTest, BlobGCCancelWaitsForPhysicalDeletionBeforeCapture) {
  EnableTier();
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "old", "obsolete"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  TestGate deleting, cancelled;
  std::atomic<unsigned> captures{0}, reclaimed{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BlobReclaimed",
                                        [&](void*) { ++reclaimed; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BlobDescriptorRemoved",
                                        [&](void*) { deleting.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierCancelled",
                                        [&](void*) { cancelled.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Delete(WriteOptions(), "old"));
  ASSERT_OK(db_->Put(WriteOptions(), "live", "readable"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  deleting.WaitUntilBlocked();
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureFiles",
                                        [&](void*) { ++captures; });
  // An unrelated mirror error cancels tier work while the GC owner is deleting.
  // It must keep its capture gate until physical deletion and dirsync finish.
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("unrelated mirror failure");
      });
  EXPECT_OK(db_->Put(WriteOptions(), "unpublished", "value"));
  cancelled.WaitUntilBlocked();
  cancelled.Release();
  EXPECT_EQ(reclaimed.load(), 0U);
  EXPECT_EQ(captures.load(), 0U);
  Check("live", "readable");
  deleting.Release();
  ASSERT_TRUE(db_->SyncBackup().IsIOError());
  ASSERT_TRUE(db_->Close().IsIOError());
}
TEST_F(MetaBypassTest, BlobGCCloseDrainsClaimedDeletion) {
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  o_.blob_file_size = 128;
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  TestGate claimed, joining;
  BlockBlobGCDirSync(observed, &claimed);
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "old", std::string(256, 'a')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Delete(WriteOptions(), "old"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  claimed.WaitUntilBlocked();
  auto waiting_stats = db_->GetBackupStats();
  EXPECT_OK(waiting_stats.error);
  EXPECT_EQ(waiting_stats.reclaimed_blob_files, 0U);
  EXPECT_GT(waiting_stats.pending_blob_gc_files, 0U);
  // Close begins with a backup barrier while the claimed deletion still owns
  // the capture gate. Install this callback after the setup SyncBackup calls.
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                        [&](void*) { joining.Block(); });
  Status closed;
  std::atomic<bool> done{false};
  std::thread closer([&] {
    closed = db_->Close();
    done = true;
  });
  joining.WaitUntilBlocked();
  EXPECT_FALSE(done.load());
  joining.Release();
  claimed.Release();
  closer.join();
  ASSERT_OK(closed);
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_GT(stats.reclaimed_blob_files, 0);
  db_.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#ifndef OS_WIN
TEST_F(MetaBypassTest, BlobGCSigkillDeletionBoundariesAndRestore) {
  EnableTier();
  for (const char* stage : {"MetaBypass::BlobDescriptorRemoved",
                            "MetaBypass::BlobPayloadRemoved"}) {
    SCOPED_TRACE(stage);
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(), "--blob-gc-crash-writer",
            index_.c_str(), m_.data_dir.c_str(), m_.backup_dir.c_str(),
            m_.staging_dir.c_str(), stage, nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    // Lose all SSD index and staging files. Restore uses only LATEST on HDD.
    Clean(index_);
    Clean(m_.staging_dir);
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("live", std::string(256, 'b'));
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "old", &value).IsNotFound());
    ASSERT_OK(db_->Close());
    db_.reset();
    // Repeat native restore: no persistent delete task survives into
    // a different file-number allocation epoch.
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("live", std::string(256, 'b'));
  }
}
#endif
TEST_F(MetaBypassTest, BlobGCNativeOpenReclaimsProjectedDebris) {
  EnableTier();
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = m_.interval_ms;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "live", "keep"));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  const std::string temporary = BlobFileName(m_.data_dir, 900001) + ".tmp";
  const std::string segment = m_.data_dir + "/segment-900002-abcd-1000000";
  ASSERT_OK(metabypass::Write(fs_.get(), temporary, "partial descriptor"));
  ASSERT_OK(metabypass::Write(fs_.get(), segment, "orphan extent"));
  // Open's full native obsolete-file scan, not a direct test reclamation call,
  // must issue both retirements and feed them through the published barrier.
  Open();
  AwaitBlobGC(2);
  ASSERT_TRUE(fs_->FileExists(temporary, IOOptions(), nullptr).IsNotFound());
  ASSERT_TRUE(fs_->FileExists(segment, IOOptions(), nullptr).IsNotFound());
  Check("live", "keep");
}
TEST_F(MetaBypassTest, DirectWriteWithoutStaging) {
  DirectWriteStorageScenario(0);
}
TEST_F(MetaBypassTest, DirectWriteWithStaging) {
  DirectWriteStorageScenario(1024 * 1024);
}
TEST_F(MetaBypassTest, DirectWriteAdaptiveStagingRestoreAndSyncWrite) {
  m_.staging_policy = MetaBypassStagingPolicy::kAdaptive;
  DirectWriteStorageScenario(1024 * 1024);
}
TEST_F(MetaBypassTest, AdaptiveStagingNativeFlushAndRestore) {
  m_.staging_policy = MetaBypassStagingPolicy::kAdaptive;
  EnableTier(1024 * 1024);
  Open();
  const std::string value(1024, 'v');
  for (unsigned i = 0; i < 32; ++i)
    ASSERT_OK(db_->Put(WriteOptions(), std::to_string(i), value));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(m_.staging_dir);
  Restore();
  for (unsigned i = 0; i < 32; ++i) Check(std::to_string(i), value);
}
TEST_F(MetaBypassTest, DirectWriteAdaptiveBlobGcAndRestore) {
  m_.staging_policy = MetaBypassStagingPolicy::kAdaptive;
  EnableDirectWrite();
  BlobGCScenario(true, false, true);
}
TEST_F(MetaBypassTest, AlwaysFullSuffixDw1FlushWaitsForSeal) {
  AlwaysFullSuffixDbBarrier("flush");
}
TEST_F(MetaBypassTest, AlwaysFullSuffixDw1SyncWriteWaitsForSeal) {
  AlwaysFullSuffixDbBarrier("sync");
}
TEST_F(MetaBypassTest, AlwaysFullSuffixDw1SyncBackupWaitsForSeal) {
  AlwaysFullSuffixDbBarrier("backup");
}
TEST_F(MetaBypassTest, AlwaysFullSuffixDw1CloseWaitsForSeal) {
  AlwaysFullSuffixDbBarrier("close");
}
TEST_F(MetaBypassTest, DirectWriteWithStagingSpill) {
  DirectWriteStorageScenario(8192);
}
TEST_F(MetaBypassTest, DirectWriteCaptureFailurePreservesPoint) {
  DirectWriteFailureScenario("MetaBypass::NativeCaptureStatus", false);
}
TEST_F(MetaBypassTest, DirectWritePublicationFailurePreservesPoint) {
  DirectWriteFailureScenario("MetaBypass::BeforePointerReplace", false);
}
TEST_F(MetaBypassTest, DirectWriteMigrationFailurePreservesPoint) {
  DirectWriteFailureScenario("MetaBypass::TierExtentSynced", true);
}
TEST_F(MetaBypassTest, DirectWriteMigrationFailureBeforeWalPreservesOldValue) {
  DirectWriteFailureScenario("MetaBypass::TierExtentSynced", true,
                             DirectWriteFailurePhase::kBeforeWal);
}
TEST_F(MetaBypassTest, DirectWriteBlobGCAndRestore) {
  EnableDirectWrite();
  BlobGCScenario(false, false, true);
}
TEST_F(MetaBypassTest, DirectWriteTieredBlobGCAndRestore) {
  EnableDirectWrite();
  BlobGCScenario(true, false, true);
}
TEST_F(MetaBypassTest, DirectWriteSstTierBlobGCAndRestore) {
  EnableDirectWrite();
  BlobGCScenario(true, true, true);
}
TEST_F(MetaBypassTest, DirectWriteIteratorSurvivesFlushAndRetirement) {
  EnableDirectWrite();
  m_.checkpoint_interval_ms = 1;
  ASSERT_NO_FATAL_FAILURE(Open());
  const std::string old(256, 'o');
  ASSERT_OK(db_->Put(WriteOptions(), "old", old));
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Delete(WriteOptions(), "old"));
  ASSERT_OK(db_->Put(WriteOptions(), "live", "new"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  iterator->Seek("old");
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key(), "old");
  ASSERT_EQ(iterator->value(), old);
  ASSERT_OK(iterator->status());
  iterator.reset();
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  AwaitBlobGC(1);
}
#ifndef OS_WIN
TEST_F(MetaBypassTest, DirectWriteCrashBeforeCheckpoint) {
  DirectWriteCrashScenario("active", false);
}
TEST_F(MetaBypassTest, DirectWriteCrashDuringCapture) {
  DirectWriteCrashScenario("MetaBypass::NativeFilesCaptured", false);
}
TEST_F(MetaBypassTest, DirectWriteCrashAfterPublication) {
  DirectWriteCrashScenario("MetaBypass::PointerSynced", false);
}
TEST_F(MetaBypassTest, DirectWriteSpillCrashBeforeCheckpoint) {
  DirectWriteCrashScenario("active", true);
}
TEST_F(MetaBypassTest, DirectWriteSpillCrashDuringCapture) {
  DirectWriteCrashScenario("MetaBypass::NativeFilesCaptured", true);
}
TEST_F(MetaBypassTest, DirectWriteSpillCrashAfterPublication) {
  DirectWriteCrashScenario("MetaBypass::PointerSynced", true);
}
#endif
TEST_F(MetaBypassTest, DirectWriteModeChangesAndOldWalPointRestore) {
  o_.write_buffer_size = 4 * 1024 * 1024;
  m_.checkpoint_interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "legacy", "wal-value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_FALSE(FilesOfType(PublishedPoint(), kWalFile).empty());
  // An old full-value WAL point is recovered by the engine before the first
  // Direct Write checkpoint converts it into a native, flushed point.
  EnableDirectWrite();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("legacy", "wal-value");
  ASSERT_OK(db_->Put(WriteOptions(), "direct", "blob-value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  o_.enable_blob_direct_write = false;
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("direct", "blob-value");
  ASSERT_OK(db_->Put(WriteOptions(), "ordinary", "full-value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("legacy", "wal-value");
  Check("direct", "blob-value");
  Check("ordinary", "full-value");
}
TEST_F(MetaBypassTest, DirectWriteCapturesAndCoalescesConcurrentBarriers) {
  EnableDirectWrite();
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "before", "first"));
  TestGate first_capture, second_capture;
  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<unsigned> captures{0}, completed{0};
  unsigned waiting = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::NativeFilesCaptured",
                                        [&](void*) {
                                          if (++captures == 1)
                                            first_capture.Block();
                                          else
                                            second_capture.Block();
                                        });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting", [&](void*) {
    std::lock_guard<std::mutex> lock(mutex);
    ++waiting;
    cv.notify_all();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Status first, second, third;
  std::thread a([&] {
    first = db_->SyncBackup();
    ++completed;
  });
  first_capture.WaitUntilBlocked();
  EXPECT_OK(db_->Put(WriteOptions(), "after", "second"));
  Check("after", "second");
  std::thread b([&] {
    second = db_->SyncBackup();
    ++completed;
  });
  std::thread c([&] {
    third = db_->SyncBackup();
    ++completed;
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(20),
                            [&] { return waiting == 3; }));
  }
  EXPECT_EQ(completed.load(), 0U);
  first_capture.Release();
  second_capture.WaitUntilBlocked();
  a.join();
  EXPECT_OK(first);
  EXPECT_EQ(completed.load(), 1U);
  const std::string first_point = PublishedPoint();
  const std::string expected = "first";
  CheckPointValue(first_point, "before", &expected);
  CheckPointValue(first_point, "after", nullptr);
  EXPECT_TRUE(FilesOfType(first_point, kWalFile).empty());
  second_capture.Release();
  b.join();
  c.join();
  EXPECT_OK(second);
  EXPECT_OK(third);
  EXPECT_EQ(captures.load(), 2U);
  SyncPoint::GetInstance()->DisableProcessing();
  const std::string after = "second";
  CheckPointValue(PublishedPoint(), "after", &after);
}
TEST_F(MetaBypassTest, DirectWriteLowTrafficCheckpointThenIdle) {
  EnableDirectWrite();
  m_.interval_ms = 1;
  m_.checkpoint_interval_ms = 1;
  ASSERT_NO_FATAL_FAILURE(Open());
  std::mutex mutex;
  std::condition_variable cv;
  unsigned published = 0, idle = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::PointerSynced", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        ++published;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CheckpointIdle", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        if (published) ++idle;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "low", "traffic"));
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(20),
                            [&] { return published && idle; }));
    // The checkpoint's own native flush notification can request one
    // conservative follow-up, which must also converge to the idle state.
    ASSERT_LE(published, 2U);
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const std::string expected = "traffic";
  CheckPointValue(PublishedPoint(), "low", &expected);
  CheckNoIdleCaptures();
}
TEST_F(MetaBypassTest, CheckpointDeadlineDoesNotSlideWithLaterWrites) {
  EnableDirectWrite();
  m_.interval_ms = 1;
  ASSERT_NO_FATAL_FAILURE(Open());
  TestGate first_wait, captured;
  std::atomic<uint64_t> clock{0};
  std::atomic<bool> second_written{false};
  uint64_t first_deadline = 0;
  unsigned deadlines = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CheckpointNow", [&](void* p) {
        if (clock.load()) *static_cast<uint64_t*>(p) = clock.load();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CheckpointDeadline", [&](void* p) {
        const uint64_t deadline = *static_cast<uint64_t*>(p);
        if (++deadlines == 1) {
          first_deadline = deadline;
          first_wait.Block();
        } else {
          EXPECT_EQ(deadline, first_deadline);
          if (second_written.load()) clock = first_deadline;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CheckpointWaitMicros",
      [&](void* p) { *static_cast<uint64_t*>(p) = 0; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::CheckpointBoundary",
                                        [&](void*) { captured.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "first", "v"));
  first_wait.WaitUntilBlocked();
  first_wait.Release();
  EXPECT_OK(db_->Put(WriteOptions(), "second", "v"));
  second_written = true;
  // Further traffic must not move the original deadline. The callbacks advance
  // and poll the scheduler clock without depending on a mirror wakeup.
  EXPECT_OK(db_->Put(WriteOptions(), "third", "v"));
  captured.WaitUntilBlocked();
  captured.Release();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->SyncBackup());
  const std::string value = "v";
  CheckPointValue(PublishedPoint(), "first", &value);
  CheckPointValue(PublishedPoint(), "second", &value);
}
TEST_F(MetaBypassTest, DirectWriteValidatesNativeConstraintsBeforeMutation) {
  EnableDirectWrite();
  const auto before = SnapshotTree(root_);
  for (bool concurrent : {true, false}) {
    Options invalid = o_;
    invalid.allow_concurrent_memtable_write = concurrent;
    invalid.experimental_mempurge_threshold = concurrent ? 0.0 : 1.0;
    ASSERT_TRUE(MetaBypassDB::Open(invalid, m_, index_, &db_).IsNotSupported());
    ASSERT_FALSE(db_);
    ASSERT_EQ(SnapshotTree(root_), before);
  }
  for (uint64_t interval : {uint64_t{0}, UINT64_MAX}) {
    auto invalid = m_;
    invalid.checkpoint_interval_ms = interval;
    ASSERT_TRUE(
        MetaBypassDB::Open(o_, invalid, index_, &db_).IsInvalidArgument());
    ASSERT_EQ(SnapshotTree(root_), before);
  }
}

TEST_F(MetaBypassTest, NativeSnapshotPinsFilesAcrossConcurrentFlush) {
  o_.write_buffer_size = 4 * 1024 * 1024;
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "old", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  TestGate captured;
  std::atomic<bool> first{true};
  std::string point;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CandidateCaptured", [&](void* arg) {
        if (point.empty()) point = *static_cast<std::string*>(arg);
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::NativeFilesCaptured",
                                        [&](void*) {
                                          if (first.exchange(false))
                                            captured.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "boundary", "before"));
  Status synced;
  std::thread sync([&] { synced = db_->SyncBackup(); });
  captured.WaitUntilBlocked();
  EXPECT_OK(db_->Put(WriteOptions(), "after", "concurrent"));
  EXPECT_OK(db_->Flush(FlushOptions()));
  EXPECT_OK(CompactBlobFiles());
  captured.Release();
  sync.join();
  ASSERT_OK(synced);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_FALSE(point.empty());
  ASSERT_EQ(PublishedPoint(), point);
  {
    // Read the exact captured point before Close publishes another boundary.
    // Its latest root protects the shared blobs while the native read-only DB
    // replays this point's bounded full-value WAL.
    auto storage =
        std::make_shared<metabypass::SeparatedStorage>(fs_, point, m_.data_dir);
    auto env = NewCompositeEnv(storage);
    Options options = o_;
    options.env = env.get();
    options.create_if_missing = false;
    options.enable_blob_files = true;
    std::unique_ptr<DB> snapshot;
    ASSERT_OK(DB::OpenForReadOnly(options, point, &snapshot));
    std::string value;
    ASSERT_OK(snapshot->Get(ReadOptions(), "old", &value));
    ASSERT_EQ(value, "value");
    ASSERT_OK(snapshot->Get(ReadOptions(), "boundary", &value));
    ASSERT_EQ(value, "before");
    ASSERT_TRUE(snapshot->Get(ReadOptions(), "after", &value).IsNotFound());
    ASSERT_OK(snapshot->Close());
  }
  ASSERT_OK(db_->Close());
  db_.reset();
  Restore();
  Check("old", "value");
  Check("boundary", "before");
  Check("after", "concurrent");
}
TEST_F(MetaBypassTest, SyncWritesWaitForPublicationAndReportFailure) {
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  uint64_t successful_checkpoint = 0;
  for (bool fail : {false, true}) {
    TestGate validating, waiters;
    std::mutex mirror_mutex;
    std::condition_variable mirror_cv;
    std::atomic<uint64_t> mirror_notifications{0};
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::BeforeMirrorWait", [&](void*) {
          std::lock_guard<std::mutex> lock(mirror_mutex);
          ++mirror_notifications;
          mirror_cv.notify_all();
        });
    std::atomic<int> waiting{0};
    std::atomic<int> completed{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                          [&](void*) { validating.Block(); });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                          [&](void*) {
                                            if (++waiting == 3) waiters.Block();
                                          });
    if (fail) {
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::BeforePointerReplace", [](void* arg) {
            *static_cast<Status*>(arg) = Status::IOError("publication failure");
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    EXPECT_OK(db_->Put(WriteOptions(), "trigger", "value"));
    std::vector<std::thread> threads;
    for (int i = 0; i < 3; ++i) {
      threads.emplace_back([&, i] {
        WriteOptions sync;
        sync.sync = true;
        Status status;
        if (i == 2) {
          status = db_->SyncBackup();
        } else {
          WriteBatch batch;
          status = batch.Put("key" + std::to_string(i), fail ? "new" : "old");
          if (status.ok()) status = batch.Put("pair" + std::to_string(i), "v");
          if (status.ok()) status = db_->Write(sync, &batch);
        }
        if (fail) {
          EXPECT_TRUE(status.IsIOError());
        } else {
          EXPECT_OK(status);
        }
        ++completed;
      });
    }
    waiters.WaitUntilBlocked();
    waiters.Release();
    validating.WaitUntilBlocked();
    EXPECT_EQ(completed.load(), 0);
    // The outstanding sync requests must urgently drain their complete WAL
    // batches even though the ordinary background interval is sixty seconds.
    // Establish that boundary before the additional asynchronous write.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(20);
    for (;;) {
      const uint64_t observed = mirror_notifications.load();
      auto stats = db_->GetBackupStats();
      EXPECT_OK(stats.error);
      // Charges include popped batches until ApplyBatch finishes. Require a
      // worker observation as well as zero queued/in-flight bytes.
      if (!stats.error.ok() ||
          (stats.queued_bytes == 0 && mirror_notifications.load() > 0))
        break;
      std::unique_lock<std::mutex> lock(mirror_mutex);
      if (!mirror_cv.wait_until(lock, deadline, [&] {
            return mirror_notifications.load() != observed;
          })) {
        ADD_FAILURE() << "mirror did not drain before publication";
        break;
      }
    }
    // Publication is blocked, but primary writes and reads can still proceed.
    EXPECT_OK(db_->Put(WriteOptions(), "async", "progress"));
    Check("async", "progress");
    validating.Release();
    for (auto& thread : threads) thread.join();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(completed.load(), 3);
    if (!fail) {
      std::string boundary;
      ASSERT_OK(metabypass::Read(fs_.get(), PublishedPoint() + "/RECOVERY",
                                 &boundary));
      std::istringstream metadata(boundary);
      ASSERT_TRUE(bool(metadata >> successful_checkpoint));
      // Trigger plus two two-operation batches must be covered. The async
      // sixth operation may have arrived after the last requested capture.
      ASSERT_GE(successful_checkpoint, 5U);
      ASSERT_LE(successful_checkpoint, 6U);
    }
  }
  Check("key0", "new");
  EXPECT_TRUE(db_->Put(WriteOptions(), "rejected", "v").IsIOError());
  EXPECT_TRUE(db_->Close().IsIOError());
  db_.reset();
  MetaBypassRecoveryReport report;
  ASSERT_NO_FATAL_FAILURE(Restore(&report));
  ASSERT_EQ(report.checkpoint_sequence, successful_checkpoint);
  ASSERT_LE(report.checkpoint_sequence, report.recovered_sequence);
  // Each round writes trigger, two complete two-operation batches, then
  // async. Both failed sync writes completed natively before their backup
  // barrier, and the mirror drained through the final complete sync batch.
  // The last asynchronous operation may or may not have reached the mirror.
  ASSERT_GE(report.recovered_sequence, 11U);
  ASSERT_LE(report.recovered_sequence, 12U);
  // Project the two-round plan onto X: both complete-batch boundaries have
  // this unique map because async repeats its first-round value. Include the
  // absence of the rejected write.
  CheckContents({{"trigger", "value"},
                 {"key0", "new"},
                 {"pair0", "v"},
                 {"key1", "new"},
                 {"pair1", "v"},
                 {"async", "progress"}});
}

TEST_F(MetaBypassTest, RejectWalTerminationBeforeMutation) {
  for (bool tiered : {false, true}) {
    SCOPED_TRACE(tiered);
    if (tiered) EnableTier();
    Open();
    WriteOptions sync;
    sync.sync = true;
    ASSERT_OK(db_->Put(sync, "a", "first"));
    ASSERT_OK(db_->Put(sync, "b", "second"));
    for (bool with_put : {false, true}) {
      WriteBatch rejected;
      ASSERT_OK(rejected.Delete("a"));
      rejected.MarkWalTerminationPoint();
      ASSERT_OK(rejected.Delete("b"));
      if (with_put) {
        ASSERT_OK(rejected.Put("new", "value"));
      }
      ASSERT_TRUE(db_->Write(sync, &rejected).IsNotSupported());
      Check("a", "first");
      Check("b", "second");
      std::string value;
      ASSERT_TRUE(db_->Get(ReadOptions(), "new", &value).IsNotFound());
    }
    WriteBatch accepted;
    ASSERT_OK(accepted.Delete("a"));
    ASSERT_OK(accepted.Put("b", "updated"));
    ASSERT_OK(db_->Write(sync, &accepted));
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(index_);
    if (tiered) Clean(m_.staging_dir);
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
    Open();
    Check("b", "updated");
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "a", &value).IsNotFound());
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDirIfMissing(root_, IOOptions(), nullptr));
  }
}

TEST_F(MetaBypassTest, TieredSyncSurvivesCompleteFastStorageLoss) {
  EnableTier(1024 * 1024);
  Open();
  WriteOptions sync;
  sync.sync = true;
  ASSERT_OK(db_->Put(sync, "key", std::string(10000, 'v')));
  Check("key", std::string(10000, 'v'));
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("key", std::string(10000, 'v'));
  ASSERT_OK(db_->Put(sync, "next", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  Open();
  Check("next", "value");
}

TEST_F(MetaBypassTest, TieredWalOnlyBackupDoesNotAllocateBlobStaging) {
  EnableTier(32 * 1024);
  o_.write_buffer_size = 16 * 1024 * 1024;
  Open();
  for (int i = 0; i < 100; ++i)
    ASSERT_OK(
        db_->Put(WriteOptions(), std::to_string(i), std::string(1000, 'v')));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_TRUE(FilesOfType(index_, kTableFile).empty());
  ASSERT_TRUE(BlobFiles().empty());
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.staging_bytes, 0U);
  ASSERT_EQ(stats.migrated_blob_bytes, 0U);
  ASSERT_GT(stats.mirrored_bytes, 100000U);
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_staging_bytes, m_.staging_capacity);
  ASSERT_GT(stats.staging_spill_files, 0U);
  ASSERT_GT(stats.staging_spill_bytes, 0U);
  for (int i = 0; i < 100; ++i)
    Check(std::to_string(i), std::string(1000, 'v'));
}

TEST_F(MetaBypassTest, TieredMigrationDoesNotBlockAsyncWrites) {
  EnableTier(1024 * 1024);
  Open();
  TestGate gate;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigration",
                                        [&](void*) { gate.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "one"));
  Status sync_status;
  std::atomic<bool> done{false};
  std::thread sync([&] {
    sync_status = db_->Flush(FlushOptions());
    if (sync_status.ok()) sync_status = db_->SyncBackup();
    done.store(true);
  });
  gate.WaitUntilBlocked();
  Status write = db_->Put(WriteOptions(), "b", "two");
  const bool returned = done.load();
  gate.Release();
  sync.join();
  ASSERT_OK(write);
  ASSERT_FALSE(returned);
  ASSERT_OK(sync_status);
  Check("b", "two");
}

TEST_F(MetaBypassTest, TieredMigrationFailurePreservesPublishedPoint) {
  EnableTier(1024 * 1024);
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  Open();
  WriteOptions sync;
  sync.sync = true;
  ASSERT_OK(db_->Put(sync, "key", "old"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  std::string published;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &published));
  TestGate migrating;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierExtentSynced", [&](void* p) {
        migrating.Block();
        *static_cast<Status*>(p) = Status::IOError("injected tier failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "new"));
  // Local blob durability lets native Flush finish while HDD migration is
  // blocked. The remote error is reported by the backup barrier afterwards.
  ASSERT_OK(db_->Flush(FlushOptions()));
  migrating.WaitUntilBlocked();
  migrating.Release();
  ASSERT_TRUE(db_->SyncBackup().IsIOError());
  const auto stats = db_->GetBackupStats();
  ASSERT_TRUE(stats.error.IsIOError());
  ASSERT_TRUE(db_->Put(WriteOptions(), "rejected", "value").IsIOError());
  Check("key", "new");
  ASSERT_TRUE(db_->Close().IsIOError());
  std::string unchanged;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &unchanged));
  ASSERT_EQ(unchanged, published);
  db_.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_NO_FATAL_FAILURE(RestorePrefix(1, 2));
  CheckContents({{"key", "new"}});
}

TEST_F(MetaBypassTest, TieredCompressionBatchIterationAndReopen) {
  EnableTier(64 * 1024);
  o_.blob_compression_type = kSnappyCompression;
  Open();
  for (int i = 0; i < 30; ++i) {
    WriteBatch batch;
    ASSERT_OK(batch.Put("a", std::string(4000, 'a' + i % 20)));
    ASSERT_OK(batch.Put("b", std::string(4000, 'b')));
    ASSERT_OK(batch.Delete("gone"));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("a", std::string(4000, 'j'));
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), "a");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), "b");
  it->Next();
  ASSERT_FALSE(it->Valid());
  ASSERT_OK(it->status());
}

#ifndef OS_WIN
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossNoHook) {
  ASSERT_NO_FATAL_FAILURE(TieredCrashScenario(""));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossExtentSynced) {
  ASSERT_NO_FATAL_FAILURE(TieredCrashScenario("MetaBypass::TierExtentSynced"));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossDescriptorSynced) {
  ASSERT_NO_FATAL_FAILURE(
      TieredCrashScenario("MetaBypass::TierDescriptorSynced"));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossCandidateSynced) {
  ASSERT_NO_FATAL_FAILURE(TieredCrashScenario("MetaBypass::CandidateSynced"));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossBeforePointerReplace) {
  ASSERT_NO_FATAL_FAILURE(
      TieredCrashScenario("MetaBypass::BeforePointerReplace"));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossPointerSynced) {
  ASSERT_NO_FATAL_FAILURE(TieredCrashScenario("MetaBypass::PointerSynced"));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossBeforeEvict) {
  ASSERT_NO_FATAL_FAILURE(TieredCrashScenario("MetaBypass::TierBeforeEvict"));
}
TEST_F(MetaBypassTest, TieredCrashCompleteFastStorageLossRouteDurable) {
  ASSERT_NO_FATAL_FAILURE(TieredCrashScenario("MetaBypass::TierRouteDurable"));
}
#endif

TEST_F(MetaBypassTest, TieredRestoreRetryDoesNotChangePublishedSource) {
  EnableTier(1024 * 1024);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  Clean(index_);
  Clean(m_.staging_dir);
  const auto logs = FilesOfType(PublishedPoint(), kWalFile);
  ASSERT_FALSE(logs.empty());
  auto failing = std::make_shared<MarkerFailureFileSystem>(fs_);
  failing->fail_path = index_ + "/" + logs.front();
  auto env = NewCompositeEnv(failing);
  Options options = o_;
  options.env = env.get();
  ASSERT_FALSE(MetaBypassDB::Restore(options, m_, index_).ok());
  ASSERT_GT(failing->failures, 0);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &after));
  ASSERT_EQ(pointer, after);
  Open();
  Check("key", "value");
}

TEST_F(MetaBypassTest, NativeTieredBlobReadDetectsPublishedExtentCorruption) {
  EnableTier(1024 * 1024);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();
  std::vector<std::string> names;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  bool corrupted = false;
  for (const auto& name : names) {
    if (name.compare(0, 8, "segment-") == 0) {
      ASSERT_OK(metabypass::Write(fs_.get(), m_.data_dir + "/" + name, "bad"));
      corrupted = true;
    }
  }
  ASSERT_TRUE(corrupted);
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  std::string identity;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/METABYPASS-IDENTITY",
                             &identity));
  auto tier = std::make_shared<metabypass::TieredStorage>(fs_, m_, identity);
  ASSERT_OK(tier->OpenRecoveryView(PublishedPoint() + "/BLOB-MAP"));
  auto storage = std::make_shared<metabypass::SeparatedStorage>(
      fs_, index_, m_.data_dir, tier);
  auto env = NewCompositeEnv(storage);
  Options native_options = o_;
  native_options.env = env.get();
  native_options.enable_blob_files = false;
  native_options.disable_auto_compactions = true;
  std::unique_ptr<DB> native;
  ASSERT_OK(DB::Open(native_options, index_, &native));
  std::string value;
  ASSERT_TRUE(native->Get(ReadOptions(), "key", &value).IsCorruption());
  ASSERT_OK(native->Close());
}

TEST_F(MetaBypassTest, TieredConcurrentReadsDuringPressureAndFlush) {
  EnableTier(32 * 1024);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "seed", std::string(1000, 's')));
  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  std::array<Status, 4> results;
  for (size_t i = 0; i < results.size(); ++i) {
    readers.emplace_back([&, i] {
      while (!stop.load()) {
        std::string value;
        results[i] = db_->Get(ReadOptions(), "seed", &value);
        if (!results[i].ok()) break;
        if (value != std::string(1000, 's')) {
          results[i] = Status::Corruption("concurrent tier read");
          break;
        }
      }
    });
  }
  Status writes;
  for (int i = 0; i < 100 && writes.ok(); ++i) {
    writes =
        db_->Put(WriteOptions(), std::to_string(i), std::string(1000, 'v'));
    if (writes.ok() && i % 20 == 0) writes = db_->Flush(FlushOptions());
  }
  stop.store(true);
  for (auto& reader : readers) reader.join();
  ASSERT_OK(writes);
  for (const auto& result : results) ASSERT_OK(result);
  ASSERT_OK(db_->SyncBackup());
}

TEST_F(MetaBypassTest, TieredReopenArchivesOrphanStaging) {
  EnableTier(32 * 1024);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "seed", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  // Simulates a newly allocated file left before its WAL record existed.
  ASSERT_OK(metabypass::Write(fs_.get(),
                              m_.staging_dir + "/segment-999999-abcd-1",
                              std::string(20000, 'x')));
  Open();
  Check("seed", "value");
  for (int i = 0; i < 30; ++i)
    ASSERT_OK(
        db_->Put(WriteOptions(), std::to_string(i), std::string(1000, 'v')));
  ASSERT_OK(db_->SyncBackup());
}

TEST_F(MetaBypassTest, TieredRandomizedRoundTrip) {
  EnableTier(64 * 1024);
  const uint32_t seed = o_.env->NowMicros() & 0xffffffff;
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random random(seed);
  std::map<std::string, std::string> expected;
  Open();
  for (int step = 0; step < 150; ++step) {
    WriteBatch batch;
    for (int j = 0; j < 2; ++j) {
      const std::string key = std::to_string(random.Uniform(30));
      if (random.OneIn(4)) {
        ASSERT_OK(batch.Delete(key));
        expected.erase(key);
      } else {
        const std::string value(random.Uniform(1000), 'a' + random.Uniform(26));
        ASSERT_OK(batch.Put(key, value));
        expected[key] = value;
      }
    }
    WriteOptions write;
    write.sync = step % 31 == 0;
    ASSERT_OK(db_->Write(write, &batch));
    if (step % 23 == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  it->SeekToFirst();
  for (const auto& entry : expected) {
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(it->key(), entry.first);
    ASSERT_EQ(it->value(), entry.second);
    it->Next();
  }
  ASSERT_FALSE(it->Valid());
  ASSERT_OK(it->status());
}

TEST_F(MetaBypassTest, TieredRejectsFormatChangesAndInvalidBudget) {
  EnableTier(0);
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsInvalidArgument());
  ASSERT_TRUE(fs_->FileExists(m_.data_dir, IOOptions(), nullptr).IsNotFound());
  m_.staging_dir.clear();
  Open();
  ASSERT_OK(db_->Close());
  db_.reset();
  EnableTier();
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsInvalidArgument());
}

TEST_F(MetaBypassTest, TieredClosePreservesWalOnlyPointWithoutFlush) {
  EnableTier();
  o_.avoid_flush_during_shutdown = true;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("key", "value");
}

TEST_F(MetaBypassTest, TieredOpenRejectsForeignStagingBeforeCleanup) {
  EnableTier();
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "owner-a", std::string(32768, 'a')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  const auto descriptors = BlobFiles();
  ASSERT_FALSE(descriptors.empty());
  std::string descriptor;
  ASSERT_OK(metabypass::Read(
      fs_.get(), m_.data_dir + "/" + *descriptors.begin(), &descriptor));
  ASSERT_EQ(descriptor.substr(0, 5), "MBT5 ");

  // A different owner's live staging file deliberately reuses the native
  // number of A's durable descriptor. Initialization must not delete it.
  MetaBypassOptions foreign = m_;
  foreign.staging_dir = root_ + "/foreign-staging";
  ASSERT_OK(fs_->CreateDir(foreign.staging_dir, IOOptions(), nullptr));
  ASSERT_OK(metabypass::Write(
      fs_.get(), foreign.staging_dir + "/METABYPASS-IDENTITY", "owner-b"));
  ASSERT_OK(metabypass::Write(fs_.get(),
                              foreign.staging_dir + "/" + *descriptors.begin(),
                              "owner-b-live-payload"));
  ASSERT_OK(metabypass::Write(fs_.get(), foreign.staging_dir + "/sentinel",
                              "preserve-foreign-tree"));
  const auto before = SnapshotTree(root_);
  ASSERT_TRUE(MetaBypassDB::Open(o_, foreign, index_, &db_).IsCorruption());
  ASSERT_FALSE(db_);
  ASSERT_EQ(SnapshotTree(root_), before);
  ASSERT_FALSE(
      MetaBypassDB::Restore(o_, foreign, root_ + "/restore-foreign").ok());
  ASSERT_EQ(SnapshotTree(root_), before);
}
TEST_F(MetaBypassTest, TieredRestoreRejectsForeignStagingIdentity) {
  EnableTier();
  Open();
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), m_.staging_dir));
  const std::string marker = m_.staging_dir + "/METABYPASS-IDENTITY";
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "foreign"));
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsCorruption());
  std::string owner;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &owner));
  ASSERT_EQ(owner, "foreign");
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/METABYPASS-IDENTITY",
                             &owner));
  ASSERT_OK(metabypass::Write(fs_.get(), marker, owner));
  auto failing = std::make_shared<MarkerFailureFileSystem>(fs_);
  failing->fail_path = marker;
  auto env = NewCompositeEnv(failing);
  Options restore_options = o_;
  restore_options.env = env.get();
  ASSERT_OK(MetaBypassDB::Restore(restore_options, m_, index_));
  ASSERT_EQ(failing->failures, 0);
}

TEST_F(MetaBypassTest, TieredConcurrentWritersFlushAndSpillWithinHardCap) {
  EnableTier(8192);
  o_.write_buffer_size = 32 * 1024;
  o_.max_write_buffer_number = 4;
  o_.max_background_jobs = 4;
  Open();
  ASSERT_TRUE(o_.allow_concurrent_memtable_write);
  std::array<Status, 4> results;
  std::vector<std::thread> writers;
  for (size_t i = 0; i < results.size(); ++i) {
    writers.emplace_back([&, i] {
      for (int j = 0; j < 20 && results[i].ok(); ++j)
        results[i] = db_->Put(WriteOptions(),
                              std::to_string(i) + "/" + std::to_string(j),
                              std::string(16384, 'a' + i));
    });
  }
  for (auto& writer : writers) writer.join();
  for (const auto& result : results) ASSERT_OK(result);
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  const auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_staging_bytes, m_.staging_capacity);
  ASSERT_GT(stats.staging_spill_files, 0U);
  ASSERT_GT(stats.staging_spill_bytes, m_.staging_capacity);
  ASSERT_GT(stats.hdd_extents, 0U);
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(m_.staging_dir);
  Restore();
  for (size_t i = 0; i < results.size(); ++i)
    Check(std::to_string(i) + "/19", std::string(16384, 'a' + i));
}

TEST_F(MetaBypassTest, TieredOversizedValuesAndBatchSpillSuccessfully) {
  EnableTier(8192);
  o_.write_buffer_size = 1024 * 1024;
  Open();
  WriteBatch batch;
  ASSERT_OK(batch.Put("large", std::string(65536, 'L')));
  ASSERT_OK(batch.Put("pair", std::string(32768, 'P')));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_staging_bytes, m_.staging_capacity);
  ASSERT_GT(stats.staging_spill_files, 0U);
  ASSERT_GT(stats.staging_spill_bytes, m_.staging_capacity);
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(m_.staging_dir);
  Restore();
  Check("large", std::string(65536, 'L'));
  Check("pair", std::string(32768, 'P'));
}

TEST_F(MetaBypassTest, SstAdaptiveOperationsColdReopenAndSmallBudgetRestore) {
  EnableSst();
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "old"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "gone"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  AwaitSst([](const auto& stats) {
    return stats.demotions > 0 && stats.ssd_bytes == 0;
  });
  std::unique_ptr<Iterator> pinned(db_->NewIterator(ReadOptions()));
  pinned->SeekToFirst();
  ASSERT_TRUE(pinned->Valid());
  WriteBatch batch;
  ASSERT_OK(batch.Put("a", "new"));
  ASSERT_OK(batch.Delete("b"));
  ASSERT_OK(batch.Put("c", "last"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->SyncBackup());
  Check("a", "new");
  Check("c", "last");
  ASSERT_EQ(pinned->value().ToString(), "old");
  pinned->Next();
  ASSERT_TRUE(pinned->Valid());
  ASSERT_EQ(pinned->key().ToString(), "b");
  ASSERT_OK(pinned->status());
  pinned.reset();
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  iterator->SeekToLast();
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key().ToString(), "c");
  iterator->Prev();
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key().ToString(), "a");
  ASSERT_OK(iterator->status());
  iterator.reset();
  ASSERT_OK(db_->Close());
  db_.reset();
  Open();
  Check("a", "new");
  Check("c", "last");
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(index_, IOOptions(), &files, nullptr));
  for (const auto& file : files) {
    uint64_t number;
    FileType type;
    ASSERT_FALSE(ParseFileName(file, &number, &type) && type == kTableFile);
  }
  Open();
  Check("a", "new");
  Check("c", "last");
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "b", &value).IsNotFound());
  ASSERT_OK(db_->Put(WriteOptions(), "after", "restore"));
  ASSERT_OK(db_->SyncBackup());
}
TEST_F(MetaBypassTest, SstForegroundFileMissesPromoteButScansDoNot) {
  EnableSst(SstTieringMode::kAdaptive, 6000);
  o_.disable_auto_compactions = true;
  ASSERT_NO_FATAL_FAILURE(Open());
  for (int round = 0; round < 6; ++round) {
    WriteBatch batch;
    for (int key = 0; key < 64; ++key)
      ASSERT_OK(
          batch.Put("range" + std::to_string(round) + "/" + std::to_string(key),
                    std::string(1000, 'a' + round)));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_OK(db_->SyncBackup());
  AwaitSst([](const auto& stats) {
    return stats.demotions > 0 && stats.pending_delete_bytes == 0 &&
           stats.ssd_bytes <= 5400;
  });
  auto before = db_->GetBackupStats();
  ASSERT_OK(before.error);
  ASSERT_EQ(before.sst_tiering.promotions, 0U);
  // Publication can protect older SSTs before newer outputs. Select an
  // actual cold, budget-admissible object rather than assuming creation order
  // determines which range is cold after pressure converges.
  std::string placement;
  ASSERT_OK(metabypass::Read(fs_.get(), index_ + "/SST-PLACEMENT", &placement));
  SCOPED_TRACE(placement);
  std::istringstream map(placement);
  std::string version, identity, object, cold_object;
  ASSERT_TRUE(bool(map >> version >> identity));
  uint64_t number, size;
  uint32_t crc;
  int hot;
  while (map >> number >> size >> crc >> hot >> object) {
    if (!hot && size <= 5400) {
      cold_object = object;
      break;
    }
  }
  ASSERT_FALSE(cold_object.empty());
  std::string cold_key, expected_value;
  {
    SstFileReader table(o_);
    ASSERT_OK(table.Open(m_.backup_dir + "/sst-store/" + cold_object));
    auto entries = table.NewTableIterator();
    entries->SeekToFirst();
    ASSERT_OK(entries->status());
    ASSERT_TRUE(entries->Valid());
    ParsedEntryInfo entry;
    ASSERT_OK(table.ParseTableIteratorKey(entries->key(), &entry));
    cold_key = entry.user_key.ToString();
  }
  SCOPED_TRACE(cold_key);
  ReadOptions scan;
  scan.fill_cache = false;
  {
    std::unique_ptr<Iterator> iterator(db_->NewIterator(scan));
    size_t count = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      if (iterator->key().ToString() == cold_key)
        expected_value = iterator->value().ToString();
      ++count;
    }
    ASSERT_OK(iterator->status());
    ASSERT_EQ(count, 384U);
  }
  ASSERT_FALSE(expected_value.empty());
  auto scanned = db_->GetBackupStats();
  ASSERT_OK(scanned.error);
  ASSERT_EQ(scanned.sst_tiering.sampled_reads,
            before.sst_tiering.sampled_reads);
  ASSERT_GT(scanned.sst_tiering.scan_reads, before.sst_tiering.scan_reads);
  ASSERT_EQ(scanned.sst_tiering.promotions, 0U);
  // Repeated misses with cache insertion disabled create real SST heat.
  std::atomic<bool> stop{false};
  std::thread reader([&] {
    ReadOptions reads;
    reads.fill_cache = false;
    while (!stop.load()) {
      std::string value;
      EXPECT_OK(db_->Get(reads, cold_key, &value));
      EXPECT_EQ(value, expected_value);
    }
  });
  AwaitSst([](const auto& stats) { return stats.promotions > 0; });
  stop = true;
  reader.join();
  auto promoted = db_->GetBackupStats();
  ASSERT_OK(promoted.error);
  ASSERT_GT(promoted.sst_tiering.sampled_reads,
            scanned.sst_tiering.sampled_reads);
  ASSERT_GT(promoted.sst_tiering.promoted_bytes, 0U);
  Check(cold_key, expected_value);
  Check("range0/0", std::string(1000, 'a'));
}
TEST_F(MetaBypassTest, SstObserveOnlyAndDisabledConfiguration) {
  EnableSst(SstTieringMode::kObserveOnly);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  AwaitSst([](const auto& stats) { return stats.observed_demotions > 0; });
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.sst_tiering.demotions, 0U);
  ASSERT_GT(stats.sst_tiering.ssd_bytes, 0U);
  ASSERT_OK(db_->Close());
  db_.reset();
  m_.sst_tiering.mode = SstTieringMode::kDisabled;
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsInvalidArgument());
  EnableSst();
  o_.allow_mmap_reads = true;
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsNotSupported());
  o_.allow_mmap_reads = false;
  m_.sst_tiering.sample_one_in = 0;
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsInvalidArgument());
}
TEST_F(MetaBypassTest, SstRandomizedConcurrentReadersCompactionAndBlobStaging) {
  const uint32_t seed = uint32_t(o_.env->NowMicros());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random random(seed);
  EnableTier(1024 * 1024);
  EnableSst();
  m_.sst_tiering.sample_one_in = 1U << random.Uniform(7);
  m_.sst_tiering.migration_queue_capacity = 1 + random.Uniform(8);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "stable", "fixed"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  std::atomic<bool> stop{false};
  std::thread reader([&] {
    while (!stop.load()) {
      std::string value;
      EXPECT_OK(db_->Get(ReadOptions(), "stable", &value));
      EXPECT_EQ(value, "fixed");
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        (void)it->value();
      }
      EXPECT_OK(it->status());
    }
  });
  for (int round = 0; round < 8; ++round) {
    WriteBatch batch;
    for (int i = 0; i < 32; ++i) {
      const auto key = "key" + std::to_string(i);
      if (random.OneIn(4))
        EXPECT_OK(batch.Delete(key));
      else
        EXPECT_OK(batch.Put(
            key, std::string(200 + random.Uniform(800), 'a' + round)));
    }
    EXPECT_OK(db_->Write(WriteOptions(), &batch));
    EXPECT_OK(db_->Flush(FlushOptions()));
    if (random.OneIn(2)) {
      EXPECT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }
    EXPECT_OK(db_->SyncBackup());
  }
  stop = true;
  reader.join();
  AwaitSst([](const auto& stats) {
    return stats.demotions > 0 && stats.ssd_bytes == 0;
  });
  Check("stable", "fixed");
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("stable", "fixed");
}
#ifndef OS_WIN
TEST_F(MetaBypassTest, SstCompleteSsdLossRestoresReportedContinuousState) {
  EnableSst();
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl(executable.c_str(), executable.c_str(), "--sst-crash-writer",
          index_.c_str(), m_.data_dir.c_str(), m_.backup_dir.c_str(), nullptr);
    _exit(127);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
  Clean(index_);
  MetaBypassRecoveryReport report;
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_, &report));
  Open();
  ASSERT_GE(report.recovered_sequence, 2U);
  ASSERT_LE(report.recovered_sequence, 5U);
  Check("updated",
        report.recovered_sequence >= 3 ? "unpublished" : "published");
  std::string value;
  if (report.recovered_sequence >= 4)
    ASSERT_TRUE(db_->Get(ReadOptions(), "deleted", &value).IsNotFound());
  else
    Check("deleted", "published");
  if (report.recovered_sequence >= 5)
    Check("tail", "unpublished");
  else
    ASSERT_TRUE(db_->Get(ReadOptions(), "tail", &value).IsNotFound());
  ASSERT_OK(db_->Put(WriteOptions(), "new-generation", "safe"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("new-generation", "safe");
}
#endif
TEST_F(MetaBypassTest, CloseRestoreAppendAndReopen) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "a", "old"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "deleted"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  WriteBatch batch;
  ASSERT_OK(batch.Put("a", "new"));
  ASSERT_OK(batch.Delete("b"));
  ASSERT_OK(batch.Put("empty", ""));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "new");
  Check("empty", "");
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "b", &value).IsNotFound());
  ASSERT_OK(db_->Put(WriteOptions(), "c", "after"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("c", "after");
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key().ToString(), "a");
  ASSERT_OK(it->status());
}
TEST_F(MetaBypassTest, RotationCompactionAndRetention) {
  ASSERT_NO_FATAL_FAILURE(Open());
  for (int round = 0; round < 8; ++round) {
    for (int i = 0; i < 100; ++i)
      ASSERT_OK(db_->Put(WriteOptions(), std::to_string(i),
                         std::string(1024, 'a' + round)));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->SyncBackup());
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->Close());
  db_.reset();
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  int points = 0;
  for (const auto& f : files)
    if (f.compare(0, 6, "point-") == 0) ++points;
  ASSERT_EQ(points, 1);
  ASSERT_NO_FATAL_FAILURE(Restore());
  for (int i = 0; i < 100; ++i)
    Check(std::to_string(i), std::string(1024, 'h'));
}
TEST_F(MetaBypassTest, ConcurrentBatchesFlushAndSync) {
  ASSERT_NO_FATAL_FAILURE(Open());
  const uint32_t seed = o_.env->NowMicros() & 0xffffffffU;
  SCOPED_TRACE("seed=" + std::to_string(seed));
  std::vector<std::thread> writers;
  std::vector<std::string> expected(4);
  for (int worker = 0; worker < 4; ++worker) {
    writers.emplace_back([&, worker] {
      SCOPED_TRACE("seed=" + std::to_string(seed) +
                   ", worker=" + std::to_string(worker));
      Random random(seed + worker);
      for (int i = 0; i < 100; ++i) {
        const std::string value = std::to_string(random.Next()) +
                                  std::string(random.Uniform(2048), 'v');
        const std::string prefix = std::to_string(worker);
        WriteBatch batch;
        EXPECT_OK(batch.Put(prefix + "a", value));
        EXPECT_OK(batch.Put(prefix + "b", value));
        EXPECT_OK(db_->Write(WriteOptions(), &batch));
        expected[worker] = value;
      }
    });
  }
  for (int i = 0; i < 10; ++i) {
    EXPECT_OK(db_->Flush(FlushOptions()));
    EXPECT_OK(db_->SyncBackup());
  }
  for (auto& writer : writers) writer.join();
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  for (int worker = 0; worker < 4; ++worker) {
    Check(std::to_string(worker) + "a", expected[worker]);
    Check(std::to_string(worker) + "b", expected[worker]);
  }
}
TEST_F(MetaBypassTest, RepeatedNativeDeletionDoesNotPoisonMirror) {
  std::unique_ptr<metabypass::Backup> backup;
  ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
  const std::string path = index_ + "/000004.log";
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(backup->NewWritableFile(path, FileOptions(), &writer, nullptr));
  ASSERT_OK(writer->Append("wal-bytes", IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  writer.reset();
  ASSERT_OK(backup->DeleteFile(path, IOOptions(), nullptr));
  // Native purge can request the same deletion again. The caller keeps the
  // filesystem's missing-file status, while the mirror stays healthy.
  Status repeated = backup->DeleteFile(path, IOOptions(), nullptr);
  ASSERT_FALSE(repeated.ok());
  ASSERT_TRUE(fs_->FileExists(path, IOOptions(), nullptr).IsNotFound());
  ASSERT_OK(backup->Error());
  ASSERT_OK(backup->Stop());
  std::string retained;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/000004.log",
                             &retained));
  ASSERT_EQ(retained, "wal-bytes");
  ASSERT_OK(backup->Error());
}
TEST_F(MetaBypassTest, PersistReusesOnlyLastSuccessfulSealedBlobs) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  metabypass::SeparatedStorage storage(observed, index_, m_.data_dir);
  ASSERT_OK(storage.Lock());
  const auto first = storage.BlobPath(1), second = storage.BlobPath(2);
  ASSERT_OK(metabypass::Write(fs_.get(), first, "one"));
  ASSERT_OK(metabypass::Write(fs_.get(), second, "second"));
  ASSERT_OK(storage.Persist({{1, 3}}));
  ASSERT_OK(storage.Persist({{1, 3}}));
  ASSERT_EQ(observed->Count("sync." + first), 1U);
  ASSERT_EQ(observed->Count("size." + first), 2U);
  ASSERT_EQ(observed->Count("dirsync." + m_.data_dir), 2U);
  ASSERT_OK(storage.Persist({{1, 3}, {2, 6}}));
  ASSERT_EQ(observed->Count("sync." + first), 1U);
  ASSERT_EQ(observed->Count("sync." + second), 1U);
  ASSERT_TRUE(storage.Persist({{1, 4}, {2, 6}}).IsCorruption());
  ASSERT_OK(storage.Persist({{1, 3}, {2, 6}}));
  ASSERT_EQ(observed->Count("sync." + first), 1U);
  ASSERT_EQ(observed->Count("sync." + second), 1U);
  // Even a cache hit must detect a truncated or missing file.
  std::unique_ptr<FSWritableFile> truncated;
  ASSERT_OK(
      fs_->ReopenWritableFile(second, FileOptions(), &truncated, nullptr));
  ASSERT_OK(truncated->Truncate(3, IOOptions(), nullptr));
  ASSERT_OK(truncated->Close(IOOptions(), nullptr));
  truncated.reset();
  ASSERT_TRUE(storage.Persist({{1, 3}, {2, 6}}).IsCorruption());
  ASSERT_OK(fs_->DeleteFile(second, IOOptions(), nullptr));
  Status missing = storage.Persist({{1, 3}, {2, 6}});
  ASSERT_TRUE(missing.IsNotFound() || missing.IsPathNotFound());
  ASSERT_OK(storage.Persist({}));
  ASSERT_OK(storage.Persist({{1, 3}}));
  ASSERT_EQ(observed->Count("sync." + first), 2U);
  // A cold storage object does not inherit another object's successful cache.
  metabypass::SeparatedStorage cold(observed, index_, m_.data_dir);
  ASSERT_OK(cold.Persist({{1, 3}}));
  ASSERT_EQ(observed->Count("sync." + first), 3U);
}
TEST_F(MetaBypassTest, PersistFailuresDoNotCachePartialSuccess) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  metabypass::SeparatedStorage storage(observed, index_, m_.data_dir);
  ASSERT_OK(storage.Lock());
  for (uint64_t number : {1, 2, 3})
    ASSERT_OK(metabypass::Write(fs_.get(), storage.BlobPath(number), "blob"));
  ASSERT_OK(storage.Persist({{1, 4}}));
  const auto first = storage.BlobPath(1), second = storage.BlobPath(2);
  const auto third = storage.BlobPath(3);
  for (const auto& failure : {"sync." + third, "dirsync." + m_.data_dir}) {
    SCOPED_TRACE(failure);
    observed->hook = [&](const std::string& key) {
      return key == failure ? IOStatus::IOError("persist failure")
                            : IOStatus::OK();
    };
    ASSERT_TRUE(storage.Persist({{1, 4}, {2, 4}, {3, 4}}).IsIOError());
    observed->hook = nullptr;
    const auto second_syncs = observed->Count("sync." + second);
    const auto third_syncs = observed->Count("sync." + third);
    ASSERT_OK(storage.Persist({{1, 4}, {2, 4}, {3, 4}}));
    ASSERT_EQ(observed->Count("sync." + first), 1U);
    ASSERT_EQ(observed->Count("sync." + second), second_syncs + 1);
    ASSERT_EQ(observed->Count("sync." + third), third_syncs + 1);
    // Replace the cache with the first root before testing the next failure.
    ASSERT_OK(storage.Persist({{1, 4}}));
  }
}
TEST_F(MetaBypassTest, PersistNativeCaptureBlobsStaySealed) {
  for (bool direct : {false, true}) {
    SCOPED_TRACE(direct);
    if (direct) EnableDirectWrite();
    o_.disable_auto_compactions = true;
    m_.enable_blob_file_gc = false;
    m_.checkpoint_interval_ms = 60000;
    auto observed = std::make_shared<ReplacementFileSystem>(fs_);
    auto env = NewCompositeEnv(observed);
    Options options = o_;
    options.env = env.get();
    ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
    ASSERT_OK(db_->Put(WriteOptions(), "first", std::string(4096, 'a')));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->SyncBackup());
    const auto blobs = BlobFiles();
    ASSERT_FALSE(blobs.empty());
    std::map<std::string, std::string> sealed;
    std::map<std::string, uint64_t> syncs;
    for (const auto& name : blobs) {
      const auto path = m_.data_dir + "/" + name;
      // Only named-file syncs belong to Persist; an active DW file can also
      // exist in the directory but is not part of the captured native root.
      if (observed->Count("sync." + path) == 0) continue;
      ASSERT_OK(metabypass::Read(fs_.get(), path, &sealed[path]));
      ASSERT_GE(sealed[path].size(), BlobLogFooter::kSize);
      BlobLogFooter footer;
      ASSERT_OK(footer.DecodeFrom(Slice(
          sealed[path].data() + sealed[path].size() - BlobLogFooter::kSize,
          BlobLogFooter::kSize)));
      syncs[path] = observed->Count("sync." + path);
      ASSERT_GT(syncs[path], 0U);
    }
    ASSERT_FALSE(sealed.empty());
    ASSERT_OK(db_->Put(WriteOptions(), "second", std::string(4096, 'b')));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->SyncBackup());
    for (const auto& entry : sealed) {
      std::string after;
      ASSERT_OK(metabypass::Read(fs_.get(), entry.first, &after));
      ASSERT_EQ(after, entry.second);
      ASSERT_EQ(observed->Count("sync." + entry.first), syncs[entry.first]);
    }
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(index_);
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
    ASSERT_NO_FATAL_FAILURE(Open());
    Check("first", std::string(4096, 'a'));
    Check("second", std::string(4096, 'b'));
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDirIfMissing(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, PreservesAppendVerificationInformation) {
  class VerificationFS : public FileSystemWrapper {
   public:
    explicit VerificationFS(std::shared_ptr<FileSystem> fs)
        : FileSystemWrapper(fs) {}
    const char* Name() const override { return "VerificationFS"; }
    bool seen = false;
    IOStatus NewWritableFile(const std::string& path,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* debug) override {
      class Writer : public FSWritableFileOwnerWrapper {
       public:
        Writer(std::unique_ptr<FSWritableFile> file, bool* seen)
            : FSWritableFileOwnerWrapper(std::move(file)), seen_(seen) {}
        using FSWritableFileOwnerWrapper::Append;
        IOStatus Append(const Slice& bytes, const IOOptions& options,
                        const DataVerificationInfo& verification,
                        IODebugContext* debug) override {
          *seen_ = true;
          return target()->Append(bytes, options, verification, debug);
        }

       private:
        bool* seen_;
      };
      IOStatus s = target()->NewWritableFile(path, options, result, debug);
      if (s.ok()) result->reset(new Writer(std::move(*result), &seen));
      return s;
    }
  };
  auto verifying = std::make_shared<VerificationFS>(fs_);
  auto storage = std::make_shared<metabypass::SeparatedStorage>(
      verifying, index_, m_.data_dir);
  ASSERT_OK(storage->Lock());
  ASSERT_OK(fs_->CreateDir(index_, IOOptions(), nullptr));
  metabypass::Backup backup(storage, index_, m_);
  ASSERT_OK(backup.Start(false));
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(backup.NewWritableFile(index_ + "/000001.log", FileOptions(), &file,
                                   nullptr));
  ASSERT_OK(
      file->Append("record", IOOptions(), DataVerificationInfo(), nullptr));
  ASSERT_TRUE(verifying->seen);
  ASSERT_OK(file->Close(IOOptions(), nullptr));
  file.reset();
  ASSERT_OK(backup.Stop());
}
TEST_F(MetaBypassTest, RejectUnsupportedWritesAndOptions) {
  Options transactions = o_;
  transactions.allow_2pc = true;
  const auto before = SnapshotTree(root_);
  ASSERT_TRUE(
      MetaBypassDB::Open(transactions, m_, index_, &db_).IsNotSupported());
  ASSERT_FALSE(db_);
  ASSERT_EQ(SnapshotTree(root_), before);
  ASSERT_TRUE(MetaBypassDB::Restore(transactions, m_, index_).IsNotSupported());
  ASSERT_EQ(SnapshotTree(root_), before);

  ASSERT_NO_FATAL_FAILURE(Open());
  WriteOptions w;
  w.disableWAL = true;
  ASSERT_TRUE(db_->Put(w, "x", "y").IsNotSupported());
  WriteBatch batch;
  ASSERT_OK(batch.Merge("x", "y"));
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).IsNotSupported());
  ASSERT_OK(db_->Close());
  db_.reset();
  o_.enable_blob_direct_write = true;
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsNotSupported());
}

TEST_F(MetaBypassTest, QueueOptionsDefaults) {
  const MetaBypassOptions defaults;
  EXPECT_EQ(defaults.wal_queue_capacity, 896U * 1024 * 1024);
  EXPECT_EQ(defaults.metadata_queue_capacity, 128U * 1024 * 1024);
  EXPECT_EQ(defaults.batch_bytes, 512U * 1024 * 1024);
  EXPECT_EQ(defaults.interval_ms, 5000U);
  EXPECT_EQ(defaults.checkpoint_interval_ms, 40000U);
}

TEST_F(MetaBypassTest, QueueOptionsRejectInvalidConfigurationBeforeMutation) {
  const auto before = SnapshotTree(root_);
  const struct {
    const char* reason;
    size_t wal, metadata, batch;
    uint64_t mirror_ms, checkpoint_ms;
  } cases[] = {
      {"zero WAL capacity", 0, 4096, 4096, 1000, 30000},
      {"zero metadata capacity", 4096, 0, 4096, 1000, 30000},
      {"capacity sum overflow", std::numeric_limits<size_t>::max(), 1, 4096,
       1000, 30000},
      {"capacity sum below minimum", 1024, 3071, 4095, 1000, 30000},
      {"zero batch threshold", 1024, 3072, 0, 1000, 30000},
      {"batch threshold above capacity sum", 1024, 3072, 4097, 1000, 30000},
      {"zero mirror interval", 1024, 3072, 4096, 0, 30000},
      {"mirror interval too large for deadline", 1024, 3072, 4096, INT64_MAX,
       30000},
      {"out of range mirror interval", 1024, 3072, 4096, UINT64_MAX, 30000},
      {"zero checkpoint interval", 1024, 3072, 4096, 1000, 0},
      {"out of range checkpoint interval", 1024, 3072, 4096, 1000, UINT64_MAX},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.reason);
    auto options = m_;
    options.wal_queue_capacity = test.wal;
    options.metadata_queue_capacity = test.metadata;
    options.batch_bytes = test.batch;
    options.interval_ms = test.mirror_ms;
    options.checkpoint_interval_ms = test.checkpoint_ms;
    ASSERT_TRUE(
        MetaBypassDB::Open(o_, options, index_, &db_).IsInvalidArgument());
    ASSERT_FALSE(db_);
    ASSERT_EQ(SnapshotTree(root_), before);
    ASSERT_TRUE(MetaBypassDB::Restore(o_, options, index_).IsInvalidArgument());
    ASSERT_EQ(SnapshotTree(root_), before);
  }
}

TEST_F(MetaBypassTest, QueueOptionsRestoreWithMinimumCombinedCapacity) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  m_.wal_queue_capacity = 1024;
  m_.metadata_queue_capacity = 3072;
  m_.batch_bytes = QueueBudget(m_);
  MetaBypassRecoveryReport report;
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_, &report));
  EXPECT_EQ(report.checkpoint_sequence, 1U);
  EXPECT_EQ(report.recovered_sequence, 1U);
}

TEST_F(MetaBypassTest, QueueOptionsEnforceIndependentEventLimits) {
  m_.wal_queue_capacity = 1024;
  m_.metadata_queue_capacity = 3072;
  m_.batch_bytes = 1;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  const size_t header = metabypass::BackupTestAccess::EventHeaderCharge();
  const std::string wal_name = "000001.log", metadata_name = "MANIFEST-000002";
  const std::string wal_bytes(m_.wal_queue_capacity - header - wal_name.size(),
                              'w');
  const std::string metadata_bytes(
      m_.metadata_queue_capacity - header - metadata_name.size(), 'm');
  const auto wal_error = wal->Append(wal_bytes + "x", IOOptions(), nullptr);
  EXPECT_TRUE(wal_error.IsIOError());
  EXPECT_NE(wal_error.ToString().find("exceeds Metabypass WAL queue capacity"),
            std::string::npos);
  const auto metadata_error =
      metadata->Append(metadata_bytes + "x", IOOptions(), nullptr);
  EXPECT_TRUE(metadata_error.IsIOError());
  EXPECT_NE(metadata_error.ToString().find(
                "exceeds Metabypass metadata queue capacity"),
            std::string::npos);
  ASSERT_OK(wal->Append(wal_bytes, IOOptions(), nullptr));
  ASSERT_OK(metadata->Append(metadata_bytes, IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  const auto stats = backup->Stats();
  ASSERT_OK(stats.error);
  EXPECT_EQ(stats.queued_bytes, 0U);
  EXPECT_EQ(stats.mirrored_wal_bytes, wal_bytes.size());
  for (const auto& dir : {index_, m_.backup_dir + "/work"}) {
    std::string bytes;
    ASSERT_OK(metabypass::Read(fs_.get(), dir + "/" + wal_name, &bytes));
    EXPECT_EQ(bytes, wal_bytes);
    ASSERT_OK(metabypass::Read(fs_.get(), dir + "/" + metadata_name, &bytes));
    EXPECT_EQ(bytes, metadata_bytes);
  }
}

#ifndef NDEBUG
TEST_F(MetaBypassTest, QueueOptionsBatchThresholdCombinesChannels) {
  const std::string wal_bytes(600, 'w'), metadata_bytes(1000, 'm');
  const size_t header = metabypass::BackupTestAccess::EventHeaderCharge();
  const size_t wal_events =
      2 * (header + std::string("000001.log").size()) + wal_bytes.size();
  const size_t metadata_events =
      2 * (header + std::string("MANIFEST-000002").size()) +
      metadata_bytes.size();
  m_.wal_queue_capacity = 1024;
  m_.metadata_queue_capacity = 3072;
  m_.batch_bytes = wal_events + metadata_events;
  m_.interval_ms = 60000;
  TestGate applying;
  std::atomic<unsigned> notifications{0};
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    applying.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  BlockFirstApply(&applying);
  sync->SetCallBack("MetaBypass::MirrorNotified",
                    [&](void*) { ++notifications; });
  sync->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  ASSERT_OK(metadata->Append(metadata_bytes, IOOptions(), nullptr));
  EXPECT_EQ(notifications.load(), 0U);
  ASSERT_OK(wal->Append(wal_bytes, IOOptions(), nullptr));
  ASSERT_TRUE(applying.WaitUntilBlockedFor(std::chrono::seconds(10)));
  EXPECT_GT(notifications.load(), 0U);
  const auto stats = backup->Stats();
  EXPECT_OK(stats.error);
  EXPECT_EQ(stats.queued_bytes, m_.batch_bytes);
  applying.Release();
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
}
#endif

TEST_F(MetaBypassTest,
       RejectIdentityAndRemoteCompactionBeforeDirectoryChanges) {
  for (bool disable_identity : {true, false}) {
    Options invalid = o_;
    if (disable_identity) {
      invalid.write_identity_file = false;
    } else {
      invalid.compaction_service =
          std::make_shared<UnsupportedCompactionService>();
    }
    ASSERT_TRUE(MetaBypassDB::Open(invalid, m_, index_, &db_).IsNotSupported());
    ASSERT_EQ(db_, nullptr);
    ASSERT_TRUE(MetaBypassDB::Restore(invalid, m_, index_).IsNotSupported());
    for (const auto& path : {index_, m_.data_dir, m_.backup_dir}) {
      ASSERT_TRUE(fs_->FileExists(path, IOOptions(), nullptr).IsNotFound());
    }
  }
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
}
TEST_F(MetaBypassTest, ReopenPreservesValidatedIdentityMarker) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  auto faults = std::make_shared<MarkerFailureFileSystem>(fs_);
  faults->fail_path = index_ + "/METABYPASS";
  auto env = NewCompositeEnv(faults);
  Options options = o_;
  options.env = env.get();
  // A truncating rewrite would fail this open and corrupt the next one.
  Status opened = MetaBypassDB::Open(options, m_, index_, &db_);
  EXPECT_OK(opened);
  if (opened.ok()) {
    Check("key", "value");
    EXPECT_OK(db_->Close());
  }
  db_.reset();
  EXPECT_EQ(faults->failures, 0);
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("key", "value");
}
TEST_F(MetaBypassTest, RestoreRetryPreservesValidatedProgressMarker) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  auto faults = std::make_shared<MarkerFailureFileSystem>(fs_);
  auto env = NewCompositeEnv(faults);
  Options options = o_;
  options.env = env.get();
  // Interrupt after the progress marker is durable but before copying finishes.
  faults->fail_path = index_ + "/CURRENT";
  ASSERT_TRUE(MetaBypassDB::Restore(options, m_, index_).IsIOError());
  ASSERT_EQ(faults->failures, 1);
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsIncomplete());
  faults->fail_path = index_ + "/METABYPASS-RESTORING";
  ASSERT_OK(MetaBypassDB::Restore(options, m_, index_));
  EXPECT_EQ(faults->failures, 1);
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("key", "value");
  ASSERT_OK(db_->Put(WriteOptions(), "after", "retry"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("after", "retry");
}
TEST_F(MetaBypassTest, BackgroundBlockedDoesNotBlockSmallForegroundWrite) {
  ASSERT_NO_FATAL_FAILURE(Open());
  TestGate gate;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply",
                                        [&](void*) { gate.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "first", "value"));
  std::thread sync([&] { EXPECT_OK(db_->SyncBackup()); });
  gate.WaitUntilBlocked();
  ASSERT_OK(db_->Put(WriteOptions(), "second", "value"));
  Check("second", "value");
  gate.Release();
  sync.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->SyncBackup());
}
TEST_F(MetaBypassTest, DirtyWriteCompletionsBypassQueueMutex) {
  auto storage =
      std::make_shared<metabypass::SeparatedStorage>(fs_, index_, m_.data_dir);
  metabypass::Backup backup(storage, index_, m_, o_);
  metabypass::BackupTestAccess::CheckDirtyWriteCompletions(&backup);
}
TEST_F(MetaBypassTest, MirrorReadinessIndependentProgressReasons) {
  auto storage =
      std::make_shared<metabypass::SeparatedStorage>(fs_, index_, m_.data_dir);
  metabypass::Backup backup(storage, index_, m_, o_);
  metabypass::BackupTestAccess::CheckIndependentReasons(&backup);
}
TEST_F(MetaBypassTest, MirrorReadinessWhilePublicationBlocked) {
  m_.interval_ms = m_.checkpoint_interval_ms = 60000;
  m_.batch_bytes = 256 * 1024;
  o_.write_buffer_size = 4 * 1024 * 1024;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
  ASSERT_OK(db_->SyncBackup());
  TestGate publishing;
  std::atomic<bool> observe{false}, returned{false};
  std::atomic<unsigned> applied_only{0}, pending{0};
  std::atomic<metabypass::Backup*> probe{nullptr};
  auto observe_readiness = [&](metabypass::Backup* backup) {
    if (!observe.load()) return;
    auto state = metabypass::BackupTestAccess::Observe(backup);
    if (state.empty || state.capture || state.capacity || state.size ||
        state.stopping || state.error)
      return;
    if (state.requested > state.applied) {
      EXPECT_TRUE(state.ready);
      ++pending;
    } else if (state.published < state.requested) {
      // The explicit request has been applied but not yet published.
      EXPECT_FALSE(state.ready);
      ++applied_only;
    }
  };
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) { publishing.Block(); });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::MirrorReadyProbe", [&](void* arg) {
        auto* backup = static_cast<metabypass::Backup*>(arg);
        probe.store(backup);
        observe_readiness(backup);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "candidate", "value"));
  std::thread first([&] {
    EXPECT_OK(db_->SyncBackup());
    returned = true;
  });
  publishing.WaitUntilBlocked();
  observe = true;
  EXPECT_OK(db_->Put(WriteOptions(), "tail", "value"));
  // WAL publication does not acquire the shared mutex or invoke WakeMirror.
  if (auto* backup = probe.load()) {
    auto lock = metabypass::BackupTestAccess::LockQueueMutex(backup);
    observe_readiness(backup);
  }
  EXPECT_GT(applied_only.load(), 0U);
  EXPECT_FALSE(returned.load());
  // A higher explicit target must resume immediate catch-up even while the
  // first candidate's publication remains blocked.
  TestGate requested;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                        [&](void*) { requested.Block(); });
  std::thread second([&] { EXPECT_OK(db_->SyncBackup()); });
  requested.WaitUntilBlocked();
  EXPECT_GT(pending.load(), 0U);
  EXPECT_FALSE(returned.load());
  requested.Release();
  publishing.Release();
  first.join();
  second.join();
  observe = false;
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("old", "retained");
  Check("candidate", "value");
  Check("tail", "value");
}
TEST_F(MetaBypassTest, ValidatorBlockedMirrorStillDrainsAndPinsFiles) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 32768;
  m_.batch_bytes = 1;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  TestGate gate;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) { gate.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "first", "value"));
  std::thread sync([&] { EXPECT_OK(db_->SyncBackup()); });
  gate.WaitUntilBlocked();
  // More WAL bytes than the entire queue: completion requires the mirror
  // to drain while validation is blocked. Flush/compaction delete old files.
  for (int i = 0; i < 1000; ++i) {
    EXPECT_OK(db_->Put(WriteOptions(),
                       std::string(128, 'k') + std::to_string(i),
                       std::string(100, 'v')));
  }
  EXPECT_OK(db_->Flush(FlushOptions()));
  EXPECT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  std::vector<std::string> files;
  EXPECT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  int points = 0;
  for (const auto& f : files)
    if (f.compare(0, 6, "point-") == 0) ++points;
  EXPECT_LE(points, 4);
  gate.Release();
  sync.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("old", "retained");
  Check(std::string(128, 'k') + "999", std::string(100, 'v'));
}
TEST_F(MetaBypassTest, ReplacementCaptureDefersSyncUntilPublication) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  m_.interval_ms = m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  std::atomic<unsigned> captures{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CandidateCaptured", [&](void*) {
        for (const auto* name : {"CURRENT", "IDENTITY"}) {
          const std::string prefix = std::string("capture.") + name;
          EXPECT_GT(observed->Count(prefix + ".append"), 0U);
          EXPECT_GT(observed->Count(prefix + ".close"), 0U);
          EXPECT_EQ(observed->Count(prefix + ".fsync"), 0U);
        }
        ++captures;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->SyncBackup());
  EXPECT_GT(captures.load(), 0U);
  for (const auto* name : {"CURRENT", "IDENTITY"})
    EXPECT_GT(observed->Count(std::string("publish.") + name + ".fsync"), 0U);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
  ASSERT_OK(db_->Close());
  db_.reset();
}
TEST_F(MetaBypassTest, CandidateCacheInvalidationIsColdAndOptional) {
  for (bool unsupported : {false, true}) {
    auto observed = std::make_shared<ReplacementFileSystem>(fs_);
    auto env = NewCompositeEnv(observed);
    Options options = o_;
    options.env = env.get();
    observed->hook = [&](const std::string& key) {
      if (unsupported && key.find(".invalidate") != std::string::npos)
        return IOStatus::NotSupported("cache advice unavailable");
      return IOStatus::OK();
    };
    ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
    ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
    ASSERT_OK(db_->SyncBackup());
    EXPECT_GT(observed->Count("publish.CURRENT.invalidate"), 0U);
    EXPECT_GT(observed->Count("publish.IDENTITY.invalidate"), 0U);
    EXPECT_EQ(observed->Count("capture.CURRENT.invalidate"), 0U);
    ASSERT_OK(db_->Close());
    db_.reset();
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("key", "value");
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, FailedCandidateSyncSkipsCacheInvalidation) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->SyncBackup());
  const auto invalidated = observed->Count("publish.CURRENT.invalidate");
  observed->hook = [](const std::string& key) {
    return key == "publish.CURRENT.fsync" ? IOStatus::IOError("sync failed")
                                          : IOStatus::OK();
  };
  ASSERT_TRUE(db_->SyncBackup().IsIOError());
  EXPECT_EQ(observed->Count("publish.CURRENT.invalidate"), invalidated);
  EXPECT_TRUE(db_->Close().IsIOError());
  db_.reset();
}
TEST_F(MetaBypassTest, ReplacementFailuresPreservePointAndWakeWaiters) {
  for (const auto* operation :
       {"capture.CURRENT.append", "capture.CURRENT.close",
        "publish.CURRENT.fsync", "publish.CURRENT.invalidate"}) {
    SCOPED_TRACE(operation);
    auto observed = std::make_shared<ReplacementFileSystem>(fs_);
    auto env = NewCompositeEnv(observed);
    Options options = o_;
    options.env = env.get();
    m_.interval_ms = m_.checkpoint_interval_ms = 60000;
    TestGate failing, waiting;
    std::atomic<bool> armed{false}, injected{false};
    observed->hook = [&](const std::string& key) {
      if (armed.load() && key == operation && !injected.exchange(true)) {
        failing.Block();
        return IOStatus::IOError("replacement operation failed");
      }
      return IOStatus::OK();
    };
    ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
    ASSERT_OK(db_->Put(WriteOptions(), "key", "old"));
    ASSERT_OK(db_->SyncBackup());
    std::string before;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &before));
    ASSERT_OK(db_->Put(WriteOptions(), "key", "new"));
    std::atomic<unsigned> waiters{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                          [&](void*) {
                                            if (++waiters == 3) waiting.Block();
                                          });
    SyncPoint::GetInstance()->EnableProcessing();
    const uint64_t closes_before = observed->Count("capture.CURRENT.close");
    armed = true;
    std::array<Status, 3> statuses;
    std::vector<std::thread> threads;
    threads.emplace_back([&] { statuses[0] = db_->SyncBackup(); });
    failing.WaitUntilBlocked();
    for (size_t i = 1; i < statuses.size(); ++i)
      threads.emplace_back([&, i] { statuses[i] = db_->SyncBackup(); });
    waiting.WaitUntilBlocked();
    waiting.Release();
    failing.Release();
    for (auto& thread : threads) thread.join();
    EXPECT_TRUE(injected.load());
    if (std::string(operation) == "capture.CURRENT.append") {
      EXPECT_EQ(observed->Count("capture.CURRENT.close"), closes_before + 1);
    }
    for (const auto& status : statuses) EXPECT_TRUE(status.IsIOError());
    EXPECT_TRUE(db_->Close().IsIOError());
    db_.reset();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    std::string after;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &after));
    EXPECT_EQ(after, before);
    ASSERT_NO_FATAL_FAILURE(RestorePrefix(1, 2));
    CheckContents({{"key", "new"}});
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, ReplacementPublishSyncAllowsMirrorAndNativeFlush) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  options.enable_blob_direct_write = false;
  options.write_buffer_size = 4 * 1024 * 1024;
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 32768;
  m_.batch_bytes = 1;
  m_.interval_ms = m_.checkpoint_interval_ms = 60000;
  TestGate publishing;
  std::atomic<bool> armed{false}, blocked{false}, returned{false};
  observed->hook = [&](const std::string& key) {
    if (armed.load() && key == "publish.CURRENT.fsync" &&
        !blocked.exchange(true))
      publishing.Block();
    return IOStatus::OK();
  };
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
  ASSERT_OK(db_->SyncBackup());
  std::string before;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &before));
  ASSERT_OK(db_->Put(WriteOptions(), "candidate", "value"));
  armed = true;
  Status synced;
  std::thread sync([&] {
    synced = db_->SyncBackup();
    returned = true;
  });
  publishing.WaitUntilBlocked();
  auto initial = db_->GetBackupStats();
  EXPECT_OK(initial.error);
  for (int i = 0; i < 200; ++i)
    EXPECT_OK(db_->Put(WriteOptions(), "tail" + std::to_string(i),
                       std::string(1024, 'v')));
  FlushOptions flush;
  flush.wait = true;
  EXPECT_OK(db_->Flush(flush));
  auto progressed = db_->GetBackupStats();
  EXPECT_OK(progressed.error);
  EXPECT_GT(progressed.mirrored_wal_bytes,
            initial.mirrored_wal_bytes + m_.wal_queue_capacity);
  EXPECT_FALSE(returned.load());
  std::string pending;
  EXPECT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pending));
  EXPECT_EQ(pending, before);
  publishing.Release();
  sync.join();
  ASSERT_OK(synced);
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("tail199", std::string(1024, 'v'));
  ASSERT_OK(db_->Close());
  db_.reset();
}
TEST_F(MetaBypassTest, OrdinaryWriteStillSynchronizes) {
  auto observed = std::make_shared<ReplacementFileSystem>(fs_);
  const std::string path = root_ + "/ordinary-write";
  ASSERT_OK(metabypass::Write(observed.get(), path, "durable"));
  EXPECT_EQ(observed->Count("capture.ordinary-write.append"), 1U);
  EXPECT_EQ(observed->Count("capture.ordinary-write.fsync"), 1U);
  EXPECT_EQ(observed->Count("capture.ordinary-write.close"), 1U);
  std::string contents;
  ASSERT_OK(metabypass::Read(fs_.get(), path, &contents));
  EXPECT_EQ(contents, "durable");
}
TEST_F(MetaBypassTest, CandidateSyncBlockedMirrorStillDrains) {
  CandidateValidationBlockedScenario("MetaBypass::BeforeCandidateFileSync");
}
TEST_F(MetaBypassTest, StagedCandidateReadFailureWakesSyncWaiters) {
  CandidateFailureScenario("MetaBypass::StageCopyReadStatus");
}
TEST_F(MetaBypassTest, StagedCandidateAppendFailureWakesSyncWaiters) {
  CandidateFailureScenario("MetaBypass::StageCopyAppendStatus");
}
TEST_F(MetaBypassTest, StagedCandidateCloseFailureWakesSyncWaiters) {
  CandidateFailureScenario("MetaBypass::StageCopyCloseStatus");
}
TEST_F(MetaBypassTest, CandidateSyncFailureWakesSyncWaiters) {
  CandidateFailureScenario("MetaBypass::CandidateFileSyncStatus");
}
TEST_F(MetaBypassTest, RetiredPointNotifiesIdleMirrorToPruneWal) {
  auto storage =
      std::make_shared<metabypass::SeparatedStorage>(fs_, index_, m_.data_dir);
  metabypass::Backup backup(storage, index_, m_, o_);
  metabypass::BackupTestAccess::CheckIdlePrune(&backup);
}
TEST_F(MetaBypassTest, CandidateAndRestoreCopyDoNotReadBackPayload) {
  class NoReadback : public FileSystemWrapper {
   public:
    explicit NoReadback(const std::shared_ptr<FileSystem>& fs)
        : FileSystemWrapper(fs) {}
    const char* Name() const override { return "NoReadback"; }
    IOStatus NewRandomAccessFile(const std::string&, const FileOptions&,
                                 std::unique_ptr<FSRandomAccessFile>*,
                                 IODebugContext*) override {
      return IOStatus::IOError("unexpected copy readback");
    }
  } fs(fs_);
  const std::string source = root_ + "/source";
  ASSERT_OK(metabypass::Write(fs_.get(), source, std::string(131073, 'c')));
  ASSERT_OK(metabypass::Copy(&fs, source, root_ + "/copied", 131073));
  ASSERT_OK(metabypass::StageCopy(&fs, source, root_ + "/staged", 131073));
}
TEST_F(MetaBypassTest, StageCopyRejectsShortSource) {
  const std::string source = root_ + "/source";
  const std::string destination = root_ + "/staged";
  ASSERT_OK(metabypass::Write(fs_.get(), source, "short"));
  ASSERT_TRUE(
      metabypass::StageCopy(fs_.get(), source, destination, 6).IsCorruption());
}
TEST_F(MetaBypassTest, PointerDirectorySyncFailureRestoresCompletePoint) {
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = m_.checkpoint_interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "old"));
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::PointerDirectorySyncStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("pointer directory sync");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "new"));
  EXPECT_TRUE(db_->SyncBackup().IsIOError());
  EXPECT_TRUE(db_->Close().IsIOError());
  db_.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  // Rename has already exposed the new pointer. This injected error does not
  // discard unsynced filesystem state, so the selected point is the new one.
  // Both old and new candidates must already be independently recoverable.
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "new");
}
TEST_F(MetaBypassTest, RetiredPointCleanupDoesNotDelaySyncOrMirror) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 32768;
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  auto initial = db_->GetBackupStats();
  ASSERT_OK(initial.error);
  ASSERT_EQ(initial.recovery_points, 1U);
  ASSERT_EQ(initial.pending_gc_points, 0U);
  TestGate gc, next_publication;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::GCStarted",
                                        [&](void*) { gc.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  WriteOptions sync_write;
  sync_write.sync = true;
  // The synchronous Put publishes the next point. Another explicit
  // barrier would wait for the GC slot that this test deliberately holds.
  ASSERT_OK(db_->Put(sync_write, "third", "durable"));
  gc.WaitUntilBlocked();
  auto pending = db_->GetBackupStats();
  ASSERT_OK(pending.error);
  ASSERT_EQ(pending.pending_gc_points, 1);
  ASSERT_GT(pending.pending_gc_bytes, 0);
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeGCSlotWait", [&](void*) { next_publication.Block(); });
  ASSERT_OK(db_->Put(WriteOptions(), "fourth", "durable"));
  Status fourth_status;
  std::thread fourth([&] { fourth_status = db_->SyncBackup(); });
  next_publication.WaitUntilBlocked();
  next_publication.Release();
  std::string unchanged;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &unchanged));
  EXPECT_EQ(unchanged, pointer);
  // Validation is waiting for the GC slot; mirroring still drains the queue.
  for (int i = 0; i < 300; ++i)
    EXPECT_OK(db_->Put(WriteOptions(), "tail" + std::to_string(i), "v"));
  gc.Release();
  fourth.join();
  ASSERT_OK(fourth_status);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  auto closed = db_->GetBackupStats();
  ASSERT_OK(closed.error);
  EXPECT_EQ(closed.pending_gc_points, 0);
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  int points = 0;
  for (const auto& file : files)
    if (file.compare(0, 6, "point-") == 0) ++points;
  EXPECT_EQ(points, 1);
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("third", "durable");
  Check("tail299", "v");
}
TEST_F(MetaBypassTest, RetiredPointCleanupFailureIsSticky) {
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  auto initial = db_->GetBackupStats();
  ASSERT_OK(initial.error);
  ASSERT_EQ(initial.recovery_points, 1U);
  ASSERT_EQ(initial.pending_gc_points, 0U);
  TestGate gc, failed;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::GCStarted",
                                        [&](void*) { gc.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::GCStatus", [](void* arg) {
    *static_cast<Status*>(arg) = Status::IOError("retired point cleanup");
  });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::GCFailureRecorded",
                                        [&](void*) { failed.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "third", "durable"));
  ASSERT_OK(db_->SyncBackup());
  gc.WaitUntilBlocked();
  gc.Release();
  failed.WaitUntilBlocked();
  EXPECT_TRUE(db_->SyncBackup().IsIOError());
  EXPECT_TRUE(db_->Put(WriteOptions(), "rejected", "v").IsIOError());
  auto stats = db_->GetBackupStats();
  EXPECT_TRUE(stats.error.IsIOError());
  EXPECT_EQ(stats.pending_gc_points, 1);
  failed.Release();
  EXPECT_TRUE(db_->Close().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("third", "durable");
}
TEST_F(MetaBypassTest, CandidateSkipsUnreferencedIndexFiles) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "first", "v"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  auto before = db_->GetBackupStats();
  ASSERT_OK(before.error);
  const std::vector<std::string> unused = {"MANIFEST-999999", "000001.log",
                                           "999999.sst"};
  std::atomic<bool> injected{false}, checked{false}, absent{true};
  Status injection;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeCaptureFiles", [&](void*) {
        if (injected.exchange(true)) return;
        for (const auto& file : unused) {
          injection = metabypass::Write(
              fs_.get(), m_.backup_dir + "/work/" + file, "unreferenced");
          if (!injection.ok()) return;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::CandidateCaptured", [&](void* arg) {
        std::vector<std::string> files;
        Status s = fs_->GetChildren(*static_cast<std::string*>(arg),
                                    IOOptions(), &files, nullptr);
        if (!s.ok()) {
          absent = false;
        } else {
          for (const auto& file : unused)
            if (std::find(files.begin(), files.end(), file) != files.end())
              absent = false;
        }
        checked = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "second", "v"));
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(injection);
  EXPECT_TRUE(injected);
  EXPECT_TRUE(checked);
  EXPECT_TRUE(absent);
  auto after = db_->GetBackupStats();
  ASSERT_OK(after.error);
  EXPECT_GT(after.candidate_copied_bytes, before.candidate_copied_bytes);
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("second", "v");
}
TEST_F(MetaBypassTest, MissingReferencedSstFailsCapture) {
  m_.checkpoint_interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "first", "v"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  ASSERT_OK(db_->Put(WriteOptions(), "second", "v"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto tables = FilesOfType(index_, kTableFile);
  const auto previous = FilesOfType(PublishedPoint(), kTableFile);
  tables.erase(std::remove_if(tables.begin(), tables.end(),
                              [&](const auto& f) {
                                return std::find(previous.begin(),
                                                 previous.end(),
                                                 f) != previous.end();
                              }),
               tables.end());
  ASSERT_FALSE(tables.empty());
  const std::string table = tables.front();
  std::atomic<bool> removed{false};
  Status removal;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeCaptureTables", [&](void*) {
        if (removed.exchange(true)) return;
        removal = fs_->DeleteFile(index_ + "/" + table, IOOptions(), nullptr);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const Status failed = db_->SyncBackup();
  EXPECT_TRUE(failed.IsIOError() || failed.IsNotFound()) << failed.ToString();
  SyncPoint::GetInstance()->DisableProcessing();
  EXPECT_TRUE(removed);
  ASSERT_OK(removal);
  auto stats = db_->GetBackupStats();
  EXPECT_EQ(stats.error, failed);
  const Status rejected = db_->Put(WriteOptions(), "rejected", "v");
  EXPECT_EQ(rejected, failed);
  std::string unchanged;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &unchanged));
  EXPECT_EQ(unchanged, pointer);
  Status close = db_->Close();
  EXPECT_FALSE(close.ok()) << close.ToString();
}
TEST_F(MetaBypassTest, CandidateSstFallsBackToCopyWhenLinkFails) {
  auto no_links = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(no_links);
  Options options = o_;
  options.env = env.get();
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  EXPECT_GT(no_links->rejected_links.load(), 0);
  EXPECT_GT(no_links->copied_tables.load(), 0);
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  EXPECT_GT(stats.candidate_copied_bytes, 0);
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
}
TEST_F(MetaBypassTest, SstOperationsPreserveExternalBoundary) {
  std::unique_ptr<metabypass::Backup> backup;
  ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
  const std::string table = index_ + "/000002.sst";
  const std::string external = root_ + "/000002.sst";
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(backup->NewWritableFile(table, FileOptions(), &writer, nullptr));
  ASSERT_OK(writer->Append("table", IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  EXPECT_TRUE(
      backup->LinkFile(table, external, IOOptions(), nullptr).IsNotSupported());
  EXPECT_TRUE(
      backup->LinkFile(external, table, IOOptions(), nullptr).IsNotSupported());
  EXPECT_TRUE(backup->RenameFile(table, external, IOOptions(), nullptr)
                  .IsNotSupported());
  EXPECT_TRUE(backup->RenameFile(external, table, IOOptions(), nullptr)
                  .IsNotSupported());
  ASSERT_OK(backup->Stop());
  EXPECT_TRUE(FilesOfType(m_.backup_dir + "/work", kTableFile).empty());
  EXPECT_OK(fs_->FileExists(table, IOOptions(), nullptr));
}
TEST_F(MetaBypassTest, SstCapturedOnlyAtCheckpoint) {
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  o_.enable_blob_files = false;
  o_.disable_auto_compactions = true;
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  std::atomic<unsigned> mirrored_tables{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::PrimaryReserved", [&](void* arg) {
        uint64_t number;
        FileType type;
        if (ParseFileName(*static_cast<std::string*>(arg), &number, &type) &&
            type == kTableFile)
          ++mirrored_tables;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int round = 0; round < 3; ++round) {
    ASSERT_OK(db_->Put(WriteOptions(), "key", std::to_string(round)));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_TRUE(FilesOfType(m_.backup_dir + "/work", kTableFile).empty());
  EXPECT_TRUE(FilesOfType(PublishedPoint(), kTableFile).empty());
  EXPECT_EQ(observed->copied_tables.load(), 0U);
  EXPECT_EQ(mirrored_tables.load(), 0U);
  const auto native_tables = FilesOfType(index_, kTableFile);
  const std::set<std::string> expected(native_tables.begin(),
                                       native_tables.end());
  ASSERT_FALSE(expected.empty());
  ASSERT_OK(
      metabypass::Write(fs_.get(), index_ + "/999999.sst", "unreferenced"));
  ASSERT_OK(db_->SyncBackup());
  const auto tables = FilesOfType(PublishedPoint(), kTableFile);
  EXPECT_EQ(std::set<std::string>(tables.begin(), tables.end()), expected);
  EXPECT_EQ(observed->copied_tables.load(), expected.size());
  std::string inventory;
  ASSERT_OK(
      metabypass::Read(fs_.get(), PublishedPoint() + "/INVENTORY", &inventory));
  uint64_t retained = inventory.size(), size;
  std::string kind, file;
  std::istringstream entries(inventory.substr(5));
  while (entries >> kind >> file >> size) {
    ASSERT_EQ(kind, "I");
    retained += size;
    uint64_t number;
    FileType type;
    ASSERT_TRUE(ParseFileName(file, &number, &type));
    if (type != kTableFile) retained += size;
  }
  const auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  EXPECT_EQ(stats.retained_index_bytes, retained);
  EXPECT_TRUE(FilesOfType(m_.backup_dir + "/work", kTableFile).empty());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "2");
}
TEST_F(MetaBypassTest, SstCopyAllowsWalDrainAndPinsCompactionInputs) {
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  o_.enable_blob_files = false;
  o_.disable_auto_compactions = true;
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  m_.batch_bytes = 1;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  for (int round = 0; round < 2; ++round) {
    ASSERT_OK(db_->Put(WriteOptions(), "key", std::to_string(round)));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  const auto sources = FilesOfType(index_, kTableFile);
  ASSERT_GE(sources.size(), 2U);
  TestGate copying, wal_batch, drained;
  std::atomic<bool> tables{false}, blocked{false}, completed{false};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureTables",
                                        [&](void*) { tables = true; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::StageCopyReadStatus",
                                        [&](void*) {
                                          if (tables && !blocked.exchange(true))
                                            copying.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status result;
  std::thread checkpoint([&] {
    result = db_->SyncBackup();
    completed = true;
  });
  copying.WaitUntilBlocked();
  const auto before = db_->GetBackupStats();
  EXPECT_OK(before.error);
  // A single-event batch does not trigger WalAppendBatch.
  BlockFirstApply(&wal_batch);
  EXPECT_OK(db_->Put(WriteOptions(), "tail", "wal"));
  wal_batch.WaitUntilBlocked();
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeMirrorWait",
                                        [&](void*) { drained.Block(); });
  wal_batch.Release();
  drained.WaitUntilBlocked();
  drained.Release();
  const auto after = db_->GetBackupStats();
  EXPECT_OK(after.error);
  EXPECT_GT(after.mirrored_wal_bytes, before.mirrored_wal_bytes);
  EXPECT_FALSE(completed);
  EXPECT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (const auto& file : sources)
    EXPECT_OK(fs_->FileExists(index_ + "/" + file, IOOptions(), nullptr));
  copying.Release();
  checkpoint.join();
  EXPECT_OK(result);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "1");
  Check("tail", "wal");
}
TEST_F(MetaBypassTest, SstCopyCloseWaitsForPinRelease) {
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  TestGate copying;
  std::atomic<bool> tables{false}, blocked{false}, closed{false};
  std::atomic<unsigned> released{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureTables",
                                        [&](void*) { tables = true; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::NativeFilesReleased",
                                        [&](void*) { ++released; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::StageCopyReadStatus",
                                        [&](void*) {
                                          if (tables && !blocked.exchange(true))
                                            copying.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status result;
  std::thread closer([&] {
    result = db_->Close();
    closed = true;
  });
  copying.WaitUntilBlocked();
  EXPECT_FALSE(closed);
  EXPECT_EQ(released.load(), 0U);
  copying.Release();
  closer.join();
  EXPECT_OK(result);
  EXPECT_EQ(released.load(), 1U);
  SyncPoint::GetInstance()->DisableProcessing();
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
}
TEST_F(MetaBypassTest, SstCopyFailureCloseWaitsInDetachDB) {
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  m_.batch_bytes = 1;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  const std::string previous = PublishedPoint();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto sources = FilesOfType(index_, kTableFile);
  ASSERT_FALSE(sources.empty());
  TestGate copying, detaching;
  std::atomic<bool> tables{false}, blocked{false}, closed{false};
  std::atomic<unsigned> released{0};
  const Status first = Status::IOError("mirror failed during SST capture");
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureTables",
                                        [&](void*) { tables = true; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::NativeFilesReleased",
                                        [&](void*) { ++released; });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::StageCopyReadStatus", [&](void* arg) {
        if (tables && !blocked.exchange(true)) {
          copying.Block();
          *static_cast<Status*>(arg) = Status::IOError("later SST copy fault");
        }
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeDetachDB",
                                        [&](void*) { detaching.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  Status checkpoint_status;
  std::thread checkpoint([&] { checkpoint_status = db_->SyncBackup(); });
  copying.WaitUntilBlocked();
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus",
      [&](void* arg) { *static_cast<Status*>(arg) = first; });
  const Status produced = db_->Put(WriteOptions(), "tail", "value");
  if (!produced.ok()) {
    EXPECT_EQ(produced.ToString(), first.ToString());
  }
  checkpoint.join();
  EXPECT_EQ(checkpoint_status.ToString(), first.ToString());
  Status close_status;
  std::thread closer([&] {
    close_status = db_->Close();
    closed = true;
  });
  detaching.WaitUntilBlocked();
  detaching.Release();
  // BeforeDetachDB holds the queue mutex. Acquiring it here proves Close has
  // passed that hook into its wait, after the failed Sync returned immediately.
  const auto waiting = db_->GetBackupStats();
  EXPECT_EQ(waiting.error.ToString(), first.ToString());
  EXPECT_FALSE(closed);
  EXPECT_EQ(released.load(), 0U);
  for (const auto& file : sources)
    EXPECT_OK(fs_->FileExists(index_ + "/" + file, IOOptions(), nullptr));
  copying.Release();
  closer.join();
  EXPECT_EQ(close_status.ToString(), first.ToString());
  EXPECT_EQ(released.load(), 1U);
  EXPECT_EQ(PublishedPoint(), previous);
  SyncPoint::GetInstance()->DisableProcessing();
  db_.reset();
}
TEST_F(MetaBypassTest, ColdSstCaptureFallsBackThroughLogicalFileSystem) {
  EnableSst();
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  observed->reject_reuse = true;
  const auto copied = db_->GetBackupStats();
  ASSERT_OK(copied.error);
  AwaitSst([](const auto& stats) {
    return stats.demotions > 0 && stats.ssd_bytes == 0;
  });
  ASSERT_TRUE(FilesOfType(index_, kTableFile).empty());
  ASSERT_TRUE(FilesOfType(m_.backup_dir + "/work", kTableFile).empty());
  ASSERT_OK(db_->SyncBackup());
  const auto cold_copy = db_->GetBackupStats();
  ASSERT_OK(cold_copy.error);
  EXPECT_GT(cold_copy.candidate_copied_bytes, copied.candidate_copied_bytes);
  EXPECT_GT(observed->rejected_links.load(), 0U);
  EXPECT_GT(observed->synced_tables.load(), 0U);
  EXPECT_FALSE(FilesOfType(PublishedPoint(), kTableFile).empty());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
}
TEST_F(MetaBypassTest, SstCopyFailuresReleasePinAndPreservePoint) {
  for (const auto* failure :
       {"MetaBypass::StageCopyReadStatus", "MetaBypass::StageCopyAppendStatus",
        "MetaBypass::StageCopyCloseStatus", "sync"}) {
    SCOPED_TRACE(failure);
    auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
    auto env = NewCompositeEnv(observed);
    Options options = o_;
    options.env = env.get();
    m_.checkpoint_interval_ms = 60000;
    ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
    ASSERT_OK(db_->Put(WriteOptions(), "key", "old"));
    ASSERT_OK(db_->SyncBackup());
    const std::string previous = PublishedPoint();
    ASSERT_OK(db_->Put(WriteOptions(), "key", "new"));
    ASSERT_OK(db_->Flush(FlushOptions()));
    std::atomic<bool> tables{false};
    std::atomic<unsigned> released{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeCaptureTables",
                                          [&](void*) { tables = true; });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::NativeFilesReleased",
                                          [&](void*) { ++released; });
    if (std::string(failure) == "sync") {
      observed->fail_sync = true;
    } else {
      SyncPoint::GetInstance()->SetCallBack(failure, [&](void* arg) {
        if (tables)
          *static_cast<Status*>(arg) = Status::IOError("SST copy fault");
      });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    EXPECT_TRUE(db_->SyncBackup().IsIOError());
    EXPECT_EQ(released.load(), 1U);
    EXPECT_EQ(PublishedPoint(), previous);
    EXPECT_TRUE(db_->Close().IsIOError());
    db_.reset();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("key", "new");
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDirIfMissing(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, PublishedSstReuseSkipsSyncAndFallsBackToCopy) {
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  options.disable_auto_compactions = true;
  m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  const auto copied = observed->copied_tables.load();
  const auto synced = observed->synced_tables.load();
  ASSERT_GT(copied, 0U);
  ASSERT_GT(synced, 0U);
  ASSERT_OK(db_->SyncBackup());
  EXPECT_GT(observed->reused_tables.load(), 0U);
  EXPECT_EQ(observed->copied_tables.load(), copied);
  EXPECT_EQ(observed->synced_tables.load(), synced);

  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->SyncBackup());
  EXPECT_EQ(observed->synced_tables.load(), synced);

  // Failed published-point links must sync either fallback: native hard links
  // and copies both own bytes whose durability cannot be inferred from LATEST.
  observed->reject_reuse = true;
  observed->reject_native = false;
  ASSERT_OK(db_->SyncBackup());
  EXPECT_EQ(observed->copied_tables.load(), copied);
  EXPECT_GT(observed->synced_tables.load(), synced);
  const auto linked_syncs = observed->synced_tables.load();
  observed->reject_native = true;
  ASSERT_OK(db_->SyncBackup());
  EXPECT_GT(observed->copied_tables.load(), copied);
  EXPECT_GT(observed->synced_tables.load(), linked_syncs);
  observed->reject_reuse = false;
  const std::string previous = PublishedPoint();
  const auto previous_tables = FilesOfType(previous, kTableFile);
  const auto before_new_table_syncs = observed->synced_tables.load();
  ASSERT_OK(db_->Put(WriteOptions(), "new-key", "new-value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  bool found_new_table = false;
  for (const auto& table : FilesOfType(index_, kTableFile)) {
    if (std::find(previous_tables.begin(), previous_tables.end(), table) !=
        previous_tables.end())
      continue;
    // Exercise the real filesystem's missing-path result, including POSIX
    // PathNotFound, rather than injecting a generic NotFound status.
    uint64_t size = 0;
    IOStatus missing =
        fs_->GetFileSize(previous + "/" + table, IOOptions(), &size, nullptr);
    ASSERT_TRUE(missing.IsNotFound() || missing.IsPathNotFound());
    found_new_table = true;
  }
  ASSERT_TRUE(found_new_table);
  ASSERT_OK(db_->SyncBackup());
  EXPECT_GT(observed->synced_tables.load(), before_new_table_syncs);
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
  Check("new-key", "new-value");
}
TEST_F(MetaBypassTest, ReusedSstLinkFallbackSyncFailurePreservesPoint) {
  auto observed = std::make_shared<CandidateNoLinkFileSystem>(fs_);
  auto env = NewCompositeEnv(observed);
  Options options = o_;
  options.env = env.get();
  m_.checkpoint_interval_ms = 60000;
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db_));
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  const std::string point = PublishedPoint();
  const auto synced = observed->synced_tables.load();
  observed->reject_reuse = true;
  observed->reject_native = false;
  observed->fail_sync = true;
  EXPECT_TRUE(db_->SyncBackup().IsIOError());
  EXPECT_GT(observed->synced_tables.load(), synced);
  EXPECT_EQ(PublishedPoint(), point);
  EXPECT_TRUE(db_->Put(WriteOptions(), "rejected", "v").IsIOError());
  EXPECT_TRUE(db_->Delete(WriteOptions(), "key").IsIOError());
  EXPECT_TRUE(db_->Close().IsIOError());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
}
TEST_F(MetaBypassTest, BackupErrorFastPathPreservesFirstStatus) {
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  ASSERT_OK(backup->Error());
  const Status first = Status::Corruption("first storage failure", "details");
  const Status later = Status::IOError("later storage failure");
  ASSERT_NE(first.ToString(), later.ToString());
  backup->StorageFailed(first);
  backup->StorageFailed(later);
  EXPECT_EQ(backup->Error().ToString(), first.ToString());
  auto stats = backup->Stats();
  EXPECT_EQ(stats.error.ToString(), first.ToString());
  EXPECT_TRUE(wal->Append("rejected", IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(wal->Close(IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(metadata->Close(IOOptions(), nullptr).IsIOError());
  EXPECT_EQ(backup->Stop().ToString(), first.ToString());
}
TEST_F(MetaBypassTest, NativeSnapshotRegeneratesCurrentFromCapturedManifest) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "first", "value"));
  ASSERT_OK(db_->SyncBackup());
  std::atomic<bool> injected{false};
  Status mutation;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeCaptureFiles", [&](void*) {
        if (injected.exchange(true)) return;
        mutation = metabypass::Write(fs_.get(), m_.backup_dir + "/work/CURRENT",
                                     "MANIFEST-not-the-captured-file\n");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "second", "value"));
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_TRUE(injected);
  ASSERT_OK(mutation);
  ASSERT_OK(db_->Close());
  db_.reset();
  Restore();
  Check("first", "value");
  Check("second", "value");
}
TEST_F(MetaBypassTest, MirrorFailureWhileValidatingDoesNotPublish) {
  m_.batch_bytes = 1;
  ASSERT_NO_FATAL_FAILURE(Open());
  std::string before;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &before));
  TestGate gate;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) { gate.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "first", "value"));
  std::thread sync([&] { EXPECT_TRUE(db_->SyncBackup().IsIOError()); });
  gate.WaitUntilBlocked();
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("mirror failed during validation");
      });
  EXPECT_OK(db_->Put(WriteOptions(), "second", "value"));
  EXPECT_TRUE(db_->SyncBackup().IsIOError());
  gate.Release();
  sync.join();
  EXPECT_TRUE(db_->Close().IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &after));
  ASSERT_EQ(before, after);
}
TEST_F(MetaBypassTest,
       NativeCheckpointDoesNotFlushMemtableOrScanBusinessRecords) {
  o_.write_buffer_size = 4 * 1024 * 1024;
  o_.avoid_flush_during_shutdown = true;
  Open();
  for (int i = 0; i < 100; ++i)
    ASSERT_OK(
        db_->Put(WriteOptions(), std::to_string(i), std::string(1024, 'v')));
  ASSERT_OK(db_->Put(WriteOptions(), "large", std::string(200000, 'L')));
  ASSERT_OK(db_->Put(WriteOptions(), "", ""));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_TRUE(FilesOfType(index_, kTableFile).empty());
  ASSERT_TRUE(BlobFiles().empty());
  ASSERT_TRUE(FilesOfType(PublishedPoint(), kTableFile).empty());
  ASSERT_FALSE(FilesOfType(PublishedPoint(), kWalFile).empty());
  ASSERT_OK(db_->Close());
  db_.reset();
  Restore();
  Check("0", std::string(1024, 'v'));
  Check("large", std::string(200000, 'L'));
  Check("", "");
}
TEST_F(MetaBypassTest, NativeRecoveryDetectsCorruptPublishedWal) {
  o_.write_buffer_size = 4 * 1024 * 1024;
  o_.avoid_flush_during_shutdown = true;
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  const std::string point = PublishedPoint();
  const auto logs = FilesOfType(point, kWalFile);
  ASSERT_FALSE(logs.empty());
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), point + "/" + logs.front(), &bytes));
  ASSERT_FALSE(bytes.empty());
  bytes.back() ^= 1;
  ASSERT_OK(metabypass::Write(fs_.get(), point + "/" + logs.front(), bytes));
  Clean(index_);
  // Restore's contract trusts the published checkpoint. Exercise the native
  // WAL CRC reader directly, without reinstating a checkpoint hash pass.
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), index_));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(point, IOOptions(), &files, nullptr));
  for (const auto& file : files) {
    if (file == "." || file == "..") continue;
    uint64_t size;
    ASSERT_OK(
        fs_->GetFileSize(point + "/" + file, IOOptions(), &size, nullptr));
    ASSERT_OK(metabypass::Copy(fs_.get(), point + "/" + file,
                               index_ + "/" + file, size));
  }
  Options native_options = o_;
  native_options.enable_blob_files = false;
  native_options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  std::unique_ptr<DB> native;
  ASSERT_TRUE(DB::Open(native_options, index_, &native).IsCorruption());
}
TEST_F(MetaBypassTest, BelowThresholdDrainsOnStopWithoutNotifications) {
  NotificationScenario(false, false, false);
}
TEST_F(MetaBypassTest, BelowThresholdDrainsOnTimer) {
  NotificationScenario(false, false, true);
}
TEST_F(MetaBypassTest, BackpressureDrainsBelowBatchThreshold) {
  NotificationScenario(true, false, false);
}
TEST_F(MetaBypassTest, BelowThresholdFailureWakesReservation) {
  NotificationScenario(true, true, false);
}
TEST_F(MetaBypassTest, CloseDrainsWithValidatorBlocked) {
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  TestGate validating, closing;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) { validating.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                        [&](void*) { closing.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread close([&] { EXPECT_OK(db_->Close()); });
  closing.WaitUntilBlocked();
  closing.Release();
  validating.WaitUntilBlocked();
  validating.Release();
  close.join();
  SyncPoint::GetInstance()->DisableProcessing();
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "value");
}
TEST_F(MetaBypassTest, AllSyncWaitersWakeOnPublicationOrFailure) {
  m_.batch_bytes = m_.wal_queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  for (bool fail : {false, true}) {
    TestGate validating, waiters;
    std::atomic<int> waiting{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                          [&](void*) { validating.Block(); });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::SyncWaiting",
                                          [&](void*) {
                                            if (++waiting == 4) waiters.Block();
                                          });
    if (fail) {
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::BeforePointerReplace", [](void* arg) {
            *static_cast<Status*>(arg) = Status::IOError("publication failure");
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->Put(WriteOptions(), "key", fail ? "new" : "old"));
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
      threads.emplace_back([&] {
        Status s = db_->SyncBackup();
        if (fail)
          EXPECT_TRUE(s.IsIOError());
        else
          EXPECT_OK(s);
      });
    }
    waiters.WaitUntilBlocked();
    waiters.Release();
    validating.WaitUntilBlocked();
    validating.Release();
    for (auto& thread : threads) thread.join();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  Check("key", "new");
  ASSERT_TRUE(db_->Close().IsIOError());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(RestorePrefix(1, 2));
  CheckContents({{"key", "new"}});
}

#ifndef NDEBUG
TEST_F(MetaBypassTest, WalPayloadSlicesSurviveRollover) {
  m_.batch_bytes = 1;
  m_.wal_queue_capacity = 256 * 1024;
  TestGate applying;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ApplySequence",
                                        [&](void* arg) {
                                          if (*static_cast<uint64_t*>(arg) == 3)
                                            applying.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  using Access = metabypass::BackupTestAccess;
  const std::string name = "000001.log";
  const size_t header = Access::EventHeaderCharge() + name.size();
  std::string expected;
  size_t charge = 0;
  auto append = [&](size_t size) {
    std::string source(size, static_cast<char>(expected.size() % 127));
    expected += source;
    EXPECT_OK(wal->Append(source, IOOptions(), nullptr));
    std::fill(source.begin(), source.end(), '!');
    charge += header + size;
  };
  append(32);
  applying.WaitUntilBlocked();
  const auto first = Access::CurrentWalBlock(backup.get());
  EXPECT_FALSE(first.expired());
  // The 32- and 992-byte slices plus 63 KiB exactly fill the first block.
  // The other three append sizes must not consume any block space.
  for (size_t size : {0, 31, 1025, 992}) append(size);
  for (size_t i = 0; i < 63; ++i) append(1024);
  const auto full = Access::CurrentWalBlock(backup.get());
  EXPECT_FALSE(first.owner_before(full) || full.owner_before(first));
  append(32);
  const auto current = Access::CurrentWalBlock(backup.get());
  EXPECT_TRUE(first.owner_before(current) || current.owner_before(first));
  EXPECT_FALSE(first.expired());
  auto stats = backup->Stats();
  EXPECT_OK(stats.error);
  EXPECT_EQ(stats.queued_bytes, charge);
  EXPECT_OK(wal->Close(IOOptions(), nullptr));
  EXPECT_OK(metadata->Close(IOOptions(), nullptr));
  applying.Release();
  EXPECT_OK(backup->Stop());
  SyncPoint::GetInstance()->DisableProcessing();
  EXPECT_TRUE(first.expired());
  stats = backup->Stats();
  EXPECT_OK(stats.error);
  EXPECT_EQ(stats.queued_bytes, 0U);
  EXPECT_EQ(stats.mirrored_wal_bytes, expected.size());
  std::string mirrored;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/" + name, &mirrored));
  EXPECT_EQ(mirrored, expected);
}
#endif

TEST_F(MetaBypassTest, WalPayloadPrimaryFailureReleasesReservation) {
  auto fault = std::make_shared<FaultInjectionTestFS>(fs_);
  fs_ = fault;
  m_.interval_ms = 60000;
  m_.batch_bytes = m_.wal_queue_capacity;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));

  fault->SetFilesystemActive(false, IOStatus::IOError("primary WAL append"));
  EXPECT_TRUE(
      wal->Append(std::string(512, 'x'), IOOptions(), nullptr).IsIOError());
  fault->SetFilesystemActive(true);
  EXPECT_TRUE(wal->Close(IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(metadata->Close(IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(backup->Stop().IsIOError());
  auto stats = backup->Stats();
  EXPECT_TRUE(stats.error.IsIOError());
  EXPECT_EQ(stats.queued_bytes, 0U);
  EXPECT_EQ(stats.mirrored_wal_bytes, 0U);
}

TEST_F(MetaBypassTest, WalCoalescingControlsAndRecreatedFile) {
  m_.batch_bytes = 1;
  TestGate applying;
  BlockFirstApply(&applying);
  size_t groups = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::WalAppendBatch",
                                        [&](void*) { ++groups; });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  EXPECT_OK(wal->Append("a", IOOptions(), nullptr));
  EXPECT_OK(wal->Append("b", IOOptions(), nullptr));
  EXPECT_OK(metadata->Append("metadata", IOOptions(), nullptr));
  EXPECT_OK(wal->Close(IOOptions(), nullptr));
  EXPECT_OK(backup->NewWritableFile(index_ + "/000001.log", FileOptions(), &wal,
                                    nullptr));
  EXPECT_OK(wal->Append("c", IOOptions(), nullptr));
  EXPECT_OK(wal->Append("d", IOOptions(), nullptr));
  EXPECT_OK(wal->Close(IOOptions(), nullptr));
  EXPECT_OK(metadata->Close(IOOptions(), nullptr));
  applying.Release();
  EXPECT_OK(backup->Stop());
  SyncPoint::GetInstance()->DisableProcessing();
  EXPECT_EQ(groups, 2U);
  std::string bytes;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log", &bytes));
  EXPECT_EQ(bytes, "cd");
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/MANIFEST-000002",
                             &bytes));
  EXPECT_EQ(bytes, "metadata");
}
TEST_F(MetaBypassTest, WalCoalescingBackpressureAndRecovery) {
  CoalescePressureScenario(false);
}
TEST_F(MetaBypassTest,
       WalCoalescingPartialFailureWakesWaitersAndRestoresOldPoint) {
  CoalescePressureScenario(true);
}
TEST_F(MetaBypassTest, WalCoalescingSelectionBoundaries) {
  using Input = metabypass::BackupTestAccess::Input;
  CoalesceSelector(std::vector<Input>(257), {256, 1});
  CoalesceSelector(
      std::vector<Input>(3, Input{"000001.log", std::string(32768, 'a')}),
      {2, 1});
  CoalesceSelector({Input{}, Input{"000001.log", ""}, Input{}}, {1, 1, 1});
  CoalesceSelector({Input{"000001.log", ""}, Input{}, Input{}}, {1, 2});
  CoalesceSelector(
      {Input{}, Input{"000001.log", std::string(65537, 'a')}, Input{}},
      {1, 1, 1});
  CoalesceSelector({Input{}, Input{"000002.log"}, Input{}}, {1, 1, 1});
  CoalesceSelector({Input{}, Input{"MANIFEST-000002"}, Input{}, Input{}},
                   {1, 1, 2});
  CoalesceSelector({Input{}, Input{"000001.log", "", 'c'}, Input{}, Input{}},
                   {1, 1, 2});
  CoalesceSelector(std::vector<Input>(6), {3}, 3);
  CoalesceSelector(std::vector<Input>(6), {3, 3}, UINT64_MAX, 3);
}
TEST_F(MetaBypassTest, WalBatchSelectionHonorsKnownCheckpointCutoff) {
  using Input = metabypass::BackupTestAccess::Input;
  CoalesceSelector(std::vector<Input>(520), {256, 3, 256, 5}, UINT64_MAX, 259);
}
TEST_F(MetaBypassTest, WalCoalescingContentAndLogicalHooks) {
  std::vector<uint64_t> sequences;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplySequence",
      [&](void* p) { sequences.push_back(*static_cast<uint64_t*>(p)); });
  SyncPoint::GetInstance()->EnableProcessing();
  CoalesceDrain(
      {{"000001.log", "ab"}, {"000001.log", "cd"}, {"000001.log", "ef"}}, false,
      3, {"abcdef"});
  EXPECT_EQ(sequences, (std::vector<uint64_t>{1, 2, 3}));
}
TEST_F(MetaBypassTest, WalCoalescingLogicalFailuresPreservePrefix) {
  constexpr size_t kEvents = 256;
  const std::vector<metabypass::BackupTestAccess::Input> input(kEvents);
  for (size_t fail_at : {size_t{1}, kEvents / 2, kEvents}) {
    SCOPED_TRACE(fail_at);
    size_t seen = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::ApplyStatus", [&](void* p) {
          if (++seen == fail_at)
            *static_cast<Status*>(p) = Status::IOError("logical event fault");
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::vector<std::string> calls;
    if (fail_at > 1) calls.push_back(std::string(fail_at - 1, 'x'));
    CoalesceDrain(input, false, fail_at - 1, calls);
    EXPECT_EQ(seen, fail_at);
    SyncPoint::GetInstance()->DisableProcessing();
  }
}
TEST_F(MetaBypassTest, WalCoalescingPrefixIOErrorPrecedesLogicalFailure) {
  size_t seen = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [&](void* p) {
        if (++seen == 3)
          *static_cast<Status*>(p) = Status::IOError("later logical fault");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  CoalesceDrain(std::vector<metabypass::BackupTestAccess::Input>(4), true, 0,
                {"xx"});
  EXPECT_EQ(seen, 3U);
}
TEST_F(MetaBypassTest, WalCoalescingPartialWriteIsNotAcknowledgedOrRetried) {
  SyncPoint::GetInstance()->EnableProcessing();
  CoalesceDrain(std::vector<metabypass::BackupTestAccess::Input>(257), true, 0,
                {std::string(256, 'x')});
}

TEST_F(MetaBypassTest, ChannelMetadataOnlyDrainsOnTimer) {
  NotificationScenario(false, false, true, true);
}
TEST_F(MetaBypassTest, ChannelWalPassesBlockedPrimaryMetadata) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 8192;
  m_.batch_bytes = 1;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  TestGate primary;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::PrimaryReserved", [&](void* arg) {
        if (*static_cast<std::string*>(arg) == "MANIFEST-000002")
          primary.Block();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread writer(
      [&] { EXPECT_OK(metadata->Append("metadata", IOOptions(), nullptr)); });
  primary.WaitUntilBlocked();
  // Must return while the metadata operation still owns its primary-operation
  // lock.
  ASSERT_OK(wal->Append("wal", IOOptions(), nullptr));
  primary.Release();
  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
}
TEST_F(MetaBypassTest, ChannelWalPassesMetadataCapacityWait) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure",
                                        [&](void*) { blocked.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  ASSERT_OK(metadata->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread writer([&] {
    EXPECT_OK(metadata->Append(std::string(3000, 'b'), IOOptions(), nullptr));
  });
  blocked.WaitUntilBlocked();
  blocked.Release();
  ASSERT_OK(wal->Append(std::string(500, 'w'), IOOptions(), nullptr));
  applying.Release();
  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  auto stats = backup->Stats();
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_queued_bytes, QueueBudget(m_));
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/MANIFEST-000002",
                             &bytes));
  ASSERT_EQ(bytes, std::string(6000, 'a') + std::string(3000, 'b'));
}
TEST_F(MetaBypassTest, ChannelBothCapacityWaitersWakeOnFailure) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  std::atomic<int> waiters{0};
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure", [&](void*) {
    if (++waiters == 2) blocked.Block();
  });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("channel mirror failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  ASSERT_OK(wal->Append(std::string(6000, 'w'), IOOptions(), nullptr));
  ASSERT_OK(metadata->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread meta_writer([&] {
    EXPECT_TRUE(metadata->Append(std::string(3000, 'b'), IOOptions(), nullptr)
                    .IsIOError());
  });
  std::thread wal_writer([&] {
    EXPECT_TRUE(
        wal->Append(std::string(3000, 'w'), IOOptions(), nullptr).IsIOError());
  });
  blocked.WaitUntilBlocked();
  blocked.Release();
  applying.Release();
  meta_writer.join();
  wal_writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_TRUE(metadata->Close(IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(wal->Close(IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(backup->Stop().IsIOError());
}

TEST_F(MetaBypassTest, ChannelMergeOrdersControlOperations) {
  m_.batch_bytes = 1;
  TestGate applying;
  std::vector<uint64_t> sequence;
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplySequence",
      [&](void* arg) { sequence.push_back(*static_cast<uint64_t*>(arg)); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata, renamed, next;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  ASSERT_OK(wal->Append("w1", IOOptions(), nullptr));
  ASSERT_OK(metadata->Append("s1", IOOptions(), nullptr));
  ASSERT_OK(metadata->Append("s2", IOOptions(), nullptr));
  ASSERT_OK(wal->Append("w2", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->RenameFile(index_ + "/000001.log",
                               index_ + "/MANIFEST-000003", IOOptions(),
                               nullptr));
  ASSERT_OK(backup->ReopenWritableFile(index_ + "/MANIFEST-000003",
                                       FileOptions(), &renamed, nullptr));
  ASSERT_OK(renamed->Append("w3", IOOptions(), nullptr));
  ASSERT_OK(renamed->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->RenameFile(index_ + "/MANIFEST-000002",
                               index_ + "/MANIFEST-000004", IOOptions(),
                               nullptr));
  ASSERT_OK(
      backup->DeleteFile(index_ + "/MANIFEST-000004", IOOptions(), nullptr));
  ASSERT_OK(backup->NewWritableFile(index_ + "/000005.log", FileOptions(),
                                    &next, nullptr));
  ASSERT_OK(next->Append("w4", IOOptions(), nullptr));
  ASSERT_OK(next->Close(IOOptions(), nullptr));
  applying.Release();
  ASSERT_OK(backup->Stop());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(sequence.size(), 16U);
  for (size_t i = 0; i < sequence.size(); ++i) ASSERT_EQ(sequence[i], i + 1);
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/MANIFEST-000003",
                             &bytes));
  ASSERT_EQ(bytes, "w1w2w3");
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000005.log", &bytes));
  ASSERT_EQ(bytes, "w4");
  ASSERT_TRUE(fs_->FileExists(m_.backup_dir + "/work/MANIFEST-000004",
                              IOOptions(), nullptr)
                  .IsNotFound());
}
TEST_F(MetaBypassTest, ChannelFailureRetainsOutstandingReservation) {
  OutstandingReservationScenario(false);
}
TEST_F(MetaBypassTest, ChannelWaitersWakeOnTheirOwnCapacityRelease) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked, applying_metadata;
  std::atomic<int> waiters{0};
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure", [&](void*) {
    if (++waiters == 2) blocked.Block();
  });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ApplySequence",
                                        [&](void* arg) {
                                          if (*static_cast<uint64_t*>(arg) == 4)
                                            applying_metadata.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  ASSERT_OK(wal->Append(std::string(6000, 'v'), IOOptions(), nullptr));
  ASSERT_OK(metadata->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  auto meta_writer = std::async(std::launch::async, [&] {
    return metadata->Append(std::string(6000, 'b'), IOOptions(), nullptr);
  });
  auto wal_writer = std::async(std::launch::async, [&] {
    return wal->Append(std::string(6000, 'w'), IOOptions(), nullptr);
  });
  blocked.WaitUntilBlocked();
  blocked.Release();
  applying.Release();
  // The WAL append has released space when the mirror starts applying the
  // metadata append. The metadata reservation is still charged.
  EXPECT_TRUE(applying_metadata.WaitUntilBlockedFor(std::chrono::seconds(10)));
  EXPECT_EQ(wal_writer.wait_for(std::chrono::seconds(10)),
            std::future_status::ready);
  EXPECT_EQ(meta_writer.wait_for(std::chrono::seconds(0)),
            std::future_status::timeout);
  applying_metadata.Release();
  EXPECT_OK(meta_writer.get());
  EXPECT_OK(wal_writer.get());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  auto stats = backup->Stats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.queued_bytes, 0U);
  ASSERT_LE(stats.peak_queued_bytes, QueueBudget(m_));
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/MANIFEST-000002",
                             &bytes));
  ASSERT_EQ(bytes, std::string(6000, 'a') + std::string(6000, 'b'));
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log", &bytes));
  ASSERT_EQ(bytes, std::string(6000, 'v') + std::string(6000, 'w'));
}

TEST_F(MetaBypassTest, ChannelStopWakesBothCapacityWaiters) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  std::atomic<int> waiters{0};
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure", [&](void*) {
    if (++waiters == 2) blocked.Block();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  ASSERT_OK(wal->Append(std::string(6000, 'w'), IOOptions(), nullptr));
  ASSERT_OK(metadata->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread meta_writer([&] {
    EXPECT_TRUE(metadata->Append(std::string(3000, 'b'), IOOptions(), nullptr)
                    .IsIOError());
  });
  std::thread wal_writer([&] {
    EXPECT_TRUE(
        wal->Append(std::string(3000, 'w'), IOOptions(), nullptr).IsIOError());
  });
  blocked.WaitUntilBlocked();
  blocked.Release();
  std::thread stop([&] { EXPECT_OK(backup->Stop()); });
  // Neither waiter requires mirror I/O to resume when shutdown begins.
  meta_writer.join();
  wal_writer.join();
  applying.Release();
  stop.join();
  ASSERT_TRUE(metadata->Close(IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(wal->Close(IOOptions(), nullptr).IsIOError());
  auto stats = backup->Stats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.queued_bytes, 0U);
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(MetaBypassTest, QueueCapacityAndOrdering) { QueueScenario(false); }
TEST_F(MetaBypassTest, MirrorFailureWakesBlockedProducer) {
  QueueScenario(true);
}

#ifndef NDEBUG
TEST_F(MetaBypassTest, WalSpscAppendPassesHeldQueueMutex) {
  m_.wal_queue_capacity = 4096;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  bool reserved = false;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("MetaBypass::PrimaryReserved", [&](void* arg) {
    reserved = *static_cast<std::string*>(arg) == "000001.log";
  });
  sync->EnableProcessing();
  auto lock = metabypass::BackupTestAccess::LockQueueMutex(backup.get());
  auto writer = std::async(std::launch::async, [&] {
    return wal->Append("ordinary WAL append", IOOptions(), nullptr);
  });
  const auto completed = writer.wait_for(std::chrono::seconds(10));
  lock.unlock();
  EXPECT_EQ(completed, std::future_status::ready);
  ASSERT_OK(writer.get());
  EXPECT_TRUE(reserved);
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
}

TEST_F(MetaBypassTest, WalSpscMetadataAnchorAndCrossChannelRename) {
  m_.wal_queue_capacity = 4096;
  m_.batch_bytes = 1;
  TestGate applying, anchor;
  std::vector<std::array<uint64_t, 3>> batches;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    applying.Release();
    anchor.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  BlockFirstApply(&applying);
  sync->SetCallBack("MetaBypass::WalAppendBatch", [&](void* arg) {
    batches.push_back(*static_cast<std::array<uint64_t, 3>*>(arg));
  });
  sync->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  ASSERT_TRUE(applying.WaitUntilBlockedFor(std::chrono::seconds(10)));
  ASSERT_OK(wal->Append("w1", IOOptions(), nullptr));
  sync->SetCallBack("MetaBypass::MetadataSequenceAssigned",
                    [&](void*) { anchor.Block(); });
  auto metadata_writer = std::async(std::launch::async, [&] {
    return metadata->Append("meta", IOOptions(), nullptr);
  });
  EXPECT_TRUE(anchor.WaitUntilBlockedFor(std::chrono::seconds(10)));
  auto wal_writer = std::async(std::launch::async, [&] {
    return wal->Append("w2", IOOptions(), nullptr);
  });
  const auto completed = wal_writer.wait_for(std::chrono::seconds(10));
  anchor.Release();
  EXPECT_EQ(completed, std::future_status::ready);
  EXPECT_OK(wal_writer.get());
  EXPECT_OK(metadata_writer.get());
  ASSERT_OK(wal->Append("w3", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));

  const std::string log = index_ + "/000001.log";
  const std::string manifest = index_ + "/MANIFEST-000003";
  const std::string final_log = index_ + "/000004.log";
  ASSERT_OK(backup->RenameFile(log, manifest, IOOptions(), nullptr));
  ASSERT_OK(backup->ReopenWritableFile(manifest, FileOptions(), &wal, nullptr));
  ASSERT_OK(wal->Append("m4", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->RenameFile(manifest, final_log, IOOptions(), nullptr));
  ASSERT_OK(
      backup->ReopenWritableFile(final_log, FileOptions(), &wal, nullptr));
  ASSERT_OK(wal->Append("w5", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));

  applying.Release();
  ASSERT_OK(backup->Stop());
  // The metadata anchor splits w1 from w2 and w3, even though w2 was
  // published before the metadata event was enqueued.
  const std::vector<std::array<uint64_t, 3>> expected_batches{{{5, 2, 4}}};
  EXPECT_EQ(batches, expected_batches);
  const std::string work = m_.backup_dir + "/work/";
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), work + "000004.log", &bytes));
  EXPECT_EQ(bytes, "w1w2w3m4w5");
  ASSERT_OK(metabypass::Read(fs_.get(), work + "MANIFEST-000002", &bytes));
  EXPECT_EQ(bytes, "meta");
  EXPECT_TRUE(
      fs_->FileExists(work + "000001.log", IOOptions(), nullptr).IsNotFound());
  EXPECT_TRUE(fs_->FileExists(work + "MANIFEST-000003", IOOptions(), nullptr)
                  .IsNotFound());
}

TEST_F(MetaBypassTest, WalBatchSelectionAllowsNewMetadataAndWalPublication) {
  m_.batch_bytes = 1;
  TestGate applying, selecting;
  std::vector<uint64_t> sequences;
  std::vector<std::array<uint64_t, 3>> batches;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    applying.Release();
    selecting.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  BlockFirstApply(&applying);
  sync->SetCallBack("MetaBypass::WalBatchSelected", [&](void* arg) {
    const auto& range = *static_cast<std::array<uint64_t, 2>*>(arg);
    if (range[0] == 3) {
      EXPECT_EQ(range[1], 4U);
      selecting.Block();
    }
  });
  sync->SetCallBack("MetaBypass::ApplySequence", [&](void* arg) {
    sequences.push_back(*static_cast<uint64_t*>(arg));
  });
  sync->SetCallBack("MetaBypass::WalAppendBatch", [&](void* arg) {
    batches.push_back(*static_cast<std::array<uint64_t, 3>*>(arg));
  });
  sync->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  ASSERT_TRUE(applying.WaitUntilBlockedFor(std::chrono::seconds(10)));
  ASSERT_OK(wal->Append("w1", IOOptions(), nullptr));
  ASSERT_OK(wal->Append("w2", IOOptions(), nullptr));
  applying.Release();
  ASSERT_TRUE(selecting.WaitUntilBlockedFor(std::chrono::seconds(10)));
  auto writer = std::async(std::launch::async, [&] {
    auto status = metadata->Append("meta", IOOptions(), nullptr);
    if (status.ok()) status = wal->Append("w3", IOOptions(), nullptr);
    return status;
  });
  // Both operations must publish before the mirror resumes selecting from its
  // earlier WAL range. The in-flight range cannot absorb either operation.
  const auto completed = writer.wait_for(std::chrono::seconds(10));
  selecting.Release();
  EXPECT_EQ(completed, std::future_status::ready);
  ASSERT_OK(writer.get());
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  const std::vector<std::array<uint64_t, 3>> expected_batches{{{3, 2, 4}}};
  EXPECT_EQ(batches, expected_batches);
  EXPECT_EQ(sequences, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6, 7, 8}));
  const std::string work = m_.backup_dir + "/work/";
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), work + "000001.log", &bytes));
  EXPECT_EQ(bytes, "w1w2w3");
  ASSERT_OK(metabypass::Read(fs_.get(), work + "MANIFEST-000002", &bytes));
  EXPECT_EQ(bytes, "meta");
}

TEST_F(MetaBypassTest, WalSpscCapacityWaiterWakesAfterDrain) {
  m_.wal_queue_capacity = m_.metadata_queue_capacity = 4096;
  m_.batch_bytes = QueueBudget(m_);
  m_.interval_ms = 60000;
  TestGate sleeping, pressure;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    sleeping.Release();
    pressure.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("MetaBypass::BeforeSpscMirrorSleep",
                    [&](void*) { sleeping.Block(); });
  sync->SetCallBack("MetaBypass::Backpressure",
                    [&](void*) { pressure.Block(); });
  sync->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
  ASSERT_TRUE(sleeping.WaitUntilBlockedFor(std::chrono::seconds(10)));

  const std::string first(2000, 'a'), second(3000, 'b');
  auto writer = std::async(std::launch::async, [&] {
    IOStatus s = backup->NewWritableFile(index_ + "/000001.log", FileOptions(),
                                         &wal, nullptr);
    if (s.ok()) s = wal->Append(first, IOOptions(), nullptr);
    if (s.ok()) s = wal->Append(second, IOOptions(), nullptr);
    return s;
  });
  // Send the full-queue wake after the mirror checks readiness and before it
  // starts sleeping. The queued bytes are below the batch threshold.
  EXPECT_TRUE(pressure.WaitUntilBlockedFor(std::chrono::seconds(10)));
  pressure.Release();
  sleeping.Release();
  const auto completed = writer.wait_for(std::chrono::seconds(10));
  EXPECT_EQ(completed, std::future_status::ready);
  if (completed != std::future_status::ready) {
    EXPECT_OK(backup->Stop());
  }
  ASSERT_OK(writer.get());
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  const auto stats = backup->Stats();
  EXPECT_OK(stats.error);
  EXPECT_LE(stats.peak_queued_bytes, m_.wal_queue_capacity);
  EXPECT_EQ(stats.queued_bytes, 0U);
  std::string bytes;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log", &bytes));
  EXPECT_EQ(bytes, first + second);
}

TEST_F(MetaBypassTest, WalSpscFailureRetainsOutstandingReservation) {
  OutstandingReservationScenario(true);
}

TEST_F(MetaBypassTest, WalSpscStopDrainsBelowThresholdAcrossSegments) {
  m_.wal_queue_capacity = 256 * 1024;
  m_.batch_bytes = QueueBudget(m_);
  m_.interval_ms = 60000;
  std::vector<std::array<uint64_t, 3>> batches;
  auto* sync = SyncPoint::GetInstance();
  sync->SetCallBack("MetaBypass::WalAppendBatch", [&](void* arg) {
    batches.push_back(*static_cast<std::array<uint64_t, 3>*>(arg));
  });
  sync->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, metadata;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  std::string expected;
  for (int i = 0; i < 600; ++i) {
    const std::string payload(32, 'a' + i % 26);
    ASSERT_OK(wal->Append(payload, IOOptions(), nullptr));
    expected += payload;
  }
  ASSERT_OK(metadata->Append("meta", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  const auto before = backup->Stats();
  EXPECT_OK(before.error);
  EXPECT_EQ(before.mirrored_wal_bytes, 0U);
  EXPECT_GT(before.queued_bytes, expected.size());
  ASSERT_OK(backup->Stop());
  const auto after = backup->Stats();
  EXPECT_OK(after.error);
  EXPECT_EQ(after.queued_bytes, 0U);
  EXPECT_EQ(after.mirrored_wal_bytes, expected.size());
  sync->DisableProcessing();
  // The create event occupies the first slot. The full append batches must
  // therefore run across segment boundaries without an extra physical write.
  const std::vector<std::array<uint64_t, 3>> expected_batches{
      {3, 256, 8192}, {259, 256, 8192}, {515, 88, 2816}};
  EXPECT_EQ(batches, expected_batches);
  const std::string work = m_.backup_dir + "/work/";
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), work + "000001.log", &bytes));
  EXPECT_EQ(bytes, expected);
  ASSERT_OK(metabypass::Read(fs_.get(), work + "MANIFEST-000002", &bytes));
  EXPECT_EQ(bytes, "meta");
}
#endif

TEST_F(MetaBypassTest, PublicationFailurePreservesReadsAndPreviousPoint) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "a", "old"));
  ASSERT_OK(db_->SyncBackup());
  std::string before;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &before));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforePointerReplace", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("injected publication failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "new"));
  ASSERT_TRUE(db_->SyncBackup().IsIOError());
  Check("a", "new");
  ASSERT_TRUE(db_->Put(WriteOptions(), "b", "rejected").IsIOError());
  ASSERT_TRUE(db_->Close().IsIOError());
  db_.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &after));
  ASSERT_EQ(before, after);
  ASSERT_NO_FATAL_FAILURE(RestorePrefix(1, 2));
  CheckContents({{"a", "new"}});
}
TEST_F(MetaBypassTest, PublishedCorruptionRejected) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "x", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  const std::string point = pointer.substr(0, pointer.find('\n'));
  const auto manifests =
      FilesOfType(m_.backup_dir + "/" + point, kDescriptorFile);
  ASSERT_EQ(manifests.size(), 1U);
  ASSERT_OK(metabypass::Write(
      fs_.get(), m_.backup_dir + "/" + point + "/" + manifests.front(),
      "corrupt"));
  Clean(index_);
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsCorruption());
}
TEST_F(MetaBypassTest, NativeBlobReadDetectsPayloadCorruption) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "x", std::string(4096, 'v')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Close());
  db_.reset();
  const auto files = BlobFiles();
  ASSERT_EQ(files.size(), 1U);
  const std::string path = m_.data_dir + "/" + *files.begin();
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), path, &bytes));
  ASSERT_GT(bytes.size(), 128U);
  bytes[bytes.size() / 2] ^= 1;
  ASSERT_OK(metabypass::Write(fs_.get(), path, bytes));
  // Verification belongs to the native BlobDB reader rather than a backup
  // scanner. Opening the existing DB never repairs or seals the damaged file.
  Open();
  std::string value;
  ReadOptions read;
  read.verify_checksums = true;
  ASSERT_TRUE(db_->Get(read, "x", &value).IsCorruption());
}
TEST_F(MetaBypassTest, RejectLegacyInventoryWithoutChangingPublishedFiles) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  const std::string inventory = PublishedPoint() + "/INVENTORY";
  std::string original;
  ASSERT_OK(metabypass::Read(fs_.get(), inventory, &original));
  ASSERT_EQ(original.substr(0, 4), "MBI6");
  std::string legacy = original;
  legacy.replace(0, 4, "MBI5");
  ASSERT_OK(metabypass::Write(fs_.get(), inventory, legacy));
  std::string pointer;
  const std::string latest = m_.backup_dir + "/LATEST";
  ASSERT_OK(metabypass::Read(fs_.get(), latest, &pointer));
  const auto before_open = SnapshotTree(root_);
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsNotSupported());
  ASSERT_FALSE(db_);
  ASSERT_EQ(SnapshotTree(root_), before_open);
  Clean(index_);
  const auto before = SnapshotTree(root_);
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsNotSupported());
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), inventory, &after));
  ASSERT_EQ(after, legacy);
  ASSERT_EQ(SnapshotTree(root_), before);
}
TEST_F(MetaBypassTest, FragmentedWalBatchIsAtomic) {
  ASSERT_NO_FATAL_FAILURE(Open());
  WriteBatch batch;
  // Large keys make the transformed WAL span several physical log blocks.
  const std::string key(40000, 'k');
  ASSERT_OK(batch.Put(key + "a", "first"));
  ASSERT_OK(batch.Put(key + "b", "second"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check(key + "a", "first");
  Check(key + "b", "second");
}
#ifndef OS_WIN
TEST_F(MetaBypassTest, SigkillPublicationStages) {
  for (const std::string stage :
       {"MetaBypass::CandidateCaptured", "MetaBypass::DependenciesSynced",
        "MetaBypass::CandidateSynced", "MetaBypass::BeforePointerReplace",
        "MetaBypass::PointerSynced"}) {
    SCOPED_TRACE(stage);
    ASSERT_NO_FATAL_FAILURE(RunCrashWriter(stage));
    ASSERT_NO_FATAL_FAILURE(
        RestorePrefix(stage == "MetaBypass::PointerSynced" ? 5 : 1, 5));
    CheckContents({{"a", "last"}, {"b", "batch"}});
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, SigkillDuringSstCopyRestoresAfterSsdLoss) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter("sst-copy"));
  // Restore deletes the complete native directory. Only the old published
  // point and the continuously mirrored WAL survive the interrupted copy.
  ASSERT_NO_FATAL_FAILURE(RestorePrefix(1, 5));
  CheckContents({{"a", "last"}, {"b", "batch"}});
}
TEST_F(MetaBypassTest, SigkillDuringRetiredPointCleanup) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter("MetaBypass::GCFileDeleted"));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  int points = 0;
  for (const auto& file : files)
    if (file.compare(0, 6, "point-") == 0) ++points;
  ASSERT_EQ(points, 2);
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  ASSERT_FALSE(pointer.empty());
  ASSERT_EQ(pointer.back(), '\n');
  ASSERT_EQ(pointer.find('\n'), pointer.size() - 1);
  ASSERT_EQ(pointer.find(' '), std::string::npos);
  const std::string current = pointer.substr(0, pointer.size() - 1);
  ASSERT_NE(current, "point-1");
  ASSERT_NE(std::find(files.begin(), files.end(), current), files.end());
  ASSERT_NE(std::find(files.begin(), files.end(), "point-1"), files.end());
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "last");
  Check("b", "batch");
  std::string deleted;
  ASSERT_TRUE(db_->Get(ReadOptions(), "deleted", &deleted).IsNotFound());
  ASSERT_OK(db_->Close());
  db_.reset();
  files.clear();
  ASSERT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  points = 0;
  for (const auto& file : files)
    if (file.compare(0, 6, "point-") == 0) ++points;
  EXPECT_EQ(points, 1);
}
TEST_F(MetaBypassTest, InterruptedNativeFileRestoreIsRetryable) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter());
  Clean(index_);
  auto failing = std::make_shared<MarkerFailureFileSystem>(fs_);
  const auto logs = FilesOfType(PublishedPoint(), kWalFile);
  ASSERT_FALSE(logs.empty());
  failing->fail_path = index_ + "/" + logs.front();
  auto env = NewCompositeEnv(failing);
  Options options = o_;
  options.env = env.get();
  ASSERT_FALSE(MetaBypassDB::Restore(options, m_, index_).ok());
  ASSERT_GT(failing->failures, 0);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("a", "last");
  Check("b", "batch");
}
TEST_F(MetaBypassTest, SigkillUnflushedBatchRecovery) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter());
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "last");
  Check("b", "batch");
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "deleted", &value).IsNotFound());
  ASSERT_OK(db_->Put(WriteOptions(), "after", "recovery"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Open());
  Check("after", "recovery");
}
#endif
}  // namespace
namespace metabypass {
class TieredStorageTest : public MetaBypassTest {
 public:
  // Probe the physical boundary without involving staging or DB metadata I/O.
  class ObserverProbe : public FileSystemWrapper {
   public:
    enum Op {
      kWrite,
      kFlush,
      kSync,
      kFsync,
      kRead,
      kDirectory,
      kSyncFile,
      kCount
    };
    explicit ObserverProbe(std::shared_ptr<FileSystem> fs)
        : FileSystemWrapper(std::move(fs)) {}
    const char* Name() const override { return "ObserverProbe"; }
    std::array<uint64_t, kCount> calls{};
    bool fail = false;
    std::function<void(Op)> before;
    IOStatus Call(Op op) {
      if (before) before(op);
      ++calls[op];
      return fail ? IOStatus::IOError("observer probe failure")
                  : IOStatus::OK();
    }
    IOStatus Get(size_t n, Slice* out) {
      IOStatus s = Call(kRead);
      *out = s.ok() ? Slice("abc", std::min(n, size_t{3})) : Slice();
      return s;
    }
    IOStatus NewWritableFile(const std::string&, const FileOptions&,
                             std::unique_ptr<FSWritableFile>* out,
                             IODebugContext*) override {
      out->reset(new Writer(this));
      return IOStatus::OK();
    }
    IOStatus ReopenWritableFile(const std::string& p, const FileOptions& o,
                                std::unique_ptr<FSWritableFile>* out,
                                IODebugContext* d) override {
      return NewWritableFile(p, o, out, d);
    }
    IOStatus ReuseWritableFile(const std::string& p, const std::string&,
                               const FileOptions& o,
                               std::unique_ptr<FSWritableFile>* out,
                               IODebugContext* d) override {
      return NewWritableFile(p, o, out, d);
    }
    IOStatus NewRandomRWFile(const std::string&, const FileOptions&,
                             std::unique_ptr<FSRandomRWFile>* out,
                             IODebugContext*) override {
      out->reset(new RandomRW(this));
      return IOStatus::OK();
    }
    IOStatus NewRandomAccessFile(const std::string&, const FileOptions&,
                                 std::unique_ptr<FSRandomAccessFile>* out,
                                 IODebugContext*) override {
      out->reset(new Reader(this));
      return IOStatus::OK();
    }
    IOStatus NewSequentialFile(const std::string&, const FileOptions&,
                               std::unique_ptr<FSSequentialFile>* out,
                               IODebugContext*) override {
      out->reset(new Sequential(this));
      return IOStatus::OK();
    }
    IOStatus NewDirectory(const std::string&, const IOOptions&,
                          std::unique_ptr<FSDirectory>* out,
                          IODebugContext*) override {
      out->reset(new Directory(this));
      return IOStatus::OK();
    }
    IOStatus SyncFile(const std::string&, const FileOptions&, const IOOptions&,
                      bool, IODebugContext*) override {
      return Call(kSyncFile);
    }

   private:
    class Writer : public FSWritableFile {
     public:
      explicit Writer(ObserverProbe* p) : probe_(p) {}
      IOStatus Append(const Slice&, const IOOptions&,
                      IODebugContext*) override {
        return probe_->Call(kWrite);
      }
      IOStatus Append(const Slice&, const IOOptions&,
                      const DataVerificationInfo&, IODebugContext*) override {
        return probe_->Call(kWrite);
      }
      IOStatus PositionedAppend(const Slice&, uint64_t, const IOOptions&,
                                IODebugContext*) override {
        return probe_->Call(kWrite);
      }
      IOStatus PositionedAppend(const Slice&, uint64_t, const IOOptions&,
                                const DataVerificationInfo&,
                                IODebugContext*) override {
        return probe_->Call(kWrite);
      }
      IOStatus Flush(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kFlush);
      }
      IOStatus Sync(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kSync);
      }
      IOStatus Fsync(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kFsync);
      }
      uint64_t GetFileSize(const IOOptions&, IODebugContext*) override {
        return 0;  // This probe records calls rather than storing file
                   // contents.
      }
      IOStatus Close(const IOOptions&, IODebugContext*) override {
        return IOStatus::OK();
      }

     private:
      ObserverProbe* probe_;
    };
    class RandomRW : public FSRandomRWFile {
     public:
      explicit RandomRW(ObserverProbe* p) : probe_(p) {}
      IOStatus Write(uint64_t, const Slice&, const IOOptions&,
                     IODebugContext*) override {
        return probe_->Call(kWrite);
      }
      IOStatus Read(uint64_t, size_t n, const IOOptions&, Slice* out, char*,
                    IODebugContext*) const override {
        return probe_->Get(n, out);
      }
      IOStatus Flush(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kFlush);
      }
      IOStatus Sync(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kSync);
      }
      IOStatus Fsync(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kFsync);
      }
      IOStatus Close(const IOOptions&, IODebugContext*) override {
        return IOStatus::OK();
      }

     private:
      ObserverProbe* probe_;
    };
    class Reader : public FSRandomAccessFile {
     public:
      explicit Reader(ObserverProbe* p) : probe_(p) {}
      IOStatus Read(uint64_t, size_t n, const IOOptions&, Slice* out, char*,
                    IODebugContext*) const override {
        return probe_->Get(n, out);
      }

     private:
      ObserverProbe* probe_;
    };
    class Sequential : public FSSequentialFile {
     public:
      explicit Sequential(ObserverProbe* p) : probe_(p) {}
      IOStatus Read(size_t n, const IOOptions&, Slice* out, char*,
                    IODebugContext*) override {
        return probe_->Get(n, out);
      }
      IOStatus Skip(uint64_t) override { return IOStatus::OK(); }

     private:
      ObserverProbe* probe_;
    };
    class Directory : public FSDirectory {
     public:
      explicit Directory(ObserverProbe* p) : probe_(p) {}
      IOStatus Fsync(const IOOptions&, IODebugContext*) override {
        return probe_->Call(kDirectory);
      }

     private:
      ObserverProbe* probe_;
    };
  };
  void WithObserverLock(HddPressure* pressure,
                        const std::function<void()>& op) {
    std::lock_guard<std::mutex> lock(pressure->mutex_);
    op();
    EXPECT_TRUE(pressure->active_.empty());
    EXPECT_EQ(pressure->next_, 0);
  }
  class CallerHddTrace : public FileSystemWrapper {
   public:
    CallerHddTrace(std::shared_ptr<FileSystem> fs, std::string dir)
        : FileSystemWrapper(std::move(fs)),
          dir_(std::move(dir)),
          caller_(std::this_thread::get_id()) {}
    const char* Name() const override { return "CallerHddTrace"; }
    std::atomic<uint64_t> writes{0}, listings{0}, stats{0};
    IOStatus GetChildren(const std::string& path, const IOOptions& o,
                         std::vector<std::string>* out,
                         IODebugContext* d) override {
      if (std::this_thread::get_id() == caller_) ++listings;
      return target()->GetChildren(path, o, out, d);
    }
    IOStatus GetFileSize(const std::string& path, const IOOptions& o,
                         uint64_t* size, IODebugContext* d) override {
      if (std::this_thread::get_id() == caller_) ++stats;
      return target()->GetFileSize(path, o, size, d);
    }
    IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                             std::unique_ptr<FSWritableFile>* out,
                             IODebugContext* d) override {
      Count(path);
      return target()->NewWritableFile(path, o, out, d);
    }
    IOStatus RenameFile(const std::string& from, const std::string& to,
                        const IOOptions& o, IODebugContext* d) override {
      Count(to);
      return target()->RenameFile(from, to, o, d);
    }
    IOStatus NewDirectory(const std::string& path, const IOOptions& o,
                          std::unique_ptr<FSDirectory>* out,
                          IODebugContext* d) override {
      Count(path);
      return target()->NewDirectory(path, o, out, d);
    }

   private:
    void Count(const std::string& path) {
      if (std::this_thread::get_id() == caller_ &&
          (path == dir_ || path.compare(0, dir_.size() + 1, dir_ + "/") == 0))
        ++writes;
    }
    const std::string dir_;
    const std::thread::id caller_;
  };
  class ExtentFailureFileSystem : public FileSystemWrapper {
   public:
    explicit ExtentFailureFileSystem(std::shared_ptr<FileSystem> fs)
        : FileSystemWrapper(std::move(fs)) {}
    const char* Name() const override { return "ExtentFailureFileSystem"; }
    std::atomic<bool> fail_fsync{false}, fail_close{false};
    std::atomic<uint64_t> closes{0}, flushes{0};
    IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                             std::unique_ptr<FSWritableFile>* out,
                             IODebugContext* d) override {
      IOStatus s = target()->NewWritableFile(path, o, out, d);
      if (s.ok() && path.find("/segment-") != std::string::npos)
        out->reset(new Writer(std::move(*out), this));
      return s;
    }

   private:
    class Writer : public FSWritableFileOwnerWrapper {
     public:
      Writer(std::unique_ptr<FSWritableFile> file, ExtentFailureFileSystem* fs)
          : FSWritableFileOwnerWrapper(std::move(file)), fs_(fs) {}
      IOStatus Flush(const IOOptions& o, IODebugContext* d) override {
        ++fs_->flushes;
        return target()->Flush(o, d);
      }
      IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
        TEST_SYNC_POINT("MetaBypassTest::ExtentFsyncInFlight");
        if (fs_->fail_fsync.exchange(false))
          return IOStatus::IOError("extent fsync failure");
        return target()->Fsync(o, d);
      }
      IOStatus Close(const IOOptions& o, IODebugContext* d) override {
        ++fs_->closes;
        if (fs_->fail_close.exchange(false))
          return IOStatus::IOError("extent close failure");
        return target()->Close(o, d);
      }

     private:
      ExtentFailureFileSystem* fs_;
    };
  };
  class LocalReclaimFailureFileSystem : public FileSystemWrapper {
   public:
    explicit LocalReclaimFailureFileSystem(std::shared_ptr<FileSystem> fs)
        : FileSystemWrapper(std::move(fs)) {}
    const char* Name() const override {
      return "LocalReclaimFailureFileSystem";
    }
    std::string local_path, staging_dir;
    bool fail_delete = false;
    std::atomic<bool> deleted{false};
    std::atomic<unsigned> delete_failures{0}, dirsync_failures{0};
    IOStatus DeleteFile(const std::string& path, const IOOptions& o,
                        IODebugContext* d) override {
      if (path == local_path && fail_delete) {
        ++delete_failures;
        return IOStatus::IOError("SSD source delete failure");
      }
      IOStatus s = target()->DeleteFile(path, o, d);
      if (s.ok() && path == local_path) deleted = true;
      return s;
    }
    IOStatus NewDirectory(const std::string& path, const IOOptions& o,
                          std::unique_ptr<FSDirectory>* out,
                          IODebugContext* d) override {
      IOStatus s = target()->NewDirectory(path, o, out, d);
      if (s.ok() && path == staging_dir)
        out->reset(new Directory(std::move(*out), this));
      return s;
    }

   private:
    class Directory : public FSDirectoryWrapper {
     public:
      Directory(std::unique_ptr<FSDirectory> dir,
                LocalReclaimFailureFileSystem* fs)
          : FSDirectoryWrapper(std::move(dir)), fs_(fs) {}
      IOStatus Fsync(const IOOptions& o, IODebugContext* d) override {
        if (fs_->deleted.load()) {
          ++fs_->dirsync_failures;
          return IOStatus::IOError("SSD source dirsync failure");
        }
        return FSDirectoryWrapper::Fsync(o, d);
      }

     private:
      LocalReclaimFailureFileSystem* fs_;
    };
  };
  class LocalReadFailureFileSystem : public FileSystemWrapper {
   public:
    explicit LocalReadFailureFileSystem(std::shared_ptr<FileSystem> fs)
        : FileSystemWrapper(std::move(fs)) {}
    const char* Name() const override { return "LocalReadFailureFileSystem"; }
    std::string local_path;
    std::atomic<bool> fail_read{true}, short_read{false}, fail_open{false};
    std::atomic<int> closed_handles{0};
    std::function<void()> on_close;
    IOStatus NewRandomAccessFile(const std::string& path, const FileOptions& o,
                                 std::unique_ptr<FSRandomAccessFile>* out,
                                 IODebugContext* d) override {
      if (path == local_path && fail_open.exchange(false))
        return IOStatus::IOError("injected open failure");
      IOStatus s = target()->NewRandomAccessFile(path, o, out, d);
      if (s.ok() && path == local_path)
        out->reset(new Reader(std::move(*out), this));
      return s;
    }

   private:
    class Reader : public FSRandomAccessFileWrapper {
     public:
      Reader(std::unique_ptr<FSRandomAccessFile> file,
             LocalReadFailureFileSystem* fs)
          : FSRandomAccessFileWrapper(file.get()),
            file_(std::move(file)),
            fs_(fs) {}
      ~Reader() override {
        // Destroy the backend before reporting a closed physical handle.
        file_.reset();
        if (fs_->on_close) fs_->on_close();
        ++fs_->closed_handles;
      }
      IOStatus Read(uint64_t offset, size_t n, const IOOptions& o, Slice* out,
                    char* scratch, IODebugContext* d) const override {
        if (fs_->fail_read.exchange(false))
          return IOStatus::IOError("injected local read failure");
        if (fs_->short_read.exchange(false)) {
          *out = Slice();
          return IOStatus::OK();
        }
        return target()->Read(offset, n, o, out, scratch, d);
      }

     private:
      std::unique_ptr<FSRandomAccessFile> file_;
      LocalReadFailureFileSystem* fs_;
    };
  };
  void PrepareAlwaysSpilled(std::unique_ptr<FSWritableFile>* writer) {
    Initialize(1);
    ASSERT_OK(CreateLocalBlob("s", writer));
    ASSERT_OK((*writer)->Append("h", IOOptions(), nullptr));
    ASSERT_EQ(AdaptiveStat(&MetaBypassStats::staging_spill_files), 1);
    // Finish the SSD prefix first so gates below refer only to suffix sealing.
    ASSERT_OK(Migrate(1));
    ASSERT_EQ(HddEnd(), 1);
  }
  void CheckAlwaysBlockedSeal(const char* hook) {
    auto physical = std::make_shared<ExtentFailureFileSystem>(fs_);
    fs_ = physical;
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_NO_FATAL_FAILURE(PrepareAlwaysSpilled(&writer));
    constexpr uint64_t extent = 32 * 1024 * 1024;
    TestGate seal;
    std::atomic<bool> first{true};
    SyncPoint::GetInstance()->SetCallBack(hook, [&](void*) {
      if (first.exchange(false)) seal.Block();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    const auto before = physical->flushes.load();
    ASSERT_OK(
        writer->Append(std::string(extent - 1, 'h'), IOOptions(), nullptr));
    seal.WaitUntilBlocked();
    // The boundary Append itself flushed every chunk on the old writer before
    // moving it. An outer Flush with no successor must not wait for HDD sync.
    EXPECT_EQ(physical->flushes.load() - before, 8);
    EXPECT_OK(writer->Flush(IOOptions(), nullptr));
    EXPECT_OK(writer->Append("tail", IOOptions(), nullptr));
    EXPECT_OK(writer->Flush(IOOptions(), nullptr));
    EXPECT_EQ(physical->flushes.load() - before, 10);
    EXPECT_EQ(HddEnd(), 1);
    std::string actual;
    EXPECT_OK(ReadBytes(0, 2, &actual));
    EXPECT_EQ(actual, "sh");
    EXPECT_OK(ReadBytes(extent - 1, 6, &actual));
    EXPECT_EQ(actual, "hhtail");
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 1);
    seal.Release();
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    ASSERT_OK(storage_->Persist({{1, extent + 5}}));
    ASSERT_OK(storage_->Stop());
  }
  void CheckAlwaysQueueWake(const std::string& reason) {
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_NO_FATAL_FAILURE(PrepareAlwaysSpilled(&writer));
    constexpr size_t extent = 32 * 1024 * 1024;
    TestGate inflight;
    std::atomic<bool> first{true};
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierBeforeHddExtentFsync", [&](void*) {
          if (first.exchange(false)) inflight.Block();
        });
    SyncPoint::GetInstance()->LoadDependency(
        {{"MetaBypass::TierSealQueueWaiting", "AlwaysQueue::Waiting"},
         {"MetaBypass::TierBlobRetiring", "AlwaysQueue::Retiring"},
         {"MetaBypass::TierStopping", "AlwaysQueue::Stopping"}});
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    ASSERT_OK(
        writer->Append(std::string(extent - 1, 'h'), IOOptions(), nullptr));
    inflight.WaitUntilBlocked();
    ASSERT_OK(writer->Append(std::string(extent, 'i'), IOOptions(), nullptr));
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 2);
    Status appended, drained;
    std::thread caller([&] {
      appended = writer->Append(std::string(extent, 'j'), IOOptions(), nullptr);
    });
    TEST_SYNC_POINT("AlwaysQueue::Waiting");
    uint64_t removed = 0;
    std::thread drain;
    if (reason == "error") {
      const Status first_error = Status::IOError("first seal error");
      const Status later_error = Status::IOError("later error");
      EXPECT_TRUE(first_error.IsIOError());
      EXPECT_TRUE(later_error.IsIOError());
      storage_->Cancel(first_error);
      storage_->Cancel(later_error);
    } else if (reason == "gc") {
      drain =
          std::thread([&] { drained = storage_->ReclaimBlob(1, &removed); });
      TEST_SYNC_POINT("AlwaysQueue::Retiring");
    } else {
      drain = std::thread([&] { drained = storage_->Stop(); });
      TEST_SYNC_POINT("AlwaysQueue::Stopping");
    }
    caller.join();  // Admission wakes while the physical worker is still held.
    if (reason == "gc") {
      EXPECT_TRUE(appended.IsAborted());
    } else {
      EXPECT_TRUE(appended.IsIOError());
      if (reason == "error") {
        EXPECT_NE(appended.ToString().find("first seal error"),
                  std::string::npos);
      }
    }
    inflight.Release();
    if (drain.joinable()) drain.join();
    EXPECT_OK(drained);
    if (reason == "error") {
      EXPECT_TRUE(storage_->Stop().IsIOError());
    } else {
      EXPECT_OK(storage_->Stop());
    }
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 0);
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_peak_pending_seals), 2);
  }
  void CheckAdaptiveBlockedSeal(const char* hook) {
    InitializeAdaptive();
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob("hdd", &writer));
    TestGate seal, waiting;
    SyncPoint::GetInstance()->SetCallBack(hook, [&](void*) { seal.Block(); });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBarrierWaiting",
                                          [&](void*) { waiting.Block(); });
    storage_->Start();
    SetPressure(true);
    ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
    seal.WaitUntilBlocked();
    ASSERT_OK(writer->Append("more", IOOptions(), nullptr));
    ASSERT_OK(writer->Flush(IOOptions(), nullptr));
    ASSERT_EQ(DurablePrefix(), 0);
    ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 1);
    std::atomic<bool> done{false};
    Status synced;
    std::thread sync([&] {
      synced = writer->Sync(IOOptions(), nullptr);
      done = true;
    });
    waiting.WaitUntilBlocked();
    EXPECT_FALSE(done.load());
    waiting.Release();
    // Sync captured S=10 and released the caller gate before waiting for HDD.
    EXPECT_OK(writer->Append("later", IOOptions(), nullptr));
    EXPECT_OK(writer->Flush(IOOptions(), nullptr));
    seal.Release();
    sync.join();
    EXPECT_OK(synced);
    ASSERT_GE(DurablePrefix(), 10);
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    ASSERT_OK(storage_->Persist({{1, 15}}));
    ASSERT_OK(storage_->Stop());
  }
  void InitializeAdaptive(uint64_t capacity = 256 * 1024 * 1024) {
    m_.staging_policy = MetaBypassStagingPolicy::kAdaptive;
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::HddClock", [&](void* p) {
      *static_cast<uint64_t*>(p) = adaptive_clock_.load();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    Initialize(capacity);
  }
  void SetPressure(bool high) {
    if (high) {
      HddPurposeScope purpose(HddPurpose::kCaller);
      const auto id = storage_->pressure_->Begin(HddPressure::Kind::kAppend, 1);
      adaptive_clock_ += m_.staging_latency_budget_us + 1;
      storage_->pressure_->End(id, true);
    } else {
      {
        auto& pressure = *storage_->pressure_;
        std::unique_lock<std::mutex> lock(pressure.mutex_);
        TEST_SYNC_POINT("MetaBypassTest::PressureRecoveryWaiting");
        pressure.wake_.wait(lock, [&] { return pressure.active_hdd_ == 0; });
        // Advance a quiet interval, not the duration of an in-flight physical
        // operation. Otherwise the jump correctly creates a fresh stall and
        // resets the controller's healthy-window origin. Begin uses this same
        // lock, so no operation can start between quiescence and the jump.
        adaptive_clock_ += 2000001;
      }
      for (int i = 0; i < 3; ++i) {
        const auto id = storage_->pressure_->Begin(HddPressure::Kind::kRead, 1);
        storage_->pressure_->End(id, true);
      }
    }
    ASSERT_EQ(storage_->pressure_->Pressured(), high);
  }
  void CheckAdaptiveRecoveryWaitsForInflight() {
    InitializeAdaptive();
    SetPressure(true);
    uint64_t inflight;
    {
      HddPurposeScope bulk(HddPurpose::kBulk);
      inflight = storage_->pressure_->Begin(HddPressure::Kind::kSync, 0);
    }
    const auto before = adaptive_clock_.load();
    SyncPoint::GetInstance()->LoadDependency(
        {{"MetaBypassTest::PressureRecoveryWaiting",
          "AdaptiveRecovery::Waiting"}});
    std::atomic<bool> recovered{false};
    std::thread recovery([&] {
      SetPressure(false);
      recovered = true;
    });
    TEST_SYNC_POINT("AdaptiveRecovery::Waiting");
    EXPECT_EQ(adaptive_clock_.load(), before);
    EXPECT_FALSE(recovered.load());
    EXPECT_TRUE(storage_->pressure_->Pressured());
    storage_->pressure_->End(inflight, true);
    recovery.join();
    EXPECT_TRUE(recovered.load());
    EXPECT_EQ(adaptive_clock_.load(), before + 2000001);
    EXPECT_FALSE(storage_->pressure_->Pressured());
    EXPECT_EQ(storage_->pressure_->Snapshot().recovered, 1);
    ASSERT_OK(storage_->Stop());
  }
  uint64_t AdaptiveStat(uint64_t MetaBypassStats::*field) {
    MetaBypassStats stats;
    storage_->AddStats(&stats);
    // AddStats adds counters only; sticky errors belong to Error/Sync/Stop.
    EXPECT_OK(stats.error);
    // Return only the counter. Moving/copying a Status-bearing snapshot makes
    // its destination unchecked even when the source Status was checked.
    return stats.*field;
  }
  Status PersistHistoricalPending(uint64_t number) {
    const auto blob = storage_->blobs_.at(number);
    auto extent = blob->caller.extent;
    extent.crc = blob->caller.crc;
    extent.open = false;
    extent.pending = true;
    return storage_->TieredStorage::Commit(
        number, blob, [extent](TieredStorage::Version* v) {
          v->size = extent.start + extent.length;
          v->extents.back() = extent;
          return Status::OK();
        });
  }
  size_t OpenAdaptiveWriters() {
    size_t count = 0;
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    for (const auto& item : storage_->blobs_) {
      count += item.second->caller.file != nullptr;
      count += item.second->migration.file != nullptr;
    }
    for (const auto& task : storage_->pending_seals_)
      count += task.active.file != nullptr;
    return count;
  }
  void WakeAdaptiveWorker() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    storage_->work_.notify_one();
  }
  Status CreateAdaptiveClosedSsd(uint64_t number, size_t length) {
    std::unique_ptr<FSWritableFile> writer;
    Status s = LocalWriter(number, &writer);
    if (s.ok())
      s = writer->Append(std::string(length, 's'), IOOptions(), nullptr);
    if (s.ok()) s = writer->Close(IOOptions(), nullptr);
    return s;
  }
  void CheckAdaptiveGlobalMigrationRate(bool pressured) {
    InitializeAdaptive();
    SetPressure(true);
    constexpr uint64_t chunk = 4 * 1024 * 1024;
    for (uint64_t n = 1; n <= 4; ++n) {
      ASSERT_OK(CreateAdaptiveClosedSsd(n, chunk));
    }
    SetPressure(pressured);
    const uint64_t interval = pressured ? 250000 : 125000;
    constexpr uint64_t start = 10000000;
    uint64_t clock = start;
    std::vector<uint64_t> starts, owners, delays;
    bool advanced_idle = false;
    TestGate copied;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierMigrationClock",
        [&](void* p) { *static_cast<uint64_t*>(p) = clock; });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierMigrationCreditGranted", [&](void* p) {
          starts.push_back(clock);
          owners.push_back(*static_cast<uint64_t*>(p));
        });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierThrottleDelay", [&](void* p) {
          auto* delay = static_cast<uint64_t*>(p);
          delays.push_back(*delay);
          // Visit the exact deadline-1 boundary, then the deadline. The real
          // condition-variable wait is zero here; only the scheduler clock
          // moves.
          clock += delays.size() == 1 ? *delay - 1 : *delay;
          *delay = 0;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierMigrationComplete", [&](void*) {
          if (starts.size() == 2 && !advanced_idle) {
            clock += 10 * interval;
            advanced_idle = true;
          }
          if (starts.size() == 4) copied.Block();
        });
    storage_->Start();
    copied.WaitUntilBlocked();
    EXPECT_EQ(starts, (std::vector<uint64_t>{start, start + interval,
                                             start + 11 * interval,
                                             start + 12 * interval}));
    EXPECT_EQ(delays, (std::vector<uint64_t>{interval, 1, interval}));
    EXPECT_EQ(std::set<uint64_t>(owners.begin(), owners.end()),
              (std::set<uint64_t>{1, 2, 3, 4}));
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::migrated_blob_bytes), 4 * chunk);
    {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      for (const auto& item : storage_->blobs_) {
        EXPECT_EQ(TieredStorage::DurableEnd(item.second->durable, true), chunk);
      }
    }
    copied.Release();
    ASSERT_OK(storage_->Stop());
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  void CheckAdaptiveBarrierWake(const std::string& blocked_phase) {
    InitializeAdaptive();
    SetPressure(true);
    constexpr uint64_t chunk = 4 * 1024 * 1024;
    ASSERT_OK(CreateAdaptiveClosedSsd(1, chunk));
    ASSERT_OK(CreateAdaptiveClosedSsd(2, chunk + 17));
    ASSERT_OK(CreateAdaptiveClosedSsd(3, chunk));
    std::atomic<uint64_t> clock{10000000}, selected{0}, credits{0}, awakened{0};
    std::atomic<uint64_t> target_calls{0}, other_calls{0}, readbacks{0};
    std::atomic<bool> done{false};
    TestGate physical;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierMigrationClock",
        [&](void* p) { *static_cast<uint64_t*>(p) = clock.load(); });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierMigrationCreditGranted", [&](void*) { ++credits; });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierThrottleDelay", [&](void* p) {
          // A missing notify cannot pass by waiting out 250 ms. The per-case
          // timeout catches it; this wait is always released by a real notify.
          *static_cast<uint64_t*>(p) = 3600000000ULL;
        });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierThrottleAwakened",
                                          [&](void* p) {
                                            EXPECT_TRUE(*static_cast<bool*>(p));
                                            ++awakened;
                                          });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierMigrationSelected", [&](void* p) {
          selected = *static_cast<uint64_t*>(p);
          if (selected == 2) ++target_calls;
          if (selected == 3) ++other_calls;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierReadbackChunk", [&](void*) {
          if (selected != 2) return;
          ++readbacks;
          if (blocked_phase == "MetaBypass::TierReadbackChunk")
            physical.Block();
        });
    if (blocked_phase != "MetaBypass::TierReadbackChunk") {
      SyncPoint::GetInstance()->SetCallBack(blocked_phase, [&](void*) {
        if (selected == 2) physical.Block();
      });
    }
    SyncPoint::GetInstance()->LoadDependency(
        {{"MetaBypass::TierThrottleWaiting", "AdaptiveWake::Waiting"}});
    storage_->Start();
    TEST_SYNC_POINT("AdaptiveWake::Waiting");
    Status persisted;
    std::thread barrier([&] {
      persisted = storage_->Persist({{2, chunk + 17}});
      done = true;
    });
    physical.WaitUntilBlocked();
    EXPECT_GE(awakened.load(), 1);
    EXPECT_EQ(clock.load(), 10000000);
    EXPECT_EQ(credits.load(), 1);  // The explicit barrier bypassed admission.
    EXPECT_GT(target_calls.load(), 0);
    EXPECT_EQ(other_calls.load(), 0);  // It also beat unrelated ready blob 3.
    EXPECT_FALSE(done.load());
    {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      const auto& target = *storage_->blobs_.at(2);
      EXPECT_FALSE(target.complete_ready);
      EXPECT_EQ(TieredStorage::DurableEnd(target.durable, true),
                blocked_phase == "MetaBypass::TierFinalHddPublished"
                    ? chunk + 17
                    : 0);
    }
    const auto attempts = target_calls.load();
    const auto writes = AdaptiveStat(&MetaBypassStats::io_data_write_bytes);
    clock += 10 * 250000;
    // Expired deadlines and repeated wakeups cannot stack probes behind an
    // unfinished HDD operation: the sole executor still owns that operation.
    for (int i = 0; i < 10; ++i) {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      storage_->work_.notify_one();
    }
    EXPECT_EQ(target_calls.load(), attempts);
    EXPECT_EQ(other_calls.load(), 0);
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::io_data_write_bytes), writes);
    EXPECT_FALSE(done.load());
    physical.Release();
    barrier.join();
    EXPECT_OK(persisted);
    EXPECT_GE(readbacks.load(), 2);
    {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      EXPECT_TRUE(storage_->blobs_.at(2)->complete_ready);
      EXPECT_EQ(
          TieredStorage::DurableEnd(storage_->blobs_.at(2)->durable, true),
          chunk + 17);
    }
    ASSERT_OK(storage_->Stop());
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  void CheckAdaptiveDrainRace(bool gc, bool readback) {
    InitializeAdaptive();
    const std::string payload(readback ? 4 * 1024 * 1024 + 17 : 17, 'h');
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob(payload, &writer));
    SetPressure(true);
    TestGate paused;
    const char* hook = readback ? "MetaBypass::TierSealBeforeRequeue"
                                : "MetaBypass::TierHandoffPersisted";
    SyncPoint::GetInstance()->SetCallBack(hook, [&](void*) { paused.Block(); });
    Status appended;
    std::thread caller;
    if (readback) {
      ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
      storage_->Start();
    } else {
      caller = std::thread(
          [&] { appended = writer->Append("ssd", IOOptions(), nullptr); });
    }
    paused.WaitUntilBlocked();
    const char* drained = gc         ? "MetaBypass::TierGcQueueDrained"
                          : readback ? "MetaBypass::TierStopping"
                                     : "MetaBypass::TierStopQueueDrained";
    SyncPoint::GetInstance()->LoadDependency(
        {{drained, "AdaptiveDrain::ClosedAdmission"}});
    Status result;
    uint64_t removed = 0;
    std::thread drain([&] {
      result = gc ? storage_->ReclaimBlob(1, &removed) : storage_->Stop();
    });
    TEST_SYNC_POINT("AdaptiveDrain::ClosedAdmission");
    paused.Release();
    if (caller.joinable()) caller.join();
    drain.join();
    EXPECT_OK(result);
    if (!readback) {
      if (gc)
        EXPECT_TRUE(appended.IsAborted());
      else
        EXPECT_TRUE(appended.IsIOError());
    } else {
      EXPECT_OK(appended);
    }
    EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 0);
    EXPECT_EQ(OpenAdaptiveWriters(), 0);
    EXPECT_OK(storage_->Stop());
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    // Stop must retain the already registered payload and empty SSD successor.
    // GC must finish deletion, including the rejected handoff's two files.
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    if (gc) {
      ASSERT_TRUE(
          storage_
              ->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
              .IsNotFound());
    } else {
      std::string actual;
      ASSERT_OK(ReadBytes(0, payload.size() + 3, &actual));
      ASSERT_EQ(actual, payload + (readback ? "ssd" : ""));
    }
    ASSERT_OK(storage_->Stop());
  }
  void CheckAdaptiveAdmissionCounters(bool migrated) {
    InitializeAdaptive();
    SetPressure(true);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob("ssd", &writer));
    ASSERT_OK(FreezeAdaptiveCaller());
    if (migrated) {
      ASSERT_OK(Migrate(3));
    }
    // Same caller medium across a freeze is not a switch even if its previous
    // SSD range has already been replaced by an HDD migration destination.
    ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
    ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_to_ssd), 0);
    ASSERT_OK(FreezeAdaptiveCaller());
    if (migrated) {
      ASSERT_OK(Migrate(6));
    }
    SetPressure(false);
    ASSERT_OK(writer->Append("hdd", IOOptions(), nullptr));
    ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_to_hdd), 1);
    SetPressure(true);
    ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
    ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_to_ssd), 1);
    ASSERT_OK(storage_->Stop());
  }
  Status FreezeAdaptiveCaller() {
    return storage_->TieredStorage::Freeze(1, storage_->blobs_.at(1));
  }
  uint64_t DurablePrefix() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return TieredStorage::DurableEnd(storage_->blobs_.at(1)->durable, false);
  }
  std::atomic<uint64_t> adaptive_clock_{10000000};
  static std::string EmptyBlob() {
    std::string blob;
    BlobLogHeader header(0, kNoCompression, false, {0, 0});
    header.EncodeTo(&blob);
    EXPECT_EQ(blob.size(), BlobLogHeader::kSize);
    BlobLogFooter footer;
    std::string footer_bytes;
    // Both encoders clear their destination, so assemble separate encodings.
    footer.EncodeTo(&footer_bytes);
    EXPECT_EQ(footer_bytes.size(), BlobLogFooter::kSize);
    blob += footer_bytes;
    EXPECT_EQ(blob.size(), BlobLogHeader::kSize + BlobLogFooter::kSize);
    return blob;
  }
  std::map<std::string, std::string> TierFiles() {
    std::map<std::string, std::string> files;
    for (const auto& dir : {m_.data_dir, m_.staging_dir}) {
      std::vector<std::string> names;
      EXPECT_OK(fs_->GetChildren(dir, IOOptions(), &names, nullptr));
      for (const auto& name : names) {
        if (name == "." || name == "..") continue;
        std::string bytes;
        EXPECT_OK(Read(fs_.get(), dir + "/" + name, &bytes));
        files.emplace(dir + "/" + name, std::move(bytes));
      }
    }
    return files;
  }
  uint64_t RemoteGeneration() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->blobs_.at(1)->remote.generation;
  }
  void CheckMetadataAccounting() {
    uint64_t bytes = 0;
    for (const auto& file : TierFiles()) {
      if (file.first.compare(0, m_.staging_dir.size() + 1,
                             m_.staging_dir + "/") == 0 &&
          file.first.find(".local") != std::string::npos)
        bytes += file.second.size();
    }
    MetaBypassStats stats;
    storage_->AddStats(&stats);
    ASSERT_OK(stats.error);
    ASSERT_EQ(stats.tier_local_metadata_bytes, bytes);
    ASSERT_GE(stats.peak_tier_local_metadata_bytes, bytes);
  }
  Status ProtectedPoint(const std::string& name, bool contains_blob,
                        std::string* pointer) {
    const auto point = m_.backup_dir + "/" + name;
    Status s = EnsureDir(fs_.get(), m_.backup_dir);
    if (s.ok()) s = EnsureDir(fs_.get(), point);
    if (s.ok())
      s = Write(fs_.get(), m_.backup_dir + "/METABYPASS-IDENTITY", "tier-test");
    if (s.ok()) s = Write(fs_.get(), point + "/IDENTITY", "tier-test");
    if (s.ok())
      s = storage_->SaveCheckpoint(point + "/BLOB-MAP",
                                   contains_blob
                                       ? std::map<uint64_t, uint64_t>{{1, 7}}
                                       : std::map<uint64_t, uint64_t>{});
    std::string map;
    if (s.ok()) s = Read(fs_.get(), point + "/BLOB-MAP", &map);
    std::string inventory =
        "MBI6\nI IDENTITY 9\nD BLOB-MAP " + std::to_string(map.size()) + "\n";
    if (contains_blob) inventory += "B 1 7\n";
    if (s.ok()) s = Write(fs_.get(), point + "/INVENTORY", inventory);
    *pointer = name + "\n";
    return s;
  }
  Status NativePointPreflight() {
    std::shared_ptr<TieredStorage> tier(storage_.get(), [](TieredStorage*) {});
    SeparatedStorage separated(fs_, index_, m_.data_dir, tier);
    return Backup::PreflightCheckpoint(fs_.get(), m_.backup_dir, separated);
  }
  Status MutateLocalDescriptor(const std::string& kind, std::string* bytes) {
    TieredStorage::Version version;
    Status st = TieredStorage::Decode(*bytes, &version);
    if (!st.ok()) return st;
    if (kind == "identity") version.identity = "deadbeef";
    if (kind == "incarnation") version.incarnation = "deadbeef";
    if (kind == "overlap") version.extents.push_back(version.extents.front());
    if (kind == "generation") version.generation = 0;
    *bytes = TieredStorage::Encode(version);
    return Status::OK();
  }
  Status RemoveRemoteProof(bool whole_descriptor) {
    if (whole_descriptor)
      return fs_->DeleteFile(BlobFileName(m_.data_dir, 1), IOOptions(),
                             nullptr);
    auto remote = storage_->blobs_.at(1)->remote;
    remote.extents.clear();
    return Write(fs_.get(), BlobFileName(m_.data_dir, 1),
                 TieredStorage::Encode(remote, false));
  }
  void MetadataStatusFailure(const std::string& phase, const char* hook) {
    SCOPED_TRACE(phase + ":" + hook);
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(LocalWriter(1, &writer));
    if (phase != "register") {
      ASSERT_OK(writer->Append("payload", IOOptions(), nullptr));
      ASSERT_OK(writer->Flush(IOOptions(), nullptr));
    }
    if (phase == "migrate" || phase == "gc") {
      ASSERT_OK(writer->Close(IOOptions(), nullptr));
    }
    bool hit = false;
    SyncPoint::GetInstance()->SetCallBack(hook, [&](void* arg) {
      if (hit) return;
      auto* status = static_cast<Status*>(arg);
      EXPECT_OK(*status);
      hit = true;
      *status = Status::IOError("metadata boundary failure");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    Status operation;
    if (phase == "register")
      operation = writer->Append("payload", IOOptions(), nullptr);
    else if (phase == "freeze")
      operation = writer->Sync(IOOptions(), nullptr);
    else if (phase == "migrate")
      operation = Migrate(7);
    else {
      uint64_t removed;
      operation = storage_->ReclaimBlob(1, &removed);
    }
    ASSERT_TRUE(hit);
    ASSERT_TRUE(operation.IsIOError());
    ASSERT_NO_FATAL_FAILURE(CheckMetadataAccounting());
    // Manual migration uses the production task body without its Run wrapper.
    // Apply the same first-error contract before checking waiters and Stop.
    if (phase == "migrate") storage_->Cancel(operation);
    ASSERT_TRUE(
        storage_->Persist({{1, phase == "register" ? 0U : 7U}}).IsIOError());
    ASSERT_EQ(TierStats().staging_bytes, 7);
    if (phase == "register" || phase == "freeze") {
      ASSERT_EQ(HddEnd(), 0);
    }
    if (phase == "register") {
      ASSERT_EQ(RemoteSize(), 0);
    }
    if (phase == "freeze" || phase == "migrate") {
      std::string actual;
      ASSERT_OK(ReadBytes(0, 7, &actual));
      ASSERT_EQ(actual, "payload");
    }
    ASSERT_TRUE(storage_->Stop().IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    // Status failures preserve uncertain publications; recovery must reconcile
    // them, or replay the tombstone, without losing accepted flushed bytes.
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    if (phase != "gc") {
      std::string actual;
      ASSERT_OK(ReadBytes(0, 7, &actual));
      ASSERT_EQ(actual, phase == "register" ? "" : "payload");
    } else {
      ASSERT_TRUE(
          storage_
              ->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
              .IsNotFound());
    }
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
  Status WriteRetirementForTest() {
    auto tomb = storage_->blobs_.at(1)->remote;
    tomb.retired = true;
    tomb.closed = false;
    tomb.size = 0;
    tomb.extents.clear();
    return Write(fs_.get(), storage_->Tombstone(1, tomb),
                 TieredStorage::Encode(tomb, false));
  }
  Status ValidateDescriptor(const std::string& body) {
    TieredStorage::Version version;
    return TieredStorage::Decode(
        "MBL5 " + std::to_string(crc32c::Value(body.data(), body.size())) +
            "\n" + body,
        &version);
  }
  Status CreateLocalBlob(const std::string& blob,
                         std::unique_ptr<FSWritableFile>* writer) {
    Status s = storage_->NewWritableFile(BlobFileName(m_.data_dir, 1),
                                         FileOptions(), writer, nullptr);
    if (s.ok()) s = (*writer)->Append(blob, IOOptions(), nullptr);
    return s;
  }
  Status Migrate(uint64_t length) {
    auto blob = storage_->blobs_.at(1);
    while (TieredStorage::DurableEnd(blob->durable, true) < length) {
      const auto before = TieredStorage::DurableEnd(blob->durable, true);
      bool ready = false;
      for (const auto& e : blob->live.extents) ready |= e.local && !e.open;
      if (!ready) return Status::OK();
      Status st = storage_->TieredStorage::Migrate(1, blob);
      if (!st.ok()) return st;
      if (TieredStorage::DurableEnd(blob->durable, true) == before &&
          blob->migration_source.empty())
        break;
    }
    Status st = storage_->TieredStorage::Finalize(1, blob);
    return st.ok() ? storage_->TieredStorage::ReclaimLocal() : st;
  }
  Status CreateClosedLocalSegments(size_t count) {
    std::unique_ptr<FSWritableFile> writer;
    Status s = LocalWriter(1, &writer);
    for (size_t i = 0; s.ok() && i < count; ++i) {
      s = writer->Append("payload", IOOptions(), nullptr);
      if (s.ok())
        s = i + 1 == count ? writer->Close(IOOptions(), nullptr)
                           : writer->Sync(IOOptions(), nullptr);
    }
    return s;
  }
  std::thread StartWorkerWithPersist(uint64_t length, Status* result) {
    SyncPoint::GetInstance()->LoadDependency(
        {{"MetaBypass::TierPersistRequested",
          "TierReclaimTest::PersistQueued"}});
    SyncPoint::GetInstance()->EnableProcessing();
    std::thread waiter([this, length, result] {
      *result = storage_->Persist({{1, length}});
    });
    TEST_SYNC_POINT("TierReclaimTest::PersistQueued");
    storage_->Start();
    return waiter;
  }
  size_t RetiredLocalCount() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->retired_local_.size();
  }
  size_t PendingBarrierCount() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->barriers_.size();
  }
  uint64_t LocalExtentBytes(uint64_t number) {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    uint64_t bytes = 0;
    for (const auto& e : storage_->blobs_.at(number)->live.extents)
      if (e.local) bytes += e.length;
    return bytes;
  }
  void CheckWorkerReclaimFailure(bool fail_delete) {
    auto failing = std::make_shared<LocalReclaimFailureFileSystem>(fs_);
    fs_ = failing;
    Initialize(14);
    ASSERT_OK(CreateClosedLocalSegments(2));
    failing->local_path = FirstExtentPath();
    failing->staging_dir = m_.staging_dir;
    failing->fail_delete = fail_delete;
    Status persisted;
    auto waiter = StartWorkerWithPersist(14, &persisted);
    waiter.join();
    // A subsequent failure cannot replace the worker's reclamation error.
    const Status later = Status::IOError("later failure");
    EXPECT_TRUE(later.IsIOError());
    storage_->Cancel(later);
    Status stopped = storage_->Stop();
    failing->fail_delete = false;
    failing->deleted = false;
    ASSERT_TRUE(persisted.IsIOError());
    ASSERT_TRUE(stopped.IsIOError());
    const std::string expected =
        Status::IOError(fail_delete ? "SSD source delete failure"
                                    : "SSD source dirsync failure")
            .ToString();
    ASSERT_EQ(persisted.ToString(), expected);
    ASSERT_EQ(stopped.ToString(), expected);
    ASSERT_EQ(HddEnd(), 7);
    ASSERT_FALSE(FinalPublished());
    ASSERT_EQ(TierStats().staging_bytes, 14);
    ASSERT_EQ(RetiredLocalCount(), 1);
    ASSERT_EQ(failing->delete_failures.load(), fail_delete ? 1 : 0);
    ASSERT_EQ(failing->dirsync_failures.load(), fail_delete ? 0 : 1);
    Status exists = fs_->FileExists(failing->local_path, IOOptions(), nullptr);
    ASSERT_EQ(exists.ok(), fail_delete);
    if (!fail_delete) {
      ASSERT_TRUE(exists.IsNotFound());
    }
  }
  Status MigrateChunk(uint64_t) {
    Status s = storage_->TieredStorage::Migrate(1, storage_->blobs_.at(1));
    return s.ok() ? storage_->TieredStorage::ReclaimLocal() : s;
  }
  // Synthetic layout tests have no worker and need a completed publication
  // before directly editing the next version. Exercise the real queued seal
  // body synchronously, preserving their non-sticky injected-error contract.
  Status FreezeFixtureCaller(const std::shared_ptr<TieredStorage::Blob>& blob) {
    assert(!storage_->worker_.joinable());
    Status s = storage_->TieredStorage::Freeze(1, blob);
    if (!s.ok()) return s;
    while (!storage_->pending_seals_.empty()) {
      auto task = std::move(storage_->pending_seals_.front());
      storage_->pending_seals_.pop_front();
      do {
        s = storage_->TieredStorage::FinishSeal(&task);
      } while (s.IsTryAgain());
      if (task.active.file)
        s.UpdateIfOk(task.active.file->Close(IOOptions(), nullptr));
      --task.blob->operations;
      --task.blob->pending_seals;
      --storage_->pending_count_;
      storage_->progress_.notify_all();
      if (!s.ok()) return s;
    }
    return s;
  }
  // Construct arbitrary remote extent boundaries without depending on the
  // worker queue, which no longer owns the caller's HDD writer.
  Status AppendPending(const std::string& bytes) {
    if (!storage_->blobs_.count(1)) {
      std::unique_ptr<FSWritableFile> writer;
      Status s = LocalWriter(1, &writer);
      if (!s.ok()) return s;
    }
    auto blob = storage_->blobs_.at(1);
    // Synthetic extent-layout fixture: reopen its logical version explicitly.
    // Real closed TierWriters cannot append. Historical encoded maps remain
    // immutable and their physical extents are never overwritten here.
    const bool was_complete = blob->remote.closed;
    blob->remote.closed = false;
    blob->remote.size = 0;
    blob->complete_ready = false;
    if (was_complete) {
      ++blob->remote.generation;
      Status rewritten = storage_->TieredStorage::StoreSnapshot(
          storage_->Descriptor(1), blob->remote, false,
          TieredStorage::Reason::kRecovery);
      if (!rewritten.ok()) return rewritten;
    }
    Status s =
        storage_->TieredStorage::Commit(1, blob, [](TieredStorage::Version* v) {
          v->closed = false;
          v->spilled = true;
          return Status::OK();
        });
    size_t offset = 0;
    while (s.ok() && offset < bytes.size()) {
      if (!blob->caller.file)
        s = storage_->TieredStorage::Register(1, blob, false);
      if (!s.ok()) break;
      auto& active = blob->caller;
      const auto take =
          std::min<uint64_t>(bytes.size() - offset,
                             TieredStorage::kExtentSize - active.extent.length);
      s = active.file->Append(Slice(bytes.data() + offset, take), IOOptions(),
                              nullptr);
      if (s.ok()) s = active.file->Flush(IOOptions(), nullptr);
      if (!s.ok()) break;
      active.crc = crc32c::Extend(active.crc, bytes.data() + offset, take);
      active.extent.length += take;
      {
        std::lock_guard<std::mutex> lock(storage_->mutex_);
        blob->live.extents.back().length = active.extent.length;
        blob->live.size += take;
      }
      offset += take;
      if (active.extent.length == TieredStorage::kExtentSize)
        s = FreezeFixtureCaller(blob);
    }
    return s;
  }
  Status SealPending() {
    auto blob = storage_->blobs_.at(1);
    Status s = FreezeFixtureCaller(blob);
    if (s.ok())
      s = storage_->TieredStorage::Commit(1, blob,
                                          [](TieredStorage::Version* v) {
                                            v->closed = true;
                                            return Status::OK();
                                          });
    return s.ok() ? storage_->TieredStorage::Finalize(1, blob) : s;
  }
  std::string LocalDescriptorPath(uint64_t n = 1) {
    return storage_->LocalDescriptor(n);
  }
  bool FinalPublished(uint64_t n = 1) {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->blobs_.at(n)->complete_ready;
  }
  bool LogicalClosed(uint64_t n = 1) {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->blobs_.at(n)->live.closed;
  }
  Status FinalizeBlob(uint64_t n = 1) {
    return storage_->TieredStorage::Finalize(n, storage_->blobs_.at(n));
  }
  uint64_t RemotePins() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    auto it = storage_->blobs_.find(1);
    return it == storage_->blobs_.end() ? 0 : it->second->operations;
  }
  uint64_t RequestedLength() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    auto it = storage_->requests_.find(1);
    return it == storage_->requests_.end() ? 0 : it->second;
  }
  void NotifyProgress() { storage_->progress_.notify_all(); }
  std::string FirstExtentPath() {
    return storage_->Path(storage_->blobs_.at(1)->durable.extents.front());
  }
  std::string FirstLocalPath() {
    std::vector<std::string> names;
    EXPECT_OK(fs_->GetChildren(m_.staging_dir, IOOptions(), &names, nullptr));
    for (const auto& name : names)
      if (name.compare(0, 10, "segment-1-") == 0)
        return m_.staging_dir + "/" + name;
    return m_.staging_dir + "/missing-segment";
  }
  Status ReplaceExtent(bool change_length) {
    auto version = storage_->blobs_.at(1)->durable;
    if (change_length) {
      ++version.extents[0].length;
      ++version.extents[0].durable;
      ++version.extents[1].start;
      --version.extents[1].length;
      --version.extents[1].durable;
    } else {
      version.extents[0].name = storage_->NewName(1);
      Status s =
          Write(fs_.get(), m_.data_dir + "/" + version.extents[0].name, "new");
      if (s.ok()) s = storage_->SyncCommitDirectory();
      if (!s.ok()) return s;
    }
    return storage_->TieredStorage::Commit(
        1, storage_->blobs_.at(1), [version](TieredStorage::Version* v) {
          *v = version;
          return Status::OK();
        });
  }
  Status CommitOldWithPendingTail() {
    Status s = AppendPending("pending");
    return s.ok() ? CommitPending() : s;
  }
  struct TierCounters {
    uint64_t staging_bytes, tier_persist_skips, tier_commit_skips;
    uint64_t local_commit_successes, local_metadata_dir_syncs;
    uint64_t tier_extent_fsyncs, tier_commit_dir_syncs,
        tier_checkpoint_digest_opens;
  };
  TierCounters TierStats() {
    MetaBypassStats stats;
    storage_->AddStats(&stats);
    EXPECT_OK(stats.error);
    return {stats.staging_bytes,
            stats.tier_persist_skips,
            stats.tier_commit_skips,
            stats.tier_local_commit_successes,
            stats.tier_local_metadata_dir_syncs,
            stats.tier_extent_fsyncs,
            stats.tier_commit_dir_syncs,
            stats.tier_checkpoint_digest_opens};
  }
  Status CommitPending() {
    auto blob = storage_->blobs_.at(1);
    if (blob->caller.file) return FreezeFixtureCaller(blob);
    return storage_->TieredStorage::Commit(
        1, blob, [](TieredStorage::Version*) { return Status::OK(); });
  }
  std::vector<uint64_t> ExtentLengths(bool durable = false) {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    std::vector<uint64_t> lengths;
    auto it = storage_->blobs_.find(1);
    if (it == storage_->blobs_.end()) return lengths;
    const auto& version = durable ? it->second->durable : it->second->live;
    for (const auto& e : version.extents)
      if (!e.local && (!durable || (!e.open && !e.pending)))
        lengths.push_back(e.length);
    return lengths;
  }
  void CheckPendingSnapshot() {
    auto old = storage_->blobs_.at(1)->live;
    const auto old_lengths = ExtentLengths();
    ASSERT_OK(AppendPending("tail"));
    ASSERT_EQ(old.size + 4, RemoteSize());
    ASSERT_EQ(old.extents.back().length, old_lengths.back());
  }
  uint64_t LocalPins() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    uint64_t pins = 0;
    for (const auto& item : storage_->readers_) {
      if (storage_->retired_local_.count(item.first))
        pins += item.second;
      else {
        auto blob = storage_->blobs_.find(1);
        if (blob != storage_->blobs_.end())
          for (const auto& e : blob->second->live.extents)
            if (e.local && e.name == item.first) pins += item.second;
      }
    }
    return pins;
  }
  bool RemoteRoute() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    auto it = storage_->blobs_.find(1);
    if (it == storage_->blobs_.end()) return true;
    for (const auto& e : it->second->live.extents)
      if (e.local) return false;
    return true;
  }
  bool RemoteSealed() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return TieredStorage::Complete(storage_->blobs_.at(1)->durable);
  }
  uint64_t RemoteSize() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->blobs_.at(1)->live.size;
  }
  uint64_t HddEnd() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return TieredStorage::DurableEnd(storage_->blobs_.at(1)->durable, true);
  }
  Status LocalExists() {
    return fs_->FileExists(FirstLocalPath(), IOOptions(), nullptr);
  }
  void Initialize(uint64_t capacity = 1024 * 1024) {
    EnableTier(capacity);
    ASSERT_OK(EnsureDir(fs_.get(), m_.data_dir));
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
  }
  Status Append(const std::string& bytes) {
    Status s = AppendPending(bytes);
    return s.ok() ? SealPending() : s;
  }
  Status ReadBytes(uint64_t offset, size_t length, std::string* bytes) {
    bytes->resize(length);
    Slice result;
    Status s = storage_->TieredStorage::ReadBlob(
        1, offset, length, IOOptions(), &result, bytes->data(), nullptr);
    if (s.ok()) {
      if (result.empty())
        bytes->clear();
      else
        bytes->assign(result.data(), result.size());
    }
    return s;
  }
  Status LocalWriter(uint64_t number, std::unique_ptr<FSWritableFile>* writer) {
    return storage_->NewWritableFile(BlobFileName(m_.data_dir, number),
                                     FileOptions(), writer, nullptr);
  }
  void QueuedBarriersBeforeReclaim(bool fail_barrier) {
    Initialize();
    ASSERT_OK(Append("remote payload"));
    // Logical syncs execute on caller handles; pin them before reclamation.
    TestGate first_barrier, second_barrier, retiring;
    std::atomic<int> barriers{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierSyncPinned",
                                          [&](void*) {
                                            if (++barriers == 1)
                                              first_barrier.Block();
                                            else
                                              second_barrier.Block();
                                          });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBlobRetiring",
                                          [&](void*) { retiring.Block(); });
    SyncPoint::GetInstance()->EnableProcessing();
    Status first, second, reclaimed;
    uint64_t bytes = 0;
    auto sync = [&](Status* status) {
      *status = storage_->SyncFile(BlobFileName(m_.data_dir, 1), FileOptions(),
                                   IOOptions(), true, nullptr);
    };
    std::thread a([&] { sync(&first); });
    first_barrier.WaitUntilBlocked();
    std::thread b([&] { sync(&second); });
    second_barrier.WaitUntilBlocked();
    std::thread gc([&] { reclaimed = storage_->ReclaimBlob(1, &bytes); });
    retiring.WaitUntilBlocked();
    retiring.Release();
    if (fail_barrier) storage_->Cancel(Status::IOError("barrier failure"));
    first_barrier.Release();
    second_barrier.Release();
    a.join();
    b.join();
    gc.join();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(barriers.load(), 2);
    if (fail_barrier) {
      EXPECT_TRUE(first.IsIOError());
      EXPECT_TRUE(second.IsIOError());
      EXPECT_TRUE(reclaimed.IsIOError());
      EXPECT_TRUE(storage_->Stop().IsIOError());
    } else {
      EXPECT_TRUE(first.IsAborted());
      EXPECT_TRUE(second.IsAborted());
      EXPECT_OK(reclaimed);
      EXPECT_OK(storage_->Stop());
      EXPECT_GT(bytes, 0);
    }
  }
  void ReclaimFailure(const char* point, bool payload_remains) {
    Initialize();
    ASSERT_OK(Append("remote payload"));
    Status recorded;
    storage_->SetFailureHandler([&](const Status& error) { recorded = error; });
    storage_->Start();
    SyncPoint::GetInstance()->SetCallBack(point, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("injected blob deletion");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    uint64_t bytes = 0;
    EXPECT_TRUE(storage_->ReclaimBlob(1, &bytes).IsIOError());
    EXPECT_TRUE(storage_->Stop().IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_TRUE(recorded.IsIOError());
    EXPECT_TRUE(storage_->ReclaimBlob(1, &bytes).IsIOError());
    storage_->SetFailureHandler({});
    std::vector<std::string> names;
    ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
    bool found_payload = false;
    for (const auto& name : names)
      if (name.compare(0, 10, "segment-1-") == 0) found_payload = true;
    ASSERT_EQ(found_payload, payload_remains);
    ASSERT_TRUE(
        fs_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
            .IsNotFound());
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    names.clear();
    ASSERT_OK(storage_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
    ASSERT_EQ(
        std::find(names.begin(), names.end(), "000001.blob") != names.end(),
        false);
    // V4 replays the durable retirement before exposing any orphan identity.
    ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
    for (const auto& name : names) {
      ASSERT_NE(name.compare(0, 10, "segment-1-"), 0);
      ASSERT_EQ(name.find(".retired"), std::string::npos);
    }
    storage_->Start();
    ASSERT_OK(storage_->Stop());
  }
  void MetadataPowerLoss(const std::string& phase, const char* hook) {
    SCOPED_TRACE(phase + ":" + hook);
    auto underlying = fs_;
    auto fault = std::make_shared<FaultInjectionTestFS>(underlying);
    fault->SetInjectUnsyncedDataLoss(true);
    fs_ = fault;
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(LocalWriter(1, &writer));
    if (phase != "register") {
      ASSERT_OK(writer->Append("payload", IOOptions(), nullptr));
      ASSERT_OK(writer->Flush(IOOptions(), nullptr));
    }
    if (phase == "migrate" || phase == "gc") {
      ASSERT_OK(writer->Close(IOOptions(), nullptr));
    }
    bool hit = false;
    SyncPoint::GetInstance()->SetCallBack(hook, [&](void*) {
      if (hit) return;
      hit = true;
      fault->SetFilesystemActive(false);
      storage_->Cancel(Status::IOError("simulated power loss"));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    Status operation;
    if (phase == "register")
      operation = writer->Append("payload", IOOptions(), nullptr);
    else if (phase == "freeze")
      operation = writer->Sync(IOOptions(), nullptr);
    else if (phase == "migrate")
      operation = Migrate(7);
    else {
      uint64_t removed;
      operation = storage_->ReclaimBlob(1, &removed);
    }
    operation.PermitUncheckedError();
    ASSERT_TRUE(hit);
    ASSERT_TRUE(storage_->Stop().IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    fault->SetFilesystemActive(true);
    ASSERT_OK(fault->DropUnsyncedFileData());
    ASSERT_OK(fault->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));
    fault->ResetState();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    if (phase == "gc") {
      ASSERT_TRUE(
          storage_
              ->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
              .IsNotFound());
      ASSERT_EQ(TierStats().staging_bytes, 0);
    } else {
      std::string actual;
      ASSERT_OK(ReadBytes(0, 100, &actual));
      ASSERT_EQ(actual, phase == "register" ? "" : "payload");
      ASSERT_TRUE(LogicalClosed());
      storage_->Start();
      ASSERT_OK(storage_->Persist({{1, actual.size()}}));
      ASSERT_TRUE(FinalPublished());
    }
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    if (phase != std::string("gc")) {
      std::string actual;
      ASSERT_OK(ReadBytes(0, 100, &actual));
      ASSERT_EQ(actual, phase == std::string("register") ? "" : "payload");
      ASSERT_TRUE(FinalPublished());
    }
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    fs_ = underlying;
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
#ifndef OS_WIN
  void MetadataProcessCrash(const char* phase, const char* hook) {
    SCOPED_TRACE(std::string(phase) + ":" + hook);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(), "--v4-tier-crash-writer",
            m_.data_dir.c_str(), (root_ + "/staging").c_str(), phase, hook,
            nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    Initialize(128);
    if (std::string(phase) == "gc") {
      ASSERT_TRUE(
          storage_
              ->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
              .IsNotFound());
      ASSERT_EQ(TierStats().staging_bytes, 0);
    } else {
      std::string actual;
      ASSERT_OK(ReadBytes(0, 100, &actual));
      ASSERT_EQ(actual, std::string(phase) == "register" ? "" : "payload");
      ASSERT_TRUE(LogicalClosed());
      storage_->Start();
      ASSERT_OK(storage_->Persist({{ 1, actual.size() }}));
      ASSERT_TRUE(FinalPublished());
    }
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    if (phase != std::string("gc")) {
      std::string actual;
      ASSERT_OK(ReadBytes(0, 100, &actual));
      ASSERT_EQ(actual, phase == std::string("register") ? "" : "payload");
      ASSERT_TRUE(FinalPublished());
    }
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
#endif
  std::unique_ptr<TieredStorage> storage_;
};

TEST_F(TieredStorageTest,
       ConcurrentAppendReservationsCannotOversubscribeQuota) {
  Initialize(64);
  storage_->Start();
  std::unique_ptr<FSWritableFile> first, second;
  ASSERT_OK(LocalWriter(1, &first));
  ASSERT_OK(LocalWriter(2, &second));
  TestGate reserved;
  std::atomic<bool> once{true};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierQuotaReserved",
                                        [&](void*) {
                                          if (once.exchange(false))
                                            reserved.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status append;
  std::thread writer([&] {
    append = first->Append(std::string(48, 'a'), IOOptions(), nullptr);
  });
  reserved.WaitUntilBlocked();
  // The first append owns 48 bytes before physical I/O starts. The second
  // must switch to HDD rather than admitting another 48 bytes to SSD.
  Status other = second->Append(std::string(48, 'b'), IOOptions(), nullptr);
  reserved.Release();
  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(append);
  ASSERT_OK(other);
  ASSERT_OK(first->Close(IOOptions(), nullptr));
  ASSERT_OK(second->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, 48}, {2, 48}}));
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_staging_bytes, 64U);
  ASSERT_GT(stats.staging_spill_files, 0U);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, PinnedLocalReadDoesNotBlockSpilledFileAppend) {
  Initialize(64);
  storage_->Start();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(LocalWriter(1, &writer));
  ASSERT_OK(writer->Append(std::string(32, 'p'), IOOptions(), nullptr));
  TestGate pinned;
  std::atomic<bool> once{true};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierLocalReadPinned",
                                        [&](void*) {
                                          if (once.exchange(false))
                                            pinned.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status read;
  std::string prefix;
  std::thread reader([&] { read = ReadBytes(0, 32, &prefix); });
  pinned.WaitUntilBlocked();
  // Both the switching append and a later HDD append must finish while
  // the old physical SSD read remains pinned.
  Status spill = writer->Append(std::string(64, 's'), IOOptions(), nullptr);
  Status tail = writer->Append("tail", IOOptions(), nullptr);
  Status sync = writer->Sync(IOOptions(), nullptr);
  const bool remote = RemoteRoute();
  const Status local = LocalExists();
  pinned.Release();
  reader.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(spill);
  ASSERT_OK(tail);
  ASSERT_OK(sync);
  ASSERT_TRUE(remote);
  ASSERT_OK(local);
  ASSERT_OK(read);
  ASSERT_EQ(prefix, std::string(32, 'p'));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  std::string all;
  ASSERT_OK(ReadBytes(0, 100, &all));
  ASSERT_EQ(all, std::string(32, 'p') + std::string(64, 's') + "tail");
  ASSERT_OK(storage_->Stop());
}

#ifndef OS_WIN
TEST_F(TieredStorageTest,
       CrashAfterRouteCommitAndUnsyncedHddTailUsesDurablePrefix) {
  for (const char* stage : {"route", "tail"}) {
    SCOPED_TRACE(stage);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(), "--raw-tier-crash-writer",
            m_.data_dir.c_str(), (root_ + "/staging").c_str(), stage, nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    Initialize(64);
    std::string bytes;
    ASSERT_OK(ReadBytes(0, 1024, &bytes));
    const std::string expected =
        std::string(stage) == "route"
            ? std::string(32, 'p')
            : std::string(32, 'p') + std::string(64, 's') + "unsynced-tail";
    ASSERT_EQ(bytes, expected);
    uint64_t length = 0;
    ASSERT_OK(storage_->GetFileSize(BlobFileName(m_.data_dir, 1), IOOptions(),
                                    &length, nullptr));
    ASSERT_EQ(length, expected.size());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}
#endif

TEST_F(TieredStorageTest, QueuedBarriersPinBlobBeforeReclamation) {
  QueuedBarriersBeforeReclaim(false);
}
TEST_F(TieredStorageTest, QueuedBarrierFailureWakesTasksAndReclamation) {
  QueuedBarriersBeforeReclaim(true);
}

TEST_F(TieredStorageTest, BlobGCPayloadUnlinkFailureReopens) {
  ReclaimFailure("MetaBypass::BlobPayloadDeleteStatus", true);
}
TEST_F(TieredStorageTest, BlobGCFinalDirectorySyncFailureReopens) {
  ReclaimFailure("MetaBypass::BlobFinalDirSyncStatus", false);
}
TEST_F(TieredStorageTest, BlobGCWaitsForLocalReadPin) {
  Initialize();
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  TestGate reading, retiring;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierLocalReadPinned",
                                        [&](void*) { reading.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBlobRetiring",
                                        [&](void*) { retiring.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  Status read, reclaimed;
  std::string value;
  uint64_t bytes = 0;
  std::thread reader([&] { read = ReadBytes(0, blob.size(), &value); });
  reading.WaitUntilBlocked();
  std::thread gc([&] { reclaimed = storage_->ReclaimBlob(1, &bytes); });
  retiring.WaitUntilBlocked();
  EXPECT_OK(LocalExists());
  retiring.Release();
  storage_->Start();
  EXPECT_OK(LocalExists());
  reading.Release();
  reader.join();
  gc.join();
  ASSERT_OK(read);
  ASSERT_EQ(value, blob);
  ASSERT_OK(reclaimed);
  ASSERT_GT(bytes, blob.size());
  ASSERT_TRUE(LocalExists().IsNotFound());
  ASSERT_OK(storage_->Stop());
}
TEST_F(TieredStorageTest, BlobGCWaitsForInFlightMigration) {
  Initialize();
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  TestGate migrating, retiring;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierExtentSynced",
                                        [&](void*) { migrating.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBlobRetiring",
                                        [&](void*) { retiring.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  storage_->Start();
  migrating.WaitUntilBlocked();
  Status reclaimed;
  uint64_t bytes = 0;
  std::atomic<bool> done{false};
  std::thread gc([&] {
    reclaimed = storage_->ReclaimBlob(1, &bytes);
    done = true;
  });
  retiring.WaitUntilBlocked();
  EXPECT_FALSE(done.load());
  EXPECT_OK(LocalExists());
  retiring.Release();
  migrating.Release();
  gc.join();
  ASSERT_OK(reclaimed);
  ASSERT_OK(storage_->Stop());
  ASSERT_TRUE(LocalExists().IsNotFound());
  std::vector<std::string> names;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  for (const auto& name : names) {
    ASSERT_NE(name, "000001.blob");
    ASSERT_NE(name.compare(0, 10, "segment-1-"), 0);
  }
}

TEST_F(TieredStorageTest, BlobGCWaitsForRemoteReadPin) {
  Initialize();
  ASSERT_OK(Append("remote payload"));
  storage_->Start();
  TestGate reading, retiring;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierRemoteReadPinned",
                                        [&](void*) { reading.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBlobRetiring",
                                        [&](void*) { retiring.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  Status read, reclaimed;
  std::string value;
  uint64_t bytes = 0;
  std::thread reader([&] { read = ReadBytes(0, 14, &value); });
  reading.WaitUntilBlocked();
  std::thread gc([&] { reclaimed = storage_->ReclaimBlob(1, &bytes); });
  retiring.WaitUntilBlocked();
  EXPECT_OK(
      fs_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr));
  retiring.Release();
  // New I/O cannot acquire a pin once retirement has been claimed.
  std::string rejected;
  EXPECT_TRUE(ReadBytes(0, 1, &rejected).IsNotFound());
  reading.Release();
  reader.join();
  gc.join();
  ASSERT_OK(read);
  ASSERT_EQ(value, "remote payload");
  ASSERT_OK(reclaimed);
  ASSERT_GT(bytes, 14);
  ASSERT_TRUE(
      fs_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
          .IsNotFound());
  ASSERT_OK(storage_->Stop());
}
TEST_F(TieredStorageTest, BlobGCDescriptorFailurePreservesPayloadAndReopens) {
  Initialize();
  ASSERT_OK(Append("remote payload"));
  storage_->Start();
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BlobDescriptorRemoved", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("sync failed");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  uint64_t bytes;
  ASSERT_TRUE(storage_->ReclaimBlob(1, &bytes).IsIOError());
  ASSERT_TRUE(storage_->Stop().IsIOError());
  std::vector<std::string> names;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  bool payload = false;
  for (const auto& name : names)
    if (name.compare(0, 10, "segment-1-") == 0) payload = true;
  ASSERT_TRUE(payload);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  names.clear();
  ASSERT_OK(storage_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  // The durable tombstone completes deletion on reopen, without resurrection.
  ASSERT_EQ(std::find(names.begin(), names.end(), "000001.blob"), names.end());
  ASSERT_TRUE(
      storage_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
          .IsNotFound());
  storage_->Start();
  ASSERT_OK(storage_->Stop());
  names.clear();
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  for (const auto& name : names)
    ASSERT_NE(name.compare(0, 10, "segment-1-"), 0);
}
TEST_F(TieredStorageTest, BlobGCTemporaryOnlyDebrisIsProjectedAndReclaimed) {
  Initialize();
  const std::string temporary = BlobFileName(m_.data_dir, 91) + ".tmp";
  ASSERT_OK(Write(fs_.get(), temporary, "incomplete descriptor"));
  ASSERT_OK(Write(fs_.get(), m_.data_dir + "/unrelated.tmp", "keep"));
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::vector<std::string> names;
  ASSERT_OK(storage_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  ASSERT_NE(std::find(names.begin(), names.end(), "000091.blob"), names.end());
  ASSERT_TRUE(
      storage_->FileExists(BlobFileName(m_.data_dir, 91), IOOptions(), nullptr)
          .IsNotFound());
  storage_->Start();
  uint64_t bytes;
  ASSERT_OK(storage_->ReclaimBlob(91, &bytes));
  ASSERT_EQ(bytes, std::string("incomplete descriptor").size());
  ASSERT_OK(storage_->Stop());
  ASSERT_TRUE(fs_->FileExists(temporary, IOOptions(), nullptr).IsNotFound());
  ASSERT_OK(
      fs_->FileExists(m_.data_dir + "/unrelated.tmp", IOOptions(), nullptr));
}
TEST_F(TieredStorageTest, BlobGCReopenDoesNotReuseRetirementTask) {
  Initialize();
  ASSERT_OK(Append("old payload"));
  storage_->Start();
  uint64_t bytes;
  ASSERT_OK(storage_->ReclaimBlob(1, &bytes));
  ASSERT_OK(storage_->Stop());
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  ASSERT_OK(Append("new payload"));  // Same number, new allocation epoch.
  storage_->Start();
  std::string value;
  ASSERT_OK(ReadBytes(0, 11, &value));
  ASSERT_EQ(value, "new payload");
  ASSERT_OK(storage_->Stop());
}
TEST_F(TieredStorageTest, ExtentBoundariesCheckpointAndPinnedVersion) {
  Initialize();
  std::string expected;
  for (int i = 0; i < 64; ++i) {
    const std::string part(i % 7 + 1, 'a' + i % 26);
    ASSERT_OK(Append(part));
    expected += part;
  }
  // An in-flight read must retain the old size and mapping after publication.
  TestGate pinned;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierRemoteReadPinned",
                                        [&](void*) { pinned.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::string old;
  Status read;
  std::thread reader([&] { read = ReadBytes(0, expected.size() + 32, &old); });
  pinned.WaitUntilBlocked();
  Status append = Append("new-tail");
  pinned.Release();
  reader.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(append);
  ASSERT_OK(read);
  ASSERT_EQ(old, expected);
  expected += "new-tail";
  // Every boundary plus zero-length, EOF, short and cross-extent reads.
  for (size_t offset = 0; offset <= expected.size() + 1; ++offset) {
    for (size_t length : {size_t{0}, size_t{1}, size_t{11}, expected.size()}) {
      std::string bytes;
      ASSERT_OK(ReadBytes(offset, length, &bytes));
      ASSERT_EQ(bytes,
                expected.substr(std::min(offset, expected.size()), length));
    }
  }
  const uint32_t seed = Env::Default()->NowMicros() & 0xffffffffU;
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random random(seed);
  ASSERT_LT(expected.size(),
            lossless_cast<size_t>(std::numeric_limits<int>::max()));
  const int bound = static_cast<int>(expected.size()) + 1;
  for (int i = 0; i < 100; ++i) {
    const size_t offset = random.Uniform(bound);
    const size_t length = random.Uniform(bound);
    std::string bytes;
    ASSERT_OK(ReadBytes(offset, length, &bytes));
    ASSERT_EQ(bytes, expected.substr(offset, length));
  }
  const size_t prefix = expected.size() - 3;
  const std::string checkpoint = root_ + "/checkpoint";
  ASSERT_TRUE(
      storage_->SaveCheckpoint(checkpoint, {{1, prefix}}).IsCorruption());
  ASSERT_TRUE(fs_->FileExists(checkpoint, IOOptions(), nullptr).IsNotFound());
  ASSERT_OK(storage_->SaveCheckpoint(checkpoint, {{1, expected.size()}}));
  std::string saved;
  ASSERT_OK(Read(fs_.get(), checkpoint, &saved));
  ASSERT_TRUE(
      storage_->SaveCheckpoint(checkpoint, {{1, prefix}}).IsCorruption());
  std::string unchanged;
  ASSERT_OK(Read(fs_.get(), checkpoint, &unchanged));
  ASSERT_EQ(unchanged, saved);
  ASSERT_OK(Append("after-checkpoint"));
  ASSERT_OK(storage_->LoadCheckpoint(checkpoint));
  std::string bytes;
  ASSERT_OK(ReadBytes(0, expected.size() + 32, &bytes));
  ASSERT_EQ(bytes, expected);
}

TEST_F(TieredStorageTest, SatisfiedPersistAndBarrierSkipIO) {
  Initialize();
  ASSERT_OK(Append("durable"));
  const auto before = TierStats();
  // No worker is needed: covered requests must not enter its queue.
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(storage_->Persist({{1, 7}}));
    ASSERT_OK(CommitPending());
  }
  ASSERT_EQ(RequestedLength(), 0);
  const auto after = TierStats();
  ASSERT_EQ(after.tier_persist_skips - before.tier_persist_skips, 3);
  ASSERT_EQ(after.tier_commit_skips - before.tier_commit_skips, 3);
  ASSERT_EQ(after.tier_extent_fsyncs, before.tier_extent_fsyncs);
  ASSERT_EQ(after.tier_commit_dir_syncs, before.tier_commit_dir_syncs);
  storage_->Cancel(Status::IOError("sticky error"));
  ASSERT_TRUE(storage_->Persist({{1, 7}}).IsIOError());
}

TEST_F(TieredStorageTest, CommitSkipRequiresExactMappingAndNoPendingWriter) {
  Initialize();
  ASSERT_OK(Append("old"));
  ASSERT_OK(Append("tail"));
  auto before = TierStats();
  ASSERT_OK(ReplaceExtent(false));
  auto after = TierStats();
  ASSERT_EQ(after.tier_commit_skips, before.tier_commit_skips);
  ASSERT_EQ(after.tier_commit_dir_syncs - before.tier_commit_dir_syncs, 1);
  ASSERT_EQ(after.local_commit_successes - before.local_commit_successes, 1);
  ASSERT_EQ(after.local_metadata_dir_syncs - before.local_metadata_dir_syncs,
            1);
  before = after;
  ASSERT_OK(ReplaceExtent(true));
  after = TierStats();
  ASSERT_EQ(after.tier_commit_skips, before.tier_commit_skips);
  ASSERT_EQ(after.tier_commit_dir_syncs, before.tier_commit_dir_syncs);
  ASSERT_EQ(after.local_commit_successes - before.local_commit_successes, 1);
  ASSERT_EQ(after.local_metadata_dir_syncs - before.local_metadata_dir_syncs,
            1);
  before = after;
  ASSERT_OK(CommitOldWithPendingTail());
  after = TierStats();
  ASSERT_EQ(after.tier_commit_skips, before.tier_commit_skips);
  ASSERT_EQ(after.tier_extent_fsyncs - before.tier_extent_fsyncs, 1);
}

TEST_F(TieredStorageTest, SealedTransitionAndPendingTailArePersisted) {
  Initialize();
  ASSERT_OK(Append("old"));
  ASSERT_OK(AppendPending("tail"));
  auto before = TierStats();
  ASSERT_OK(CommitPending());
  ASSERT_FALSE(RemoteSealed());
  ASSERT_EQ(ExtentLengths(true), (std::vector<uint64_t>{3, 4}));
  auto after = TierStats();
  ASSERT_EQ(after.tier_commit_skips, before.tier_commit_skips);
  ASSERT_GT(after.tier_extent_fsyncs, before.tier_extent_fsyncs);
  before = after;
  ASSERT_OK(SealPending());
  ASSERT_TRUE(RemoteSealed());
  after = TierStats();
  ASSERT_EQ(after.tier_commit_skips, before.tier_commit_skips);
  ASSERT_GT(after.tier_commit_dir_syncs, before.tier_commit_dir_syncs);
}

TEST_F(TieredStorageTest, PersistKeepsEachCallersWatermark) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> one, two;
  ASSERT_OK(LocalWriter(1, &one));
  ASSERT_OK(LocalWriter(2, &two));
  ASSERT_OK(one->Append("abc", IOOptions(), nullptr));
  ASSERT_OK(two->Append("abcdefg", IOOptions(), nullptr));
  ASSERT_OK(one->Close(IOOptions(), nullptr));
  ASSERT_OK(two->Close(IOOptions(), nullptr));
  TestGate requested;
  std::atomic<int> calls{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierPersistRequested",
                                        [&](void*) {
                                          if (++calls == 1) requested.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status high;
  std::atomic<bool> completed{false};
  std::thread waiter([&] {
    high = storage_->Persist({{1, 3}, {2, 7}});
    completed = true;
  });
  requested.WaitUntilBlocked();
  requested.Release();
  EXPECT_EQ(RequestedLength(), 3);
  EXPECT_OK(Migrate(3));
  NotifyProgress();
  EXPECT_OK(storage_->Persist({{1, 3}}));
  EXPECT_FALSE(completed.load());
  storage_->Start();
  waiter.join();
  ASSERT_OK(high);
  ASSERT_TRUE(completed.load());
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, SatisfiedPersistStillValidatesLocalWriter) {
  Initialize();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("abcdefgh", &writer));
  ASSERT_OK(Migrate(3));
  ASSERT_TRUE(storage_->Persist({{1, 3}}).IsInvalidArgument());
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_TRUE(storage_->Persist({{1, 3}}).IsInvalidArgument());
}

TEST_F(TieredStorageTest,
       PersistFailureDoesNotAdvanceDurableOrReleaseCapacity) {
  auto failing = std::make_shared<ExtentFailureFileSystem>(fs_);
  fs_ = failing;
  Initialize();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("payload", &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  const auto before = TierStats();
  failing->fail_fsync = true;
  storage_->Start();
  ASSERT_TRUE(storage_->Persist({{1, 7}}).IsIOError());
  ASSERT_TRUE(storage_->Stop().IsIOError());
  ASSERT_TRUE(ExtentLengths(true).empty());
  ASSERT_OK(LocalExists());
  const auto after = TierStats();
  // AddStats only merges counters; the storage error has its own accessor.
  ASSERT_TRUE(storage_->Error().IsIOError());
  ASSERT_EQ(after.staging_bytes, before.staging_bytes);
  ASSERT_EQ(after.tier_persist_skips, 0);
  ASSERT_EQ(after.tier_commit_skips, before.tier_commit_skips);
}

TEST_F(TieredStorageTest, SatisfiedPersistStillReturnsShutdown) {
  Initialize();
  ASSERT_OK(Append("durable"));
  storage_->Start();
  ASSERT_OK(storage_->Stop());
  ASSERT_TRUE(storage_->Persist({{1, 7}}).IsShutdownInProgress());
}

TEST_F(TieredStorageTest, MigrationAggregatesAcrossSchedulingYields) {
  constexpr uint64_t chunk = 4 * 1024 * 1024, extent = 8 * chunk;
  Initialize(extent + 17);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(std::string(extent + 17, 'm'), &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  const auto before = TierStats();
  for (uint64_t i = 1; i <= 7; ++i) {
    ASSERT_OK(MigrateChunk(extent + 17));
    ASSERT_EQ(HddEnd(), 0);
    ASSERT_TRUE(ExtentLengths(true).empty());
    ASSERT_OK(LocalExists());
    ASSERT_EQ(TierStats().tier_extent_fsyncs, before.tier_extent_fsyncs);
    ASSERT_EQ(TierStats().tier_commit_dir_syncs, before.tier_commit_dir_syncs);
  }
  ASSERT_OK(MigrateChunk(extent + 17));
  ASSERT_EQ(HddEnd(), extent);
  ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>{extent});
  ASSERT_OK(MigrateChunk(extent + 17));
  ASSERT_EQ(ExtentLengths(true), (std::vector<uint64_t>{extent, 17}));
  ASSERT_TRUE(RemoteSealed());
  ASSERT_TRUE(LocalExists().IsNotFound());
  ASSERT_EQ(TierStats().tier_extent_fsyncs - before.tier_extent_fsyncs, 2);
  std::string bytes;
  ASSERT_OK(ReadBytes(extent - 3, 20, &bytes));
  ASSERT_EQ(bytes, std::string(20, 'm'));
}

TEST_F(TieredStorageTest, Migration256MiBUsesEightDataSyncs) {
  constexpr uint64_t chunk = 4 * 1024 * 1024;
  constexpr uint64_t size = 256 * 1024 * 1024;
  Initialize(size);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("", &writer));
  const std::string bytes(chunk, 'x');
  for (uint64_t offset = 0; offset < size; offset += chunk)
    ASSERT_OK(writer->Append(bytes, IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(Migrate(size));
  ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>(8, 32 * 1024 * 1024));
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.tier_extent_fsyncs, 8);
  ASSERT_GE(stats.tier_commit_dir_syncs, 2);
}

TEST_F(TieredStorageTest, PendingSnapshotsAndMixedExtentLengths) {
  constexpr uint64_t old_extent = 4 * 1024 * 1024;
  constexpr uint64_t extent = 32 * 1024 * 1024;
  Initialize();
  // Short 4 MiB segments remain readable beside full 32 MiB segments.
  ASSERT_OK(Append(std::string(old_extent, 'o')));
  ASSERT_OK(AppendPending("a"));
  ASSERT_NO_FATAL_FAILURE(CheckPendingSnapshot());
  ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>{old_extent});
  ASSERT_OK(AppendPending(std::string(extent - 5, 'n')));
  ASSERT_OK(AppendPending("z"));
  ASSERT_OK(CommitPending());
  ASSERT_EQ(ExtentLengths(true),
            (std::vector<uint64_t>{old_extent, extent, 1}));
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::string bytes;
  ASSERT_OK(ReadBytes(old_extent - 1, 7, &bytes));
  ASSERT_EQ(bytes, "oatailn");
  ASSERT_OK(ReadBytes(old_extent + extent - 1, 2, &bytes));
  ASSERT_EQ(bytes, "nz");
}

TEST_F(TieredStorageTest, PartialCommitNeverExtendsDurablePhysicalExtent) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("abc", &writer));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(Migrate(3));
  ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>{3});
  const auto old_path = FirstExtentPath();
  ASSERT_OK(writer->Append("defgh", IOOptions(), nullptr));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(Migrate(8));
  ASSERT_EQ(ExtentLengths(true), (std::vector<uint64_t>{3, 5}));
  uint64_t size;
  ASSERT_OK(fs_->GetFileSize(old_path, IOOptions(), &size, nullptr));
  ASSERT_EQ(size, 3);
  ASSERT_FALSE(RemoteSealed());
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_TRUE(RemoteSealed());
}

TEST_F(TieredStorageTest, CommitFailureOrderKeepsPreviousDurableVersion) {
  auto counting = std::make_shared<ExtentFailureFileSystem>(fs_);
  fs_ = counting;
  Initialize();
  ASSERT_OK(Append("old"));
  for (const std::string stage :
       {"MetaBypass::TierExtentSynced", "MetaBypass::TierDependenciesSynced",
        "MetaBypass::TierDescriptorSynced",
        "MetaBypass::TierPublicationSynced"}) {
    SCOPED_TRACE(stage);
    std::vector<std::string> visited;
    for (const std::string point :
         {"MetaBypass::TierExtentSynced", "MetaBypass::TierDependenciesSynced",
          "MetaBypass::TierDescriptorSynced",
          "MetaBypass::TierPublicationSynced"}) {
      SyncPoint::GetInstance()->SetCallBack(point, [&, point](void* arg) {
        auto* status = static_cast<Status*>(arg);
        if (status->ok()) {
          visited.push_back(point);
          if (point == stage) *status = Status::IOError("commit failure");
        }
      });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_TRUE(Append("new").IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_EQ(visited.back(), stage);
    ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>{3});
    // These failures occur after the extent handle has closed successfully.
    // Neither worker teardown nor destruction may close that handle again.
    const uint64_t closes = counting->closes.load();
    storage_->Start();
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    ASSERT_EQ(counting->closes.load(), closes);
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
    ASSERT_OK(EnsureDir(fs_.get(), m_.data_dir));
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    ASSERT_OK(Append("old"));
  }
}

TEST_F(TieredStorageTest, CancelClosesPendingTailWithoutPublishing) {
  Initialize();
  ASSERT_OK(Append("old"));
  ASSERT_OK(AppendPending("uncommitted"));
  storage_->Cancel(Status::IOError("cancel"));
  storage_->Start();
  ASSERT_TRUE(storage_->Stop().IsIOError());
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::string bytes;
  ASSERT_OK(ReadBytes(0, 64, &bytes));
  ASSERT_EQ(bytes, "olduncommitted");
}

TEST_F(TieredStorageTest, ReclaimClosesAndDeletesPendingTail) {
  Initialize();
  ASSERT_OK(Append("old"));
  ASSERT_OK(AppendPending("pending"));
  storage_->Start();
  uint64_t reclaimed = 0;
  ASSERT_OK(storage_->ReclaimBlob(1, &reclaimed));
  ASSERT_GT(reclaimed, 10);
  ASSERT_OK(storage_->Stop());
  std::vector<std::string> names;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  for (const auto& name : names)
    ASSERT_NE(name.compare(0, 10, "segment-1-"), 0);
}

TEST_F(TieredStorageTest, FailedExtentFsyncIsCountedBeforeAnyDirectorySync) {
  auto failing = std::make_shared<ExtentFailureFileSystem>(fs_);
  fs_ = failing;
  Initialize();
  ASSERT_OK(AppendPending("tail"));
  const auto before = TierStats();
  failing->fail_fsync = true;
  ASSERT_TRUE(CommitPending().IsIOError());
  ASSERT_EQ(TierStats().tier_extent_fsyncs - before.tier_extent_fsyncs, 1);
  ASSERT_EQ(TierStats().tier_commit_dir_syncs, before.tier_commit_dir_syncs);
  ASSERT_TRUE(ExtentLengths(true).empty());
  ASSERT_OK(storage_->Stop());
  ASSERT_EQ(failing->closes.load(), 1);
}

TEST_F(TieredStorageTest, ReclaimCloseFailurePreservesDescriptorAndPayload) {
  auto failing = std::make_shared<ExtentFailureFileSystem>(fs_);
  fs_ = failing;
  Initialize();
  ASSERT_OK(Append("old"));
  ASSERT_OK(AppendPending("pending"));
  std::string before;
  ASSERT_OK(Read(fs_.get(), BlobFileName(m_.data_dir, 1), &before));
  failing->fail_close = true;
  storage_->Start();
  uint64_t reclaimed = 0;
  ASSERT_TRUE(storage_->ReclaimBlob(1, &reclaimed).IsIOError());
  ASSERT_TRUE(storage_->Stop().IsIOError());
  std::string after;
  ASSERT_OK(Read(fs_.get(), BlobFileName(m_.data_dir, 1), &after));
  ASSERT_EQ(after, before);
  ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>{3});
  // One committed close, one failed GC close, then worker teardown retry.
  ASSERT_EQ(failing->closes.load(), 3);
}

TEST_F(TieredStorageTest, OrphanArchiveSealsTailAtCommit) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("orphan", &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->ArchiveUnreferenced({}));
  ASSERT_TRUE(LocalExists().IsNotFound());
  ASSERT_EQ(ExtentLengths(true), std::vector<uint64_t>{6});
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.tier_extent_fsyncs, 1);
  ASSERT_GE(stats.tier_commit_dir_syncs, 2);
}

TEST_F(TieredStorageTest, ActiveAndPartialMigrationRetainLocalSource) {
  Initialize(128);
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  ASSERT_OK(Migrate(blob.size()));
  ASSERT_EQ(HddEnd(), 0);
  ASSERT_FALSE(RemoteRoute());
  ASSERT_OK(LocalExists());
  std::string bytes;
  ASSERT_OK(ReadBytes(0, blob.size(), &bytes));
  ASSERT_EQ(bytes, blob);
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(Migrate(blob.size()));
  ASSERT_TRUE(RemoteRoute());
  ASSERT_FALSE(RemoteSealed());
  ASSERT_TRUE(LocalExists().IsNotFound());
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_TRUE(RemoteSealed());
}

TEST_F(TieredStorageTest, LocalReadErrorReleasesPinBeforeMigration) {
  auto failing = std::make_shared<LocalReadFailureFileSystem>(fs_);
  fs_ = failing;
  Initialize(128);
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  failing->local_path = FirstLocalPath();
  uint64_t pins_at_close = 0;
  failing->on_close = [&] { pins_at_close = LocalPins(); };
  std::string bytes;
  ASSERT_TRUE(ReadBytes(0, blob.size(), &bytes).IsIOError());
  failing->on_close = {};
  ASSERT_EQ(pins_at_close, 1);
  ASSERT_EQ(failing->closed_handles.load(), 1);
  ASSERT_EQ(LocalPins(), 0);
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(Migrate(blob.size()));
  ASSERT_TRUE(LocalExists().IsNotFound());
  ASSERT_OK(ReadBytes(0, blob.size(), &bytes));
  ASSERT_EQ(bytes, blob);
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.staging_bytes, 0);
}

TEST_F(TieredStorageTest, DescriptorCommitFailureRetainsValidLocalSource) {
  Initialize(128);
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierDescriptorSynced", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("descriptor commit failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status migrate = Migrate(blob.size());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_TRUE(migrate.IsIOError());
  ASSERT_EQ(HddEnd(), 0);
  ASSERT_FALSE(RemoteRoute());
  ASSERT_OK(LocalExists());
  std::string bytes;
  ASSERT_OK(ReadBytes(0, blob.size(), &bytes));
  ASSERT_EQ(bytes, blob);
  ASSERT_EQ(TierStats().staging_bytes, blob.size());
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  ASSERT_OK(Migrate(blob.size()));
  ASSERT_TRUE(LocalExists().IsNotFound());
}

TEST_F(TieredStorageTest, MigratePinnedLocalReadBeforeReclaimingSpace) {
  Initialize(128);
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  TestGate local_read, migrated;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierLocalReadPinned",
                                        [&](void*) { local_read.Block(); });
  std::mutex reclaimed_mutex;
  std::condition_variable reclaimed_cv;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierMigrationComplete", [&](void*) {
        migrated.Block();
        std::lock_guard<std::mutex> lock(reclaimed_mutex);
        reclaimed_cv.notify_all();
      });
  std::atomic<int> remote_reads{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierRemoteReadPinned",
                                        [&](void*) { ++remote_reads; });
  SyncPoint::GetInstance()->EnableProcessing();
  Status read;
  std::string old;
  std::thread reader([&] { read = ReadBytes(0, blob.size(), &old); });
  local_read.WaitUntilBlocked();
  Status close = writer->Close(IOOptions(), nullptr);
  storage_->Start();
  migrated.WaitUntilBlocked();
  std::string remote;
  Status remote_status = ReadBytes(0, blob.size(), &remote);
  MetaBypassStats before;
  storage_->AddStats(&before);
  Status exists = LocalExists();
  const bool switched = RemoteRoute();
  const uint64_t pins = LocalPins();
  local_read.Release();
  reader.join();
  migrated.Release();
  {
    std::unique_lock<std::mutex> lock(reclaimed_mutex);
    EXPECT_TRUE(reclaimed_cv.wait_for(lock, std::chrono::seconds(20), [&] {
      return LocalExists().IsNotFound();
    }));
  }
  Status stop = storage_->Stop();
  ASSERT_OK(stop);
  ASSERT_OK(read);
  ASSERT_OK(close);
  ASSERT_OK(remote_status);
  ASSERT_OK(exists);
  ASSERT_TRUE(switched);
  ASSERT_EQ(pins, 1);
  ASSERT_EQ(LocalPins(), 0);
  ASSERT_EQ(remote_reads.load(), 1);
  ASSERT_OK(before.error);
  ASSERT_EQ(before.staging_bytes, blob.size());
  ASSERT_EQ(old, blob);
  ASSERT_EQ(remote, blob);
  ASSERT_TRUE(
      fs_->FileExists(BlobFileName(m_.staging_dir, 1), IOOptions(), nullptr)
          .IsNotFound());
  MetaBypassStats after;
  storage_->AddStats(&after);
  ASSERT_OK(after.error);
  ASSERT_EQ(after.staging_bytes, 0);
}
TEST_F(TieredStorageTest, FrozenSegmentMigratesBeforeLogicalClose) {
  constexpr uint64_t segment = 32 * 1024 * 1024;
  Initialize(2 * segment);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(std::string(segment, 'a'), &writer));
  TestGate reclaim;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBeforeQuotaRelease",
                                        [&](void*) { reclaim.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  storage_->Start();
  reclaim.WaitUntilBlocked();
  EXPECT_TRUE(RemoteRoute());
  EXPECT_FALSE(RemoteSealed());
  EXPECT_EQ(HddEnd(), segment);
  EXPECT_EQ(TierStats().staging_bytes, segment);
  EXPECT_TRUE(LocalExists().IsNotFound());
  EXPECT_OK(writer->Append("next", IOOptions(), nullptr));
  reclaim.Release();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, segment + 4}}));
  std::string bytes;
  ASSERT_OK(ReadBytes(segment - 2, 6, &bytes));
  ASSERT_EQ(bytes, "aanext");
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, WorkerReclaimsQuotaBeforePendingHddBarrier) {
  Initialize(14);
  ASSERT_OK(CreateClosedLocalSegments(2));
  const auto source = FirstExtentPath();
  TestGate second_migration;
  unsigned migrations = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationSelected",
                                        [&](void*) {
                                          if (++migrations == 2)
                                            second_migration.Block();
                                        });
  Status persisted;
  auto waiter = StartWorkerWithPersist(14, &persisted);
  second_migration.WaitUntilBlocked();
  EXPECT_EQ(HddEnd(), 7);
  EXPECT_EQ(PendingBarrierCount(), 1);
  EXPECT_FALSE(FinalPublished());
  EXPECT_EQ(RetiredLocalCount(), 0);
  EXPECT_EQ(TierStats().staging_bytes, 7);
  EXPECT_TRUE(fs_->FileExists(source, IOOptions(), nullptr).IsNotFound());
  std::unique_ptr<FSWritableFile> next;
  Status created = LocalWriter(2, &next);
  EXPECT_OK(created);
  if (created.ok()) {
    EXPECT_OK(next->Append("reused!", IOOptions(), nullptr));
    EXPECT_EQ(LocalExtentBytes(2), 7);
    EXPECT_EQ(TierStats().staging_bytes, 14);
    EXPECT_OK(next->Close(IOOptions(), nullptr));
  }
  second_migration.Release();
  waiter.join();
  Status stopped = storage_->Stop();
  ASSERT_OK(persisted);
  ASSERT_OK(stopped);
}

TEST_F(TieredStorageTest, WorkerReclaimBacklogDoesNotDrainBeforeHddBarrier) {
  Initialize(28);
  ASSERT_OK(CreateClosedLocalSegments(4));
  TestGate local_read, third_published, fourth_migration;
  unsigned published = 0, migrations = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierLocalReadPinned",
                                        [&](void*) { local_read.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierRouteDurable",
                                        [&](void*) {
                                          if (++published == 3)
                                            third_published.Block();
                                        });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationSelected",
                                        [&](void*) {
                                          if (++migrations == 4)
                                            fourth_migration.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status read, persisted;
  std::string bytes;
  std::thread reader([&] { read = ReadBytes(0, 28, &bytes); });
  local_read.WaitUntilBlocked();
  auto waiter = StartWorkerWithPersist(28, &persisted);
  third_published.WaitUntilBlocked();
  EXPECT_EQ(RetiredLocalCount(), 3);
  EXPECT_EQ(TierStats().staging_bytes, 28);
  local_read.Release();
  reader.join();
  third_published.Release();
  fourth_migration.WaitUntilBlocked();
  // One source was reclaimed, then the next HDD task was selected while two
  // more unpinned sources remained available for reclamation.
  EXPECT_EQ(RetiredLocalCount(), 2);
  EXPECT_EQ(TierStats().staging_bytes, 21);
  EXPECT_EQ(LocalPins(), 0);
  EXPECT_EQ(HddEnd(), 21);
  EXPECT_EQ(PendingBarrierCount(), 1);
  fourth_migration.Release();
  waiter.join();
  Status stopped = storage_->Stop();
  ASSERT_OK(read);
  ASSERT_EQ(bytes, "payloadpayloadpayloadpayload");
  ASSERT_OK(persisted);
  ASSERT_OK(stopped);
}

TEST_F(TieredStorageTest,
       WorkerReclaimsReleasedReaderPinWithHddBarrierPending) {
  Initialize(21);
  ASSERT_OK(CreateClosedLocalSegments(3));
  const auto source = FirstExtentPath();
  TestGate local_read, second_migration, third_migration;
  unsigned migrations = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierLocalReadPinned",
                                        [&](void*) { local_read.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationSelected",
                                        [&](void*) {
                                          if (++migrations == 2)
                                            second_migration.Block();
                                          else if (migrations == 3)
                                            third_migration.Block();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Status read, persisted;
  std::string bytes;
  std::thread reader([&] { read = ReadBytes(0, 7, &bytes); });
  local_read.WaitUntilBlocked();
  auto waiter = StartWorkerWithPersist(21, &persisted);
  second_migration.WaitUntilBlocked();
  EXPECT_EQ(HddEnd(), 7);
  EXPECT_EQ(PendingBarrierCount(), 1);
  EXPECT_EQ(LocalPins(), 1);
  EXPECT_EQ(RetiredLocalCount(), 1);
  EXPECT_EQ(TierStats().staging_bytes, 21);
  EXPECT_OK(fs_->FileExists(source, IOOptions(), nullptr));
  local_read.Release();
  reader.join();
  second_migration.Release();
  third_migration.WaitUntilBlocked();
  EXPECT_EQ(LocalPins(), 0);
  EXPECT_EQ(RetiredLocalCount(), 1);
  EXPECT_EQ(TierStats().staging_bytes, 14);
  EXPECT_EQ(HddEnd(), 14);
  EXPECT_EQ(PendingBarrierCount(), 1);
  EXPECT_TRUE(fs_->FileExists(source, IOOptions(), nullptr).IsNotFound());
  third_migration.Release();
  waiter.join();
  Status stopped = storage_->Stop();
  ASSERT_OK(read);
  ASSERT_EQ(bytes, "payload");
  ASSERT_OK(persisted);
  ASSERT_OK(stopped);
}

TEST_F(TieredStorageTest, WorkerLocalDeleteFailureRetainsQuotaAndFirstError) {
  CheckWorkerReclaimFailure(true);
}

TEST_F(TieredStorageTest, WorkerLocalDirsyncFailureRetainsQuotaAndFirstError) {
  CheckWorkerReclaimFailure(false);
}

TEST_F(TieredStorageTest, BlockedPrefixAllowsSuffixButSyncWaitsForHddPrefix) {
  Initialize(64);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(std::string(32, 'p'), &writer));
  TestGate copying, waiting;
  std::atomic<bool> once{true}, synced{false};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigration",
                                        [&](void*) {
                                          if (once.exchange(false))
                                            copying.Block();
                                        });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBarrierWaiting",
                                        [&](void*) { waiting.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  storage_->Start();
  ASSERT_OK(writer->Append(std::string(64, 's'), IOOptions(), nullptr));
  copying.WaitUntilBlocked();
  ASSERT_OK(writer->Append("tail", IOOptions(), nullptr));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  std::string bytes;
  ASSERT_OK(ReadBytes(28, 72, &bytes));
  ASSERT_EQ(bytes, std::string(4, 'p') + std::string(64, 's') + "tail");
  Status sync;
  std::thread caller([&] {
    sync = writer->Sync(IOOptions(), nullptr);
    synced = true;
  });
  waiting.WaitUntilBlocked();
  waiting.Release();
  EXPECT_EQ(HddEnd(), 0);
  EXPECT_FALSE(synced.load());
  copying.Release();
  caller.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(sync);
  ASSERT_EQ(HddEnd(), 100);
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_TRUE(RemoteSealed());
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.tier_append_tasks, 0);
  ASSERT_EQ(stats.tier_suffix_appends, 2);
  ASSERT_EQ(stats.staging_spill_bytes, 68);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, LargeAppendSplitsQuotaAndIndependentSuffixExtents) {
  constexpr uint64_t segment = 32 * 1024 * 1024;
  Initialize(segment);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(LocalWriter(1, &writer));
  const std::string bytes(2 * segment + 17, 'x');
  // No worker: the whole Append still completes, proving it cannot wait for
  // prefix migration. Only explicit Sync/Close waits for that dependency.
  ASSERT_OK(writer->Append(bytes, IOOptions(), nullptr));
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_staging_bytes, segment);
  ASSERT_EQ(stats.staging_bytes, segment);
  ASSERT_EQ(stats.tier_append_tasks, 0);
  ASSERT_GT(stats.tier_suffix_appends, 0);
  std::string actual;
  ASSERT_OK(ReadBytes(segment - 3, 7, &actual));
  ASSERT_EQ(actual, "xxxxxxx");
  storage_->Start();
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, bytes.size()}}));
  ASSERT_EQ(ExtentLengths(true), (std::vector<uint64_t>{segment, segment, 17}));
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, MigrationPublicationFaultNeverReleasesSourceQuota) {
  for (const char* point :
       {"MetaBypass::TierExtentSynced", "MetaBypass::TierDependenciesSynced",
        "MetaBypass::TierDescriptorSynced", "MetaBypass::TierDescriptorRenamed",
        "MetaBypass::TierPublicationSynced"}) {
    SCOPED_TRACE(point);
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob("payload", &writer));
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    SyncPoint::GetInstance()->SetCallBack(point, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("publication injection");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    ASSERT_TRUE(storage_->Persist({{1, 7}}).IsIOError());
    ASSERT_TRUE(storage_->Stop().IsIOError());
    ASSERT_EQ(HddEnd(), 0);
    ASSERT_EQ(TierStats().staging_bytes, 7);
    ASSERT_OK(LocalExists());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    std::string actual;
    ASSERT_OK(ReadBytes(0, 7, &actual));
    ASSERT_EQ(actual, "payload");
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, UnlinkDirectoryFailureRetainsQuotaUntilRestart) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("payload", &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  TestGate unlinked;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierLocalUnlinked", [&](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("unlink directory failure");
        unlinked.Block();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  storage_->Start();
  unlinked.WaitUntilBlocked();
  EXPECT_EQ(TierStats().staging_bytes, 7);
  EXPECT_EQ(HddEnd(), 7);
  unlinked.Release();
  // Join after the hook returned and published the sticky error.
  SyncPoint::GetInstance()->DisableProcessing();
  Status stopped = storage_->Stop();
  ASSERT_TRUE(stopped.IsIOError());
  ASSERT_EQ(TierStats().staging_bytes, 7);
}

TEST_F(TieredStorageTest,
       RegisteredFlushedTailRecoversWithoutClaimingPriorDurability) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("flushed", &writer));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  ASSERT_EQ(HddEnd(), 0);
  const auto descriptor = LocalDescriptorPath();
  std::string before;
  ASSERT_OK(Read(fs_.get(), descriptor, &before));
  writer.reset();
  ASSERT_OK(storage_->Stop());
  storage_.reset();
  // Reopen performs real synchronization before promoting the recovered tail.
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::string actual;
  ASSERT_OK(ReadBytes(0, 7, &actual));
  ASSERT_EQ(actual, "flushed");
  ASSERT_EQ(HddEnd(), 0);
  storage_->Start();
  ASSERT_OK(storage_->Persist({{1, 7}}));
  ASSERT_EQ(HddEnd(), 7);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, LegacyDescriptorRejectedWithoutChangingFiles) {
  Initialize();
  storage_.reset();
  const auto descriptor = BlobFileName(m_.data_dir, 1);
  const std::string old = "MBT2 0\nold descriptor";
  ASSERT_OK(Write(fs_.get(), descriptor, old));
  ASSERT_OK(Write(fs_.get(), m_.staging_dir + "/000001.blob", "old payload"));
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  std::string actual;
  ASSERT_OK(Read(fs_.get(), descriptor, &actual));
  ASSERT_EQ(actual, old);
  ASSERT_OK(Read(fs_.get(), m_.staging_dir + "/000001.blob", &actual));
  ASSERT_EQ(actual, "old payload");
}

TEST_F(TieredStorageTest,
       RestorePrunesLaterMixedDescriptorsAndRetriesPublication) {
  for (const char* fault : {"MetaBypass::TierRestoreDescriptorPublished",
                            "MetaBypass::TierRestoreDescriptorRemoved",
                            "MetaBypass::TierRestoreDirectorySynced"}) {
    SCOPED_TRACE(fault);
    Initialize(128);
    ASSERT_OK(Append("checkpoint"));
    const auto point = root_ + "/selected-map";
    ASSERT_OK(storage_->SaveCheckpoint(point, {{1, 10}}));
    const auto historical_extent = FirstExtentPath();
    std::unique_ptr<FSWritableFile> later;
    ASSERT_OK(LocalWriter(2, &later));
    ASSERT_OK(later->Append("later SSD", IOOptions(), nullptr));
    ASSERT_OK(later->Flush(IOOptions(), nullptr));
    std::unique_ptr<FSWritableFile> historical;
    ASSERT_OK(LocalWriter(3, &historical));
    ASSERT_OK(historical->Append(std::string(256, 'h'), IOOptions(), nullptr));
    // The spilled writer's Close waits for its queued HDD seal and publication.
    storage_->Start();
    ASSERT_OK(historical->Close(IOOptions(), nullptr));
    const auto other_point = root_ + "/historical-map";
    ASSERT_OK(storage_->SaveCheckpoint(other_point, {{1, 10}, {3, 256}}));
    historical.reset();
    later.reset();
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.staging_dir);
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(true));
    SyncPoint::GetInstance()->SetCallBack(fault, [](void* arg) {
      *static_cast<Status*>(arg) =
          Status::IOError("restore publication injection");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_TRUE(storage_->LoadCheckpoint(point).IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(storage_->LoadCheckpoint(point));
    ASSERT_OK(fs_->FileExists(historical_extent, IOOptions(), nullptr));
    ASSERT_TRUE(
        fs_->FileExists(BlobFileName(m_.data_dir, 2), IOOptions(), nullptr)
            .IsNotFound());
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    std::string actual;
    ASSERT_OK(ReadBytes(0, 10, &actual));
    ASSERT_EQ(actual, "checkpoint");
    // The older restore pruned blob 3's current descriptor but retained its
    // payload: a different retained checkpoint can reconstruct that mapping.
    ASSERT_OK(storage_->LoadCheckpoint(other_point));
    std::unique_ptr<FSRandomAccessFile> old_reader;
    ASSERT_OK(storage_->NewRandomAccessFile(
        BlobFileName(m_.data_dir, 3), FileOptions(), &old_reader, nullptr));
    std::string scratch(256, '\0');
    Slice result;
    ASSERT_OK(old_reader->Read(0, scratch.size(), IOOptions(), &result,
                               scratch.data(), nullptr));
    ASSERT_EQ(result.ToString(), std::string(256, 'h'));
    old_reader.reset();
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, AlwaysFullSuffixFsyncDoesNotBlockAppendOrFlush) {
  CheckAlwaysBlockedSeal("MetaBypass::TierBeforeHddExtentFsync");
}
TEST_F(TieredStorageTest, AlwaysFullSuffixReadbackDoesNotBlockAppendOrFlush) {
  CheckAlwaysBlockedSeal("MetaBypass::TierPayloadVerified");
}
TEST_F(TieredStorageTest,
       AlwaysFullSuffixPublicationDoesNotBlockAppendOrFlush) {
  CheckAlwaysBlockedSeal("MetaBypass::TierDescriptorSynced");
}
TEST_F(TieredStorageTest, AlwaysThirdSealBackpressureWakesOnFirstError) {
  CheckAlwaysQueueWake("error");
}
TEST_F(TieredStorageTest, AlwaysThirdSealBackpressureWakesOnStop) {
  CheckAlwaysQueueWake("stop");
}
TEST_F(TieredStorageTest, AlwaysThirdSealBackpressureWakesOnGc) {
  CheckAlwaysQueueWake("gc");
}
TEST_F(TieredStorageTest, AlwaysSealBarriersKeepIndependentHddWatermarks) {
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_NO_FATAL_FAILURE(PrepareAlwaysSpilled(&writer));
  constexpr uint64_t extent = 32 * 1024 * 1024;
  TestGate first_seal, second_seal;
  std::atomic<unsigned> seals{0}, waits{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBeforeHddExtentFsync",
                                        [&](void*) {
                                          if (++seals == 1)
                                            first_seal.Block();
                                          else
                                            second_seal.Block();
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierBarrierWaiting", [&](void*) {
        const auto count = ++waits;
        if (count == 1) TEST_SYNC_POINT("AlwaysBarrier::LowQueued");
        if (count == 2) TEST_SYNC_POINT("AlwaysBarrier::CloseQueued");
        if (count == 3) TEST_SYNC_POINT("AlwaysBarrier::FsyncQueued");
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"AlwaysBarrier::LowQueued", "AlwaysBarrier::LowWaiting"},
       {"AlwaysBarrier::CloseQueued", "AlwaysBarrier::CloseWaiting"},
       {"AlwaysBarrier::FsyncQueued", "AlwaysBarrier::FsyncWaiting"},
       {"MetaBypass::TierPersistRequested", "AlwaysBarrier::PersistWaiting"}});
  SyncPoint::GetInstance()->EnableProcessing();
  storage_->Start();
  ASSERT_OK(writer->Append(std::string(extent - 1, 'h'), IOOptions(), nullptr));
  first_seal.WaitUntilBlocked();
  Status low, closed, fsynced, persisted;
  std::atomic<bool> low_done{false}, close_done{false}, fsync_done{false},
      persist_done{false};
  std::thread sync([&] {
    low = writer->Sync(IOOptions(), nullptr);
    low_done = true;
  });
  TEST_SYNC_POINT("AlwaysBarrier::LowWaiting");
  EXPECT_OK(writer->Append("tail", IOOptions(), nullptr));
  std::thread close([&] {
    closed = writer->Close(IOOptions(), nullptr);
    close_done = true;
  });
  TEST_SYNC_POINT("AlwaysBarrier::CloseWaiting");
  std::thread fsync([&] {
    fsynced = writer->Fsync(IOOptions(), nullptr);
    fsync_done = true;
  });
  TEST_SYNC_POINT("AlwaysBarrier::FsyncWaiting");
  std::thread persist([&] {
    persisted = storage_->Persist({{1, extent + 5}});
    persist_done = true;
  });
  TEST_SYNC_POINT("AlwaysBarrier::PersistWaiting");
  EXPECT_FALSE(low_done.load());
  EXPECT_FALSE(close_done.load());
  EXPECT_FALSE(fsync_done.load());
  EXPECT_FALSE(persist_done.load());
  EXPECT_TRUE(storage_->SaveCheckpoint(root_ + "/early", {{1, extent + 5}})
                  .IsCorruption());
  first_seal.Release();
  second_seal.WaitUntilBlocked();
  sync.join();
  EXPECT_OK(low);
  EXPECT_EQ(HddEnd(), extent + 1);
  EXPECT_FALSE(close_done.load());
  EXPECT_FALSE(fsync_done.load());
  EXPECT_FALSE(persist_done.load());
  EXPECT_FALSE(FinalPublished());
  second_seal.Release();
  close.join();
  fsync.join();
  persist.join();
  EXPECT_OK(closed);
  EXPECT_OK(fsynced);
  EXPECT_OK(persisted);
  EXPECT_TRUE(FinalPublished());
  ASSERT_OK(storage_->SaveCheckpoint(root_ + "/complete", {{1, extent + 5}}));
  ASSERT_OK(storage_->Stop());
  writer.reset();
  storage_.reset();
  Clean(m_.staging_dir);
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::string actual;
  ASSERT_OK(ReadBytes(extent - 1, 6, &actual));
  ASSERT_EQ(actual, "hhtail");
  ASSERT_TRUE(FinalPublished());
}

#ifndef OS_WIN
TEST_F(TieredStorageTest, AlwaysProcessCrashSealOwnershipAndPublication) {
  for (const char* hook :
       {"MetaBypass::TierHandoffPersisted", "MetaBypass::TierHandoffQueued",
        "MetaBypass::TierPayloadSynced", "MetaBypass::TierPayloadVerified",
        "MetaBypass::TierPublicationSynced", "MetaBypass::TierRouteDurable"}) {
    SCOPED_TRACE(hook);
    EnableTier(8192);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(), "--v5-always-crash-writer",
            m_.data_dir.c_str(), m_.staging_dir.c_str(), hook, nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    Initialize(1);
    constexpr size_t extent = 32 * 1024 * 1024;
    std::string actual;
    ASSERT_OK(ReadBytes(0, extent + 2, &actual));
    ASSERT_EQ(actual, "s" + std::string(extent, 'h'));
    storage_->Start();
    ASSERT_OK(storage_->Persist({{1, extent + 1}}));
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}
#endif

TEST_F(TieredStorageTest, AlwaysPendingPowerLossFailsClosed) {
  auto fault = std::make_shared<FaultInjectionTestFS>(fs_);
  fault->SetInjectUnsyncedDataLoss(true);
  fs_ = fault;
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_NO_FATAL_FAILURE(PrepareAlwaysSpilled(&writer));
  constexpr size_t extent = 32 * 1024 * 1024;
  ASSERT_OK(writer->Append(std::string(extent - 1, 'h'), IOOptions(), nullptr));
  ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 1);
  ASSERT_OK(writer->Append("later", IOOptions(), nullptr));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  ASSERT_OK(storage_->Stop());
  writer.reset();
  storage_.reset();
  ASSERT_OK(fault->DropUnsyncedFileData());
  ASSERT_OK(fault->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));
  fault->ResetState();
  const auto before = TierFiles();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  ASSERT_EQ(TierFiles(), before);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptiveLowPressureDoesNotWriteSsdPayload) {
  InitializeAdaptive();
  storage_->Start();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("low pressure", &writer));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, 12}}));
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.direct_blob_bytes, 12);
  ASSERT_EQ(stats.staged_blob_bytes, 0);
  ASSERT_EQ(stats.migrated_blob_bytes, 0);
  ASSERT_EQ(stats.peak_staging_bytes, 0);
  ASSERT_EQ(stats.tier_append_tasks, 0);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptiveThreeRoundTripsPreserveAllRanges) {
  InitializeAdaptive();
  storage_->Start();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  std::string expected = "hdd";
  for (unsigned round = 0; round < 3; ++round) {
    SetPressure(true);
    ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
    ASSERT_OK(writer->Flush(IOOptions(), nullptr));
    expected += "ssd";
    ASSERT_OK(writer->Sync(IOOptions(), nullptr));
    SetPressure(false);
    ASSERT_OK(writer->Append("hdd", IOOptions(), nullptr));
    expected += "hdd";
    ASSERT_OK(writer->Sync(IOOptions(), nullptr));
    std::string actual;
    ASSERT_OK(ReadBytes(0, expected.size() + 100, &actual));
    ASSERT_EQ(actual, expected);
  }
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, expected.size()}}));
  ASSERT_EQ(HddEnd(), expected.size());
  ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_to_ssd), 3);
  ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_to_hdd), 3);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptiveBlockedSealDoesNotBlockSsdAppendOrFlush) {
  CheckAdaptiveBlockedSeal("MetaBypass::TierBeforeHddExtentFsync");
}
TEST_F(TieredStorageTest, AdaptiveBlockedReadbackDoesNotBlockSsdAppendOrFlush) {
  CheckAdaptiveBlockedSeal("MetaBypass::TierPayloadVerified");
}
TEST_F(TieredStorageTest,
       AdaptiveBlockedDescriptorDoesNotBlockSsdAppendOrFlush) {
  CheckAdaptiveBlockedSeal("MetaBypass::TierDescriptorSynced");
}
TEST_F(TieredStorageTest,
       AdaptivePendingHoleRejectsRecoveryWithoutModification) {
  InitializeAdaptive();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  const auto hdd = FirstExtentPath();
  SetPressure(true);
  ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  ASSERT_OK(storage_->Stop());
  writer.reset();
  storage_.reset();
  ASSERT_OK(Write(fs_.get(), hdd, "h"));
  const auto before = TierFiles();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  ASSERT_EQ(TierFiles(), before);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptiveQuotaFallbackCanStageAgain) {
  InitializeAdaptive(8192);
  storage_->Start();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  SetPressure(true);
  ASSERT_OK(writer->Append(std::string(16384, 'x'), IOOptions(), nullptr));
  ASSERT_GT(AdaptiveStat(&MetaBypassStats::adaptive_quota_fallbacks), 0);
  ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
  ASSERT_EQ(AdaptiveStat(&MetaBypassStats::staged_blob_bytes), 3);
  ASSERT_LE(AdaptiveStat(&MetaBypassStats::peak_staging_bytes), 8192);
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, 16390}}));
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest,
       AdaptiveGlobalQueueIncludesQueuedAndStopWakesAdmission) {
  InitializeAdaptive();
  std::vector<std::unique_ptr<FSWritableFile>> writers(9);
  for (uint64_t i = 0; i < writers.size(); ++i) {
    SetPressure(false);
    ASSERT_OK(LocalWriter(i + 1, &writers[i]));
    ASSERT_OK(writers[i]->Append("hdd", IOOptions(), nullptr));
    if (i == 8) break;
    SetPressure(true);
    ASSERT_OK(writers[i]->Append("ssd", IOOptions(), nullptr));
  }
  ASSERT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 8);
  SetPressure(true);
  SyncPoint::GetInstance()->LoadDependency(
      {{"MetaBypass::TierSealQueueWaiting", "AdaptiveQueue::Waiting"}});
  Status admission;
  std::thread caller(
      [&] { admission = writers[8]->Append("ssd", IOOptions(), nullptr); });
  TEST_SYNC_POINT("AdaptiveQueue::Waiting");
  EXPECT_OK(storage_->Stop());
  caller.join();
  EXPECT_TRUE(admission.IsIOError());
  EXPECT_NE(admission.ToString().find("Shutdown"), std::string::npos);
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 0);
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_peak_pending_seals), 8);
}

TEST_F(TieredStorageTest,
       AdaptivePerBlobQueueIncludesInflightAndGcCancelsQueued) {
  InitializeAdaptive();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  TestGate inflight;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBeforeHddExtentFsync",
                                        [&](void*) { inflight.Block(); });
  storage_->Start();
  SetPressure(true);
  ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
  inflight.WaitUntilBlocked();
  SetPressure(false);
  ASSERT_OK(writer->Append("hdd", IOOptions(), nullptr));
  SetPressure(true);
  ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 2);
  SetPressure(false);
  ASSERT_OK(writer->Append("hdd", IOOptions(), nullptr));
  SetPressure(true);
  SyncPoint::GetInstance()->LoadDependency(
      {{"MetaBypass::TierSealQueueWaiting", "AdaptiveGc::Waiting"},
       {"MetaBypass::TierBlobRetiring", "AdaptiveGc::Retiring"}});
  Status admission, reclaimed;
  std::thread caller(
      [&] { admission = writer->Append("ssd", IOOptions(), nullptr); });
  TEST_SYNC_POINT("AdaptiveGc::Waiting");
  uint64_t removed = 0;
  std::thread gc([&] { reclaimed = storage_->ReclaimBlob(1, &removed); });
  TEST_SYNC_POINT("AdaptiveGc::Retiring");
  inflight.Release();
  caller.join();
  gc.join();
  EXPECT_TRUE(admission.IsAborted());
  EXPECT_OK(reclaimed);
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 0);
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_peak_pending_seals), 2);
  EXPECT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptivePowerLossCannotSkipMissingHddAndKeepSsdTail) {
  auto fault = std::make_shared<FaultInjectionTestFS>(fs_);
  fault->SetInjectUnsyncedDataLoss(true);
  fs_ = fault;
  InitializeAdaptive();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  SetPressure(true);
  ASSERT_OK(writer->Append("durable ssd", IOOptions(), nullptr));
  ASSERT_OK(FreezeAdaptiveCaller());
  ASSERT_EQ(DurablePrefix(), 0);
  ASSERT_OK(storage_->Stop());
  writer.reset();
  storage_.reset();
  ASSERT_OK(fault->DropUnsyncedFileData());
  ASSERT_OK(fault->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));
  fault->ResetState();
  const auto before = TierFiles();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  ASSERT_EQ(TierFiles(), before);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest,
       AdaptivePressureRateIsGlobalAndDoesNotAccumulateCredit) {
  CheckAdaptiveGlobalMigrationRate(true);
}
TEST_F(TieredStorageTest,
       AdaptiveHealthyRateIsGlobalAndDoesNotAccumulateCredit) {
  CheckAdaptiveGlobalMigrationRate(false);
}
TEST_F(TieredStorageTest,
       AdaptiveStalledBackgroundDoesNotStackMigrationProbes) {
  fs_ = std::make_shared<ExtentFailureFileSystem>(fs_);
  InitializeAdaptive();
  SetPressure(true);
  constexpr uint64_t chunk = 4 * 1024 * 1024;
  ASSERT_OK(CreateAdaptiveClosedSsd(1, chunk));
  ASSERT_OK(CreateAdaptiveClosedSsd(2, chunk));
  std::atomic<uint64_t> clock{10000000}, started{0};
  TestGate fsync;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierMigrationClock",
      [&](void* p) { *static_cast<uint64_t*>(p) = clock.load(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationSelected",
                                        [&](void*) { ++started; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypassTest::ExtentFsyncInFlight",
                                        [&](void*) { fsync.Block(); });
  storage_->Start();
  fsync.WaitUntilBlocked();
  EXPECT_EQ(started.load(), 1);
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::migrated_blob_bytes), chunk);
  const auto writes = AdaptiveStat(&MetaBypassStats::io_data_write_bytes);
  clock += 10 * 250000;
  for (int i = 0; i < 10; ++i) {
    WakeAdaptiveWorker();
  }
  EXPECT_EQ(started.load(), 1);
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::io_data_write_bytes), writes);
  EXPECT_EQ(HddEnd(), 0);
  fsync.Release();
  ASSERT_OK(storage_->Stop());
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
TEST_F(TieredStorageTest, AdaptiveBarrierWakesThrottleButWaitsForHddFsync) {
  CheckAdaptiveBarrierWake("MetaBypass::TierBeforeHddExtentFsync");
}
TEST_F(TieredStorageTest, AdaptiveBarrierWakesThrottleButWaitsForReadback) {
  CheckAdaptiveBarrierWake("MetaBypass::TierReadbackChunk");
}
TEST_F(TieredStorageTest, AdaptiveBarrierWakesThrottleButWaitsForPublication) {
  CheckAdaptiveBarrierWake("MetaBypass::TierDescriptorSynced");
}
TEST_F(TieredStorageTest, AdaptiveBarrierWakesThrottleButWaitsForComplete) {
  CheckAdaptiveBarrierWake("MetaBypass::TierFinalHddPublished");
}

TEST_F(TieredStorageTest, AdaptivePersistedHandoffCannotEnqueueAfterStop) {
  CheckAdaptiveDrainRace(false, false);
}
TEST_F(TieredStorageTest, AdaptivePersistedHandoffCannotEnqueueAfterGc) {
  CheckAdaptiveDrainRace(true, false);
}
TEST_F(TieredStorageTest, AdaptiveReadbackQuantumCannotRequeueAfterStop) {
  CheckAdaptiveDrainRace(false, true);
}
TEST_F(TieredStorageTest, AdaptiveReadbackQuantumCannotRequeueAfterGc) {
  CheckAdaptiveDrainRace(true, true);
}
TEST_F(TieredStorageTest, AdaptiveAdmissionCountersBeforeMigration) {
  CheckAdaptiveAdmissionCounters(false);
}
TEST_F(TieredStorageTest, AdaptiveAdmissionCountersAfterMigration) {
  CheckAdaptiveAdmissionCounters(true);
}

TEST_F(TieredStorageTest, PhysicalObserverWriteEntriesPreserveAccounting) {
  auto pressure = std::make_shared<HddPressure>(20000);
  pressure->Stop();
  auto probe = std::make_shared<ObserverProbe>(fs_);
  auto observed = ObserveHdd(probe, "/observer/data", "/observer/backup",
                             pressure, "/observer/index", "/observer/staging");
  for (const std::string area : {"index", "staging", "data", "backup"}) {
    SCOPED_TRACE(area);
    const std::string path = "/observer/" + area + "/payload";
    std::unique_ptr<FSWritableFile> file, reopened, reused;
    std::unique_ptr<FSRandomRWFile> rw;
    ASSERT_OK(observed->NewWritableFile(path, FileOptions(), &file, nullptr));
    ASSERT_OK(
        observed->ReopenWritableFile(path, FileOptions(), &reopened, nullptr));
    ASSERT_OK(observed->ReuseWritableFile(path, path, FileOptions(), &reused,
                                          nullptr));
    ASSERT_OK(observed->NewRandomRWFile(path, FileOptions(), &rw, nullptr));
    const DataVerificationInfo verification;
    const std::vector<std::function<IOStatus()>> writes{
        [&] { return file->Append("abc", IOOptions(), nullptr); },
        [&] { return file->Append("abc", IOOptions(), verification, nullptr); },
        [&] { return file->PositionedAppend("abc", 0, IOOptions(), nullptr); },
        [&] {
          return file->PositionedAppend("abc", 0, IOOptions(), verification,
                                        nullptr);
        },
        [&] { return reopened->Append("abc", IOOptions(), nullptr); },
        [&] { return reused->Append("abc", IOOptions(), nullptr); },
        [&] { return rw->Write(0, "abc", IOOptions(), nullptr); }};
    for (const auto purpose :
         {HddPurpose::kCaller, HddPurpose::kBulk, HddPurpose::kBarrier}) {
      HddPurposeScope scope(purpose);
      for (bool fail : {false, true}) {
        probe->fail = fail;
        for (const auto& write : writes) {
          // Change purpose inside the backend, leaving it changed until the
          // observer returns. Accounting must use the call's entry purpose.
          std::unique_ptr<HddPurposeScope> backend_scope;
          probe->before = [&](ObserverProbe::Op) {
            backend_scope.reset(new HddPurposeScope(HddPurpose::kBulk));
          };
          auto status = write();
          EXPECT_EQ(status.IsIOError(), fail);
          backend_scope.reset();
          probe->before = {};
        }
      }
    }
    ASSERT_OK(file->Close(IOOptions(), nullptr));
    ASSERT_OK(reopened->Close(IOOptions(), nullptr));
    ASSERT_OK(reused->Close(IOOptions(), nullptr));
    ASSERT_OK(rw->Close(IOOptions(), nullptr));
  }
  const auto stats = pressure->Snapshot();
  for (auto bytes : stats.written_bytes) EXPECT_EQ(bytes, 63);
  for (auto bytes : stats.purpose_write_bytes) EXPECT_EQ(bytes, 84);
  EXPECT_EQ(stats.failed_writes, 84);
  EXPECT_EQ(stats.calls[0], 84);
  EXPECT_EQ(stats.bytes[0], 126);
  EXPECT_EQ(probe->calls[ObserverProbe::kWrite], 168);
}

TEST_F(TieredStorageTest, PhysicalObserverSsdBypassesPressureAndForwardsIo) {
  auto pressure = std::make_shared<HddPressure>(20000);
  pressure->Stop();
  unsigned clock_calls = 0;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::HddClock",
                                        [&](void*) { ++clock_calls; });
  SyncPoint::GetInstance()->EnableProcessing();
  auto probe = std::make_shared<ObserverProbe>(fs_);
  auto observed = ObserveHdd(probe, "/observer/data", "/observer/backup",
                             pressure, "/observer/index", "/observer/staging");
  for (const std::string area : {"index", "staging"}) {
    const std::string path = "/observer/" + area + "/payload";
    std::unique_ptr<FSWritableFile> writer;
    std::unique_ptr<FSRandomRWFile> rw;
    std::unique_ptr<FSRandomAccessFile> reader;
    std::unique_ptr<FSSequentialFile> sequential;
    std::unique_ptr<FSDirectory> directory;
    ASSERT_OK(observed->NewWritableFile(path, FileOptions(), &writer, nullptr));
    ASSERT_OK(observed->NewRandomRWFile(path, FileOptions(), &rw, nullptr));
    ASSERT_OK(
        observed->NewRandomAccessFile(path, FileOptions(), &reader, nullptr));
    ASSERT_OK(
        observed->NewSequentialFile(path, FileOptions(), &sequential, nullptr));
    ASSERT_OK(observed->NewDirectory(path, IOOptions(), &directory, nullptr));
    for (bool fail : {false, true}) {
      probe->fail = fail;
      const auto check = [&](IOStatus status) {
        EXPECT_EQ(status.IsIOError(), fail);
      };
      // Holding the HDD observation lock also proves that SSD forwarding and
      // its accounting do not depend on an in-flight HDD evaluation.
      WithObserverLock(pressure.get(), [&] {
        check(writer->Append("abc", IOOptions(), nullptr));
        check(writer->Flush(IOOptions(), nullptr));
        check(writer->Sync(IOOptions(), nullptr));
        check(writer->Fsync(IOOptions(), nullptr));
        check(rw->Write(0, "abc", IOOptions(), nullptr));
        check(rw->Flush(IOOptions(), nullptr));
        check(rw->Sync(IOOptions(), nullptr));
        check(rw->Fsync(IOOptions(), nullptr));
        check(observed->SyncFile(path, FileOptions(), IOOptions(), true,
                                 nullptr));
        check(directory->Fsync(IOOptions(), nullptr));
        check(directory->FsyncWithDirOptions(IOOptions(), nullptr,
                                             DirFsyncOptions()));
        char scratch[3];
        Slice value;
        check(reader->Read(0, 3, IOOptions(), &value, scratch, nullptr));
        EXPECT_EQ(value.ToString(), fail ? "" : "abc");
        check(rw->Read(0, 3, IOOptions(), &value, scratch, nullptr));
        check(sequential->Read(3, IOOptions(), &value, scratch, nullptr));
        FSReadRequest requests[2];
        for (auto& request : requests) {
          request.len = 3;
          request.scratch = scratch;
        }
        ASSERT_OK(reader->MultiRead(requests, 2, IOOptions(), nullptr));
        for (const auto& request : requests) check(request.status);
        unsigned callbacks = 0;
        FSReadRequest request;
        request.len = 3;
        request.scratch = scratch;
        void* handle = nullptr;
        IOHandleDeleter deleter;
        ASSERT_OK(reader->ReadAsync(
            request, IOOptions(),
            [&](FSReadRequest& result, void*) {
              check(result.status);
              ++callbacks;
            },
            nullptr, &handle, &deleter, nullptr));
        EXPECT_FALSE(reader->SubmitReadAsync(
            request, IOOptions(),
            [&](FSReadRequest& result) {
              check(result.status);
              ++callbacks;
            },
            nullptr));
        EXPECT_EQ(callbacks, 2);
      });
    }
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    ASSERT_OK(rw->Close(IOOptions(), nullptr));
  }
  EXPECT_EQ(clock_calls, 0);
  for (const auto op :
       {ObserverProbe::kWrite, ObserverProbe::kFlush, ObserverProbe::kSync,
        ObserverProbe::kFsync, ObserverProbe::kDirectory})
    EXPECT_EQ(probe->calls[op], 8);
  EXPECT_EQ(probe->calls[ObserverProbe::kRead], 28);
  EXPECT_EQ(probe->calls[ObserverProbe::kSyncFile], 4);
  const auto stats = pressure->Snapshot();
  EXPECT_EQ(stats.written_bytes[0], 6);
  EXPECT_EQ(stats.written_bytes[1], 6);
  EXPECT_EQ(stats.failed_writes, 4);
  for (auto calls : stats.calls) EXPECT_EQ(calls, 0);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(TieredStorageTest, PhysicalObserverSsdProgressCannotRecoverHddPressure) {
  auto pressure = std::make_shared<HddPressure>(20000);
  pressure->Stop();
  std::atomic<uint64_t> now{10000000};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::HddClock", [&](void* p) {
    *static_cast<uint64_t*>(p) = now.load();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  auto probe = std::make_shared<ObserverProbe>(fs_);
  auto observed = ObserveHdd(probe, "/observer/data", "/observer/backup",
                             pressure, "/observer/index", "/observer/staging");
  std::unique_ptr<FSWritableFile> hdd, ssd;
  ASSERT_OK(observed->NewWritableFile("/observer/data/payload", FileOptions(),
                                      &hdd, nullptr));
  ASSERT_OK(observed->NewWritableFile("/observer/staging/payload",
                                      FileOptions(), &ssd, nullptr));
  TestGate blocked;
  probe->before = [&](ObserverProbe::Op op) {
    if (op == ObserverProbe::kFsync) blocked.Block();
  };
  Status synced;
  std::thread thread([&] { synced = hdd->Fsync(IOOptions(), nullptr); });
  blocked.WaitUntilBlocked();
  now += 20001;
  EXPECT_OK(ssd->Append("abc", IOOptions(), nullptr));
  EXPECT_OK(ssd->Flush(IOOptions(), nullptr));
  EXPECT_TRUE(pressure->Pressured());
  blocked.Release();
  thread.join();
  EXPECT_OK(synced);
  probe->before = {};
  now += 2000001;
  for (unsigned i = 0; i < 5; ++i) {
    ASSERT_OK(ssd->Append("abc", IOOptions(), nullptr));
    ASSERT_OK(ssd->Flush(IOOptions(), nullptr));
  }
  EXPECT_TRUE(pressure->Pressured());
  EXPECT_EQ(pressure->Snapshot().recovered, 0);
  // The successful blocked Fsync supplied progress one. Only real HDD
  // completions can provide the remaining two pieces of recovery evidence.
  for (unsigned i = 0; i < 2; ++i) ASSERT_OK(hdd->Flush(IOOptions(), nullptr));
  EXPECT_FALSE(pressure->Pressured());
  EXPECT_EQ(pressure->Snapshot().recovered, 1);
  ASSERT_OK(hdd->Close(IOOptions(), nullptr));
  ASSERT_OK(ssd->Close(IOOptions(), nullptr));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(TieredStorageTest, PhysicalObserverConcurrentSsdSnapshotsStayBalanced) {
  auto pressure = std::make_shared<HddPressure>(20000);
  pressure->Stop();
  std::atomic<unsigned> done{0};
  std::vector<std::thread> writers;
  constexpr unsigned kThreads = 6, kWrites = 1000;
  for (unsigned t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t] {
      auto probe = std::make_shared<ObserverProbe>(fs_);
      auto observed =
          ObserveHdd(probe, "/observer/data", "/observer/backup", pressure,
                     "/observer/index", "/observer/staging");
      const std::array<HddPurpose, 3> purposes{
          HddPurpose::kCaller, HddPurpose::kBulk, HddPurpose::kBarrier};
      HddPurposeScope purpose(purposes[t % purposes.size()]);
      std::unique_ptr<FSWritableFile> file;
      EXPECT_OK(observed->NewWritableFile(
          t % 2 == 0 ? "/observer/index/payload" : "/observer/staging/payload",
          FileOptions(), &file, nullptr));
      for (unsigned i = 0; i < kWrites; ++i) {
        probe->fail = i % 7 == 0;
        auto status = file->Append("abc", IOOptions(), nullptr);
        EXPECT_EQ(status.IsIOError(), probe->fail);
      }
      EXPECT_OK(file->Close(IOOptions(), nullptr));
      ++done;
    });
  }
  do {
    const auto stats = pressure->Snapshot();
    EXPECT_EQ(stats.written_bytes[0] + stats.written_bytes[1],
              stats.purpose_write_bytes[0] + stats.purpose_write_bytes[1] +
                  stats.purpose_write_bytes[2]);
  } while (done.load() != kThreads);
  for (auto& writer : writers) writer.join();
  const auto stats = pressure->Snapshot();
  constexpr uint64_t kFailures = (kWrites + 6) / 7;
  EXPECT_EQ(stats.failed_writes, kThreads * kFailures);
  EXPECT_EQ(stats.written_bytes[0], 3 * (kWrites - kFailures) * 3);
  EXPECT_EQ(stats.written_bytes[1], 3 * (kWrites - kFailures) * 3);
  for (auto bytes : stats.purpose_write_bytes)
    EXPECT_EQ(bytes, 2 * (kWrites - kFailures) * 3);
  WithObserverLock(pressure.get(), [] {});
}

TEST_F(TieredStorageTest, AdaptivePhysicalObserverCountsPerOperationPurpose) {
  InitializeAdaptive();
  auto pressure = std::make_shared<HddPressure>(20000);
  auto observed = ObserveHdd(fs_, m_.data_dir, m_.backup_dir, pressure, index_,
                             m_.staging_dir);
  for (const auto& dir : {index_, m_.staging_dir, m_.data_dir, m_.backup_dir}) {
    ASSERT_OK(EnsureDir(observed.get(), dir));
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(observed->NewWritableFile(dir + "/observer-payload",
                                        FileOptions(), &file, nullptr));
    // Reuse the same handle across all purposes: purpose belongs to each call.
    for (const auto purpose :
         {HddPurpose::kCaller, HddPurpose::kBulk, HddPurpose::kBarrier}) {
      HddPurposeScope scope(purpose);
      ASSERT_OK(file->Append("abc", IOOptions(), nullptr));
      ASSERT_OK(file->Flush(IOOptions(), nullptr));
      ASSERT_OK(file->Fsync(IOOptions(), nullptr));
    }
    ASSERT_OK(file->Close(IOOptions(), nullptr));
    ASSERT_OK(SyncDir(observed.get(), dir));
  }
  const auto stats = pressure->Snapshot();
  for (const auto bytes : stats.written_bytes) EXPECT_EQ(bytes, 9);
  for (const auto bytes : stats.purpose_write_bytes) EXPECT_EQ(bytes, 12);
  EXPECT_EQ(stats.failed_writes, 0);
  EXPECT_EQ(stats.calls[0], 6);  // Only data/backup enter the HDD counters.
  EXPECT_EQ(stats.calls[1], 6);
  EXPECT_EQ(stats.calls[2], 6);
  EXPECT_EQ(stats.calls[3], 2);
  pressure->Stop();
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptivePressureClockIsSerializedAndMonotonic) {
  std::atomic<uint64_t> now{10000000};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::HddClock", [&](void* p) {
    *static_cast<uint64_t*>(p) = now.load();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  HddPressure pressure(20000);
  // Stop the timer only; direct observation remains available for deterministic
  // ordering of completions, new starts and pressure queries.
  pressure.Stop();
  for (const bool ending : {false, true}) {
    SCOPED_TRACE(ending);
    const auto old = now.load();
    const auto id = pressure.Begin(HddPressure::Kind::kAppend, 1);
    TestGate delayed;
    std::atomic<bool> first{true};
    const char* hook = ending ? "MetaBypass::HddEndBeforeLock"
                              : "MetaBypass::HddPressureBeforeLock";
    SyncPoint::GetInstance()->SetCallBack(hook, [&](void*) {
      if (first.exchange(false)) delayed.Block();
    });
    bool pressured = false;
    std::thread observer([&] {
      if (ending)
        pressure.End(id, true);
      else
        pressured = pressure.Pressured();
    });
    delayed.WaitUntilBlocked();
    now = old + 100;
    const auto newer = pressure.Begin(HddPressure::Kind::kFlush, 0);
    pressure.End(newer, true);
    // Even a regressing injected sample must not underflow start/progress.
    now = old;
    delayed.Release();
    observer.join();
    EXPECT_FALSE(pressured);
    EXPECT_FALSE(pressure.Pressured());
    if (!ending) pressure.End(id, true);
    EXPECT_EQ(pressure.Snapshot().entered, 0);
    EXPECT_EQ(pressure.PressureAge(), 0);
    now = old + 1000;
    SyncPoint::GetInstance()->ClearCallBack(hook);
  }
  const auto slow = pressure.Begin(HddPressure::Kind::kFlush, 0);
  now += 20001;
  pressure.End(slow, true);
  ASSERT_TRUE(pressure.Pressured());
  now += 100;
  EXPECT_EQ(pressure.PressureAge(), 100);
  now -= 1000;
  EXPECT_EQ(pressure.PressureAge(), 100);
  EXPECT_EQ(pressure.Snapshot().residence_us, 100);
  EXPECT_LT(pressure.Snapshot().micros[0], 1000);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(TieredStorageTest, AdaptiveRecoveryClockWaitsForInflightHdd) {
  CheckAdaptiveRecoveryWaitsForInflight();
}

TEST_F(TieredStorageTest, AdaptiveRandomizedStateMachineStress) {
  InitializeAdaptive(256 * 1024);
  storage_->Start();
  const uint32_t seed = Env::Default()->NowMicros() & 0x7fffffff;
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random random(seed);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(LocalWriter(1, &writer));
  std::string expected;
  for (unsigned i = 0; i < 64; ++i) {
    SetPressure((i % 4) < 2);
    std::string payload(1 + random.Uniform(65536), 'a' + random.Uniform(26));
    ASSERT_OK(writer->Append(payload, IOOptions(), nullptr));
    ASSERT_OK(writer->Flush(IOOptions(), nullptr));
    expected += payload;
    if (i % 7 == 0) {
      ASSERT_OK(writer->Sync(IOOptions(), nullptr));
    }
    ASSERT_LE(expected.size(), size_t{std::numeric_limits<int>::max()});
    const uint64_t offset = random.Uniform(static_cast<int>(expected.size()));
    const size_t length = std::min<size_t>(65537, expected.size() - offset);
    std::string actual;
    ASSERT_OK(ReadBytes(offset, length, &actual));
    ASSERT_EQ(actual, expected.substr(offset, length));
    ASSERT_LE(AdaptiveStat(&MetaBypassStats::peak_staging_bytes),
              m_.staging_capacity);
    ASSERT_LE(AdaptiveStat(&MetaBypassStats::adaptive_peak_pending_seals), 2);
  }
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, expected.size()}}));
  ASSERT_OK(storage_->Stop());
  writer.reset();
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::string actual;
  ASSERT_OK(ReadBytes(0, expected.size(), &actual));
  ASSERT_EQ(actual, expected);
  uint64_t reclaimed = 0;
  ASSERT_OK(storage_->ReclaimBlob(1, &reclaimed));
  ASSERT_GT(reclaimed, 0);
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest,
       AdaptivePressureNeedsFreshProgressAndBulkUsesStallBudget) {
  std::atomic<uint64_t> now{10000000};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::HddClock", [&](void* p) {
    *static_cast<uint64_t*>(p) = now.load();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  {
    HddPressure pressure(20000);
    uint64_t bulk;
    {
      HddPurposeScope purpose(HddPurpose::kBulk);
      bulk = pressure.Begin(HddPressure::Kind::kSync, 0);
    }
    now += 20001;
    ASSERT_FALSE(pressure.Pressured());
    now += 1000000;
    ASSERT_TRUE(pressure.Pressured());
    pressure.End(bulk, true);
    now += 2000001;
    ASSERT_TRUE(pressure.Pressured());
    for (unsigned i = 0; i < 2; ++i) {
      const auto id = pressure.Begin(HddPressure::Kind::kRead, 1);
      pressure.End(id, true);
    }
    ASSERT_FALSE(pressure.Pressured());
    const auto sensitive = pressure.Begin(HddPressure::Kind::kFlush, 0);
    now += 20001;
    pressure.End(sensitive, true);
    ASSERT_TRUE(pressure.Pressured());
    now += 3000000;
    ASSERT_TRUE(pressure.Pressured());
    ASSERT_EQ(pressure.Snapshot().entered, 2);
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

#ifndef OS_WIN
TEST_F(TieredStorageTest, AdaptiveProcessCrashOwnershipAndPublicationMatrix) {
  for (const char* hook :
       {"MetaBypass::TierHandoffSsdCreated", "MetaBypass::TierHandoffSsdSynced",
        "MetaBypass::TierHandoffPersisted", "MetaBypass::TierHandoffQueued",
        "MetaBypass::TierPayloadSynced", "MetaBypass::TierPayloadVerified",
        "MetaBypass::TierPublicationSynced", "MetaBypass::TierRouteDurable"}) {
    SCOPED_TRACE(hook);
    EnableTier(8192);
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(),
            "--v5-adaptive-crash-writer", m_.data_dir.c_str(),
            m_.staging_dir.c_str(), hook, nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(WTERMSIG(status), SIGKILL);
    InitializeAdaptive(8192);
    std::string actual;
    ASSERT_OK(ReadBytes(0, 6, &actual));
    ASSERT_TRUE(actual == "hdd" || actual == "hddssd") << actual;
    storage_->Start();
    ASSERT_OK(storage_->Persist({{1, actual.size()}}));
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}
#endif

TEST_F(TieredStorageTest, AdaptiveRecoveryRebuildsMoreThanEightPendingWriters) {
  InitializeAdaptive();
  std::vector<std::unique_ptr<FSWritableFile>> writers(9);
  for (uint64_t i = 0; i < writers.size(); ++i) {
    ASSERT_OK(LocalWriter(i + 1, &writers[i]));
    ASSERT_OK(writers[i]->Append("hdd", IOOptions(), nullptr));
    // Construct the persisted handoff-before-enqueue crash cut directly.
    ASSERT_OK(PersistHistoricalPending(i + 1));
  }
  ASSERT_OK(storage_->Stop());
  writers.clear();
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  ASSERT_EQ(AdaptiveStat(&MetaBypassStats::tier_extent_fsyncs), 9);
  storage_->Start();
  std::map<uint64_t, uint64_t> lengths;
  for (uint64_t i = 1; i <= 9; ++i) lengths[i] = 3;
  ASSERT_OK(storage_->Persist(lengths));
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, AdaptiveSealFailureWakesIndependentBarrierTargets) {
  InitializeAdaptive();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  TestGate seal, first_wait, second_wait;
  std::atomic<unsigned> waiters{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBeforeHddExtentFsync",
                                        [&](void*) { seal.Block(); });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierPayloadVerified", [&](void* p) {
        *static_cast<Status*>(p) = Status::IOError("seal failure");
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBarrierWaiting",
                                        [&](void*) {
                                          if (++waiters == 1)
                                            first_wait.Block();
                                          else
                                            second_wait.Block();
                                        });
  storage_->Start();
  SetPressure(true);
  ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
  seal.WaitUntilBlocked();
  Status first, second;
  std::thread a([&] { first = writer->Sync(IOOptions(), nullptr); });
  first_wait.WaitUntilBlocked();
  first_wait.Release();
  EXPECT_OK(writer->Append("later", IOOptions(), nullptr));
  std::thread b([&] { second = writer->Sync(IOOptions(), nullptr); });
  second_wait.WaitUntilBlocked();
  second_wait.Release();
  seal.Release();
  a.join();
  b.join();
  EXPECT_TRUE(first.IsIOError());
  EXPECT_TRUE(second.IsIOError());
  EXPECT_TRUE(writer->Append("rejected", IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(storage_->Stop().IsIOError());
  EXPECT_TRUE(storage_->Error().IsIOError());
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 0);
}

TEST_F(TieredStorageTest, AdaptiveCancelInsideWorkerThenStopDrainsOwnership) {
  InitializeAdaptive();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("hdd", &writer));
  SetPressure(true);
  ASSERT_OK(writer->Append("ssd", IOOptions(), nullptr));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierPayloadVerified", [&](void*) {
        storage_->Cancel(Status::IOError("cancel inside worker callback"));
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"MetaBypass::TierCancelled", "AdaptiveCancel::Issued"}});
  storage_->Start();
  TEST_SYNC_POINT("AdaptiveCancel::Issued");
  EXPECT_TRUE(storage_->Stop().IsIOError());
  EXPECT_TRUE(storage_->Error().IsIOError());
  EXPECT_EQ(AdaptiveStat(&MetaBypassStats::adaptive_pending_seals), 0);
  EXPECT_EQ(RemotePins(), 0);
  EXPECT_EQ(OpenAdaptiveWriters(), 0);
}

TEST_F(TieredStorageTest, AdaptiveEmptyCloseStillNeedsCompleteForPersist) {
  InitializeAdaptive();
  storage_->Start();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(LocalWriter(1, &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, 0}}));
  ASSERT_TRUE(FinalPublished());
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, DescriptorRejectsInvalidCoverageAndOpenState) {
  const std::string valid =
      "aa 1 bb 1 3 1 1\n0 3 3 33554432 0 0 0 0 segment-1-bb-1 "
      "segment-1-bb-1\n";
  ASSERT_OK(ValidateDescriptor(valid));
  for (const char* body :
       {"aa 1 bb 1 3 1 1\n1 3 3 33554432 0 0 0 0 segment-1-bb-1 "
        "segment-1-bb-1\n",
        "aa 1 bb 1 3 1 1\n0 3 4 33554432 0 0 0 0 segment-1-bb-1 "
        "segment-1-bb-1\n",
        "aa 1 bb 1 3 1 1\n0 3 3 33554432 0 1 0 0 segment-1-bb-1 "
        "segment-1-bb-1\n",
        "aa 1 bb 1 3 1 1\n0 3 3 33554432 0 0 0 0 ../segment-1-bb-1 "
        "segment-1-bb-1\n",
        "aa 1 bb 1 3 1 1\n0 3 3 33554432 1 0 1 0 segment-1-bb-1 "
        "segment-1-bb-1\n",
        "aa 1 bb 1 3 1 2\n0 2 2 33554432 0 0 0 0 segment-1-bb-1 "
        "segment-1-bb-1\n1 1 1 33554432 0 0 0 0 "
        "segment-1-bb-2 segment-1-bb-2\n",
        "aa 1 bb 1 3 1 1\n0 3 3 33554432 0 0 0 0 segment-1-bb-1 "
        "segment-1-bb-1\nextra"}) {
    SCOPED_TRACE(body);
    ASSERT_TRUE(ValidateDescriptor(body).IsCorruption());
  }
}

TEST_F(TieredStorageTest, FrozenSourceCorruptionDoesNotPublishHddRange) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("original", &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(Write(fs_.get(), FirstLocalPath(), "modified"));
  ASSERT_TRUE(Migrate(8).IsCorruption());
  ASSERT_EQ(HddEnd(), 0);
  ASSERT_EQ(TierStats().staging_bytes, 8);
  ASSERT_OK(LocalExists());
}

TEST_F(TieredStorageTest, TruncatedDurableRangeRejectedOnReopen) {
  Initialize();
  ASSERT_OK(Append("durable"));
  const auto path = FirstExtentPath();
  storage_.reset();
  ASSERT_OK(Write(fs_.get(), path, "short"));
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  std::string contents;
  ASSERT_OK(Read(fs_.get(), path, &contents));
  ASSERT_EQ(contents, "short");
}

TEST_F(TieredStorageTest, SimulatedPowerLossDropsOnlyUnsyncedRegisteredTail) {
  auto fault = std::make_shared<FaultInjectionTestFS>(fs_);
  fault->SetInjectUnsyncedDataLoss(true);
  fs_ = fault;
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("durable", &writer));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(writer->Append("unsynced", IOOptions(), nullptr));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  writer.reset();
  ASSERT_OK(storage_->Stop());
  storage_.reset();
  ASSERT_OK(fault->DropUnsyncedFileData());
  ASSERT_OK(fault->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));
  fault->ResetState();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  std::string actual;
  ASSERT_OK(ReadBytes(0, 100, &actual));
  ASSERT_EQ(actual, "durable");
  storage_->Start();
  ASSERT_OK(storage_->Persist({{1, 7}}));
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest,
       MigrationFailureKeepsDurablePrefixAndFlushedTailReadable) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("durable", &writer));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(writer->Append("tail", IOOptions(), nullptr));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  TestGate failed;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierExtentSynced", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("background seal failure");
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationComplete",
                                        [&](void*) { failed.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  storage_->Start();
  failed.WaitUntilBlocked();
  std::string actual;
  EXPECT_OK(ReadBytes(0, 11, &actual));
  EXPECT_EQ(actual, "durabletail");
  EXPECT_TRUE(writer->Append("rejected", IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(writer->Sync(IOOptions(), nullptr).IsIOError());
  EXPECT_TRUE(storage_->Persist({{1, 11}}).IsIOError());
  uint64_t reclaimed = 0;
  EXPECT_TRUE(storage_->ReclaimBlob(1, &reclaimed).IsIOError());
  EXPECT_EQ(reclaimed, 0);
  EXPECT_EQ(HddEnd(), 0);
  EXPECT_EQ(TierStats().staging_bytes, 11);
  EXPECT_OK(LocalExists());
  failed.Release();
  ASSERT_TRUE(storage_->Stop().IsIOError());
  ASSERT_FALSE(ReadBytes(0, 11, &actual).ok());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(TieredStorageTest, LocalSealFailurePreservesRegisteredTailAndQuota) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("durable", &writer));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(writer->Append("tail", IOOptions(), nullptr));
  ASSERT_OK(writer->Flush(IOOptions(), nullptr));
  const std::string descriptor = LocalDescriptorPath();
  std::string before, after;
  ASSERT_OK(Read(fs_.get(), descriptor, &before));
  unsigned local_seals = 0, hdd_seals = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierLocalExtentSynced", [&](void* arg) {
        ++local_seals;
        *static_cast<Status*>(arg) = Status::IOError("local seal failure");
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierExtentSynced",
                                        [&](void*) { ++hdd_seals; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(writer->Sync(IOOptions(), nullptr).IsIOError());
  ASSERT_EQ(local_seals, 1U);
  ASSERT_EQ(hdd_seals, 0U);
  ASSERT_OK(Read(fs_.get(), descriptor, &after));
  ASSERT_EQ(before, after);  // No durable watermark or range publication.
  ASSERT_EQ(HddEnd(), 0);
  ASSERT_EQ(RemoteSize(), 11);
  ASSERT_EQ(TierStats().staging_bytes, 11);
  ASSERT_OK(LocalExists());
  std::string actual;
  ASSERT_OK(ReadBytes(0, 11, &actual));
  ASSERT_EQ(actual, "durabletail");
  ASSERT_TRUE(writer->Append("rejected", IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(writer->Sync(IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(storage_->Persist({{1, 11}}).IsIOError());
  uint64_t reclaimed = 0;
  ASSERT_TRUE(storage_->ReclaimBlob(1, &reclaimed).IsIOError());
  ASSERT_EQ(reclaimed, 0);
  ASSERT_OK(LocalExists());
  ASSERT_TRUE(writer->Close(IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(storage_->Stop().IsIOError());
  ASSERT_FALSE(ReadBytes(0, 11, &actual).ok());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  writer.reset();
  storage_.reset();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_OK(storage_->Initialize(false));
  ASSERT_OK(ReadBytes(0, 11, &actual));
  ASSERT_EQ(actual, "durabletail");
  ASSERT_EQ(HddEnd(), 0);
  ASSERT_EQ(TierStats().staging_bytes, 11);
  storage_->Start();
  ASSERT_OK(storage_->Persist({{1, 11}}));
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, TailRegistrationFailureNeverAcceptsPayload) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(LocalWriter(1, &writer));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierBeforeTailRegister", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("registration failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(writer->Append("payload", IOOptions(), nullptr).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  uint64_t size;
  ASSERT_OK(fs_->GetFileSize(FirstLocalPath(), IOOptions(), &size, nullptr));
  ASSERT_EQ(size, 0);
  ASSERT_EQ(RemoteSize(), 0);
  ASSERT_EQ(TierStats().staging_bytes, 7);  // Failed admission remains charged.
  ASSERT_TRUE(storage_->Stop().IsIOError());
}

TEST_F(TieredStorageTest, SsdOperationsDoNotWaitForHddDataOrMetadata) {
  const uint64_t segment = 32 * 1024 * 1024;
  for (const char* point : {"MetaBypass::TierBeforeHddExtentFsync",
                            "MetaBypass::TierDescriptorSynced"}) {
    SCOPED_TRACE(point);
    auto traced = std::make_shared<CallerHddTrace>(fs_, m_.data_dir);
    fs_ = traced;
    Initialize(3 * segment);
    const auto initial_hdd_operations = traced->writes.load();
    const auto initial_listings = traced->listings.load();
    const auto initial_stats = traced->stats.load();
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob(std::string(segment, 'a'), &writer));
    ASSERT_TRUE(
        fs_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
            .IsNotFound());
    TestGate hdd;
    SyncPoint::GetInstance()->SetCallBack(point, [&](void*) { hdd.Block(); });
    const auto caller = std::this_thread::get_id();
    std::atomic<unsigned> caller_hdd_commits{0};
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierDependenciesSynced", [&](void*) {
          if (std::this_thread::get_id() == caller) ++caller_hdd_commits;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    hdd.WaitUntilBlocked();
    // These operations must finish before the HDD owner is allowed to resume.
    EXPECT_OK(
        writer->Append(std::string(segment + 7, 'b'), IOOptions(), nullptr));
    EXPECT_OK(writer->Sync(IOOptions(), nullptr));
    EXPECT_OK(writer->Close(IOOptions(), nullptr));
    EXPECT_EQ(caller_hdd_commits.load(), 0U);
    EXPECT_EQ(traced->writes.load(), initial_hdd_operations);
    EXPECT_EQ(traced->listings.load(), initial_listings);
    EXPECT_EQ(traced->stats.load(), initial_stats);
    EXPECT_EQ(RemoteSize(), 2 * segment + 7);
    EXPECT_TRUE(LogicalClosed());
    hdd.Release();
    ASSERT_OK(storage_->Persist({{1, 2 * segment + 7}}));
    ASSERT_TRUE(FinalPublished());
    ASSERT_OK(storage_->Stop());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, RecoveryClosesEveryAbandonedLogicalWriter) {
  for (const std::string state : {"open", "frozen", "migrated", "empty"}) {
    SCOPED_TRACE(state);
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(LocalWriter(1, &writer));
    const std::string value = state == "empty" ? "" : "payload";
    ASSERT_OK(writer->Append(value, IOOptions(), nullptr));
    if (state == "frozen" || state == "migrated") {
      ASSERT_OK(writer->Sync(IOOptions(), nullptr));
    }
    if (state == "migrated") {
      ASSERT_OK(Migrate(value.size()));
    }
    EXPECT_FALSE(LogicalClosed());
    if (state != "migrated") {
      ASSERT_TRUE(
          fs_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr)
              .IsNotFound());
    }
    writer.reset();
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    ASSERT_TRUE(LogicalClosed());
    ASSERT_EQ(RemoteSize(), value.size());
    std::string actual;
    ASSERT_OK(ReadBytes(0, value.size() + 1, &actual));
    ASSERT_EQ(actual, value);
    storage_->Start();
    ASSERT_OK(storage_->Persist({{1, value.size()}}));
    ASSERT_TRUE(FinalPublished());
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, RemotePublicationLocalFailureRecoversBothVersions) {
  for (const char* point : {"MetaBypass::TierLocalDescriptorSynced",
                            "MetaBypass::TierLocalDescriptorRenamed",
                            "MetaBypass::TierLocalPublicationSynced"}) {
    SCOPED_TRACE(point);
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob("payload", &writer));
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    TestGate failed;
    SyncPoint::GetInstance()->SetCallBack(point, [](void* arg) {
      *static_cast<Status*>(arg) =
          Status::IOError("local reconciliation failure");
    });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationComplete",
                                          [&](void*) { failed.Block(); });
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    failed.WaitUntilBlocked();
    EXPECT_EQ(HddEnd(), 0);
    EXPECT_EQ(TierStats().staging_bytes, 7);
    EXPECT_OK(LocalExists());
    EXPECT_OK(
        fs_->FileExists(BlobFileName(m_.data_dir, 1), IOOptions(), nullptr));
    EXPECT_TRUE(storage_->Persist({{1, 7}}).IsIOError());
    failed.Release();
    ASSERT_TRUE(storage_->Stop().IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    std::string actual;
    ASSERT_OK(ReadBytes(0, 7, &actual));
    ASSERT_EQ(actual, "payload");
    storage_->Start();
    ASSERT_OK(storage_->Persist({{1, 7}}));
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, PublishedHddDoesNotHideCorruptRetainedSsd) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("payload", &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierLocalDescriptorSynced", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("retain old local view");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(Migrate(7).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(Write(fs_.get(), FirstLocalPath(), "corrupt"));
  writer.reset();
  ASSERT_OK(storage_->Stop());
  storage_.reset();
  const auto before = TierFiles();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  ASSERT_EQ(TierFiles(), before);
}

TEST_F(TieredStorageTest, FinalMapDoesNotSatisfyPersistBeforeLocalCleanup) {
  for (bool empty : {false, true}) {
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    const std::string value = empty ? "" : "payload";
    ASSERT_OK(CreateLocalBlob(value, &writer));
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierFinalLocalSynced", [](void* arg) {
          *static_cast<Status*>(arg) = Status::IOError("local cleanup failed");
        });
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    ASSERT_TRUE(storage_->Persist({{1, value.size()}}).IsIOError());
    ASSERT_FALSE(FinalPublished());
    ASSERT_TRUE(storage_->Stop().IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    writer.reset();
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    ASSERT_TRUE(FinalPublished());
    ASSERT_OK(storage_->Persist({{1, value.size()}}));
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, SuffixCanApplyBeforeEarlierPrefixPublication) {
  Initialize(4);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("pref", &writer));
  ASSERT_OK(writer->Append("suffix", IOOptions(), nullptr));
  TestGate prefix, suffix_wait;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationSealed",
                                        [&](void*) { prefix.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBarrierWaiting",
                                        [&](void*) { suffix_wait.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  // Queue the suffix before starting the sole physical executor, without
  // installing an H barrier that would prioritize its earlier SSD hole.
  ASSERT_OK(FreezeAdaptiveCaller());
  storage_->Start();
  prefix.WaitUntilBlocked();
  Status synced;
  std::thread suffix([&] { synced = writer->Sync(IOOptions(), nullptr); });
  suffix_wait.WaitUntilBlocked();
  suffix_wait.Release();
  EXPECT_GE(RemoteGeneration(), 2);
  std::string actual;
  EXPECT_OK(ReadBytes(0, 10, &actual));
  EXPECT_EQ(actual, "prefsuffix");
  prefix.Release();
  suffix.join();
  ASSERT_OK(synced);
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Persist({{1, 10}}));
  ASSERT_OK(ReadBytes(0, 10, &actual));
  ASSERT_EQ(actual, "prefsuffix");
  ASSERT_OK(storage_->Stop());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(TieredStorageTest, DurableRetirementReplaysEveryDeletionBoundary) {
  for (const char* point :
       {"MetaBypass::TierRetirementSynced", "MetaBypass::BlobDescriptorRemoved",
        "MetaBypass::BlobPayloadDeleteStatus",
        "MetaBypass::BlobFinalDirSyncStatus",
        "MetaBypass::TierRetirementRemoved"}) {
    SCOPED_TRACE(point);
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob("payload", &writer));
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    SyncPoint::GetInstance()->SetCallBack(point, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("retirement injection");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    uint64_t reclaimed;
    ASSERT_TRUE(storage_->ReclaimBlob(1, &reclaimed).IsIOError());
    ASSERT_TRUE(storage_->Stop().IsIOError());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    std::vector<std::string> names;
    ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
    for (const auto& name : names) {
      if (name.size() > 8 && name.substr(name.size() - 8) == ".retired") {
        std::string marker;
        ASSERT_OK(Read(fs_.get(), m_.data_dir + "/" + name, &marker));
        ASSERT_OK(Write(fs_.get(), m_.data_dir + "/" + name + ".tmp", marker));
      }
    }
    writer.reset();
    storage_.reset();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_OK(storage_->Initialize(false));
    uint64_t size;
    ASSERT_TRUE(storage_
                    ->GetFileSize(BlobFileName(m_.data_dir, 1), IOOptions(),
                                  &size, nullptr)
                    .IsPathNotFound());
    ASSERT_EQ(TierStats().staging_bytes, 0);
    ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
    for (const auto& name : names) {
      ASSERT_EQ(name.find(".retired"), std::string::npos);
    }
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, InvalidLocalRecoveryNeverChangesEitherNamespace) {
  for (const std::string kind :
       {"checksum", "truncation", "identity", "incarnation", "overlap",
        "generation", "missing", "length", "payload_crc", "legacy"}) {
    SCOPED_TRACE(kind);
    Initialize(128);
    std::unique_ptr<FSWritableFile> writer;
    ASSERT_OK(CreateLocalBlob("payload", &writer));
    ASSERT_OK(writer->Close(IOOptions(), nullptr));
    const auto descriptor = LocalDescriptorPath();
    const auto payload = FirstLocalPath();
    std::string bytes;
    ASSERT_OK(Read(fs_.get(), descriptor, &bytes));
    writer.reset();
    ASSERT_OK(storage_->Stop());
    storage_.reset();

    if (kind == "checksum") bytes.back() = 'x';
    if (kind == "truncation") bytes.resize(bytes.size() / 2);
    if (kind == "legacy") bytes.replace(0, 4, "MBL3");
    if (kind == "identity" || kind == "incarnation" || kind == "overlap" ||
        kind == "generation") {
      ASSERT_OK(MutateLocalDescriptor(kind, &bytes));
    }
    ASSERT_OK(Write(fs_.get(), descriptor, bytes));
    if (kind == "missing") {
      ASSERT_OK(fs_->DeleteFile(payload, IOOptions(), nullptr));
    }
    if (kind == "length") {
      ASSERT_OK(Write(fs_.get(), payload, "short"));
    }
    if (kind == "payload_crc") {
      ASSERT_OK(Write(fs_.get(), payload, "changed"));
    }
    ASSERT_OK(Write(fs_.get(), descriptor + ".tmp", "uncommitted debris"));
    const auto before = TierFiles();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_FALSE(storage_->Initialize(false).ok());
    ASSERT_EQ(TierFiles(), before);
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
  }
}

TEST_F(TieredStorageTest, AppliedHddRangesRequireIndependentPublicationProof) {
  for (const bool whole_descriptor : {false, true}) {
    for (const std::string state : {"open", "closed", "mixed"}) {
      SCOPED_TRACE(state +
                   (whole_descriptor ? ":missing_map" : ":missing_range"));
      Initialize(128);
      std::unique_ptr<FSWritableFile> writer;
      ASSERT_OK(CreateLocalBlob("payload", &writer));
      ASSERT_OK(writer->Sync(IOOptions(), nullptr));
      ASSERT_OK(Migrate(7));  // Logical writer is open, so MBT remains PARTIAL.
      if (state == "closed") {
        ASSERT_OK(writer->Close(IOOptions(), nullptr));
      } else if (state == "mixed") {
        ASSERT_OK(writer->Append("tail", IOOptions(), nullptr));
        ASSERT_OK(writer->Flush(IOOptions(), nullptr));
      }
      ASSERT_OK(RemoveRemoteProof(whole_descriptor));
      writer.reset();
      ASSERT_OK(storage_->Stop());
      storage_.reset();
      const auto before = TierFiles();
      storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
      ASSERT_TRUE(storage_->Preflight(false).IsCorruption());
      ASSERT_EQ(TierFiles(), before);
      ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
      ASSERT_EQ(TierFiles(), before);
      storage_.reset();
      Clean(m_.data_dir);
      Clean(m_.staging_dir);
    }
  }
}

TEST_F(TieredStorageTest,
       NativePreflightRechecksRetiredPointAfterDependencyFailure) {
  Initialize(128);
  ASSERT_OK(Append("payload"));
  std::string current, previous;
  ASSERT_OK(ProtectedPoint("point-current", true, &current));
  ASSERT_OK(ProtectedPoint("point-previous", false, &previous));
  const auto point = m_.backup_dir + "/point-current";
  std::string inventory;
  ASSERT_OK(Read(fs_.get(), point + "/INVENTORY", &inventory));
  inventory += "I missing-native-file 7\n";
  ASSERT_OK(Write(fs_.get(), point + "/INVENTORY", inventory));
  current = "point-current\n";
  ASSERT_OK(Write(fs_.get(), m_.backup_dir + "/LATEST", current));
  const auto before = TierFiles();
  // A stable missing dependency is still an error, with no writes.
  uint64_t missing_size;
  Status expected_missing = fs_->GetFileSize(
      point + "/missing-native-file", IOOptions(), &missing_size, nullptr);
  ASSERT_FALSE(expected_missing.ok());
  ASSERT_EQ(NativePointPreflight(), expected_missing);
  ASSERT_EQ(TierFiles(), before);
  TestGate checked;
  std::atomic<unsigned> attempts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::RestorePreflightValidated", [&](void* arg) {
        if (++attempts == 1) {
          EXPECT_EQ(*static_cast<Status*>(arg), expected_missing);
          checked.Block();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status validated;
  std::thread validator([&] { validated = NativePointPreflight(); });
  checked.WaitUntilBlocked();
  ASSERT_OK(Write(fs_.get(), m_.backup_dir + "/LATEST", previous));
  checked.Release();
  validator.join();
  ASSERT_OK(validated);
  ASSERT_EQ(attempts.load(), 2U);
  ASSERT_EQ(TierFiles(), before);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(storage_->Stop());
}

TEST_F(TieredStorageTest, CheckpointInventoryRejectsIncompleteTail) {
  Initialize(128);
  ASSERT_OK(Append("payload"));
  std::string pointer;
  ASSERT_OK(ProtectedPoint("point-test", true, &pointer));
  ASSERT_OK(Write(fs_.get(), m_.backup_dir + "/LATEST", pointer));
  const auto path = m_.backup_dir + "/point-test/INVENTORY";
  std::string valid;
  ASSERT_OK(Read(fs_.get(), path, &valid));
  for (const std::string tail : {"I", "I orphan", "B", "B 2", "D BLOB-MAP"}) {
    SCOPED_TRACE(tail);
    ASSERT_OK(Write(fs_.get(), path, valid + tail));
    const auto before = TierFiles();
    ASSERT_TRUE(NativePointPreflight().IsCorruption());
    ASSERT_EQ(TierFiles(), before);
  }
  ASSERT_OK(Write(fs_.get(), path, valid));
  ASSERT_OK(NativePointPreflight());
}
TEST_F(TieredStorageTest, EveryProtectedMapMustMatchItsOuterInventory) {
  for (const std::string corrupt :
       {"map_swap", "missing_D", "duplicate_D", "B_set"}) {
    SCOPED_TRACE(corrupt);
    Initialize(128);
    ASSERT_OK(Append("payload"));
    std::string current, previous;
    ASSERT_OK(ProtectedPoint("point-current", true, &current));
    ASSERT_OK(ProtectedPoint("point-previous", false, &previous));
    const auto point = m_.backup_dir + "/point-previous";
    std::string inventory, map;
    ASSERT_OK(Read(fs_.get(), point + "/INVENTORY", &inventory));
    if (corrupt == "map_swap") {
      // Same database, valid internal checksum, wrong protected inventory.
      ASSERT_OK(
          Read(fs_.get(), m_.backup_dir + "/point-current/BLOB-MAP", &map));
      ASSERT_OK(Write(fs_.get(), point + "/BLOB-MAP", map));
    } else {
      if (corrupt == "missing_D") inventory = "MBI6\n";
      if (corrupt == "duplicate_D") inventory += inventory.substr(5);
      if (corrupt == "B_set") inventory += "B 1 7\n";
      ASSERT_OK(Write(fs_.get(), point + "/INVENTORY", inventory));
      previous = "point-previous\n";
    }
    ASSERT_OK(Write(fs_.get(), m_.backup_dir + "/LATEST", previous));
    ASSERT_OK(storage_->Stop());
    storage_.reset();
    const auto before = TierFiles();
    storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
    ASSERT_TRUE(storage_->Preflight(false).IsCorruption());
    ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
    ASSERT_EQ(TierFiles(), before);
    std::string unchanged;
    ASSERT_OK(Read(fs_.get(), point + "/INVENTORY", &unchanged));
    ASSERT_EQ(unchanged, inventory);
    storage_.reset();
    Clean(m_.data_dir);
    Clean(m_.staging_dir);
    Clean(m_.backup_dir);
  }
}

TEST_F(TieredStorageTest, ProtectedCheckpointPreventsRetirementAndReplay) {
  Initialize(128);
  ASSERT_OK(Append("payload"));
  const auto point = m_.backup_dir + "/point-test";
  ASSERT_OK(EnsureDir(fs_.get(), m_.backup_dir));
  ASSERT_OK(EnsureDir(fs_.get(), point));
  ASSERT_OK(storage_->SaveCheckpoint(point + "/BLOB-MAP", {{1, 7}}));
  std::string map;
  ASSERT_OK(Read(fs_.get(), point + "/BLOB-MAP", &map));
  const std::string inventory =
      "MBI6\nD BLOB-MAP " + std::to_string(map.size()) + "\nB 1 7\n";
  ASSERT_OK(Write(fs_.get(), point + "/INVENTORY", inventory));
  ASSERT_OK(Write(fs_.get(), m_.backup_dir + "/LATEST", "point-test\n"));
  const auto before = TierFiles();
  uint64_t removed = 0;
  ASSERT_TRUE(storage_->ReclaimBlob(1, &removed).IsCorruption());
  ASSERT_EQ(removed, 0);
  ASSERT_EQ(TierFiles(), before);
  ASSERT_OK(WriteRetirementForTest());
  ASSERT_TRUE(storage_->Stop().IsCorruption());
  storage_.reset();
  const auto with_tomb = TierFiles();
  storage_.reset(new TieredStorage(fs_, m_, "tier-test"));
  ASSERT_TRUE(storage_->Initialize(false).IsCorruption());
  ASSERT_EQ(TierFiles(), with_tomb);
}

TEST_F(TieredStorageTest, PreflightRetriesDependencyFailureAfterConcurrentGc) {
  auto failing = std::make_shared<LocalReadFailureFileSystem>(fs_);
  fs_ = failing;
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("payload", &writer));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  failing->local_path = FirstLocalPath();
  failing->fail_read = true;
  TieredStorage reopening(fs_, m_, "tier-test");
  TestGate validated;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierPreflightDependenciesChecked", [&](void* arg) {
        if (!static_cast<Status*>(arg)->ok()) validated.Block();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status opened;
  std::thread preflight([&] { opened = reopening.Preflight(false); });
  validated.WaitUntilBlocked();
  uint64_t reclaimed;
  EXPECT_OK(storage_->ReclaimBlob(1, &reclaimed));
  validated.Release();
  preflight.join();
  ASSERT_OK(opened);
  ASSERT_OK(storage_->Stop());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(TieredStorageTest, RegistrationStatusFailureBoundaries) {
  for (const char* hook :
       {"MetaBypass::TierTailCreated", "MetaBypass::TierTailIdentitySynced",
        "MetaBypass::TierBeforeTailRegister",
        "MetaBypass::TierLocalDescriptorSynced",
        "MetaBypass::TierLocalDescriptorRenamed",
        "MetaBypass::TierLocalPublicationSynced"}) {
    ASSERT_NO_FATAL_FAILURE(MetadataStatusFailure("register", hook));
  }
}

TEST_F(TieredStorageTest, FreezeStatusFailureBoundaries) {
  for (const char* hook :
       {"MetaBypass::TierPayloadSynced", "MetaBypass::TierLocalExtentSynced",
        "MetaBypass::TierLocalDescriptorSynced",
        "MetaBypass::TierLocalDescriptorRenamed",
        "MetaBypass::TierLocalPublicationSynced"}) {
    ASSERT_NO_FATAL_FAILURE(MetadataStatusFailure("freeze", hook));
  }
}

TEST_F(TieredStorageTest, MigrationStatusFailureBoundaries) {
  for (const char* hook :
       {"MetaBypass::TierPayloadSynced", "MetaBypass::TierPayloadVerified",
        "MetaBypass::TierDependenciesSynced",
        "MetaBypass::TierDescriptorSynced", "MetaBypass::TierDescriptorRenamed",
        "MetaBypass::TierPublicationSynced",
        "MetaBypass::TierLocalDescriptorSynced",
        "MetaBypass::TierLocalDescriptorRenamed",
        "MetaBypass::TierLocalPublicationSynced",
        "MetaBypass::TierLocalUnlinked", "MetaBypass::TierBeforeQuotaRelease",
        "MetaBypass::TierFinalLocalRemoved",
        "MetaBypass::TierFinalLocalSynced"}) {
    ASSERT_NO_FATAL_FAILURE(MetadataStatusFailure("migrate", hook));
  }
}

TEST_F(TieredStorageTest, RetirementStatusFailureBoundaries) {
  for (const char* hook :
       {"MetaBypass::TierRetirementSynced", "MetaBypass::BlobDescriptorRemoved",
        "MetaBypass::BlobPayloadDeleteStatus",
        "MetaBypass::BlobFinalDirSyncStatus",
        "MetaBypass::TierRetirementRemoved"}) {
    ASSERT_NO_FATAL_FAILURE(MetadataStatusFailure("gc", hook));
  }
}

static const char* const kRegistrationBoundaries[] = {
    "MetaBypass::TierTailCreated",
    "MetaBypass::TierTailIdentitySynced",
    "MetaBypass::TierBeforeTailRegister",
    "MetaBypass::TierLocalDescriptorSynced",
    "MetaBypass::TierLocalDescriptorRenamed",
    "MetaBypass::TierLocalPublicationSynced",
    "MetaBypass::TierTailRegistered"};
TEST_F(TieredStorageTest, RegistrationPowerLossBoundaries) {
  for (const auto* hook : kRegistrationBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataPowerLoss("register", hook));
  }
}
#ifndef OS_WIN
TEST_F(TieredStorageTest, RegistrationProcessCrashBoundaries) {
  for (const auto* hook : kRegistrationBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataProcessCrash("register", hook));
  }
}
#endif

static const char* const kFreezeBoundaries[] = {
    "MetaBypass::TierPayloadSynced",
    "MetaBypass::TierLocalExtentSynced",
    "MetaBypass::TierLocalDescriptorSynced",
    "MetaBypass::TierLocalDescriptorRenamed",
    "MetaBypass::TierLocalPublicationSynced",
    "MetaBypass::TierLocalPersistedBeforeRoute",
    "MetaBypass::TierSegmentFrozen"};
TEST_F(TieredStorageTest, FreezePowerLossBoundaries) {
  for (const auto* hook : kFreezeBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataPowerLoss("freeze", hook));
  }
}
#ifndef OS_WIN
TEST_F(TieredStorageTest, FreezeProcessCrashBoundaries) {
  for (const auto* hook : kFreezeBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataProcessCrash("freeze", hook));
  }
}
#endif

static const char* const kMigrationBoundaries[] = {
    "MetaBypass::TierPayloadSynced",
    "MetaBypass::TierPayloadVerified",
    "MetaBypass::TierDependenciesSynced",
    "MetaBypass::TierDescriptorSynced",
    "MetaBypass::TierDescriptorRenamed",
    "MetaBypass::TierPublicationSynced",
    "MetaBypass::TierLocalDescriptorSynced",
    "MetaBypass::TierLocalDescriptorRenamed",
    "MetaBypass::TierLocalPublicationSynced",
    "MetaBypass::TierLocalPersistedBeforeRoute",
    "MetaBypass::TierRouteDurable",
    "MetaBypass::TierBeforeEvict",
    "MetaBypass::TierLocalUnlinked",
    "MetaBypass::TierBeforeQuotaRelease",
    "MetaBypass::TierFinalHddPublished",
    "MetaBypass::TierFinalLocalRemoved",
    "MetaBypass::TierFinalLocalSynced"};
TEST_F(TieredStorageTest, MigrationPowerLossBoundaries) {
  for (const auto* hook : kMigrationBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataPowerLoss("migrate", hook));
  }
}
#ifndef OS_WIN
TEST_F(TieredStorageTest, MigrationProcessCrashBoundaries) {
  for (const auto* hook : kMigrationBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataProcessCrash("migrate", hook));
  }
}
#endif

static const char* const kRetirementBoundaries[] = {
    "MetaBypass::TierRetirementSynced",
    "MetaBypass::BlobDescriptorRemoved",
    "MetaBypass::BlobDescriptorSynced",
    "MetaBypass::BlobPayloadRemoved",
    "MetaBypass::TierRetirementPayloadSynced",
    "MetaBypass::TierRetirementRemoved"};
TEST_F(TieredStorageTest, RetirementPowerLossBoundaries) {
  for (const auto* hook : kRetirementBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataPowerLoss("gc", hook));
  }
}
#ifndef OS_WIN
TEST_F(TieredStorageTest, RetirementProcessCrashBoundaries) {
  for (const auto* hook : kRetirementBoundaries) {
    ASSERT_NO_FATAL_FAILURE(MetadataProcessCrash("gc", hook));
  }
}
#endif

TEST_F(TieredStorageTest, SuffixPointReadDoesNotPinUnrelatedSsdPrefix) {
  Initialize(128);
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob("prefix", &writer));
  ASSERT_OK(writer->Sync(IOOptions(), nullptr));
  ASSERT_OK(writer->Append(std::string(128, 's'), IOOptions(), nullptr));
  TestGate reading;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierRemoteReadPinned",
                                        [&](void*) { reading.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  Status status;
  std::string actual;
  std::thread reader([&] { status = ReadBytes(100, 4, &actual); });
  reading.WaitUntilBlocked();
  EXPECT_EQ(LocalPins(), 0);
  // The unrelated SSD source can be migrated and physically deleted while
  // the suffix-only read retains its independent HDD range snapshot.
  EXPECT_OK(Migrate(6));
  EXPECT_TRUE(LocalExists().IsNotFound());
  reading.Release();
  reader.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(status);
  ASSERT_EQ(actual, "ssss");
  storage_->Start();
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(storage_->Stop());
}

}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  using ROCKSDB_NAMESPACE::MetaBypassDB;
  using ROCKSDB_NAMESPACE::MetaBypassOptions;
  using ROCKSDB_NAMESPACE::Options;
  using ROCKSDB_NAMESPACE::Status;
  using ROCKSDB_NAMESPACE::SyncPoint;
  using ROCKSDB_NAMESPACE::TestMetaBypassOptions;
  using ROCKSDB_NAMESPACE::WriteBatch;
  using ROCKSDB_NAMESPACE::WriteOptions;
#ifndef OS_WIN
  if (argc == 7 && std::string(argv[1]) == "--incremental-restore") {
    Options options;
    options.enable_blob_files = true;
    options.enable_blob_direct_write = true;
    options.allow_concurrent_memtable_write = false;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    bypass.staging_dir = argv[5];
    bypass.staging_capacity = bypass.staging_dir.empty() ? 0 : 8192;
    SyncPoint::GetInstance()->SetCallBack(
        argv[6], [](void*) { kill(getpid(), SIGKILL); });
    SyncPoint::GetInstance()->EnableProcessing();
    Status s = MetaBypassDB::Restore(options, bypass, argv[2]);
    fprintf(stderr, "restore did not reach hook: %s\n", s.ToString().c_str());
    return 2;
  }
  if (argc == 8 && std::string(argv[1]) == "--incremental-crash-writer") {
    const int requested_mode = std::stoi(argv[6]);
    const bool combined = requested_mode == 9;
    const int mode = combined ? 3 : requested_mode;
    Options options;
    options.create_if_missing = true;
    options.enable_blob_files = mode != 0 && mode != 5;
    options.enable_blob_direct_write = mode >= 2 && mode != 5;
    options.allow_concurrent_memtable_write = !options.enable_blob_direct_write;
    options.blob_direct_write_partitions = 1;
    options.blob_file_size = mode >= 7 ? 64 * 1024 * 1024 : 4096;
    options.blob_compression_type = ROCKSDB_NAMESPACE::kNoCompression;
    const size_t tail_size = mode >= 7 ? 8 * 1024 * 1024 : 4096;
    options.write_buffer_size = 64 * 1024 * 1024;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    bypass.staging_dir = argv[5];
    bypass.staging_capacity = bypass.staging_dir.empty() ? 0 : 8192;
    if (mode == 4 || mode == 8)
      bypass.staging_policy =
          ROCKSDB_NAMESPACE::MetaBypassStagingPolicy::kAdaptive;
    if (mode == 5 || combined) {
      bypass.sst_tiering.mode = ROCKSDB_NAMESPACE::SstTieringMode::kAdaptive;
      bypass.sst_tiering.ssd_capacity_bytes = 1;
    }
    bypass.checkpoint_interval_ms = 600000;
    bypass.batch_bytes = 1;
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    if (s.ok()) s = db->Put(WriteOptions(), "base", std::string(4096, 'a'));
    if (s.ok()) s = db->Put(WriteOptions(), "gone", std::string(4096, 'b'));
    if (s.ok())
      s = db->Put(WriteOptions(), "overwrite", std::string(4096, 'c'));
    if (s.ok()) s = db->SyncBackup();
    if (s.ok() && mode == 6) {
      s = db->Close();
      db.reset();
      if (s.ok()) s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    }
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      _exit(2);
    }
    const auto baseline = db->GetBackupStats();
    baseline.error.PermitUncheckedError();
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<uint64_t> notifications{0};
    auto notify = [&](void*) {
      ++notifications;
      cv.notify_all();
    };
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::BeforeMirrorWait",
                                          notify);
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierSealCompleted",
                                          notify);
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationComplete",
                                          notify);
    SyncPoint::GetInstance()->EnableProcessing();
    for (int batch_number = 0; s.ok() && batch_number < 3; ++batch_number) {
      WriteBatch batch;
      if (batch_number == 0) {
        s = batch.Put("overwrite", std::string(tail_size, 'd'));
        if (s.ok()) s = batch.Delete("gone");
      } else if (batch_number == 1) {
        s = batch.Put("first", std::string(tail_size, 'e'));
        if (s.ok()) s = batch.Put("second", std::string(tail_size, 'f'));
      } else {
        s = batch.Delete("first");
        if (s.ok()) s = batch.Put("last", std::string(tail_size, 'g'));
      }
      if (s.ok()) s = db->Write(WriteOptions(), &batch);
      if (s.ok() && argv[7][0] == '1')
        s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
    }
    // Rotate the last complete value out of the active file without flushing
    // the native memtable. This new active value has no durable tier mapping.
    if (s.ok() && (mode == 3 || mode == 4))
      s = db->Put(WriteOptions(), "unavailable", std::string(4096, 'h'));
    if (s.ok() && argv[7][0] == '1')
      s = db->CompactRange(ROCKSDB_NAMESPACE::CompactRangeOptions(), nullptr,
                           nullptr);
    if (s.ok()) {
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(20);
      while (s.ok()) {
        const auto observed = notifications.load();
        auto stats = db->GetBackupStats();
        s.UpdateIfOk(stats.error);
        bool remote_ready = bypass.staging_dir.empty();
        if (!remote_ready && stats.adaptive_pending_seals == 0) {
          auto disk = options.env->GetFileSystem();
          std::vector<std::string> names;
          s = disk->GetChildren(bypass.data_dir, ROCKSDB_NAMESPACE::IOOptions(),
                                &names, nullptr);
          size_t complete = 0;
          bool partial_extent = false;
          for (const auto& name : names) {
            uint64_t number;
            ROCKSDB_NAMESPACE::FileType type;
            if (!ROCKSDB_NAMESPACE::ParseFileName(name, &number, &type) ||
                type != ROCKSDB_NAMESPACE::kBlobFile)
              continue;
            std::string bytes;
            Status read = ROCKSDB_NAMESPACE::metabypass::Read(
                disk.get(), bypass.data_dir + "/" + name, &bytes);
            if (read.ok()) {
              complete += bytes.find(" COMPLETE ") != std::string::npos;
              partial_extent |= bytes.find(" PARTIAL ") != std::string::npos &&
                                bytes.find(" 33554432 ") != std::string::npos;
            }
          }
          remote_ready = mode >= 7 ? partial_extent : complete >= 7;
        }
        if (!s.ok() || (stats.queued_bytes == 0 && remote_ready)) break;
        std::unique_lock<std::mutex> lock(mutex);
        if (!cv.wait_until(lock, deadline,
                           [&] { return notifications != observed; })) {
          s = Status::TimedOut("incremental mirror drain");
          break;
        }
      }
    }
    if (s.ok()) {
      auto stats = db->GetBackupStats();
      s.UpdateIfOk(stats.error);
      if (stats.checkpoint_captures != baseline.checkpoint_captures)
        s = Status::Corruption("unexpected incremental checkpoint flush");
    }
    if (!s.ok()) fprintf(stderr, "%s\n", s.ToString().c_str());
    _exit(s.ok() ? 0 : 2);
  }
  if (argc == 7 && std::string(argv[1]) == "--direct-write-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_direct_write = true;
    options.allow_concurrent_memtable_write = false;
    options.blob_direct_write_partitions = 1;
    options.write_buffer_size = 4 * 1024 * 1024;
    options.blob_file_size = 1024 * 1024;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    bypass.staging_dir = argv[5];
    bypass.staging_capacity = bypass.staging_dir.empty() ? 0 : 8192;
    bypass.checkpoint_interval_ms = 60000;
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    WriteBatch batch;
    if (s.ok()) s = batch.Put("a", std::string(16 * 1024, 'o'));
    if (s.ok()) s = batch.Put("b", std::string(16 * 1024, 'o'));
    if (s.ok()) s = db->Write(WriteOptions(), &batch);
    if (s.ok()) s = db->SyncBackup();
    batch.Clear();
    if (s.ok()) s = batch.Put("a", std::string(16 * 1024, 'n'));
    if (s.ok()) s = batch.Put("b", std::string(16 * 1024, 'n'));
    if (s.ok()) s = db->Write(WriteOptions(), &batch);
    if (s.ok() && std::string(argv[6]) == "active") kill(getpid(), SIGKILL);
    SyncPoint::GetInstance()->SetCallBack(
        argv[6], [](void*) { kill(getpid(), SIGKILL); });
    SyncPoint::GetInstance()->EnableProcessing();
    if (s.ok()) s = db->SyncBackup();
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    return 3;
  }
  if (argc == 5 && std::string(argv[1]) == "--v5-always-crash-writer") {
    using ROCKSDB_NAMESPACE::BlobFileName;
    using ROCKSDB_NAMESPACE::Env;
    using ROCKSDB_NAMESPACE::FileOptions;
    using ROCKSDB_NAMESPACE::FSWritableFile;
    using ROCKSDB_NAMESPACE::IOOptions;
    using ROCKSDB_NAMESPACE::metabypass::EnsureDir;
    using ROCKSDB_NAMESPACE::metabypass::TieredStorage;
    MetaBypassOptions options;
    options.data_dir = argv[2];
    options.staging_dir = argv[3];
    options.staging_capacity = 1;
    auto fs = Env::Default()->GetFileSystem();
    Status s = EnsureDir(fs.get(), options.data_dir);
    TieredStorage storage(fs, options, "tier-test");
    if (s.ok()) s = storage.Initialize(false);
    std::unique_ptr<FSWritableFile> writer;
    if (s.ok())
      s = storage.NewWritableFile(BlobFileName(options.data_dir, 1),
                                  FileOptions(), &writer, nullptr);
    if (s.ok()) s = writer->Append("s", IOOptions(), nullptr);
    if (s.ok()) s = writer->Append("h", IOOptions(), nullptr);
    SyncPoint::GetInstance()->SetCallBack(
        argv[4], [](void*) { kill(getpid(), SIGKILL); });
    SyncPoint::GetInstance()->EnableProcessing();
    constexpr size_t extent = 32 * 1024 * 1024;
    if (s.ok())
      s = writer->Append(std::string(extent - 1, 'h'), IOOptions(), nullptr);
    storage.Start();
    if (s.ok()) s = writer->Close(IOOptions(), nullptr);
    fprintf(stderr, "always crash boundary not reached: %s\n",
            s.ToString().c_str());
    storage.Stop().PermitUncheckedError();
    return 3;
  }
  if (argc == 5 && std::string(argv[1]) == "--v5-adaptive-crash-writer") {
    using ROCKSDB_NAMESPACE::BlobFileName;
    using ROCKSDB_NAMESPACE::Env;
    using ROCKSDB_NAMESPACE::FileOptions;
    using ROCKSDB_NAMESPACE::FSWritableFile;
    using ROCKSDB_NAMESPACE::IOOptions;
    using ROCKSDB_NAMESPACE::MetaBypassStagingPolicy;
    using ROCKSDB_NAMESPACE::metabypass::EnsureDir;
    using ROCKSDB_NAMESPACE::metabypass::HddPressure;
    using ROCKSDB_NAMESPACE::metabypass::ObserveHdd;
    using ROCKSDB_NAMESPACE::metabypass::TieredStorage;
    MetaBypassOptions options;
    options.data_dir = argv[2];
    options.staging_dir = argv[3];
    options.staging_capacity = 8192;
    options.staging_policy = MetaBypassStagingPolicy::kAdaptive;
    std::atomic<uint64_t> now{10000000};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::HddClock", [&](void* p) {
      *static_cast<uint64_t*>(p) = now.load();
    });
    SyncPoint::GetInstance()->EnableProcessing();
    auto pressure = std::make_shared<HddPressure>(20000);
    auto fs = ObserveHdd(Env::Default()->GetFileSystem(), options.data_dir,
                         options.backup_dir, pressure, {}, options.staging_dir);
    Status s = EnsureDir(fs.get(), options.data_dir);
    TieredStorage storage(fs, options, "tier-test", pressure);
    if (s.ok()) s = storage.Initialize(false);
    std::unique_ptr<FSWritableFile> writer;
    if (s.ok())
      s = storage.NewWritableFile(BlobFileName(options.data_dir, 1),
                                  FileOptions(), &writer, nullptr);
    if (s.ok()) s = writer->Append("hdd", IOOptions(), nullptr);
    const auto slow = pressure->Begin(HddPressure::Kind::kAppend, 1);
    now += 20001;
    pressure->End(slow, true);
    SyncPoint::GetInstance()->SetCallBack(
        argv[4], [](void*) { kill(getpid(), SIGKILL); });
    storage.Start();
    if (s.ok()) s = writer->Append("ssd", IOOptions(), nullptr);
    if (s.ok()) s = writer->Close(IOOptions(), nullptr);
    if (s.ok()) s = storage.Persist({{1, 6}});
    fprintf(stderr, "adaptive crash boundary not reached: %s\n",
            s.ToString().c_str());
    storage.Stop().PermitUncheckedError();
    return 3;
  }
  if (argc == 6 && std::string(argv[1]) == "--v4-tier-crash-writer") {
    using ROCKSDB_NAMESPACE::BlobFileName;
    using ROCKSDB_NAMESPACE::Env;
    using ROCKSDB_NAMESPACE::FileOptions;
    using ROCKSDB_NAMESPACE::FSWritableFile;
    using ROCKSDB_NAMESPACE::IOOptions;
    using ROCKSDB_NAMESPACE::metabypass::EnsureDir;
    using ROCKSDB_NAMESPACE::metabypass::TieredStorage;
    auto fs = Env::Default()->GetFileSystem();
    MetaBypassOptions options;
    options.data_dir = argv[2];
    options.staging_dir = argv[3];
    options.staging_capacity = 128;
    Status s = EnsureDir(fs.get(), options.data_dir);
    TieredStorage storage(fs, options, "tier-test");
    if (s.ok()) s = storage.Initialize(false);
    std::unique_ptr<FSWritableFile> writer;
    if (s.ok())
      s = storage.NewWritableFile(BlobFileName(options.data_dir, 1),
                                  FileOptions(), &writer, nullptr);
    const std::string phase = argv[4];
    if (s.ok() && phase != "register")
      s = writer->Append("payload", IOOptions(), nullptr);
    if (s.ok() && phase != "register") s = writer->Flush(IOOptions(), nullptr);
    if (s.ok() && (phase == "migrate" || phase == "gc"))
      s = writer->Close(IOOptions(), nullptr);
    SyncPoint::GetInstance()->SetCallBack(
        argv[5], [](void*) { kill(getpid(), SIGKILL); });
    SyncPoint::GetInstance()->EnableProcessing();
    if (s.ok()) {
      if (phase == "register")
        s = writer->Append("payload", IOOptions(), nullptr);
      else if (phase == "freeze")
        s = writer->Sync(IOOptions(), nullptr);
      else if (phase == "gc") {
        uint64_t bytes;
        s = storage.ReclaimBlob(1, &bytes);
      } else {
        storage.Start();
        s = storage.Persist({{1, 7}});
        // Reclamation can follow the completed publication barrier.
        if (s.ok()) {
          std::mutex mutex;
          std::condition_variable cv;
          std::unique_lock<std::mutex> lock(mutex);
          cv.wait(lock, [] { return false; });
        }
      }
    }
    fprintf(stderr, "crash boundary not reached (%s): %s\n", argv[5],
            s.ToString().c_str());
    storage.Stop().PermitUncheckedError();
    return 3;
  }
  if (argc == 5 && std::string(argv[1]) == "--raw-tier-crash-writer") {
    using ROCKSDB_NAMESPACE::FileOptions;
    using ROCKSDB_NAMESPACE::FSWritableFile;
    using ROCKSDB_NAMESPACE::IOOptions;
    using ROCKSDB_NAMESPACE::metabypass::EnsureDir;
    using ROCKSDB_NAMESPACE::metabypass::TieredStorage;
    auto fs = ROCKSDB_NAMESPACE::Env::Default()->GetFileSystem();
    MetaBypassOptions options;
    options.data_dir = argv[2];
    options.staging_dir = argv[3];
    options.staging_capacity = 64;
    Status s = EnsureDir(fs.get(), options.data_dir);
    TieredStorage storage(fs, options, "tier-test");
    if (s.ok()) s = storage.Initialize(false);
    storage.Start();
    std::unique_ptr<FSWritableFile> writer;
    if (s.ok())
      s = storage.NewWritableFile(
          ROCKSDB_NAMESPACE::BlobFileName(options.data_dir, 1), FileOptions(),
          &writer, nullptr);
    if (s.ok()) s = writer->Append(std::string(32, 'p'), IOOptions(), nullptr);
    if (std::string(argv[4]) == "route") {
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::TierBeforeEvict",
          [](void*) { kill(getpid(), SIGKILL); });
      SyncPoint::GetInstance()->EnableProcessing();
      if (s.ok()) s = writer->Close(IOOptions(), nullptr);
      if (s.ok()) s = storage.Persist({{1, 32}});
      // The publication hook kills the child before source deletion.
      if (s.ok()) {
        std::mutex mutex;
        std::condition_variable cv;
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [] { return false; });
      }
    }
    if (s.ok()) s = writer->Append(std::string(64, 's'), IOOptions(), nullptr);
    if (s.ok()) s = writer->Append("unsynced-tail", IOOptions(), nullptr);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      storage.Stop().PermitUncheckedError();
      return 2;
    }
    kill(getpid(), SIGKILL);
    return 3;
  }
  if (argc == 7 && std::string(argv[1]) == "--blob-gc-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.blob_file_size = 128;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    bypass.staging_dir = argv[5];
    bypass.staging_capacity = 1024 * 1024;
    bypass.interval_ms = 1;
    bypass.checkpoint_interval_ms = bypass.interval_ms;
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    if (s.ok()) s = db->Put(WriteOptions(), "old", std::string(256, 'a'));
    if (s.ok()) s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
    if (s.ok()) s = db->SyncBackup();
    SyncPoint::GetInstance()->SetCallBack(
        argv[6], [](void*) { kill(getpid(), SIGKILL); });
    SyncPoint::GetInstance()->EnableProcessing();
    if (s.ok()) s = db->Delete(WriteOptions(), "old");
    if (s.ok()) s = db->Put(WriteOptions(), "live", std::string(256, 'b'));
    if (s.ok()) s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
    ROCKSDB_NAMESPACE::CompactRangeOptions compact;
    compact.bottommost_level_compaction =
        ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForce;
    if (s.ok()) s = db->CompactRange(compact, nullptr, nullptr);
    if (s.ok()) s = db->SyncBackup();
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    // No extra writes: the GC scheduler must advance the retained older root.
    std::mutex mutex;
    std::condition_variable cv;
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::seconds(20));
    return 3;
  }
  if (argc == 5 && std::string(argv[1]) == "--sst-crash-writer") {
    using ROCKSDB_NAMESPACE::FlushOptions;
    using ROCKSDB_NAMESPACE::SstTieringMode;
    Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    bypass.sst_tiering.mode = SstTieringMode::kAdaptive;
    bypass.sst_tiering.ssd_capacity_bytes = 1;
    bypass.sst_tiering.interval_ms = 1;
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    if (s.ok()) s = db->Put(WriteOptions(), "updated", "published");
    if (s.ok()) s = db->Put(WriteOptions(), "deleted", "published");
    if (s.ok()) s = db->Flush(FlushOptions());
    if (s.ok()) s = db->SyncBackup();
    // Freeze publication after the chosen point, while letting the mirror and
    // primary WAL accumulate unflushed updates and a deletion.
    std::mutex mutex;
    std::condition_variable cv;
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::ValidateCandidate", [&](void*) {
          std::unique_lock<std::mutex> lock(mutex);
          cv.wait(lock, [] { return false; });
        });
    SyncPoint::GetInstance()->EnableProcessing();
    if (s.ok()) s = db->Put(WriteOptions(), "updated", "unpublished");
    if (s.ok()) s = db->Delete(WriteOptions(), "deleted");
    if (s.ok()) s = db->Put(WriteOptions(), "tail", "unpublished");
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    kill(getpid(), SIGKILL);
    return 3;
  }
  if (argc == 7 && std::string(argv[1]) == "--tiered-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    MetaBypassOptions m = TestMetaBypassOptions();
    m.data_dir = argv[3];
    m.backup_dir = argv[4];
    m.staging_dir = argv[5];
    m.staging_capacity = 1024 * 1024;
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, m, argv[2], &db);
    WriteOptions sync;
    sync.sync = true;
    WriteBatch batch;
    if (s.ok()) s = batch.Put("a", "old");
    if (s.ok()) s = batch.Put("b", "old");
    if (s.ok()) s = db->Write(sync, &batch);
    if (s.ok()) s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
    if (s.ok()) s = db->SyncBackup();
    if (argv[6][0]) {
      SyncPoint::GetInstance()->SetCallBack(
          argv[6], [](void*) { kill(getpid(), SIGKILL); });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    batch.Clear();
    if (s.ok()) s = batch.Put("a", "new");
    if (s.ok()) s = batch.Put("b", "new");
    if (s.ok()) s = db->Write(sync, &batch);
    if (s.ok()) s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
    if (s.ok()) s = db->SyncBackup();
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    if (argv[6][0]) {
      fprintf(stderr, "requested tier crash hook did not run: %s\n", argv[6]);
      _exit(3);
    }
    kill(getpid(), SIGKILL);
    return 3;
  }
#endif
#ifndef OS_WIN
  if ((argc == 5 || argc == 6) &&
      std::string(argv[1]) == "--metabypass-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.enable_blob_files = true;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    const bool sst_copy_crash = argc == 6 && std::string(argv[5]) == "sst-copy";
    auto no_links =
        std::make_shared<ROCKSDB_NAMESPACE::CandidateNoLinkFileSystem>(
            options.env->GetFileSystem());
    auto env = ROCKSDB_NAMESPACE::NewCompositeEnv(no_links);
    if (sst_copy_crash) options.env = env.get();
    const bool gc_crash =
        argc == 6 && std::string(argv[5]) == "MetaBypass::GCFileDeleted";
    if (gc_crash || sst_copy_crash) {
      bypass.batch_bytes = bypass.wal_queue_capacity;
      bypass.interval_ms = 60000;
      bypass.checkpoint_interval_ms = bypass.interval_ms;
    }
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    if (s.ok()) s = db->Put(WriteOptions(), "a", "old");
    // The GC crash starts with Open's only root. Its first retirement must
    // belong to the batch below, after "a" has been overwritten with "last".
    if (s.ok() && !gc_crash) s = db->SyncBackup();
    std::atomic<bool> copying_tables{false};
    if (sst_copy_crash) {
      SyncPoint::GetInstance()->SetCallBack(
          "MetaBypass::BeforeCaptureTables",
          [&](void*) { copying_tables = true; });
      SyncPoint::GetInstance()->SetCallBack("MetaBypass::StageCopyAppendStatus",
                                            [&](void*) {
                                              if (copying_tables)
                                                kill(getpid(), SIGKILL);
                                            });
      SyncPoint::GetInstance()->EnableProcessing();
    } else if (argc == 6) {
      SyncPoint::GetInstance()->SetCallBack(
          argv[5], [](void*) { kill(getpid(), SIGKILL); });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    if (s.ok()) s = db->Put(WriteOptions(), "deleted", "old");
    WriteBatch batch;
    if (s.ok()) s = batch.Put("a", "last");
    if (s.ok()) s = batch.Put("b", "batch");
    if (s.ok()) s = batch.Delete("deleted");
    WriteOptions sync;
    sync.sync = !sst_copy_crash;
    if (s.ok()) s = db->Write(sync, &batch);
    if (sst_copy_crash) {
      if (s.ok()) s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
      if (s.ok()) s = db->SyncBackup();
      if (s.ok()) {
        fprintf(stderr, "SST copy crash hook did not run\n");
        _exit(3);
      }
    }
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    if (gc_crash) {
      // Only the successful-delete hook may kill this process. Do not Close:
      // that would capture another candidate while the retired root is deleted.
      std::mutex mutex;
      std::condition_variable cv;
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait_for(lock, std::chrono::seconds(20), [] { return false; });
      fprintf(stderr, "GCFileDeleted hook did not run\n");
      fflush(stderr);
      _exit(3);  // Avoid implicit Close during failure cleanup as well.
    }
    kill(getpid(), SIGKILL);
    return 3;
  }
#endif
  ROCKSDB_NAMESPACE::executable = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
