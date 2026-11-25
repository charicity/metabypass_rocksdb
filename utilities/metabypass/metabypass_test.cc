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

#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
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
#include "utilities/metabypass/sst_storage.h"

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
  static void CheckIdlePrune(Backup* backup) {
    ASSERT_OK(EnsureDir(backup->disk_, backup->options_.backup_dir));
    ASSERT_OK(EnsureDir(backup->disk_, backup->work_));
    ASSERT_OK(
        EnsureDir(backup->disk_, backup->options_.backup_dir + "/point-1"));
    ASSERT_OK(Write(backup->disk_, backup->work_ + "/000001.log", "obsolete"));
    ASSERT_OK(Write(backup->disk_, backup->work_ + "/000002.log", "live"));
    backup->published_point_ = Backup::Point{"point-2", 0, 2};
    backup->retired_point_ = Backup::Point{"point-1", 0, 1};
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
  std::atomic<bool> fail_reuse_link{false}, fail_native_link{false};
  IOStatus LinkFile(const std::string& from, const std::string& to,
                    const IOOptions& options, IODebugContext* dbg) override {
    const bool reuse = from.find("/point-") != std::string::npos;
    if ((reuse ? fail_reuse_link.load() : fail_native_link.load()) &&
        to.find("/point-") != std::string::npos) {
      return IOStatus::IOError("injected candidate hard link error");
    }
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
    m_.backup_dir = root_ + "/backup";
    o_.create_if_missing = true;
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
  void Open() { ASSERT_OK(MetaBypassDB::Open(o_, m_, index_, &db_)); }
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
  Status CompactTables() {
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
  void CheckPointValue(const std::string& point, const std::string& key,
                       const std::string* expected) {
    auto env = NewCompositeEnv(fs_);
    Options options = o_;
    options.env = env.get();
    options.create_if_missing = false;
    std::unique_ptr<DB> snapshot;
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(DB::OpenForReadOnly(
        DBOptions(options), point,
        {{kDefaultColumnFamilyName, ColumnFamilyOptions(options)}}, &handles,
        &snapshot));
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
  void RunCrashWriter(const std::string& stage = "") {
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      if (stage.empty()) {
        execl(executable.c_str(), executable.c_str(),
              "--metabypass-crash-writer", index_.c_str(),
              m_.backup_dir.c_str(), nullptr);
      } else {
        execl(executable.c_str(), executable.c_str(),
              "--metabypass-crash-writer", index_.c_str(),
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
    ASSERT_OK(fs_->CreateDir(index_, IOOptions(), nullptr));
    *backup = std::make_unique<metabypass::Backup>(fs_, index_, m_);
    ASSERT_OK((*backup)->Lock());
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
    metabypass::Backup backup(fs_, index_, m_);
    EXPECT_EQ(
        metabypass::BackupTestAccess::Select(&backup, input, boundary, capture),
        expected);
  }
  void CoalesceDrain(
      const std::vector<metabypass::BackupTestAccess::Input>& input,
      bool partial, size_t successful, const std::vector<std::string>& calls) {
    metabypass::Backup backup(fs_, index_, m_);
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
    // Park a WAL producer on capacity by holding the mirror's drain.
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
    ASSERT_OK(fs_->CreateDir(index_, IOOptions(), nullptr));
    metabypass::Backup backup(fs_, index_, m_);
    ASSERT_OK(backup.Lock());
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

class MetaBypassSstTest : public MetaBypassTest {
 public:
  MetaBypassSstTest() {
    o_ = Options();
    o_.create_if_missing = true;
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
  }
  void CrudScenario() {
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
TEST_F(MetaBypassSstTest, RecoveryOrdinaryBatchKeepsOriginalBytes) {
  ASSERT_NO_FATAL_FAILURE(Open());
  for (int i = 0; i < 4; ++i)
    ASSERT_OK(db_->Put(WriteOptions(), std::to_string(i), "trusted"));
  ASSERT_OK(db_->Close());
  db_.reset();
  unsigned checked = 0;
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
        // An unsupported post-checkpoint update must leave the frontier
        // unchanged.
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
}
TEST_F(MetaBypassSstTest, RecoveryUnsupportedUpdateStopsWholeBatch) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Close());
  db_.reset();
  unsigned checked = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::RecoveryFilterCreated", [&](void* arg) {
        auto* filter = static_cast<WalFilter*>(arg);
        WriteBatch batch, output;
        EXPECT_OK(batch.Put("ordinary", "must-not-be-applied"));
        EXPECT_OK(batch.SingleDelete("unsupported"));
        EXPECT_OK(batch.Delete("after-unsupported"));
        WriteBatchInternal::SetSequence(&batch, 1);
        bool changed = true;
        EXPECT_EQ(filter->LogRecordFound(1, "test", batch, &output, &changed),
                  WalFilter::WalProcessingOption::kStopReplay);
        EXPECT_FALSE(changed);
        EXPECT_EQ(output.Count(), 0U);
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
TEST_F(MetaBypassSstTest, RecoverySingle512MiBBatchDoesNotRebuild) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Close());
  db_.reset();
  unsigned checked = 0;
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
}
TEST_F(MetaBypassSstTest, DefaultOptionsCrudAndReopen) {
  MetaBypassOptions defaults;
  defaults.backup_dir = m_.backup_dir;
  m_ = std::move(defaults);
  ASSERT_NO_FATAL_FAILURE(CrudScenario());
}
#ifndef OS_WIN
class MetaBypassIncrementalTest : public MetaBypassTest {
 public:
  void Crash(int mode, bool rotate = false, bool keep_index = false) {
    const auto mode_text = std::to_string(mode);
    o_.write_buffer_size = 64 * 1024 * 1024;
    if (mode == 5) EnableSst();
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      execl(executable.c_str(), executable.c_str(),
            "--incremental-crash-writer", index_.c_str(), m_.backup_dir.c_str(),
            mode_text.c_str(), rotate ? "1" : "0", nullptr);
      _exit(127);
    }
    int status;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    if (!keep_index) Clean(index_);
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
        expected[operation.key] = std::string(4096, operation.value);
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
          index_.c_str(), m_.backup_dir.c_str(),
          "MetaBypass::RecoveryPersisted", nullptr);
    _exit(127);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
  std::string persisted;
  ASSERT_OK(metabypass::Read(fs_.get(), index_ + "/METABYPASS-RECOVERED",
                             &persisted));
  std::istringstream marker(persisted);
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
TEST_F(MetaBypassIncrementalTest, SstTieringContinuousPrefix) {
  ASSERT_NO_FATAL_FAILURE(Crash(5));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
TEST_F(MetaBypassIncrementalTest, ContinuousPrefixAfterNormalRestart) {
  ASSERT_NO_FATAL_FAILURE(Crash(6));
  ASSERT_NO_FATAL_FAILURE(Verify(9));
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
class MetaBypassRecoveryStageTest
    : public MetaBypassIncrementalTest,
      public testing::WithParamInterface<const char*> {};
TEST_P(MetaBypassRecoveryStageTest, ResumeWithoutLosingPrefix) {
  const char* stage = GetParam();
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl(executable.c_str(), executable.c_str(), "--incremental-restore",
          index_.c_str(), m_.backup_dir.c_str(), stage, nullptr);
    _exit(127);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFSIGNALED(status));
  ASSERT_EQ(WTERMSIG(status), SIGKILL);
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
INSTANTIATE_TEST_CASE_P(Stages, MetaBypassRecoveryStageTest,
                        testing::Values("MetaBypass::RecoveryPrepared",
                                        "MetaBypass::RecoveryNativeClosed",
                                        "MetaBypass::RecoveryPersisted",
                                        "MetaBypass::RecoveryFinalized"));
TEST_F(MetaBypassIncrementalTest, PersistedStageSupportsBackupPathWithSpaces) {
  m_.backup_dir += " with spaces";
  ASSERT_NO_FATAL_FAILURE(Crash(0));
  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl(executable.c_str(), executable.c_str(), "--incremental-restore",
          index_.c_str(), m_.backup_dir.c_str(),
          "MetaBypass::RecoveryPersisted", nullptr);
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
    metabypass::Backup backup(failing, index_, m_);
    ASSERT_OK(backup.Lock());
    ASSERT_OK(backup.LoadLatest());
    ASSERT_OK(backup.Start(true));
    ASSERT_OK(backup.Stop());
  }
  ASSERT_EQ(failing->failures, 0);
  ASSERT_OK(metabypass::Read(fs_.get(), logs.front(), &after));
  ASSERT_EQ(before, after);
  Clean(index_);
  ASSERT_NO_FATAL_FAILURE(Verify(9));
}
#endif
TEST_F(MetaBypassTest, SyncWriteReleasesCheckpointCaptureFence) {
  m_.checkpoint_interval_ms = 60000;
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
  m_.checkpoint_interval_ms = 60000;
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
  m_.checkpoint_interval_ms = 60000;
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
TEST_F(MetaBypassTest, RecoverySamplerUsesOptionsClock) {
  auto clock =
      std::make_shared<MockSystemClock>(Env::Default()->GetSystemClock());
  clock->SetCurrentTime(10);
  CompositeEnvWrapper env(Env::Default(), fs_, clock);
  Options options = o_;
  options.env = &env;
  std::unique_ptr<MetaBypassDB> db;
  TestGate sampled;
  auto* sync = SyncPoint::GetInstance();
  Defer cleanup([&] {
    sampled.Release();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  sync->SetCallBack("MetaBypass::ProgressSampled",
                    [&](void*) { sampled.Block(); });
  sync->EnableProcessing();
  ASSERT_OK(MetaBypassDB::Open(options, m_, index_, &db));
  const bool sample_arrived =
      sampled.WaitUntilBlockedFor(std::chrono::seconds(10));
  sampled.Release();
  ASSERT_TRUE(sample_arrived);
  ASSERT_OK(db->Close());

  std::string samples;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/PROGRESS", &samples));
  MetaBypassRecoveryReport report;
  metabypass::Backup::EstimateRecoveryTime(samples, &report);
  ASSERT_TRUE(report.has_time_lower_bound);
  EXPECT_EQ(report.time_lower_bound_micros, 10000000U);
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
TEST_F(MetaBypassSstTest, WalOnlyBackupDoesNotFlushAndRestores) {
  m_.wal_queue_capacity = 256 * 1024;
  m_.batch_bytes = QueueBudget(m_);
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(RecoveryScenario(false));
}
TEST_F(MetaBypassSstTest, SstBackupRestores) {
  ASSERT_NO_FATAL_FAILURE(RecoveryScenario(true));
}
TEST_F(MetaBypassSstTest, SstDisabledReadReopenAndRestore) {
  ASSERT_NO_FATAL_FAILURE(SstScenario(SstTieringMode::kDisabled));
}
TEST_F(MetaBypassSstTest, SstObserveReadReopenAndRestore) {
  ASSERT_NO_FATAL_FAILURE(SstScenario(SstTieringMode::kObserveOnly));
}
TEST_F(MetaBypassSstTest, SstAdaptiveColdReadReopenAndRestore) {
  ASSERT_NO_FATAL_FAILURE(SstScenario(SstTieringMode::kAdaptive));
}
TEST_F(MetaBypassSstTest, RejectBlobFeaturesBeforeDirectoryChanges) {
  for (bool existing : {false, true}) {
    SCOPED_TRACE(existing);
    if (existing) {
      ASSERT_NO_FATAL_FAILURE(Open());
      ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
      ASSERT_OK(db_->Close());
      db_.reset();
    }
    constexpr bool Options::*blob_options[] = {
        &Options::enable_blob_files, &Options::enable_blob_direct_write,
        &Options::enable_blob_garbage_collection};
    for (auto member : blob_options) {
      Options invalid = o_;
      invalid.*member = true;
      const auto before = SnapshotTree(root_);
      ASSERT_TRUE(
          MetaBypassDB::Open(invalid, m_, index_, &db_).IsNotSupported());
      ASSERT_FALSE(db_);
      ASSERT_EQ(SnapshotTree(root_), before);
      ASSERT_TRUE(MetaBypassDB::Restore(invalid, m_, index_).IsNotSupported());
      ASSERT_EQ(SnapshotTree(root_), before);
    }
  }
}
TEST_F(MetaBypassTest, OpenAndRestoreExcludeOtherBackupOwners) {
  m_.enable_recovery_time_sampling = false;
  ASSERT_NO_FATAL_FAILURE(Open());
  const auto point = PublishedPoint();
  const auto destination = root_ + "/restore";
  std::unique_ptr<MetaBypassDB> second;
  ASSERT_FALSE(MetaBypassDB::Open(o_, m_, index_, &second).ok());
  ASSERT_FALSE(second);
  ASSERT_FALSE(MetaBypassDB::Restore(o_, m_, destination).ok());
  ASSERT_TRUE(fs_->FileExists(destination, IOOptions(), nullptr).IsNotFound());
  ASSERT_EQ(PublishedPoint(), point);
  ASSERT_OK(db_->Put(WriteOptions(), "active", "owner"));
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, destination));
  ASSERT_OK(MetaBypassDB::Open(o_, m_, destination, &second));
  std::string value;
  ASSERT_OK(second->Get(ReadOptions(), "active", &value));
  ASSERT_EQ(value, "owner");
}
TEST_F(MetaBypassTest, LockOnlyDirectoriesRemainUsable) {
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), index_));
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), m_.backup_dir));
  ASSERT_OK(metabypass::Write(fs_.get(), index_ + "/METABYPASS-LOCK", ""));
  ASSERT_OK(metabypass::Write(fs_.get(), m_.backup_dir + "/LOCK", ""));
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Close());
  db_.reset();
  Clean(index_);
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), index_));
  ASSERT_OK(metabypass::Write(fs_.get(), index_ + "/METABYPASS-LOCK", ""));
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  ASSERT_OK(fs_->FileExists(index_ + "/METABYPASS-LOCK", IOOptions(), nullptr));
}
TEST_F(MetaBypassSstTest, RandomizedRoundTrip) {
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
TEST_F(MetaBypassSstTest, ConcurrentBackupAllowsWritesAndFlush) {
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
TEST_F(MetaBypassTest, RejectLegacyDatabaseFormatWithoutMutation) {
  ASSERT_OK(metabypass::EnsureDir(fs_.get(), index_));
  const std::string marker = index_ + "/METABYPASS-FORMAT";
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "MBN6\n"));
  ASSERT_OK(metabypass::Write(fs_.get(), index_ + "/sentinel", "untouched"));
  const auto before = SnapshotTree(root_);
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsNotSupported());
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &bytes));
  ASSERT_EQ(bytes, "MBN6\n");
  ASSERT_OK(metabypass::Read(fs_.get(), index_ + "/sentinel", &bytes));
  ASSERT_EQ(bytes, "untouched");
  ASSERT_TRUE(
      fs_->FileExists(m_.backup_dir, IOOptions(), nullptr).IsNotFound());
  ASSERT_EQ(SnapshotTree(root_), before);
}
TEST_F(MetaBypassTest, ReopenDoesNotCreateWrongBackupDirectory) {
  Open();
  ASSERT_OK(db_->Close());
  db_.reset();

  const auto before = SnapshotTree(root_);
  auto wrong = m_;
  wrong.backup_dir += "-missing";
  ASSERT_TRUE(MetaBypassDB::Open(o_, wrong, index_, &db_).IsNotSupported());
  ASSERT_FALSE(db_);
  ASSERT_EQ(SnapshotTree(root_), before);
}
TEST_F(MetaBypassTest, RejectLegacyBackupFormatWithoutMutation) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  const std::string marker = m_.backup_dir + "/METABYPASS-FORMAT";
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "MBN6\n"));
  const std::string point = PublishedPoint();
  std::string inventory;
  ASSERT_OK(metabypass::Read(fs_.get(), point + "/INVENTORY", &inventory));
  Clean(index_);
  const auto before = SnapshotTree(root_);
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsNotSupported());
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &after));
  ASSERT_EQ(after, "MBN6\n");
  ASSERT_OK(metabypass::Read(fs_.get(), point + "/INVENTORY", &after));
  ASSERT_EQ(after, inventory);
  ASSERT_TRUE(fs_->FileExists(index_, IOOptions(), nullptr).IsNotFound());
  ASSERT_EQ(SnapshotTree(root_), before);
}
#ifndef OS_WIN
TEST_F(MetaBypassTest, WalOnlyRecoveryHonorsAvoidFlushAndManifestReuse) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter());
  o_.avoid_flush_during_recovery = true;
  o_.reuse_manifest_on_open = true;
  ASSERT_TRUE(FilesOfType(PublishedPoint(), kTableFile).empty());
  ASSERT_NO_FATAL_FAILURE(RestorePrefix(5, 5));
  // Restore persists native replay even when the subsequent normal Open avoids
  // recovery flushes. Checkpoint capture above remains WAL-only.
  ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
  Check("a", "last");
  Check("b", "batch");
  ASSERT_OK(db_->Put(WriteOptions(), "a", "replacement"));
  ASSERT_OK(db_->Delete(WriteOptions(), "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_FALSE(FilesOfType(index_, kTableFile).empty());
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "replacement");
}
#endif
TEST_F(MetaBypassTest, CapturesAndCoalescesConcurrentBarriers) {
  m_.checkpoint_interval_ms = 60000;
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
  EXPECT_FALSE(FilesOfType(first_point, kWalFile).empty());
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
TEST_F(MetaBypassTest, LowTrafficCheckpointThenIdle) {
  m_.checkpoint_interval_ms = 60000;
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
    ASSERT_EQ(published, 1U);
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const std::string expected = "traffic";
  CheckPointValue(PublishedPoint(), "low", &expected);
  CheckNoIdleCaptures();
}
TEST_F(MetaBypassTest, CheckpointDeadlineDoesNotSlideWithLaterWrites) {
  m_.checkpoint_interval_ms = 60000;
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
  EXPECT_OK(CompactTables());
  captured.Release();
  sync.join();
  ASSERT_OK(synced);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_FALSE(point.empty());
  ASSERT_EQ(PublishedPoint(), point);
  {
    // Read the exact captured point before Close publishes another boundary.
    // The native read-only DB replays the bounded WAL from this point.
    auto env = NewCompositeEnv(fs_);
    Options options = o_;
    options.env = env.get();
    options.create_if_missing = false;
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
    if (tiered) EnableSst();
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
TEST_F(MetaBypassTest, SstRandomizedConcurrentReadersAndCompaction) {
  const uint32_t seed = uint32_t(o_.env->NowMicros());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random random(seed);
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
          index_.c_str(), m_.backup_dir.c_str(), nullptr);
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
  ASSERT_OK(fs_->CreateDir(index_, IOOptions(), nullptr));
  metabypass::Backup backup(verifying, index_, m_);
  ASSERT_OK(backup.Lock());
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
}

TEST_F(MetaBypassTest, QueueOptionsDefaults) {
  const MetaBypassOptions defaults;
  EXPECT_EQ(defaults.wal_queue_capacity, 896U * 1024 * 1024);
  EXPECT_EQ(defaults.metadata_queue_capacity, 128U * 1024 * 1024);
  EXPECT_EQ(defaults.batch_bytes, 512U * 1024 * 1024);
  EXPECT_EQ(defaults.interval_ms, 1000U);
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
    for (const auto& path : {index_, m_.backup_dir}) {
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
  metabypass::Backup backup(fs_, index_, m_);
  metabypass::BackupTestAccess::CheckDirtyWriteCompletions(&backup);
}
TEST_F(MetaBypassTest, MirrorReadinessIndependentProgressReasons) {
  metabypass::Backup backup(fs_, index_, m_);
  metabypass::BackupTestAccess::CheckIndependentReasons(&backup);
}
TEST_F(MetaBypassTest, MirrorReadinessWhilePublicationBlocked) {
  m_.interval_ms = m_.checkpoint_interval_ms = 60000;
  m_.batch_bytes = 256 * 1024;
  o_.write_buffer_size = 4 * 1024 * 1024;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "old", "retained"));
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
  observe = false;
  requested.Release();
  publishing.Release();
  first.join();
  second.join();
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
  metabypass::Backup backup(fs_, index_, m_);
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
  ASSERT_EQ(inventory.substr(0, 5), "MBI7\n");
  std::string file;
  std::istringstream entries(inventory.substr(5));
  while (entries >> file >> size) {
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
TEST_F(MetaBypassTest, CandidateHardLinkIoErrorPreservesPoint) {
  for (const bool reuse : {false, true}) {
    SCOPED_TRACE(reuse);
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
    const auto copied = observed->copied_tables.load();

    observed->reject_reuse = !reuse;
    observed->fail_reuse_link = reuse;
    observed->fail_native_link = !reuse;
    const Status failed = db_->SyncBackup();
    EXPECT_TRUE(failed.IsIOError());
    EXPECT_NE(failed.ToString().find("injected candidate hard link error"),
              std::string::npos);
    EXPECT_EQ(PublishedPoint(), point);
    EXPECT_EQ(observed->copied_tables.load(), copied);
    EXPECT_TRUE(db_->Close().IsIOError());
    db_.reset();

    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("key", "value");
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
  }
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
  std::unique_ptr<FSWritableFile> wal, metadata, temporary, renamed, next;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &metadata));
  applying.WaitUntilBlocked();
  ASSERT_OK(wal->Append("w1", IOOptions(), nullptr));
  ASSERT_OK(metadata->Append("s1", IOOptions(), nullptr));
  ASSERT_OK(metadata->Append("s2", IOOptions(), nullptr));
  ASSERT_OK(wal->Append("w2", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(metadata->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->NewWritableFile(index_ + "/000003.dbtmp", FileOptions(),
                                    &temporary, nullptr));
  ASSERT_OK(temporary->Append("tmp", IOOptions(), nullptr));
  ASSERT_OK(temporary->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->RenameFile(index_ + "/000003.dbtmp", index_ + "/CURRENT",
                               IOOptions(), nullptr));
  ASSERT_OK(backup->ReopenWritableFile(index_ + "/CURRENT", FileOptions(),
                                       &renamed, nullptr));
  ASSERT_OK(renamed->Append("current", IOOptions(), nullptr));
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
  ASSERT_EQ(sequence.size(), 19U);
  for (size_t i = 0; i < sequence.size(); ++i) ASSERT_EQ(sequence[i], i + 1);
  std::string bytes;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/CURRENT", &bytes));
  ASSERT_EQ(bytes, "tmpcurrent");
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

TEST_F(MetaBypassTest, WalSpscMetadataAnchorAndRejectWalRename) {
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
  const std::string metadata_file = index_ + "/MANIFEST-000002";
  const std::string renamed = index_ + "/000004.log";
  for (const auto& paths :
       {std::pair{log, metadata_file}, std::pair{metadata_file, log},
        std::pair{log, renamed}}) {
    ASSERT_TRUE(
        backup->RenameFile(paths.first, paths.second, IOOptions(), nullptr)
            .IsNotSupported());
  }
  std::string primary;
  ASSERT_OK(metabypass::Read(fs_.get(), log, &primary));
  ASSERT_EQ(primary, "w1w2w3");
  ASSERT_OK(metabypass::Read(fs_.get(), metadata_file, &primary));
  ASSERT_EQ(primary, "meta");
  ASSERT_TRUE(fs_->FileExists(renamed, IOOptions(), nullptr).IsNotFound());

  applying.Release();
  ASSERT_OK(backup->Stop());
  // The metadata anchor splits w1 from w2 and w3, even though w2 was
  // published before the metadata event was enqueued.
  const std::vector<std::array<uint64_t, 3>> expected_batches{{{5, 2, 4}}};
  EXPECT_EQ(batches, expected_batches);
  const std::string work = m_.backup_dir + "/work/";
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), work + "000001.log", &bytes));
  EXPECT_EQ(bytes, "w1w2w3");
  ASSERT_OK(metabypass::Read(fs_.get(), work + "MANIFEST-000002", &bytes));
  EXPECT_EQ(bytes, "meta");
  EXPECT_TRUE(
      fs_->FileExists(work + "000004.log", IOOptions(), nullptr).IsNotFound());
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
TEST_F(MetaBypassTest, RejectLegacyInventoryWithoutChangingPublishedFiles) {
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  const std::string inventory = PublishedPoint() + "/INVENTORY";
  std::string original;
  ASSERT_OK(metabypass::Read(fs_.get(), inventory, &original));
  ASSERT_EQ(original.substr(0, 4), "MBI7");
  std::string legacy = original;
  legacy.replace(0, 4, "MBI6");
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
TEST_F(MetaBypassTest, RejectInvalidPublishedPointWithoutMutation) {
  struct Damage {
    const char* file;
    const char* bytes;
  };
  for (const Damage damage :
       {Damage{"RECOVERY", nullptr}, Damage{"RECOVERY", "12\n"},
        Damage{"RECOVERY", "12 4 1\n"}, Damage{"LATEST", ""},
        Damage{"LATEST", "point-1\npoint-2\n"},
        Damage{"LATEST", "point-01\n"}}) {
    SCOPED_TRACE(damage.file);
    SCOPED_TRACE(damage.bytes == nullptr ? "missing" : damage.bytes);
    ASSERT_NO_FATAL_FAILURE(Open());
    ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
    ASSERT_OK(db_->Close());
    db_.reset();

    const std::string file = std::string(damage.file) == "LATEST"
                                 ? m_.backup_dir + "/LATEST"
                                 : PublishedPoint() + "/RECOVERY";
    if (damage.bytes == nullptr) {
      ASSERT_OK(fs_->DeleteFile(file, IOOptions(), nullptr));
    } else {
      ASSERT_OK(metabypass::Write(fs_.get(), file, damage.bytes));
    }
    const auto before_open = SnapshotTree(root_);
    ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsCorruption());
    ASSERT_FALSE(db_);
    ASSERT_EQ(SnapshotTree(root_), before_open);

    Clean(index_);
    const auto before_restore = SnapshotTree(root_);
    ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsCorruption());
    ASSERT_EQ(SnapshotTree(root_), before_restore);
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, FragmentedWalBatchIsAtomic) {
  ASSERT_NO_FATAL_FAILURE(Open());
  WriteBatch batch;
  // Large keys make the batch span several physical log blocks.
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
       {"MetaBypass::CandidateCaptured", "MetaBypass::CandidateFileSyncStatus",
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
  if (argc == 5 && std::string(argv[1]) == "--incremental-restore") {
    Options options;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.backup_dir = argv[3];
    SyncPoint::GetInstance()->SetCallBack(
        argv[4], [](void*) { kill(getpid(), SIGKILL); });
    SyncPoint::GetInstance()->EnableProcessing();
    Status s = MetaBypassDB::Restore(options, bypass, argv[2]);
    fprintf(stderr, "restore did not reach hook: %s\n", s.ToString().c_str());
    return 2;
  }
  if (argc == 6 && std::string(argv[1]) == "--incremental-crash-writer") {
    const int mode = std::stoi(argv[4]);
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 64 * 1024 * 1024;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.backup_dir = argv[3];
    if (mode == 5) {
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
    SyncPoint::GetInstance()->EnableProcessing();
    for (int batch_number = 0; s.ok() && batch_number < 3; ++batch_number) {
      WriteBatch batch;
      if (batch_number == 0) {
        s = batch.Put("overwrite", std::string(4096, 'd'));
        if (s.ok()) s = batch.Delete("gone");
      } else if (batch_number == 1) {
        s = batch.Put("first", std::string(4096, 'e'));
        if (s.ok()) s = batch.Put("second", std::string(4096, 'f'));
      } else {
        s = batch.Delete("first");
        if (s.ok()) s = batch.Put("last", std::string(4096, 'g'));
      }
      if (s.ok()) s = db->Write(WriteOptions(), &batch);
      if (s.ok() && argv[5][0] == '1')
        s = db->Flush(ROCKSDB_NAMESPACE::FlushOptions());
    }
    if (s.ok() && argv[5][0] == '1')
      s = db->CompactRange(ROCKSDB_NAMESPACE::CompactRangeOptions(), nullptr,
                           nullptr);
    if (s.ok()) {
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(20);
      while (s.ok()) {
        const auto observed = notifications.load();
        auto stats = db->GetBackupStats();
        s.UpdateIfOk(stats.error);
        if (!s.ok() || stats.queued_bytes == 0) break;
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
  if (argc == 4 && std::string(argv[1]) == "--sst-crash-writer") {
    using ROCKSDB_NAMESPACE::FlushOptions;
    using ROCKSDB_NAMESPACE::SstTieringMode;
    Options options;
    options.create_if_missing = true;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.backup_dir = argv[3];
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
#endif
#ifndef OS_WIN
  if ((argc == 4 || argc == 5) &&
      std::string(argv[1]) == "--metabypass-crash-writer") {
    Options options;
    options.create_if_missing = true;
    MetaBypassOptions bypass = TestMetaBypassOptions();
    bypass.backup_dir = argv[3];
    const bool sst_copy_crash = argc == 5 && std::string(argv[4]) == "sst-copy";
    auto no_links =
        std::make_shared<ROCKSDB_NAMESPACE::CandidateNoLinkFileSystem>(
            options.env->GetFileSystem());
    auto env = ROCKSDB_NAMESPACE::NewCompositeEnv(no_links);
    if (sst_copy_crash) options.env = env.get();
    const bool gc_crash =
        argc == 5 && std::string(argv[4]) == "MetaBypass::GCFileDeleted";
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
    } else if (argc == 5) {
      SyncPoint::GetInstance()->SetCallBack(
          argv[4], [](void*) { kill(getpid(), SIGKILL); });
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
