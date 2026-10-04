//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "rocksdb/utilities/metabypass.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "db/blob/blob_log_format.h"
#include "file/filename.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "utilities/metabypass/backup.h"
#include "utilities/metabypass/tiered_storage.h"

#ifndef OS_WIN
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {
namespace {
std::string executable;
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
  IOStatus LinkFile(const std::string& from, const std::string& to,
                    const IOOptions& options, IODebugContext* dbg) override {
    if (from.find("/work/") != std::string::npos &&
        to.find("/point-") != std::string::npos) {
      ++rejected_links;
      return IOStatus::NotSupported("injected candidate link failure");
    }
    return target()->LinkFile(from, to, options, dbg);
  }
  IOStatus NewSequentialFile(const std::string& path,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    if (path.find("/work/") != std::string::npos && path.size() >= 4 &&
        path.substr(path.size() - 4) == ".sst")
      ++copied_tables;
    return target()->NewSequentialFile(path, options, result, dbg);
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
    o_.allow_concurrent_memtable_write = false;
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
  void Open() { ASSERT_OK(MetaBypassDB::Open(o_, m_, index_, &db_)); }
  void EnsureTwoPoints() {
    for (int i = 0; i < 3; ++i) {
      auto stats = db_->GetBackupStats();
      ASSERT_OK(stats.error);
      if (stats.recovery_points >= 2) return;
      ASSERT_OK(db_->Put(WriteOptions(), "point" + std::to_string(i), "value"));
      ASSERT_OK(db_->SyncBackup());
    }
    auto stats = db_->GetBackupStats();
    ASSERT_OK(stats.error);
    ASSERT_GE(stats.recovery_points, 2);
  }
  void Check(const std::string& key, const std::string& expected) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, expected);
  }
  void Restore() {
    Clean(index_);
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
    ASSERT_NO_FATAL_FAILURE(Open());
  }
#ifndef OS_WIN
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
  void StartBackupFiles(std::unique_ptr<metabypass::Backup>* backup,
                        std::unique_ptr<FSWritableFile>* wal,
                        std::unique_ptr<FSWritableFile>* sst) {
    ASSERT_NO_FATAL_FAILURE(StartBackup(backup));
    ASSERT_OK((*backup)->NewWritableFile(index_ + "/000001.log", FileOptions(),
                                         wal, nullptr));
    ASSERT_OK((*backup)->NewWritableFile(index_ + "/000002.sst", FileOptions(),
                                         sst, nullptr));
  }
  void NotificationScenario(bool pressure, bool fail, bool timer,
                            bool metadata = false) {
    const std::string file_name = metadata ? "000001.sst" : "000001.log";
    m_.queue_capacity = m_.batch_bytes = 4096;
    m_.interval_ms = timer ? 1 : 60000;
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
      EXPECT_LE(stats.peak_queued_bytes, m_.queue_capacity);
    }
    SyncPoint::GetInstance()->DisableProcessing();
  }
  void QueueScenario(bool fail) {
    m_.queue_capacity = 4096;
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
    ASSERT_LE(stats.peak_queued_bytes, m_.queue_capacity);
    ASSERT_GT(stats.backpressure_micros, 0);
    std::string bytes;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log",
                               &bytes));
    ASSERT_EQ(bytes, std::string(2000, 'a') + std::string(3000, 'b'));
  }
  std::string root_, index_;
  Options o_;
  MetaBypassOptions m_;
  std::shared_ptr<FileSystem> fs_;
  std::unique_ptr<MetaBypassDB> db_;
};
TEST_F(MetaBypassTest, DirectSyncWritesWaitForPublicationAndReportFailure) {
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  for (bool fail : {false, true}) {
    TestGate validating, waiters;
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
    // Publication is blocked, but primary writes and reads can still proceed.
    EXPECT_OK(db_->Put(WriteOptions(), "async", "progress"));
    Check("async", "progress");
    validating.Release();
    for (auto& thread : threads) thread.join();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(completed.load(), 3);
  }
  Check("key0", "new");
  EXPECT_TRUE(db_->Put(WriteOptions(), "rejected", "v").IsIOError());
  EXPECT_TRUE(db_->Close().IsIOError());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key0", "old");
  Check("key1", "old");
  Check("pair0", "v");
  Check("pair1", "v");
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

TEST_F(MetaBypassTest, TieredPressureEvictsWithoutMemtableFlush) {
  EnableTier(32 * 1024);
  o_.write_buffer_size = 16 * 1024 * 1024;
  Open();
  for (int i = 0; i < 100; ++i)
    ASSERT_OK(
        db_->Put(WriteOptions(), std::to_string(i), std::string(1000, 'v')));
  ASSERT_OK(db_->SyncBackup());
  for (int i = 0; i < 100; ++i)
    Check(std::to_string(i), std::string(1000, 'v'));
  auto stats = db_->GetBackupStats();
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_staging_bytes, m_.staging_capacity);
  ASSERT_GT(stats.migrated_blob_bytes, m_.staging_capacity);
  ASSERT_GT(stats.staging_backpressure_micros, 0);
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(index_, IOOptions(), &files, nullptr));
  for (const auto& f : files) ASSERT_EQ(f.find(".sst"), std::string::npos);
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
    sync_status = db_->SyncBackup();
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
  Open();
  WriteOptions sync;
  sync.sync = true;
  ASSERT_OK(db_->Put(sync, "key", "old"));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierExtentSynced", [](void* p) {
        *static_cast<Status*>(p) = Status::IOError("injected tier failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_FALSE(db_->Put(sync, "key", "new").ok());
  ASSERT_FALSE(db_->Put(WriteOptions(), "rejected", "value").ok());
  Check("key", "new");
  ASSERT_FALSE(db_->SyncBackup().ok());
  ASSERT_FALSE(db_->Close().ok());
  db_.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  Clean(index_);
  Clean(m_.staging_dir);
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("key", "old");
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
TEST_F(MetaBypassTest, TieredCrashBoundariesAndCompleteFastStorageLoss) {
  EnableTier(1024 * 1024);
  for (const std::string stage :
       {"", "MetaBypass::TierExtentSynced", "MetaBypass::TierDescriptorSynced",
        "MetaBypass::CandidateSynced", "MetaBypass::BeforePointerReplace",
        "MetaBypass::PointerSynced", "MetaBypass::TierBeforeEvict"}) {
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
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierDescriptorSynced", [](void* p) {
        *static_cast<Status*>(p) = Status::IOError("recovery interruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_FALSE(MetaBypassDB::Restore(o_, m_, index_).ok());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &after));
  ASSERT_EQ(pointer, after);
  Open();
  Check("key", "value");
}

TEST_F(MetaBypassTest, TieredPublishedExtentCorruptionRejected) {
  EnableTier(1024 * 1024);
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
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
  ASSERT_FALSE(MetaBypassDB::Restore(o_, m_, index_).ok());
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
  ASSERT_OK(metabypass::Write(fs_.get(), m_.staging_dir + "/999999.blob",
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

TEST_F(MetaBypassTest, TieredCloseWithoutFlushingActiveBlob) {
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

TEST_F(MetaBypassTest, TieredRequestedDependenciesPreemptBackgroundCopy) {
  EnableTier(8 * 1024 * 1024);
  o_.write_buffer_size = 16 * 1024 * 1024;
  o_.blob_file_size = 2 * 1024 * 1024;
  Open();
  TestGate validation, extent, request;
  std::atomic<bool> first{true};
  std::atomic<int> yields{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) { validation.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierExtentSynced",
                                        [&](void*) {
                                          if (first.exchange(false))
                                            extent.Block();
                                        });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierPersistRequested",
                                        [&](void*) { request.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationYield",
                                        [&](void*) { ++yields; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "a", std::string(1024 * 1024, 'a')));
  ASSERT_OK(db_->Put(WriteOptions(), "b", std::string(1024 * 1024, 'b')));
  extent.WaitUntilBlocked();
  Status result;
  std::thread sync([&] { result = db_->SyncBackup(); });
  validation.Release();
  request.WaitUntilBlocked();
  request.Release();
  extent.Release();
  sync.join();
  ASSERT_OK(result);
  ASSERT_GT(yields.load(), 0);
  Check("a", std::string(1024 * 1024, 'a'));
  Check("b", std::string(1024 * 1024, 'b'));
}

TEST_F(MetaBypassTest, TieredRejectsOversizedBatchBeforeWrite) {
  EnableTier(8192);
  Open();
  ASSERT_TRUE(db_->Put(WriteOptions(), "too-large", std::string(8192, 'v'))
                  .IsInvalidArgument());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "too-large", &value).IsNotFound());
  ASSERT_OK(db_->Put(WriteOptions(), "small", "value"));
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
TEST_F(MetaBypassTest, SstCompleteSsdLossRestoresExactlyPublishedState) {
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
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  Open();
  Check("updated", "published");
  Check("deleted", "published");
  std::string value;
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
  ASSERT_EQ(points, 2);
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
  ASSERT_NO_FATAL_FAILURE(Open());
  WriteOptions w;
  w.disableWAL = true;
  ASSERT_TRUE(db_->Put(w, "x", "y").IsNotSupported());
  WriteBatch batch;
  ASSERT_OK(batch.Merge("x", "y"));
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).IsNotSupported());
  ASSERT_OK(db_->Close());
  db_.reset();
  o_.allow_concurrent_memtable_write = true;
  ASSERT_TRUE(MetaBypassDB::Open(o_, m_, index_, &db_).IsNotSupported());
}
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
TEST_F(MetaBypassTest, ValidatorBlockedMirrorStillDrainsAndPinsFiles) {
  m_.queue_capacity = 32768;
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
TEST_F(MetaBypassTest, RetiredPointCleanupDoesNotDelaySyncOrMirror) {
  m_.queue_capacity = 32768;
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_NO_FATAL_FAILURE(EnsureTwoPoints());
  TestGate gc, next_publication;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::GCStarted",
                                        [&](void*) { gc.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  WriteOptions sync_write;
  sync_write.sync = true;
  ASSERT_OK(db_->Put(sync_write, "third", "durable"));
  ASSERT_OK(db_->SyncBackup());
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
  EXPECT_EQ(points, 2);
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("third", "durable");
  Check("tail299", "v");
}
TEST_F(MetaBypassTest, RetiredPointCleanupFailureIsSticky) {
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_NO_FATAL_FAILURE(EnsureTwoPoints());
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
  metabypass::NativeState state;
  ASSERT_OK(metabypass::Inspect(
      fs_.get(), m_.backup_dir + "/" + pointer.substr(0, pointer.find(' ')),
      &state));
  ASSERT_GT(state.log_number, 0);
  auto before = db_->GetBackupStats();
  ASSERT_OK(before.error);
  const std::vector<std::string> unused = {"MANIFEST-999999", "000000.log",
                                           "999999.sst"};
  std::atomic<bool> injected{false}, checked{false}, absent{true};
  Status injection;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeCaptureSnapshot", [&](void* arg) {
        if (injected.exchange(true)) return;
        static_cast<std::set<std::string>*>(arg)->insert("999999.sst");
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
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "first", "v"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  metabypass::NativeState state;
  ASSERT_OK(metabypass::Inspect(
      fs_.get(), m_.backup_dir + "/" + pointer.substr(0, pointer.find(' ')),
      &state));
  ASSERT_FALSE(state.tables.empty());
  const std::string table = MakeTableFileName(state.tables.begin()->first);
  std::atomic<bool> removed{false};
  Status removal;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeCaptureFiles", [&](void*) {
        if (removed.exchange(true)) return;
        removal = fs_->DeleteFile(m_.backup_dir + "/work/" + table, IOOptions(),
                                  nullptr);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "second", "v"));
  EXPECT_TRUE(db_->SyncBackup().IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();
  EXPECT_TRUE(removed);
  ASSERT_OK(removal);
  auto stats = db_->GetBackupStats();
  EXPECT_TRUE(stats.error.IsCorruption());
  EXPECT_TRUE(db_->Put(WriteOptions(), "rejected", "v").IsCorruption());
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
TEST_F(MetaBypassTest, IncompleteCaptureDefersUntilNextBoundary) {
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "first", "v"));
  ASSERT_OK(db_->SyncBackup());
  std::string current;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/CURRENT", &current));
  TestGate deferred;
  std::atomic<bool> injected{false};
  Status mutation;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::BeforeCaptureFiles", [&](void*) {
        if (injected.exchange(true)) return;
        mutation = metabypass::Write(fs_.get(), m_.backup_dir + "/work/CURRENT",
                                     "MANIFEST-");
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::CaptureDeferred",
                                        [&](void*) { deferred.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "second", "v"));
  Status sync_status;
  std::thread sync([&] { sync_status = db_->SyncBackup(); });
  deferred.WaitUntilBlocked();
  ASSERT_OK(mutation);
  ASSERT_OK(
      metabypass::Write(fs_.get(), m_.backup_dir + "/work/CURRENT", current));
  ASSERT_OK(db_->Put(WriteOptions(), "third", "v"));
  deferred.Release();
  sync.join();
  ASSERT_OK(sync_status);
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("second", "v");
  Check("third", "v");
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
TEST_F(MetaBypassTest, IncrementalValidationReusesPrefixesAndTables) {
  o_.write_buffer_size = 4 * 1024 * 1024;
  o_.disable_auto_compactions = true;
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  for (int i = 0; i < 100; ++i)
    ASSERT_OK(
        db_->Put(WriteOptions(), std::to_string(i), std::string(1024, 'v')));
  ASSERT_OK(db_->SyncBackup());
  auto before = db_->GetBackupStats();
  ASSERT_OK(before.error);
  std::atomic<int> records{0}, tables{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::WalRecordValidated",
                                        [&](void*) { ++records; });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TableValidated",
                                        [&](void*) { ++tables; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "new", std::string(1024, 'n')));
  ASSERT_OK(db_->SyncBackup());
  auto after = db_->GetBackupStats();
  ASSERT_OK(after.error);
  ASSERT_EQ(records.load(), 1);
  ASSERT_GT(after.validated_blob_bytes, before.validated_blob_bytes);
  ASSERT_LT(after.validated_blob_bytes - before.validated_blob_bytes, 4096);
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_GT(tables.load(), 0);
  const int table_count = tables.load();
  auto flushed = db_->GetBackupStats();
  ASSERT_OK(flushed.error);
  ASSERT_OK(db_->Put(WriteOptions(), "next", "tail"));
  ASSERT_OK(db_->SyncBackup());
  auto final = db_->GetBackupStats();
  ASSERT_OK(final.error);
  ASSERT_EQ(tables.load(), table_count);
  ASSERT_GT(final.reused_tables, flushed.reused_tables);
  // Exercise multiple validation-buffer reads and a zero-payload record,
  // then verify their CRCs through the full offline recovery path.
  ASSERT_OK(db_->Put(WriteOptions(), "large", std::string(200000, 'L')));
  ASSERT_OK(db_->Put(WriteOptions(), "", ""));
  ASSERT_OK(db_->SyncBackup());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(db_->Close());
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("0", std::string(1024, 'v'));
  Check("new", std::string(1024, 'n'));
  Check("next", "tail");
  Check("large", std::string(200000, 'L'));
  Check("", "");
}
TEST_F(MetaBypassTest, IncrementalValidationRejectsNewCorruption) {
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "old", "valid"));
  ASSERT_OK(db_->SyncBackup());
  ASSERT_OK(db_->Put(WriteOptions(), "new", "damaged"));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &files, nullptr));
  bool corrupted = false;
  for (const auto& f : files) {
    if (f.size() < 5 || f.substr(f.size() - 5) != ".blob") continue;
    std::string bytes;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.data_dir + "/" + f, &bytes));
    ASSERT_FALSE(bytes.empty());
    bytes.back() ^= 1;
    ASSERT_OK(metabypass::Write(fs_.get(), m_.data_dir + "/" + f, bytes));
    corrupted = true;
  }
  ASSERT_TRUE(corrupted);
  ASSERT_TRUE(db_->SyncBackup().IsCorruption());
  ASSERT_FALSE(db_->Close().ok());
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
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  TestGate validating, closing;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::ValidateCandidate",
                                        [&](void*) { validating.Block(); });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::CloseWaitingForValidation",
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
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
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
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("key", "old");
}

TEST_F(MetaBypassTest, ChannelMetadataOnlyDrainsOnTimer) {
  NotificationScenario(false, false, true, true);
}
TEST_F(MetaBypassTest, ChannelWalPassesBlockedPrimarySst) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  TestGate primary;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::PrimaryReserved", [&](void* arg) {
        if (*static_cast<std::string*>(arg) == "000002.sst") primary.Block();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread writer(
      [&] { EXPECT_OK(sst->Append("sst", IOOptions(), nullptr)); });
  primary.WaitUntilBlocked();
  // Must return while the SST operation still owns its primary-operation lock.
  ASSERT_OK(wal->Append("wal", IOOptions(), nullptr));
  primary.Release();
  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(sst->Close(IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
}
TEST_F(MetaBypassTest, ChannelWalPassesSstCapacityWait) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure",
                                        [&](void*) { blocked.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  applying.WaitUntilBlocked();
  ASSERT_OK(sst->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread writer([&] {
    EXPECT_OK(sst->Append(std::string(3000, 'b'), IOOptions(), nullptr));
  });
  blocked.WaitUntilBlocked();
  blocked.Release();
  ASSERT_OK(wal->Append(std::string(500, 'w'), IOOptions(), nullptr));
  applying.Release();
  writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(sst->Close(IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  auto stats = backup->Stats();
  ASSERT_OK(stats.error);
  ASSERT_LE(stats.peak_queued_bytes, m_.queue_capacity);
  std::string bytes;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000002.sst", &bytes));
  ASSERT_EQ(bytes, std::string(6000, 'a') + std::string(3000, 'b'));
}
TEST_F(MetaBypassTest, ChannelBothCapacityWaitersWakeOnFailure) {
  m_.queue_capacity = 8192;
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
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  applying.WaitUntilBlocked();
  ASSERT_OK(sst->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread meta_writer([&] {
    EXPECT_TRUE(
        sst->Append(std::string(3000, 'b'), IOOptions(), nullptr).IsIOError());
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
  ASSERT_TRUE(sst->Close(IOOptions(), nullptr).IsIOError());
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
  std::unique_ptr<FSWritableFile> wal, sst, renamed, next;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  applying.WaitUntilBlocked();
  ASSERT_OK(wal->Append("w1", IOOptions(), nullptr));
  ASSERT_OK(sst->Append("s1", IOOptions(), nullptr));
  ASSERT_OK(sst->Append("s2", IOOptions(), nullptr));
  ASSERT_OK(wal->Append("w2", IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(sst->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->RenameFile(index_ + "/000001.log", index_ + "/000003.sst",
                               IOOptions(), nullptr));
  ASSERT_OK(backup->ReopenWritableFile(index_ + "/000003.sst", FileOptions(),
                                       &renamed, nullptr));
  ASSERT_OK(renamed->Append("w3", IOOptions(), nullptr));
  ASSERT_OK(renamed->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->RenameFile(index_ + "/000002.sst", index_ + "/000004.sst",
                               IOOptions(), nullptr));
  ASSERT_OK(backup->DeleteFile(index_ + "/000004.sst", IOOptions(), nullptr));
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
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000003.sst", &bytes));
  ASSERT_EQ(bytes, "w1w2w3");
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000005.log", &bytes));
  ASSERT_EQ(bytes, "w4");
  ASSERT_TRUE(
      fs_->FileExists(m_.backup_dir + "/work/000004.sst", IOOptions(), nullptr)
          .IsNotFound());
}
TEST_F(MetaBypassTest, ChannelFailureRetainsOutstandingReservation) {
  m_.batch_bytes = 1;
  TestGate applying, primary, discarded;
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::PrimaryReserved", [&](void* arg) {
        if (*static_cast<std::string*>(arg) == "000002.sst") primary.Block();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("mirror failure with primary in flight");
      });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::QueuesDiscarded",
                                        [&](void*) { discarded.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  applying.WaitUntilBlocked();
  std::thread writer([&] {
    EXPECT_OK(sst->Append(std::string(1024, 'x'), IOOptions(), nullptr));
  });
  primary.WaitUntilBlocked();
  applying.Release();
  discarded.WaitUntilBlocked();
  discarded.Release();
  ASSERT_TRUE(backup->Stop().IsIOError());
  auto before = backup->Stats();
  ASSERT_TRUE(before.error.IsIOError());
  ASSERT_GE(before.queued_bytes, 1024U);
  ASSERT_LE(before.queued_bytes, m_.queue_capacity);
  primary.Release();
  writer.join();
  auto after = backup->Stats();
  ASSERT_TRUE(after.error.IsIOError());
  ASSERT_EQ(after.queued_bytes, 0U);
  ASSERT_TRUE(sst->Close(IOOptions(), nullptr).IsIOError());
  ASSERT_TRUE(wal->Close(IOOptions(), nullptr).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
}
TEST_F(MetaBypassTest, ChannelBothWaitersCompeteForReleasedCapacity) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  std::atomic<int> waiters{0};
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure", [&](void*) {
    if (++waiters == 2) blocked.Block();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  applying.WaitUntilBlocked();
  ASSERT_OK(sst->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread meta_writer([&] {
    EXPECT_OK(sst->Append(std::string(6000, 'b'), IOOptions(), nullptr));
  });
  std::thread wal_writer([&] {
    EXPECT_OK(wal->Append(std::string(6000, 'w'), IOOptions(), nullptr));
  });
  blocked.WaitUntilBlocked();
  blocked.Release();
  applying.Release();
  meta_writer.join();
  wal_writer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(sst->Close(IOOptions(), nullptr));
  ASSERT_OK(wal->Close(IOOptions(), nullptr));
  ASSERT_OK(backup->Stop());
  auto stats = backup->Stats();
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.queued_bytes, 0U);
  ASSERT_LE(stats.peak_queued_bytes, m_.queue_capacity);
  std::string bytes;
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000002.sst", &bytes));
  ASSERT_EQ(bytes, std::string(6000, 'a') + std::string(6000, 'b'));
  ASSERT_OK(
      metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log", &bytes));
  ASSERT_EQ(bytes, std::string(6000, 'w'));
}

TEST_F(MetaBypassTest, ChannelStopWakesBothCapacityWaiters) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  std::atomic<int> waiters{0};
  BlockFirstApply(&applying);
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure", [&](void*) {
    if (++waiters == 2) blocked.Block();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_NO_FATAL_FAILURE(StartBackupFiles(&backup, &wal, &sst));
  applying.WaitUntilBlocked();
  ASSERT_OK(sst->Append(std::string(6000, 'a'), IOOptions(), nullptr));
  std::thread meta_writer([&] {
    EXPECT_TRUE(
        sst->Append(std::string(3000, 'b'), IOOptions(), nullptr).IsIOError());
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
  ASSERT_TRUE(sst->Close(IOOptions(), nullptr).IsIOError());
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
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "old");
}
TEST_F(MetaBypassTest, PublishedCorruptionRejected) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "x", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  std::string pointer;
  ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/LATEST", &pointer));
  const std::string point = pointer.substr(0, pointer.find(' '));
  metabypass::NativeState state;
  ASSERT_OK(
      metabypass::Inspect(fs_.get(), m_.backup_dir + "/" + point, &state));
  ASSERT_OK(metabypass::Write(
      fs_.get(), m_.backup_dir + "/" + point + "/" + state.manifest,
      "corrupt"));
  Clean(index_);
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsCorruption());
}
TEST_F(MetaBypassTest, BlobCorruptionRejectedBeforeMutation) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "x", "value"));
  ASSERT_OK(db_->Close());
  db_.reset();
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &files, nullptr));
  std::string damaged;
  for (const auto& file : files) {
    if (file.size() > 5 && file.substr(file.size() - 5) == ".blob") {
      damaged = m_.data_dir + "/" + file;
      break;
    }
  }
  ASSERT_FALSE(damaged.empty());
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), damaged, &bytes));
  ASSERT_GT(bytes.size(), 64);
  for (const size_t offset : {size_t{64}, bytes.size() - 1}) {
    SCOPED_TRACE(offset);
    std::string corrupted = bytes;
    corrupted[offset] ^= 1;
    ASSERT_OK(metabypass::Write(fs_.get(), damaged, corrupted));
    Clean(index_);
    ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsCorruption());
    std::string after;
    ASSERT_OK(metabypass::Read(fs_.get(), damaged, &after));
    ASSERT_EQ(corrupted, after);
  }
}
TEST_F(MetaBypassTest, CandidateRejectsMismatchedBlobHeader) {
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.data_dir, IOOptions(), &files, nullptr));
  bool changed = false;
  for (const auto& file : files) {
    if (file.size() <= 5 || file.substr(file.size() - 5) != ".blob") continue;
    std::string bytes;
    ASSERT_OK(metabypass::Read(fs_.get(), m_.data_dir + "/" + file, &bytes));
    ASSERT_GE(bytes.size(), 30);
    bytes[13] = 1;  // Change the header compression without changing the index.
    ASSERT_OK(metabypass::Write(fs_.get(), m_.data_dir + "/" + file, bytes));
    changed = true;
  }
  ASSERT_TRUE(changed);
  ASSERT_TRUE(db_->SyncBackup().IsCorruption());
  ASSERT_FALSE(db_->Close().ok());
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
       {"MetaBypass::DependenciesSynced", "MetaBypass::CandidateSynced",
        "MetaBypass::BeforePointerReplace", "MetaBypass::PointerSynced"}) {
    SCOPED_TRACE(stage);
    ASSERT_NO_FATAL_FAILURE(RunCrashWriter(stage));
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("a", stage == "MetaBypass::PointerSynced" ? "last" : "old");
    ASSERT_OK(db_->Close());
    db_.reset();
    Clean(root_);
    ASSERT_OK(fs_->CreateDir(root_, IOOptions(), nullptr));
  }
}
TEST_F(MetaBypassTest, SigkillDuringRetiredPointCleanup) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter("MetaBypass::GCFileDeleted"));
  std::vector<std::string> files;
  ASSERT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  int points = 0;
  for (const auto& file : files)
    if (file.compare(0, 6, "point-") == 0) ++points;
  ASSERT_GE(points, 3);
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("a", "last");
  ASSERT_OK(db_->Close());
  db_.reset();
  files.clear();
  ASSERT_OK(fs_->GetChildren(m_.backup_dir, IOOptions(), &files, nullptr));
  points = 0;
  for (const auto& file : files)
    if (file.compare(0, 6, "point-") == 0) ++points;
  EXPECT_EQ(points, 2);
}
TEST_F(MetaBypassTest, InterruptedBlobPreparationIsRetryable) {
  ASSERT_NO_FATAL_FAILURE(RunCrashWriter());
  Clean(index_);
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::RecoveryBlobSealed", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("interrupted sealing");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsIOError());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
  ASSERT_NO_FATAL_FAILURE(Open());
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
  class LocalReadFailureFileSystem : public FileSystemWrapper {
   public:
    explicit LocalReadFailureFileSystem(std::shared_ptr<FileSystem> fs)
        : FileSystemWrapper(std::move(fs)) {}
    const char* Name() const override { return "LocalReadFailureFileSystem"; }
    std::string local_path;
    std::atomic<bool> fail_read{true};
    std::atomic<int> closed_handles{0};
    std::function<void()> on_close;
    IOStatus NewRandomAccessFile(const std::string& path, const FileOptions& o,
                                 std::unique_ptr<FSRandomAccessFile>* out,
                                 IODebugContext* d) override {
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
        return target()->Read(offset, n, o, out, scratch, d);
      }

     private:
      std::unique_ptr<FSRandomAccessFile> file_;
      LocalReadFailureFileSystem* fs_;
    };
  };
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
  Status CreateLocalBlob(const std::string& blob,
                         std::unique_ptr<FSWritableFile>* writer) {
    Status s = storage_->NewWritableFile(BlobFileName(m_.data_dir, 1),
                                         FileOptions(), writer, nullptr);
    if (s.ok()) s = (*writer)->Append(blob, IOOptions(), nullptr);
    return s;
  }
  Status Migrate(uint64_t length) {
    return storage_->TieredStorage::Migrate(1, length, false);
  }
  uint64_t LocalPins() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    const auto it = storage_->local_.find(1);
    return it == storage_->local_.end() ? 0 : it->second.readers;
  }
  bool RemoteRoute() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    const auto it = storage_->local_.find(1);
    return it == storage_->local_.end() || it->second.evicting;
  }
  bool RemoteSealed() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->versions_.at(1)->sealed;
  }
  uint64_t RemoteSize() {
    std::lock_guard<std::mutex> lock(storage_->mutex_);
    return storage_->versions_.at(1)->size;
  }
  Status LocalExists() {
    return fs_->FileExists(BlobFileName(m_.staging_dir, 1), IOOptions(),
                           nullptr);
  }
  void Initialize(uint64_t capacity = 1024 * 1024) {
    EnableTier(capacity);
    ASSERT_OK(EnsureDir(fs_.get(), m_.data_dir));
    storage_.reset(new TieredStorage(fs_, m_));
    ASSERT_OK(storage_->Initialize(false));
  }
  Status Append(const std::string& bytes) {
    TieredStorage::Version v;
    auto previous = storage_->versions_.find(1);
    if (previous != storage_->versions_.end()) v = *previous->second;
    Status s = storage_->TieredStorage::AppendExtent(1, bytes, &v);
    v.sealed = true;
    return s.ok() ? storage_->TieredStorage::Commit(1, std::move(v)) : s;
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
  std::unique_ptr<TieredStorage> storage_;
};

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
  for (int i = 0; i < 100; ++i) {
    const size_t offset = random.Uniform(expected.size() + 1);
    const size_t length = random.Uniform(expected.size() + 1);
    std::string bytes;
    ASSERT_OK(ReadBytes(offset, length, &bytes));
    ASSERT_EQ(bytes, expected.substr(offset, length));
  }
  const size_t prefix = expected.size() - 3;
  ASSERT_OK(storage_->SaveCheckpoint(root_ + "/checkpoint", {{1, prefix}}));
  ASSERT_OK(storage_->LoadCheckpoint(root_ + "/checkpoint"));
  std::string bytes;
  ASSERT_OK(ReadBytes(0, expected.size(), &bytes));
  ASSERT_EQ(bytes, expected.substr(0, prefix));
}

TEST_F(TieredStorageTest, ActiveAndPartialMigrationRetainLocalSource) {
  Initialize(128);
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
  const uint64_t prefix = BlobLogHeader::kSize;
  ASSERT_OK(Migrate(prefix));
  ASSERT_EQ(RemoteSize(), prefix);
  ASSERT_FALSE(RemoteSealed());
  ASSERT_FALSE(RemoteRoute());
  ASSERT_OK(LocalExists());
  // Even a fully copied active file has no sealed remote route yet.
  ASSERT_OK(Migrate(blob.size()));
  ASSERT_EQ(RemoteSize(), blob.size());
  ASSERT_FALSE(RemoteSealed());
  ASSERT_FALSE(RemoteRoute());
  ASSERT_OK(LocalExists());
  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  // A closed source and a prefix request still cannot authorize deletion.
  ASSERT_OK(Migrate(prefix));
  ASSERT_FALSE(RemoteRoute());
  ASSERT_OK(LocalExists());
  std::string bytes;
  ASSERT_OK(ReadBytes(0, blob.size(), &bytes));
  ASSERT_EQ(bytes, blob);
  ASSERT_OK(Migrate(blob.size()));
  ASSERT_TRUE(RemoteSealed());
  ASSERT_TRUE(RemoteRoute());
  ASSERT_TRUE(LocalExists().IsNotFound());
}

TEST_F(TieredStorageTest, LocalReadErrorReleasesPinBeforeMigration) {
  auto failing = std::make_shared<LocalReadFailureFileSystem>(fs_);
  fs_ = failing;
  Initialize(128);
  failing->local_path = BlobFileName(m_.staging_dir, 1);
  const std::string blob = EmptyBlob();
  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(CreateLocalBlob(blob, &writer));
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
  ASSERT_OK(Migrate(BlobLogHeader::kSize));
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
  ASSERT_EQ(RemoteSize(), BlobLogHeader::kSize);
  ASSERT_FALSE(RemoteSealed());
  ASSERT_FALSE(RemoteRoute());
  ASSERT_OK(LocalExists());
  std::string bytes;
  ASSERT_OK(ReadBytes(0, blob.size(), &bytes));
  ASSERT_EQ(bytes, blob);
  MetaBypassStats stats;
  storage_->AddStats(&stats);
  ASSERT_OK(stats.error);
  ASSERT_EQ(stats.staging_bytes, blob.size());
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
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierMigrationComplete",
                                        [&](void*) { migrated.Block(); });
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
  Status reserve = storage_->Reserve(100, [] { return Status::OK(); });
  storage_->ReleaseReservation();
  Status stop = storage_->Stop();
  ASSERT_OK(reserve);
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
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  using ROCKSDB_NAMESPACE::MetaBypassDB;
  using ROCKSDB_NAMESPACE::MetaBypassOptions;
  using ROCKSDB_NAMESPACE::Options;
  using ROCKSDB_NAMESPACE::Status;
  using ROCKSDB_NAMESPACE::SyncPoint;
  using ROCKSDB_NAMESPACE::WriteBatch;
  using ROCKSDB_NAMESPACE::WriteOptions;
#ifndef OS_WIN
  if (argc == 5 && std::string(argv[1]) == "--sst-crash-writer") {
    using ROCKSDB_NAMESPACE::FlushOptions;
    using ROCKSDB_NAMESPACE::SstTieringMode;
    Options options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = false;
    MetaBypassOptions bypass;
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
    options.allow_concurrent_memtable_write = false;
    MetaBypassOptions m;
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
    kill(getpid(), SIGKILL);
    return 3;
  }
#endif
#ifndef OS_WIN
  if ((argc == 5 || argc == 6) &&
      std::string(argv[1]) == "--metabypass-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = false;
    MetaBypassOptions bypass;
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    const bool gc_crash =
        argc == 6 && std::string(argv[5]) == "MetaBypass::GCFileDeleted";
    if (gc_crash) {
      bypass.batch_bytes = bypass.queue_capacity;
      bypass.interval_ms = 60000;
    }
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    if (s.ok()) s = db->Put(WriteOptions(), "a", "old");
    if (s.ok()) s = db->SyncBackup();
    if (s.ok() && gc_crash) {
      auto stats = db->GetBackupStats();
      s.UpdateIfOk(stats.error);
      for (int i = 0; s.ok() && stats.recovery_points < 2 && i < 3; ++i) {
        s = db->Put(WriteOptions(), "middle" + std::to_string(i), "value");
        if (s.ok()) s = db->SyncBackup();
        stats = db->GetBackupStats();
        s.UpdateIfOk(stats.error);
      }
    }
    if (argc == 6) {
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
    sync.sync = true;
    if (s.ok()) s = db->Write(sync, &batch);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    if (gc_crash) {
      // Close must wait for GC. Only the GCFileDeleted hook may kill us here.
      s = db->Close();
      fprintf(stderr, "GCFileDeleted hook did not run: %s\n",
              s.ToString().c_str());
      return 3;
    }
    kill(getpid(), SIGKILL);
    return 3;
  }
#endif
  ROCKSDB_NAMESPACE::executable = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
