//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "rocksdb/utilities/metabypass.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "utilities/metabypass/backup.h"

#ifndef OS_WIN
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {
namespace {
std::string executable;
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
  void Open() { ASSERT_OK(MetaBypassDB::Open(o_, m_, index_, &db_)); }
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
  void NotificationScenario(bool pressure, bool fail, bool timer) {
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
    ASSERT_OK(backup->NewWritableFile(index_ + "/000001.log", FileOptions(),
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
      ASSERT_OK(metabypass::Read(fs_.get(), m_.backup_dir + "/work/000001.log",
                                 &bytes));
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
  EXPECT_LE(points, 3);
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
  if ((argc == 5 || argc == 6) &&
      std::string(argv[1]) == "--metabypass-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = false;
    MetaBypassOptions bypass;
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    std::unique_ptr<MetaBypassDB> db;
    Status s = MetaBypassDB::Open(options, bypass, argv[2], &db);
    if (s.ok()) s = db->Put(WriteOptions(), "a", "old");
    if (s.ok()) s = db->SyncBackup();
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
    if (s.ok()) s = db->Write(WriteOptions(), &batch);
    if (s.ok()) s = db->SyncBackup();
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return 2;
    }
    kill(getpid(), SIGKILL);
    return 3;
  }
#endif
  ROCKSDB_NAMESPACE::executable = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
