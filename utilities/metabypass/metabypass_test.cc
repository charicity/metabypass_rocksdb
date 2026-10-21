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
#include "util/crc32c.h"
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
    return m_.backup_dir + "/" + pointer.substr(0, pointer.find(' '));
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
  void DirectWriteFailureScenario(const std::string& point, bool tier) {
    EnableDirectWrite();
    if (tier) EnableTier();
    ASSERT_NO_FATAL_FAILURE(Open());
    WriteOptions sync;
    sync.sync = true;
    ASSERT_OK(db_->Put(sync, "key", "old"));
    const std::string published = PublishedPoint();
    SyncPoint::GetInstance()->SetCallBack(point, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("direct checkpoint failure");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_TRUE(db_->Put(sync, "key", "new").IsIOError());
    Check("key", "new");
    ASSERT_EQ(PublishedPoint(), published);
    ASSERT_TRUE(db_->SyncBackup().IsIOError());
    ASSERT_TRUE(db_->Put(WriteOptions(), "rejected", "x").IsIOError());
    ASSERT_TRUE(db_->Close().IsIOError());
    db_.reset();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (tier) Clean(m_.staging_dir);
    ASSERT_NO_FATAL_FAILURE(Restore());
    Check("key", "old");
    ASSERT_OK(db_->Put(sync, "after", "recovered"));
  }
  void Restore() {
    Clean(index_);
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
    ASSERT_NO_FATAL_FAILURE(Open());
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
    ASSERT_OK(MetaBypassDB::Restore(o_, m_, index_));
    ASSERT_NO_FATAL_FAILURE(Open());
    const std::string expected(
        16 * 1024, stage == "MetaBypass::PointerSynced" ? 'n' : 'o');
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
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "MBN1\n"));
  ASSERT_OK(metabypass::Write(fs_.get(), index_ + "/sentinel", "untouched"));
  const auto before = SnapshotTree(root_);
  ASSERT_FALSE(MetaBypassDB::Open(o_, m_, index_, &db_).ok());
  std::string bytes;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &bytes));
  ASSERT_EQ(bytes, "MBN1\n");
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
  ASSERT_OK(metabypass::Write(fs_.get(), marker, "MBN1\n"));
  const std::string point = PublishedPoint();
  std::string inventory;
  ASSERT_OK(metabypass::Read(fs_.get(), point + "/INVENTORY", &inventory));
  Clean(index_);
  const auto before = SnapshotTree(root_);
  ASSERT_FALSE(MetaBypassDB::Restore(o_, m_, index_).ok());
  std::string after;
  ASSERT_OK(metabypass::Read(fs_.get(), marker, &after));
  ASSERT_EQ(after, "MBN1\n");
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
  ASSERT_NO_FATAL_FAILURE(Restore());
  ASSERT_TRUE(FilesOfType(index_, kTableFile).empty());
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
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "old", std::string(256, 'a')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  TestGate claimed, applied;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BlobGCClaimed",
                                        [&](void*) { claimed.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Delete(WriteOptions(), "old"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  claimed.WaitUntilBlocked();
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
  std::atomic<unsigned> captures{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BlobDescriptorRemoved",
                                        [&](void*) { deleting.Block(); });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::TierReclaimAwaitingCompletion",
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
  // An unrelated mirror error cancels tier work after the worker has started
  // deleting. The backup owner must keep its gate until that worker returns.
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("unrelated mirror failure");
      });
  EXPECT_OK(db_->Put(WriteOptions(), "unpublished", "value"));
  cancelled.WaitUntilBlocked();
  cancelled.Release();
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
  Open();
  ASSERT_OK(db_->Put(WriteOptions(), "old", std::string(256, 'a')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->SyncBackup());
  TestGate claimed, joining;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::BlobGCClaimed",
                                        [&](void*) { claimed.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Delete(WriteOptions(), "old"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(CompactBlobFiles());
  ASSERT_OK(db_->SyncBackup());
  claimed.WaitUntilBlocked();
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
  const std::string segment = m_.data_dir + "/segment-900002-1000000";
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
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::CheckpointWaitMicros",
                                        [&](void* p) {
                                          if (clock.load())
                                            *static_cast<uint64_t*>(p) = 0;
                                        });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::CheckpointBoundary",
                                        [&](void*) { captured.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->Put(WriteOptions(), "first", "v"));
  first_wait.WaitUntilBlocked();
  first_wait.Release();
  EXPECT_OK(db_->Put(WriteOptions(), "second", "v"));
  second_written = true;
  // Mirror completion wakes the unchanged deadline. The callback advances the
  // scheduler clock; a further event makes it observe that time without sleep.
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
  m_.batch_bytes = m_.queue_capacity;
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
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
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
  m_.batch_bytes = m_.queue_capacity;
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

TEST_F(MetaBypassTest, TieredPublishedExtentCorruptionRejected) {
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
  ASSERT_EQ(descriptor.substr(0, 5), "MBT2 ");

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
  ASSERT_TRUE(
      fs_->FileExists(m_.backup_dir + "/work/000004.log", IOOptions(), nullptr)
          .IsNotFound());
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
  m_.checkpoint_interval_ms = m_.interval_ms;
  ASSERT_NO_FATAL_FAILURE(Open());
  ASSERT_NO_FATAL_FAILURE(EnsureTwoPoints());
  TestGate gc, next_publication;
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::GCStarted",
                                        [&](void*) { gc.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  WriteOptions sync_write;
  sync_write.sync = true;
  // The synchronous Put already published the third point. Another explicit
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
  EXPECT_EQ(points, 2);
  db_.reset();
  ASSERT_NO_FATAL_FAILURE(Restore());
  Check("third", "durable");
  Check("tail299", "v");
}
TEST_F(MetaBypassTest, RetiredPointCleanupFailureIsSticky) {
  m_.batch_bytes = m_.queue_capacity;
  m_.interval_ms = 60000;
  m_.checkpoint_interval_ms = m_.interval_ms;
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
  const auto tables = FilesOfType(PublishedPoint(), kTableFile);
  ASSERT_FALSE(tables.empty());
  const std::string table = tables.front();
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
  ASSERT_TRUE(MetaBypassDB::Restore(o_, m_, index_).IsCorruption());
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
  m_.batch_bytes = m_.queue_capacity;
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
  ASSERT_EQ(original.substr(0, 4), "MBI2");
  std::string legacy = original;
  legacy.replace(0, 4, "MBI1");
  ASSERT_OK(metabypass::Write(fs_.get(), inventory, legacy));
  std::string pointer;
  const std::string latest = m_.backup_dir + "/LATEST";
  ASSERT_OK(metabypass::Read(fs_.get(), latest, &pointer));
  const auto separator = pointer.find(' ');
  const auto newline = pointer.find('\n');
  ASSERT_NE(separator, std::string::npos);
  ASSERT_NE(newline, std::string::npos);
  pointer.replace(separator + 1, newline - separator - 1,
                  std::to_string(crc32c::Value(legacy.data(), legacy.size())));
  ASSERT_OK(metabypass::Write(fs_.get(), latest, pointer));
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
  Status LocalWriter(uint64_t number, std::unique_ptr<FSWritableFile>* writer) {
    return storage_->NewWritableFile(BlobFileName(m_.data_dir, number),
                                     FileOptions(), writer, nullptr);
  }
  void QueuedBarriersBeforeReclaim(bool fail_second_barrier) {
    Initialize();
    ASSERT_OK(Append("remote payload"));
    TestGate first_barrier, second_queued, retiring;
    std::atomic<int> queued{0}, barriers{0};
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierTaskQueued",
                                          [&](void*) {
                                            if (++queued == 2)
                                              second_queued.Block();
                                          });
    SyncPoint::GetInstance()->SetCallBack(
        "MetaBypass::TierDescriptorSynced", [&](void* arg) {
          const int barrier = ++barriers;
          if (barrier == 1) first_barrier.Block();
          if (barrier == 2 && fail_second_barrier)
            *static_cast<Status*>(arg) =
                Status::IOError("queued barrier failure");
        });
    SyncPoint::GetInstance()->SetCallBack("MetaBypass::TierBlobRetiring",
                                          [&](void*) { retiring.Block(); });
    SyncPoint::GetInstance()->EnableProcessing();
    storage_->Start();
    Status first, second, reclaimed;
    uint64_t bytes = 0;
    const auto sync = [&](Status* result) {
      *result = storage_->SyncFile(BlobFileName(m_.data_dir, 1), FileOptions(),
                                   IOOptions(), true, nullptr);
    };
    std::thread first_thread([&] { sync(&first); });
    first_barrier.WaitUntilBlocked();
    std::thread second_thread([&] { sync(&second); });
    second_queued.WaitUntilBlocked();
    second_queued.Release();
    std::thread gc([&] { reclaimed = storage_->ReclaimBlob(1, &bytes); });
    retiring.WaitUntilBlocked();
    // The first barrier is executing, a second barrier pins the same remote
    // blob, and GC is its only maintenance request. After the first finishes,
    // task_turn is false but GC must not masquerade as executable work.
    retiring.Release();
    first_barrier.Release();
    first_thread.join();
    second_thread.join();
    gc.join();
    Status stopped = storage_->Stop();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(queued.load(), 2);
    EXPECT_EQ(barriers.load(), 2);
    if (fail_second_barrier) {
      EXPECT_TRUE(first.ok() || first.IsIOError());
      EXPECT_TRUE(second.IsIOError());
      EXPECT_TRUE(reclaimed.IsIOError());
      EXPECT_TRUE(stopped.IsIOError());
    } else {
      EXPECT_OK(first);
      EXPECT_OK(second);
      EXPECT_OK(reclaimed);
      EXPECT_OK(stopped);
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
    storage_.reset(new TieredStorage(fs_, m_));
    ASSERT_OK(storage_->Initialize(false));
    names.clear();
    ASSERT_OK(storage_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
    ASSERT_EQ(
        std::find(names.begin(), names.end(), "000001.blob") != names.end(),
        payload_remains);
    storage_->Start();
    if (payload_remains) {
      ASSERT_OK(storage_->ReclaimBlob(1, &bytes));
    }
    ASSERT_OK(storage_->Stop());
  }
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
    if (std::string(stage) == "route") {
      // The descriptor is durable before SSD deletion. A stale local file
      // must never regain authority after restart.
      ASSERT_OK(fs_->FileExists(root_ + "/staging/000001.blob", IOOptions(),
                                nullptr));
      ASSERT_OK(Write(fs_.get(), root_ + "/staging/000001.blob",
                      std::string(32, 'x')));
    }
    Initialize(64);
    std::string bytes;
    ASSERT_OK(ReadBytes(0, 1024, &bytes));
    ASSERT_EQ(bytes, std::string(32, 'p'));
    uint64_t length = 0;
    ASSERT_OK(storage_->GetFileSize(BlobFileName(m_.data_dir, 1), IOOptions(),
                                    &length, nullptr));
    ASSERT_EQ(length, 32U);
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
  ASSERT_EQ(bytes, blob.size());
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
  storage_.reset(new TieredStorage(fs_, m_));
  ASSERT_OK(storage_->Initialize(false));
  names.clear();
  ASSERT_OK(storage_->GetChildren(m_.data_dir, IOOptions(), &names, nullptr));
  ASSERT_NE(std::find(names.begin(), names.end(), "000001.blob"), names.end());
  storage_->Start();
  ASSERT_OK(storage_->ReclaimBlob(1, &bytes));
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
  storage_.reset(new TieredStorage(fs_, m_));
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
  storage_.reset(new TieredStorage(fs_, m_));
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
  for (int i = 0; i < 100; ++i) {
    const size_t offset = random.Uniform(expected.size() + 1);
    const size_t length = random.Uniform(expected.size() + 1);
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
  if (argc == 7 && std::string(argv[1]) == "--direct-write-crash-writer") {
    Options options;
    options.create_if_missing = true;
    options.enable_blob_direct_write = true;
    options.allow_concurrent_memtable_write = false;
    options.blob_direct_write_partitions = 1;
    options.write_buffer_size = 4 * 1024 * 1024;
    options.blob_file_size = 1024 * 1024;
    MetaBypassOptions bypass;
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
    TieredStorage storage(fs, options);
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
          "MetaBypass::TierRouteDurable",
          [](void*) { kill(getpid(), SIGKILL); });
      SyncPoint::GetInstance()->EnableProcessing();
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
    options.blob_file_size = 128;
    MetaBypassOptions bypass;
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
    MetaBypassOptions bypass;
    bypass.data_dir = argv[3];
    bypass.backup_dir = argv[4];
    const bool gc_crash =
        argc == 6 && std::string(argv[5]) == "MetaBypass::GCFileDeleted";
    if (gc_crash) {
      bypass.batch_bytes = bypass.queue_capacity;
      bypass.interval_ms = 60000;
      bypass.checkpoint_interval_ms = bypass.interval_ms;
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
