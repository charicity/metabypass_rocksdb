//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <thread>

#include "file/filename.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "utilities/metabypass/sst_storage.h"

#ifndef OS_WIN
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
class SstFaultFileSystem : public FileSystemWrapper {
 public:
  explicit SstFaultFileSystem(std::shared_ptr<FileSystem> fs)
      : FileSystemWrapper(std::move(fs)) {}
  const char* Name() const override { return "SstFaultFileSystem"; }
  bool no_links = false, no_space = false;
  std::atomic<uint64_t> placement_writes{0};
  std::atomic<uint64_t> placement_renames{0};
  IOStatus LinkFile(const std::string& a, const std::string& b,
                    const IOOptions& o, IODebugContext* d) override {
    if (no_links) return IOStatus::NotSupported("injected no hardlinks");
    return target()->LinkFile(a, b, o, d);
  }
  IOStatus NewWritableFile(const std::string& path, const FileOptions& o,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* d) override {
    if (no_space && path.find("sst-tier-tmp") != std::string::npos)
      return IOStatus::NoSpace("injected SSD full");
    IOStatus s = target()->NewWritableFile(path, o, result, d);
    if (s.ok() && path.find("/SST-PLACEMENT.tmp") != std::string::npos)
      ++placement_writes;
    return s;
  }
  IOStatus RenameFile(const std::string& from, const std::string& to,
                      const IOOptions& o, IODebugContext* d) override {
    IOStatus s = target()->RenameFile(from, to, o, d);
    if (s.ok() && to.find("/SST-PLACEMENT") != std::string::npos)
      ++placement_renames;
    return s;
  }
};
class SstStorageTest : public testing::Test {
 public:
  SstStorageTest() {
    root_ = test::PerThreadDBPath("sst-tiering");
    index_ = root_ + "/index";
    point_ = root_ + "/point";
    options_.backup_dir = root_ + "/backup";
    options_.sst_tiering.mode = SstTieringMode::kAdaptive;
    options_.sst_tiering.ssd_capacity_bytes = 4096;
    options_.sst_tiering.sample_one_in = 1;
    options_.sst_tiering.migration_bytes_per_sec = 0;
    options_.sst_tiering.min_residency_ms = 0;
    fs_ = FileSystem::Default();
    Clean(root_);
    EXPECT_OK(EnsureDir(fs_.get(), root_));
    EXPECT_OK(EnsureDir(fs_.get(), index_));
    EXPECT_OK(EnsureDir(fs_.get(), point_));
    EXPECT_OK(EnsureDir(fs_.get(), options_.backup_dir));
  }
  ~SstStorageTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    storage_.reset();
    Clean(root_);
  }
  void Clean(const std::string& dir) {
    std::vector<std::string> children;
    Status s = fs_->GetChildren(dir, IOOptions(), &children, nullptr);
    if (s.IsNotFound()) return;
    ASSERT_OK(s);
    for (const auto& file : children) {
      if (file == "." || file == "..") continue;
      bool is_dir;
      ASSERT_OK(
          fs_->IsDirectory(dir + "/" + file, IOOptions(), &is_dir, nullptr));
      if (is_dir)
        Clean(dir + "/" + file);
      else
        ASSERT_OK(fs_->DeleteFile(dir + "/" + file, IOOptions(), nullptr));
    }
    ASSERT_OK(fs_->DeleteDir(dir, IOOptions(), nullptr));
  }
  void Open() {
    storage_ = std::make_unique<SstStorage>(fs_, index_, options_,
                                            [&] { return now_; });
    ASSERT_OK(storage_->Initialize("test-identity", false));
  }
  void Add(uint64_t number, const std::string& data, bool protect = true) {
    ASSERT_OK(Write(fs_.get(), MakeTableFileName(index_, number), data));
    if (protect) {
      ASSERT_OK(Write(fs_.get(), MakeTableFileName(point_, number), data));
      NativeState state;
      state.tables[number] = data.size();
      ASSERT_OK(storage_->Protect(point_, state));
    }
  }
  void Move(uint64_t number, bool hot) {
    {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      if (hot)
        storage_->desired_.insert(number);
      else
        storage_->desired_.erase(number);
    }
    ASSERT_OK(storage_->SstStorage::Migrate({number, hot}));
    storage_->SstStorage::Reap();
  }
  Status TryDemote(uint64_t number) {
    return storage_->SstStorage::Migrate({number, false});
  }
  std::string Object(uint64_t number) {
    return options_.backup_dir + "/sst-store/" +
           storage_->SstStorage::Find(number)->object;
  }
  bool HasEntry(uint64_t number) {
    return storage_->SstStorage::Find(number) != nullptr;
  }
  void Reap() { storage_->SstStorage::Reap(); }
  Status TryPromote(uint64_t number) {
    {
      std::lock_guard<std::mutex> lock(storage_->mutex_);
      storage_->desired_.insert(number);
    }
    return storage_->SstStorage::Migrate({number, true});
  }
  void Tick() { storage_->SstStorage::Tick(); }
  void Check(FSRandomAccessFile* file, const std::string& expected,
             Env::IOActivity activity = Env::IOActivity::kGet) {
    std::string scratch(expected.size(), '\0');
    Slice result;
    IOOptions io;
    io.io_activity = activity;
    ASSERT_OK(file->Read(0, scratch.size(), io, &result, &scratch[0], nullptr));
    ASSERT_EQ(result.ToString(), expected);
    ASSERT_EQ(result.data(), scratch.data());
  }
  std::unique_ptr<FSRandomAccessFile> Reader(uint64_t number) {
    std::unique_ptr<FSRandomAccessFile> reader;
    EXPECT_OK(storage_->NewRandomAccessFile(MakeTableFileName(index_, number),
                                            FileOptions(), &reader, nullptr));
    return reader;
  }
  SstTieringStats Stats() {
    MetaBypassStats stats;
    storage_->AddStats(&stats);
    EXPECT_OK(stats.error);
    return stats.sst_tiering;
  }

 protected:
  std::shared_ptr<FileSystem> fs_;
  std::string root_, index_, point_;
  MetaBypassOptions options_;
  std::unique_ptr<SstStorage> storage_;
  uint64_t now_ = 100000000;
};
TEST_F(SstStorageTest, ExistingReaderSwitchesBothDirectionsAndSurvivesDelete) {
  Open();
  Add(1, "immutable table");
  auto reader = Reader(1);
  Check(reader.get(), "immutable table");
  Move(1, false);
  ASSERT_TRUE(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr)
          .IsNotFound());
  Check(reader.get(), "immutable table");
  Move(1, true);
  ASSERT_OK(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr));
  Check(reader.get(), "immutable table");
  ASSERT_OK(
      storage_->DeleteFile(MakeTableFileName(index_, 1), IOOptions(), nullptr));
  Check(reader.get(), "immutable table");
  reader.reset();
  storage_.reset();
  Open();
  ASSERT_TRUE(
      storage_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr)
          .IsNotFound());
}
TEST_F(SstStorageTest, ColdMapReopensAndUsesLogicalEnumeration) {
  Open();
  Add(8, std::string(300, 's'));
  Move(8, false);
  storage_.reset();
  Open();
  uint64_t size = 0;
  ASSERT_OK(storage_->GetFileSize(MakeTableFileName(index_, 8), IOOptions(),
                                  &size, nullptr));
  ASSERT_EQ(size, 300U);
  std::vector<std::string> children;
  ASSERT_OK(storage_->GetChildren(index_, IOOptions(), &children, nullptr));
  ASSERT_NE(std::find(children.begin(), children.end(), "000008.sst"),
            children.end());
  auto reader = Reader(8);
  Check(reader.get(), std::string(300, 's'));
  ASSERT_OK(storage_->LinkFile(MakeTableFileName(index_, 8), point_ + "/linked",
                               IOOptions(), nullptr));
  std::string data;
  ASSERT_OK(Read(fs_.get(), point_ + "/linked", &data));
  ASSERT_EQ(data, std::string(300, 's'));
}
TEST_F(SstStorageTest, RestoreManyColdTablesCommitsPlacementOnce) {
  auto fault = std::make_shared<SstFaultFileSystem>(fs_);
  fs_ = fault;
  storage_ = std::make_unique<SstStorage>(fs_, index_, options_);
  ASSERT_OK(storage_->Initialize("test-identity", true));
  std::vector<SstStorage::RestoreFile> files;
  for (uint64_t number = 1; number <= 16; ++number) {
    const std::string data = "restored table " + std::to_string(number);
    const std::string source = MakeTableFileName(point_, number);
    ASSERT_OK(Write(fs_.get(), source, data));
    files.push_back({source, number, data.size()});
  }
  ASSERT_OK(storage_->RestoreTables(files));
  ASSERT_EQ(fault->placement_writes.load(), 1U);
  ASSERT_EQ(fault->placement_renames.load(), 1U);
  storage_.reset();
  Open();
  for (uint64_t number = 1; number <= 16; ++number) {
    const std::string data = "restored table " + std::to_string(number);
    ASSERT_TRUE(
        fs_->FileExists(MakeTableFileName(index_, number), IOOptions(), nullptr)
            .IsNotFound());
    auto reader = Reader(number);
    ASSERT_NE(reader, nullptr);
    Check(reader.get(), data);
  }
}
TEST_F(SstStorageTest, RestoreEmptyTableSetClearsOldPlacement) {
  Open();
  Add(1, "stale table");
  storage_.reset();
  storage_ = std::make_unique<SstStorage>(fs_, index_, options_);
  ASSERT_OK(storage_->Initialize("test-identity", true));
  ASSERT_OK(storage_->RestoreTables({}));
  std::string placement;
  ASSERT_OK(Read(fs_.get(), index_ + "/SST-PLACEMENT", &placement));
  ASSERT_EQ(placement.find("\n1 "), std::string::npos);
}
TEST_F(SstStorageTest, RestorePrepareAndPlacementFailuresCanRetry) {
  auto fault = std::make_shared<SstFaultFileSystem>(fs_);
  fs_ = fault;
  const std::string first = MakeTableFileName(point_, 1);
  const std::string second = MakeTableFileName(point_, 2);
  ASSERT_OK(Write(fs_.get(), first, "first"));
  const std::vector<SstStorage::RestoreFile> files = {{first, 1, 5},
                                                      {second, 2, 6}};
  storage_ = std::make_unique<SstStorage>(fs_, index_, options_);
  ASSERT_OK(storage_->Initialize("test-identity", true));
  Status missing = storage_->RestoreTables(files);
  ASSERT_TRUE(missing.IsNotFound() || missing.IsPathNotFound())
      << missing.ToString();
  ASSERT_EQ(fault->placement_writes.load(), 0U);
  ASSERT_FALSE(HasEntry(1));
  ASSERT_OK(Write(fs_.get(), second, "second"));
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypassSst::PlacementRenamed", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("restore placement fault");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_TRUE(storage_->RestoreTables(files).IsIOError());
  ASSERT_EQ(fault->placement_writes.load(), 1U);
  ASSERT_EQ(fault->placement_renames.load(), 1U);
  ASSERT_FALSE(HasEntry(1));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  storage_.reset();
  storage_ = std::make_unique<SstStorage>(fs_, index_, options_);
  ASSERT_OK(storage_->Initialize("test-identity", true));
  ASSERT_OK(storage_->RestoreTables(files));
  storage_.reset();
  Open();
  auto first_reader = Reader(1);
  auto second_reader = Reader(2);
  ASSERT_NE(first_reader, nullptr);
  ASSERT_NE(second_reader, nullptr);
  Check(first_reader.get(), "first");
  Check(second_reader.get(), "second");
  std::vector<std::string> objects;
  ASSERT_OK(fs_->GetChildren(options_.backup_dir + "/sst-store", IOOptions(),
                             &objects, nullptr));
  size_t object_count = 0;
  for (const auto& object : objects)
    if (object != "." && object != "..") ++object_count;
  ASSERT_EQ(object_count, 2U);
}
TEST_F(SstStorageTest, UnprotectedCannotEvictAndOversizedCannotPromote) {
  options_.sst_tiering.ssd_capacity_bytes = 64;
  Open();
  Add(1, std::string(100, 'x'), false);
  Tick();
  Move(1, false);
  ASSERT_OK(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr));
  Add(1, std::string(100, 'x'));
  Move(1, false);
  Move(1, true);
  ASSERT_TRUE(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr)
          .IsNotFound());
  Tick();
  ASSERT_EQ(Stats().oversized_files, 1U);
}
TEST_F(SstStorageTest, PlacementFailuresKeepOriginalReaderAndRetry) {
  for (const std::string hook :
       {"MetaBypassSst::PlacementWritten", "MetaBypassSst::PlacementRenamed",
        "MetaBypassSst::PlacementSynced"}) {
    Open();
    Add(1, "protected");
    auto reader = Reader(1);
    SyncPoint::GetInstance()->SetCallBack(hook, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("placement fault");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_TRUE(TryDemote(1).IsIOError());
    Check(reader.get(), "protected");
    ASSERT_OK(
        fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    reader.reset();
    storage_.reset();
    Open();
    reader = Reader(1);
    Check(reader.get(), "protected");
    reader.reset();
    Move(1, false);
    ASSERT_OK(storage_->DeleteFile(MakeTableFileName(index_, 1), IOOptions(),
                                   nullptr));
    storage_.reset();
  }
}
TEST_F(SstStorageTest, CorruptObjectAndForeignPlacementFailClosed) {
  Open();
  Add(7, "seven");
  Move(7, false);
  const auto object = Object(7);
  storage_.reset();
  ASSERT_OK(Write(fs_.get(), object, "short"));
  storage_ = std::make_unique<SstStorage>(fs_, index_, options_);
  ASSERT_TRUE(storage_->Initialize("test-identity", false).IsCorruption());
  storage_.reset();
  storage_ = std::make_unique<SstStorage>(fs_, index_, options_);
  ASSERT_TRUE(storage_->Initialize("other-identity", false).IsCorruption());
}
#ifndef OS_WIN
TEST_F(SstStorageTest, CrashAtEveryDemotionAndPromotionPersistenceBoundary) {
  for (bool promotion : {false, true}) {
    const std::vector<std::string> stages =
        promotion
            ? std::vector<std::string>{"MetaBypassSst::PromotionCopied",
                                       "MetaBypassSst::PromotionInstalled",
                                       "MetaBypassSst::PlacementWritten",
                                       "MetaBypassSst::PlacementRenamed",
                                       "MetaBypassSst::PlacementSynced",
                                       "MetaBypassSst::RouteSwitched"}
            : std::vector<std::string>{"MetaBypassSst::PlacementWritten",
                                       "MetaBypassSst::PlacementRenamed",
                                       "MetaBypassSst::PlacementSynced",
                                       "MetaBypassSst::RouteSwitched",
                                       "MetaBypassSst::LocalDeleted"};
    for (const auto& stage : stages) {
      SCOPED_TRACE(stage);
      Open();
      Add(1, "crash-safe");
      if (promotion) Move(1, false);
      const pid_t child = fork();
      ASSERT_GE(child, 0);
      if (child == 0) {
        SyncPoint::GetInstance()->SetCallBack(
            stage, [](void*) { kill(getpid(), SIGKILL); });
        SyncPoint::GetInstance()->EnableProcessing();
        Move(1, promotion);
        _exit(7);
      }
      int status;
      ASSERT_EQ(waitpid(child, &status, 0), child);
      ASSERT_TRUE(WIFSIGNALED(status));
      ASSERT_EQ(WTERMSIG(status), SIGKILL);
      storage_.reset();
      Open();
      auto reader = Reader(1);
      Check(reader.get(), "crash-safe");
      reader.reset();
      ASSERT_OK(storage_->DeleteFile(MakeTableFileName(index_, 1), IOOptions(),
                                     nullptr));
      storage_.reset();
    }
  }
}
#endif
TEST_F(SstStorageTest, ReadPinsDelayUnlinkButNotRouteSwitch) {
  Open();
  Add(1, "retained scratch");
  auto reader = Reader(1);
  std::mutex mutex;
  std::condition_variable cv;
  bool pinned = false, release = false;
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypassSst::ReadPinned", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        pinned = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread reading([&] { Check(reader.get(), "retained scratch"); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return pinned; });
  }
  Move(1, false);
  EXPECT_OK(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr));
  EXPECT_EQ(Stats().pending_delete_bytes, 16U);
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    cv.notify_all();
  }
  reading.join();
  SyncPoint::GetInstance()->DisableProcessing();
  Reap();
  ASSERT_TRUE(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr)
          .IsNotFound());
  Check(reader.get(), "retained scratch");
  ASSERT_EQ(Stats().pending_delete_bytes, 0U);
}
TEST_F(SstStorageTest, HardlinkFallbackNoSpaceAndChecksumFailure) {
  auto fault = std::make_shared<SstFaultFileSystem>(fs_);
  fault->no_links = true;
  fs_ = fault;
  Open();
  Add(3, std::string(128, 'z'));
  ASSERT_EQ(Stats().copied_bytes, 128U);
  ASSERT_EQ(Stats().reused_links, 0U);
  Move(3, false);
  auto reader = Reader(3);
  fault->no_space = true;
  ASSERT_FALSE(TryPromote(3).ok());
  Check(reader.get(), std::string(128, 'z'));
  ASSERT_EQ(Stats().reserved_bytes, 0U);
  fault->no_space = false;
  Move(3, true);
  Move(3, false);
  ASSERT_OK(Write(fs_.get(), Object(3), "short"));
  ASSERT_TRUE(TryPromote(3).IsCorruption());
  ASSERT_TRUE(
      fs_->FileExists(MakeTableFileName(index_, 3), IOOptions(), nullptr)
          .IsNotFound());
}
TEST_F(SstStorageTest, FailedInstalledPromotionRemainsChargedUntilSafeCleanup) {
  for (const auto& stage :
       {"MetaBypassSst::PromotionInstalled", "MetaBypassSst::PlacementWritten",
        "MetaBypassSst::PlacementRenamed", "MetaBypassSst::PlacementSynced"}) {
    SCOPED_TRACE(stage);
    Open();
    Add(1, "charged");
    Move(1, false);
    auto reader = Reader(1);
    SyncPoint::GetInstance()->SetCallBack(stage, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("installed promotion fault");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_TRUE(TryPromote(1).IsIOError());
    ASSERT_EQ(Stats().reserved_bytes, 7U);
    ASSERT_OK(
        fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr));
    Check(reader.get(), "charged");
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Reap();
    ASSERT_EQ(Stats().reserved_bytes, 0U);
    ASSERT_TRUE(
        fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr)
            .IsNotFound());
    Check(reader.get(), "charged");
    reader.reset();
    storage_.reset();
    Open();
    reader = Reader(1);
    Check(reader.get(), "charged");
    reader.reset();
    ASSERT_OK(storage_->DeleteFile(MakeTableFileName(index_, 1), IOOptions(),
                                   nullptr));
    storage_.reset();
  }
}
TEST_F(SstStorageTest,
       CopyAllowsPolicyAndForegroundProgressAndCancelsDeletedFile) {
  options_.sst_tiering.ssd_capacity_bytes = 1024 * 1024;
  Open();
  Add(1, std::string(128 * 1024, 'p'));
  Move(1, false);
  auto reader = Reader(1);
  Check(reader.get(), std::string(128 * 1024, 'p'));
  Tick();
  std::mutex mutex;
  std::condition_variable cv;
  bool copying = false, release = false;
  SyncPoint::GetInstance()->SetCallBack("MetaBypassSst::CopyChunk", [&](void*) {
    std::unique_lock<std::mutex> lock(mutex);
    copying = true;
    cv.notify_all();
    cv.wait(lock, [&] { return release; });
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Status copied;
  std::thread migration([&] { copied = TryPromote(1); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return copying; });
  }
  // These operations all need the placement transaction lock. They must finish
  // while the physical copy is blocked, without waiting for that copy.
  Add(2, "foreground", false);
  auto foreground = Reader(2);
  Check(foreground.get(), "foreground");
  Tick();
  EXPECT_EQ(Stats().reserved_bytes, 128U * 1024);
  EXPECT_OK(
      storage_->DeleteFile(MakeTableFileName(index_, 1), IOOptions(), nullptr));
  Tick();
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    cv.notify_all();
  }
  migration.join();
  ASSERT_TRUE(copied.IsAborted());
  SyncPoint::GetInstance()->DisableProcessing();
  Reap();
  ASSERT_EQ(Stats().reserved_bytes, 0U);
  ASSERT_TRUE(fs_->FileExists(MakeTableFileName(index_, 1) + ".sst-tier-tmp",
                              IOOptions(), nullptr)
                  .IsNotFound());
  Check(reader.get(), std::string(128 * 1024, 'p'));
}
TEST_F(SstStorageTest, QueueAndDeletedMetadataAreBounded) {
  options_.sst_tiering.ssd_capacity_bytes = 1;
  options_.sst_tiering.migration_queue_capacity = 2;
  Open();
  for (uint64_t n = 1; n <= 10; ++n) Add(n, std::string(100, 'q'));
  for (int round = 0; round < 8; ++round) {
    Tick();
    ASSERT_EQ(Stats().queued_migrations, 2U);
  }
  storage_->Stop();
  ASSERT_EQ(Stats().queued_migrations, 0U);
  for (uint64_t n = 1; n <= 10; ++n)
    ASSERT_OK(storage_->DeleteFile(MakeTableFileName(index_, n), IOOptions(),
                                   nullptr));
  Tick();
  ASSERT_EQ(Stats().queued_migrations, 0U);
  std::vector<std::string> objects;
  ASSERT_OK(fs_->GetChildren(options_.backup_dir + "/sst-store", IOOptions(),
                             &objects, nullptr));
  for (const auto& object : objects)
    ASSERT_TRUE(object == "." || object == "..");
}
TEST_F(SstStorageTest, DamagedProtectionNeverEvictsTheValidSsdCopy) {
  Open();
  Add(1, "valid SSD copy");
  auto reader = Reader(1);
  const auto object = Object(1);
  ASSERT_OK(Write(fs_.get(), object, "truncated"));
  ASSERT_TRUE(TryDemote(1).IsCorruption());
  Check(reader.get(), "valid SSD copy");
  ASSERT_OK(
      fs_->FileExists(MakeTableFileName(index_, 1), IOOptions(), nullptr));
  ASSERT_OK(Write(fs_.get(), object, "invalid HDD!!!"));
  ASSERT_TRUE(TryDemote(1).IsCorruption());
  Check(reader.get(), "valid SSD copy");
}
TEST_F(SstStorageTest, SamplesExcludeScanAndInternalIoAndBoundMemory) {
  uint64_t now = 0;
  SstTieringOptions options;
  options.sample_one_in = 1;
  options.sample_buffer_capacity = 2;
  SstSampler sampler(
      options, [&] { return now; }, [] { return 0; });
  sampler.Record(1, Env::IOActivity::kGet);
  sampler.Record(1, Env::IOActivity::kGet);
  sampler.Record(1, Env::IOActivity::kGet);
  sampler.Record(2, Env::IOActivity::kDBIterator);
  sampler.Record(3, Env::IOActivity::kCompaction);
  auto heat = sampler.HeatSnapshot({1, 2, 3});
  ASSERT_EQ(heat.size(), 1U);
  ASSERT_EQ(heat[1], 2);
  now += options.heat_half_life_ms * 1000;
  ASSERT_DOUBLE_EQ(sampler.HeatSnapshot({1})[1], 1);
  ASSERT_TRUE(sampler.HeatSnapshot({}).empty());
  SstTieringStats stats;
  sampler.AddStats(&stats);
  ASSERT_EQ(stats.sampled_reads, 2U);
  ASSERT_EQ(stats.dropped_samples, 1U);
  ASSERT_EQ(stats.scan_reads, 1U);
}
TEST_F(SstStorageTest, PolicyBudgetHysteresisResidencyAndReplacementMargin) {
  SstTieringOptions o;
  o.ssd_capacity_bytes = 1000;
  o.min_residency_ms = 10;
  std::vector<SstFileState> files{{1, 600, 0, true, true, 10, 0, 0},
                                  {2, 600, 0, false, true, 12, 1, 0}};
  auto plan = PlanSstPlacement(o, files, 600, 20000);
  ASSERT_EQ(plan.target, std::set<uint64_t>({1}));
  files[1].heat = 13;
  plan = PlanSstPlacement(o, files, 600, 20000);
  ASSERT_EQ(plan.target, std::set<uint64_t>({2}));
  ASSERT_EQ(plan.migrations.size(), 1U);
  ASSERT_TRUE(plan.migrations[0].hot);
  files[0].out_rounds = 2;
  plan = PlanSstPlacement(o, files, 600, 20000);
  ASSERT_EQ(plan.migrations.size(), 2U);
  ASSERT_FALSE(plan.migrations[0].hot);
  files[0].changed_micros = files[1].changed_micros = 19000;
  ASSERT_TRUE(PlanSstPlacement(o, files, 600, 20000).migrations.empty());
  plan = PlanSstPlacement(o, files, 1200, 20000);
  ASSERT_EQ(plan.migrations.size(), 1U);
  ASSERT_FALSE(plan.migrations[0].hot);
  files[0].protected_copy = false;
  plan = PlanSstPlacement(o, files, 1200, 20000);
  ASSERT_TRUE(plan.migrations.empty());
}
TEST_F(SstStorageTest, ObserveOnlyQueuesNoMigrationsAndRemembersEarlyReader) {
  options_.sst_tiering.mode = SstTieringMode::kObserveOnly;
  options_.sst_tiering.ssd_capacity_bytes = 1;
  Open();
  Add(1, "new table", false);
  auto reader = Reader(1);
  Add(1, "new table");
  Tick();
  ASSERT_EQ(Stats().queued_migrations, 0U);
  ASSERT_EQ(Stats().observed_demotions, 1U);
  Move(1, false);
  Check(reader.get(), "new table");
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
