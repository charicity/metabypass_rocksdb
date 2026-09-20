// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
// Included in an isolated copy of metabypass_test.cc inside its namespace.
#ifdef AB_SPLIT
TEST_F(MetaBypassTest, ABWalPassesBlockedPrimarySst) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  std::unique_ptr<metabypass::Backup> backup;
  ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_OK(backup->NewWritableFile(index_ + "/000001.log", FileOptions(), &wal,
                                    nullptr));
  ASSERT_OK(backup->NewWritableFile(index_ + "/000002.sst", FileOptions(), &sst,
                                    nullptr));
  TestGate primary;
  SyncPoint::GetInstance()->SetCallBack("AB::Reserved", [&](void* arg) {
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
TEST_F(MetaBypassTest, ABWalPassesSstCapacityWait) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  std::atomic<bool> first{true};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply", [&](void*) {
    if (first.exchange(false)) applying.Block();
  });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure",
                                        [&](void*) { blocked.Block(); });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_OK(backup->NewWritableFile(index_ + "/000001.log", FileOptions(), &wal,
                                    nullptr));
  applying.WaitUntilBlocked();
  ASSERT_OK(backup->NewWritableFile(index_ + "/000002.sst", FileOptions(), &sst,
                                    nullptr));
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
TEST_F(MetaBypassTest, ABBothCapacityWaitersWakeOnFailure) {
  m_.queue_capacity = 8192;
  m_.batch_bytes = 1;
  TestGate applying, blocked;
  std::atomic<bool> first{true};
  std::atomic<int> waiters{0};
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Apply", [&](void*) {
    if (first.exchange(false)) applying.Block();
  });
  SyncPoint::GetInstance()->SetCallBack("MetaBypass::Backpressure", [&](void*) {
    if (++waiters == 2) blocked.Block();
  });
  SyncPoint::GetInstance()->SetCallBack(
      "MetaBypass::ApplyStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("AB mirror failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<metabypass::Backup> backup;
  ASSERT_NO_FATAL_FAILURE(StartBackup(&backup));
  std::unique_ptr<FSWritableFile> wal, sst;
  ASSERT_OK(backup->NewWritableFile(index_ + "/000001.log", FileOptions(), &wal,
                                    nullptr));
  applying.WaitUntilBlocked();
  ASSERT_OK(backup->NewWritableFile(index_ + "/000002.sst", FileOptions(), &sst,
                                    nullptr));
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
#endif
