//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <array>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {
struct MetaBypassStats;
namespace metabypass {
// Purpose is scoped at each logical operation, never captured by a file handle.
// Nested wrappers therefore observe the current operation, including barriers
// using a previously opened writer. Only the physical boundary records I/O.
enum class HddPurpose { kCaller, kBulk, kBarrier };
class HddPurposeScope {
 public:
  explicit HddPurposeScope(HddPurpose);
  ~HddPurposeScope();
  static HddPurpose Current();

 private:
  HddPurpose previous_;
};
class HddPressure {
  friend class TieredStorageTest;

 public:
  enum class Kind { kAppend, kFlush, kSync, kDirSync, kRead, kCount };
  struct Counters {
    std::array<uint64_t, 5> calls{}, bytes{}, micros{};
    std::array<uint64_t, 4> written_bytes{};
    std::array<uint64_t, 3> purpose_write_bytes{};
    uint64_t failed_writes = 0;
    uint64_t entered = 0, recovered = 0, residence_us = 0;
    uint64_t latency_entries = 0, stall_entries = 0;
  };
  explicit HddPressure(uint64_t budget_us);
  ~HddPressure();
  void Stop();
  uint64_t Begin(Kind, uint64_t bytes, unsigned area = 2);
  void End(uint64_t id, bool success);
  // SSD writes retain accounting without entering the HDD pressure timeline.
  void RecordSsdWrite(unsigned area, HddPurpose, uint64_t bytes, bool success);
  bool Pressured();
  uint64_t PressureAge();
  Counters Snapshot();
  void AddStats(MetaBypassStats*);

 private:
  struct Operation {
    uint64_t start, bytes;
    Kind kind;
    bool sensitive;
    unsigned area;
    HddPurpose purpose;
  };
  uint64_t NowLocked();  // mutex_ held
  void Evaluate(uint64_t now, bool new_progress = false);
  void Monitor();
  std::mutex mutex_;
  std::mutex ssd_mutex_;
  std::condition_variable wake_;
  std::map<uint64_t, Operation> active_;
  uint64_t active_hdd_ = 0, clock_ = 0;
  uint64_t next_ = 0, budget_, last_progress_ = 0, last_bad_ = 0;
  uint64_t entered_at_ = 0, healthy_progress_ = 0;
  bool pressure_ = false, stop_ = false;
  Counters counters_;
  Counters ssd_counters_;  // Only write accounting fields; ssd_mutex_ held.
  std::thread monitor_;
};
std::shared_ptr<FileSystem> ObserveHdd(std::shared_ptr<FileSystem>,
                                       const std::string& data,
                                       const std::string& backup,
                                       std::shared_ptr<HddPressure>,
                                       const std::string& index = {},
                                       const std::string& staging = {});
Status Read(FileSystem* fs, const std::string& path, std::string* data);
Status Write(FileSystem* fs, const std::string& path, const Slice& data);
Status EnsureDir(FileSystem* fs, const std::string& path);
Status SyncDir(FileSystem* fs, const std::string& path);
Status Copy(FileSystem* fs, const std::string& from, const std::string& to,
            uint64_t length);
struct StagedCopy {
  uint64_t length = 0;
  uint32_t crc = 0;
};
// Copies an independent byte snapshot and closes it without syncing or reading
// it back. The caller must sync and verify the entire destination before use as
// a recovery point. The result is set only after a successful Close.
Status StageCopy(FileSystem* fs, const std::string& from, const std::string& to,
                 uint64_t length, StagedCopy* result);
Status Digest(FileSystem* fs, const std::string& path, uint64_t length,
              uint32_t* crc);
// Extend an existing CRC with [begin, end), without rereading its prefix.
// If provided, increment opens for each physical reader open attempt.
Status ExtendDigest(FileSystem* fs, const std::string& path, uint64_t begin,
                    uint64_t end, uint32_t* crc, uint64_t* opens = nullptr);
Status RemoveDir(FileSystem* fs, const std::string& path);
// Immutable SST inventory obtained from the engine checkpoint API.
struct NativeState {
  std::map<uint64_t, uint64_t> tables;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
