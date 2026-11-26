//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <array>
#include <memory>
#include <mutex>
#include <string>

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
class IoStats {
 public:
  enum class Area { kIndex, kBackup, kOther };
  enum class Kind { kAppend, kFlush, kSync, kDirSync, kRead, kCount };
  void Record(Area area, Kind kind, uint64_t micros, uint64_t bytes,
              HddPurpose purpose, bool success);
  void AddStats(MetaBypassStats*) const;

 private:
  struct WriteStats {
    mutable std::mutex mutex;
    std::array<uint64_t, 3> purpose_bytes{};
    uint64_t bytes = 0;
    uint64_t failures = 0;
  };
  WriteStats index_, backup_;
  // Protected by backup_.mutex.
  std::array<uint64_t, static_cast<size_t>(Kind::kCount)> calls_{}, micros_{};
};
std::shared_ptr<FileSystem> ObserveIo(std::shared_ptr<FileSystem> fs,
                                      const std::string& index,
                                      const std::string& backup,
                                      std::shared_ptr<IoStats> stats);
Status Read(FileSystem* fs, const std::string& path, std::string* data);
// LATEST contains one canonical point name, optionally terminated by a newline.
Status ParseLatestPoint(const std::string& latest, std::string* name);
Status Write(FileSystem* fs, const std::string& path, const Slice& data);
Status EnsureDir(FileSystem* fs, const std::string& path);
Status SyncDir(FileSystem* fs, const std::string& path);
Status Copy(FileSystem* fs, const std::string& from, const std::string& to,
            uint64_t length);
// Copies an independent snapshot without syncing. Caller must sync it.
Status StageCopy(FileSystem* fs, const std::string& from, const std::string& to,
                 uint64_t length);
Status Digest(FileSystem* fs, const std::string& path, uint64_t length,
              uint32_t* crc);
Status RemoveDir(FileSystem* fs, const std::string& path);
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
