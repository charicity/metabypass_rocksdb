// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class FileSystem;
}

namespace mb_ycsb {

struct WalOperationMirrorOptions {
  uint64_t queue_capacity_bytes = 896ULL * 1024 * 1024;
  uint64_t batch_bytes = 512ULL * 1024 * 1024;
  uint64_t interval_ms = 1000;
};

// Captures WAL operations from a fresh source DB and applies them to a
// separate filesystem. The source DB must close before Close() or destruction
// of this object. Mirror writes are buffered in the destination filesystem.
class WalOperationMirror {
 public:
  WalOperationMirror(std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> source_fs,
                     std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> mirror_fs,
                     std::filesystem::path source_wal_dir,
                     std::filesystem::path mirror_dir,
                     WalOperationMirrorOptions options = {});
  ~WalOperationMirror();

  WalOperationMirror(const WalOperationMirror&) = delete;
  WalOperationMirror& operator=(const WalOperationMirror&) = delete;

  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> CaptureFileSystem() const;

  // Call only after the backup authorizing the floor is published and older
  // backups have been retired.
  void AdvanceFloor(uint64_t floor);

  // Seals the queue and joins the worker after pending operations are applied
  // or a mirror error occurs.
  ROCKSDB_NAMESPACE::Status Close();
  ROCKSDB_NAMESPACE::Status GetError() const;

 private:
  class State;
  std::shared_ptr<State> state_;
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> capture_fs_;
};

}  // namespace mb_ycsb
