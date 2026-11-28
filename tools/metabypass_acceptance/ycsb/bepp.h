// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include "rocksdb/utilities/backup_engine.h"

namespace mb_ycsb {

std::filesystem::path WalMirrorDirectory(const std::filesystem::path& backup);

struct BackupResult {
  uint32_t backup_id = 0;
  uint64_t wal_floor = 0;
};

struct PreparedRestore {
  uint32_t backup_id = 0;
  uint64_t wal_floor = 0;
  std::unique_ptr<ROCKSDB_NAMESPACE::BackupEngineReadOnly> engine;

  // Copies the retained WAL files into a new target. Restore() installs the
  // base in that same directory.
  void StageWal(const std::filesystem::path& target) const;
  ROCKSDB_NAMESPACE::IOStatus Restore(
      const std::filesystem::path& target) const;

 private:
  friend class BackupEnginePlusPlus;
  std::filesystem::path mirror_;
};

// One writer process per sample. The source DB must outlive this object.
class BackupEnginePlusPlus {
 public:
  static void ConfigureWalSourceOptions(ROCKSDB_NAMESPACE::Options& options);
  BackupEnginePlusPlus(ROCKSDB_NAMESPACE::DB& db,
                       const std::filesystem::path& backup);

  // Publishes a base backup and retires older backups before returning the
  // WAL floor the mirror can advance to.
  BackupResult Backup();

  // The writer must be stopped. Opens the backup and selects a base for
  // recovery without changing the backup or WAL mirror.
  static PreparedRestore OpenRestore(const std::filesystem::path& backup);

 private:
  ROCKSDB_NAMESPACE::DB& db_;
  std::unique_ptr<ROCKSDB_NAMESPACE::BackupEngine> engine_;
};

}  // namespace mb_ycsb
