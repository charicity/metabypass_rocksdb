// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "bepp.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace mb_ycsb {
namespace {
namespace rdb = ROCKSDB_NAMESPACE;
namespace fs = std::filesystem;

constexpr std::string_view kMetadataPrefix = "backupengine-plus-plus-v1:";
constexpr size_t kCopyBufferSize = 256 * 1024;

template <typename Status>
void Check(const Status& status, std::string_view operation) {
  if (!status.ok()) {
    throw std::runtime_error(std::string(operation) + ": " + status.ToString());
  }
}

[[noreturn]] void FileError(std::string_view operation, const fs::path& path,
                            int error) {
  throw std::system_error(error, std::generic_category(),
                          std::string(operation) + " " + path.string());
}

class FileDescriptor {
 public:
  explicit FileDescriptor(int fd) : fd_(fd) {}
  ~FileDescriptor() {
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

  int Get() const { return fd_; }

  void Close(const fs::path& path) {
    if (close(std::exchange(fd_, -1)) != 0) {
      FileError("close", path, errno);
    }
  }

 private:
  int fd_;
};

int Open(const fs::path& path, int flags) {
  int fd;
  do {
    fd = open(path.c_str(), flags | O_CLOEXEC, 0644);
  } while (fd < 0 && errno == EINTR);
  return fd;
}

uint64_t Size(int fd, const fs::path& path) {
  struct stat info {};
  if (fstat(fd, &info) != 0) {
    FileError("stat", path, errno);
  }
  if (!S_ISREG(info.st_mode) || info.st_size < 0) {
    throw std::runtime_error("invalid WAL file: " + path.string());
  }
  return info.st_size;
}

void Sync(int fd, const fs::path& path, bool directory) {
  int result;
  do {
    result = directory ? fsync(fd) : fdatasync(fd);
  } while (result != 0 && errno == EINTR);
  if (result != 0) {
    FileError("sync", path, errno);
  }
}

void SyncDirectory(const fs::path& directory) {
  FileDescriptor fd(Open(directory, O_RDONLY | O_DIRECTORY));
  if (fd.Get() < 0) {
    FileError("open", directory, errno);
  }
  Sync(fd.Get(), directory, true);
}

void CopyWal(int input, const fs::path& source, const fs::path& destination,
             uint64_t remaining) {
  FileDescriptor output(Open(destination, O_WRONLY | O_CREAT | O_EXCL));
  if (output.Get() < 0) {
    FileError("open", destination, errno);
  }
  std::array<char, kCopyBufferSize> buffer;
  while (remaining > 0) {
    const auto count = std::min<uint64_t>(buffer.size(), remaining);
    ssize_t bytes_read;
    do {
      bytes_read = read(input, buffer.data(), count);
    } while (bytes_read < 0 && errno == EINTR);
    if (bytes_read < 0) {
      FileError("read", source, errno);
    }
    if (bytes_read == 0) {
      throw std::runtime_error("short WAL read: " + source.string());
    }
    ssize_t written = 0;
    while (written < bytes_read) {
      const auto result =
          write(output.Get(), buffer.data() + written, bytes_read - written);
      if (result <= 0) {
        if (result < 0 && errno == EINTR) {
          continue;
        }
        FileError("write", destination, result < 0 ? errno : EIO);
      }
      written += result;
    }
    remaining -= bytes_read;
  }
  Sync(output.Get(), destination, false);
  output.Close(destination);
}

uint64_t ParseNumber(std::string_view digits) {
  uint64_t number = 0;
  const auto end = digits.data() + digits.size();
  const auto parsed = std::from_chars(digits.data(), end, number);
  if (parsed.ec != std::errc{} || parsed.ptr != end) {
    throw std::runtime_error("invalid WAL number: " + std::string(digits));
  }
  return number;
}

using WalFiles = std::map<uint64_t, fs::path>;

void FindWalFiles(const fs::path& directory, uint64_t floor, WalFiles& files) {
  for (const auto& entry : fs::directory_iterator(directory)) {
    const auto filename = entry.path().filename();
    const auto& name = filename.native();
    constexpr std::string_view kSuffix = ".log";
    if (!name.ends_with(kSuffix)) {
      continue;
    }
    const auto number = ParseNumber(
        std::string_view(name).substr(0, name.size() - kSuffix.size()));
    if (number != 0 && number >= floor) {
      files.try_emplace(number, filename);
    }
  }
}

rdb::BackupEngineOptions BackupOptions(const fs::path& backup) {
  rdb::BackupEngineOptions options(backup.string());
  options.backup_log_files = false;
  options.share_table_files = true;
  options.sync = true;
  return options;
}

}  // namespace

fs::path WalMirrorDirectory(const fs::path& backup) {
  auto directory = fs::absolute(backup).lexically_normal();
  if (!directory.has_filename()) {
    directory = directory.parent_path();
  }
  return directory.parent_path() / "wal-mirror";
}

void BackupEnginePlusPlus::ConfigureWalSourceOptions(rdb::Options& options) {
  options.WAL_ttl_seconds = std::numeric_limits<uint64_t>::max();
  options.WAL_size_limit_MB = 0;
  options.recycle_log_file_num = 0;
  options.async_wal_precreate = false;
  options.manual_wal_flush = false;
}

BackupEnginePlusPlus::BackupEnginePlusPlus(rdb::DB& db, const fs::path& backup)
    : db_(db) {
  rdb::BackupEngine* engine = nullptr;
  Check(rdb::BackupEngine::Open(BackupOptions(backup), db.GetEnv(), &engine),
        "BE++ open BackupEngine");
  engine_.reset(engine);
}

BackupResult BackupEnginePlusPlus::Backup() {
  BackupResult result;
  // A backup started after this read can capture a higher native WAL floor.
  // Keeping everything from this earlier floor is conservative.
  if (!db_.GetIntProperty(rdb::DB::Properties::kMinLogNumberToKeep,
                          &result.wal_floor)) {
    throw std::runtime_error("read source min-log-number-to-keep");
  }
  const auto metadata =
      std::string(kMetadataPrefix) + std::to_string(result.wal_floor);
  rdb::CreateBackupOptions options;
  options.flush_before_backup = true;
  Check(engine_->CreateNewBackupWithMetadata(options, &db_, metadata,
                                             &result.backup_id),
        "BE++ create base backup");
  Check(engine_->PurgeOldBackups(1), "BE++ purge old base backups");
  return result;
}

PreparedRestore BackupEnginePlusPlus::OpenRestore(const fs::path& backup) {
  rdb::BackupEngineReadOnly* engine = nullptr;
  Check(rdb::BackupEngineReadOnly::Open(BackupOptions(backup),
                                        rdb::Env::Default(), &engine),
        "BE++ open backup for recovery");
  PreparedRestore prepared;
  prepared.engine.reset(engine);
  rdb::BackupInfo latest;
  Check(prepared.engine->GetLatestBackupInfo(&latest),
        "BE++ get latest backup");
  if (!latest.app_metadata.starts_with(kMetadataPrefix)) {
    throw std::runtime_error("invalid BE++ backup metadata");
  }
  prepared.backup_id = latest.backup_id;
  prepared.wal_floor = ParseNumber(
      std::string_view(latest.app_metadata).substr(kMetadataPrefix.size()));

  prepared.mirror_ = WalMirrorDirectory(backup);
  return prepared;
}

void PreparedRestore::StageWal(const fs::path& target) const {
  WalFiles files;
  FindWalFiles(mirror_, wal_floor, files);
  if (!fs::create_directory(target)) {
    throw std::runtime_error("restore target must be new: " + target.string());
  }
  for (const auto& [number, filename] : files) {
    const auto path = mirror_ / filename;
    FileDescriptor input(Open(path, O_RDONLY));
    if (input.Get() < 0) {
      FileError("open mirrored WAL", path, errno);
    }
    const auto size = Size(input.Get(), path);
    CopyWal(input.Get(), path, target / filename, size);
  }
  SyncDirectory(target);
}

rdb::IOStatus PreparedRestore::Restore(const fs::path& target) const {
  rdb::RestoreOptions options;
  options.keep_log_files = true;
  return engine->RestoreDBFromBackup(options, backup_id, target.string(),
                                     target.string());
}

}  // namespace mb_ycsb
