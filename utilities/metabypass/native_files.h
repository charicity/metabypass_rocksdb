//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <functional>
#include <map>
#include <string>
#include <vector>

#include "db/version_edit.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
Status Read(FileSystem* fs, const std::string& path, std::string* data);
Status Write(FileSystem* fs, const std::string& path, const Slice& data);
Status EnsureDir(FileSystem* fs, const std::string& path);
Status SyncDir(FileSystem* fs, const std::string& path);
Status Copy(FileSystem* fs, const std::string& from, const std::string& to,
            uint64_t length);
Status Digest(FileSystem* fs, const std::string& path, uint64_t length,
              uint32_t* crc);
Status RemoveDir(FileSystem* fs, const std::string& path);
Status ReadLog(FileSystem* fs, const std::string& path, uint64_t number,
               const std::function<Status(const Slice&)>& visit, uint64_t* end);
struct NativeState {
  std::string manifest;
  uint64_t manifest_end = 0;
  uint64_t log_number = 0;
  uint64_t next_file = 0;
  std::map<uint64_t, uint64_t> tables;
  std::map<uint64_t, BlobFileAddition> blobs;
  std::vector<std::string> edits;
};
Status Inspect(FileSystem* fs, const std::string& dir, NativeState* state);
Status RewriteManifest(FileSystem* fs, const std::string& dir,
                       const NativeState& state, const VersionEdit& extra);
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
