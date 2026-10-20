//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

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
// Extend an existing CRC with [begin, end), without rereading its prefix.
Status ExtendDigest(FileSystem* fs, const std::string& path, uint64_t begin,
                    uint64_t end, uint32_t* crc);
Status RemoveDir(FileSystem* fs, const std::string& path);
// Immutable SST inventory obtained from the engine checkpoint API.
struct NativeState {
  std::map<uint64_t, uint64_t> tables;
};
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
