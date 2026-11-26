// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "rocksdb/iterator.h"

namespace ycsbc {
namespace utils {
class Properties;
}
}  // namespace ycsbc
namespace mb_ycsb {
void SetDatasetProperties(ycsbc::utils::Properties&, uint64_t record_count,
                          uint32_t value_size);
struct Metadata {
  uint64_t record_count = 0;
  uint32_t value_size = 0;
  std::string logical_sha256;
  std::string file_sha256;
};
// v1 fixes usertable, field0, one constant-size raw value, hashed user keys
// padded to 20 digits. Index memory is O(records); values are read on demand.
// Fixtures must remain immutable while open. Errors throw runtime_error.
class Dataset {
 public:
  enum class Access { kFull, kSamplesOnly };
  // Samples-only trusts a previously verified immutable fixture; digests are
  // empty and Replay/Verify are unavailable. No record scan or index is built.
  explicit Dataset(const std::string& path, Access access = Access::kFull);
  ~Dataset();
  Dataset(const Dataset&) = delete;
  Dataset& operator=(const Dataset&) = delete;
  const Metadata& metadata() const;
  void Replay(
      const std::function<void(const std::string&, const std::string&)>&) const;
  std::pair<std::string, std::string> ReadRecord(uint64_t load_ordinal) const;
  // Bytewise comparator required. Checks all keys/values and iterator status.
  Metadata Verify(ROCKSDB_NAMESPACE::Iterator&) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
// Never replaces an existing path. Partial output is rejected by Dataset.
void GenerateDataset(const std::string& path, uint64_t count,
                     uint32_t value_size);
}  // namespace mb_ycsb
