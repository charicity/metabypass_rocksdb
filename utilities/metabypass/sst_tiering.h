//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <vector>

#include "rocksdb/utilities/metabypass.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
using SstClock = std::function<uint64_t()>;
uint64_t SstNowMicros();
// Sampling owns no placement state. Snapshot drains a bounded sample buffer
// and forgets files absent from the supplied live set.
class SstSampler {
 public:
  explicit SstSampler(const SstTieringOptions& options,
                      SstClock clock = SstNowMicros,
                      std::function<uint32_t()> random = {});
  void Record(uint64_t file, Env::IOActivity activity);
  std::map<uint64_t, double> HeatSnapshot(const std::set<uint64_t>& live);
  void AddStats(SstTieringStats* stats) const;

 private:
  const SstTieringOptions options_;
  SstClock clock_;
  std::function<uint32_t()> random_;
  mutable std::mutex mutex_;
  std::vector<uint64_t> pending_;
  std::map<uint64_t, double> heat_;
  uint64_t last_;
  std::atomic<uint64_t> samples_{0}, drops_{0}, scans_{0};
};
struct SstFileState {
  uint64_t number = 0, size = 0, changed_micros = 0;
  bool hot = true, protected_copy = false;
  double heat = 0;
  uint32_t in_rounds = 0, out_rounds = 0;
};
struct SstMigrationIntent {
  uint64_t number;
  bool hot;
};
struct SstPolicyResult {
  std::set<uint64_t> target;
  std::vector<SstMigrationIntent> migrations;
  uint64_t oversized = 0;
};
// Pure, replaceable policy. No I/O, sampling, clocks, or mutable history.
SstPolicyResult PlanSstPlacement(const SstTieringOptions& options,
                                 const std::vector<SstFileState>& files,
                                 uint64_t charged_bytes, uint64_t now);
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
