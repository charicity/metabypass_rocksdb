// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <vector>

namespace mb_native {
constexpr uint64_t kVerificationSeedSalt = 0xd1b54a32d192ed03ULL;
constexpr uint64_t kDeletedVersion = uint64_t{1} << 63;

inline uint64_t VerificationSeed(uint64_t seed) {
  auto derived = seed ^ kVerificationSeedSalt;
  return derived == 0 ? kVerificationSeedSalt : derived;
}

inline std::vector<uint64_t> VerificationKeys(
    const std::vector<uint64_t>& versions, uint64_t threads, uint64_t requested,
    uint64_t seed) {
  const uint64_t population = versions.size();
  if (population == 0 || threads == 0 || threads > population || requested == 0) {
    throw std::runtime_error("invalid verification sampling dimensions");
  }
  const uint64_t target = std::min(requested, population);
  std::set<uint64_t> selected{0, population - 1};
  for (uint64_t worker = 0; worker < threads; ++worker) {
    const auto lo = population * worker / threads;
    const auto hi = population * (worker + 1) / threads;
    selected.insert(lo);
    selected.insert(hi - 1);
    bool live = false, deleted = false;
    for (auto key = lo; key < hi && !(live && deleted); ++key) {
      if (!deleted && (versions[key] & kDeletedVersion) != 0) {
        selected.insert(key);
        deleted = true;
      } else if (!live && versions[key] != 0 &&
                 (versions[key] & kDeletedVersion) == 0) {
        selected.insert(key);
        live = true;
      }
    }
  }
  if (selected.size() > target) {
    throw std::runtime_error("verification sample budget below mandatory keys");
  }
  if (target == population) {
    for (uint64_t key = 0; key < population; ++key) selected.insert(key);
  } else {
    auto random = VerificationSeed(seed);
    while (selected.size() < target) {
      random ^= random << 13;
      random ^= random >> 7;
      random ^= random << 17;
      selected.insert(random % population);
    }
  }
  return {selected.begin(), selected.end()};
}
}  // namespace mb_native
