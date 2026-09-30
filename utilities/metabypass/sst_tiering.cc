//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/sst_tiering.h"

#include <algorithm>
#include <chrono>
#include <cmath>

#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
uint64_t SstNowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
SstSampler::SstSampler(const SstTieringOptions& o, SstClock clock,
                       std::function<uint32_t()> random)
    : options_(o),
      clock_(std::move(clock)),
      random_(std::move(random)),
      last_(clock_()) {
  pending_.reserve(o.sample_buffer_capacity);
}
void SstSampler::Record(uint64_t file, Env::IOActivity activity) {
  if (activity == Env::IOActivity::kDBIterator) {
    scans_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  if (activity != Env::IOActivity::kGet) return;
  const uint32_t random =
      random_ ? random_() : Random::GetTLSInstance()->Next();
  if (random % options_.sample_one_in != 0) return;
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock() || pending_.size() == options_.sample_buffer_capacity) {
    drops_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  pending_.push_back(file);
  samples_.fetch_add(1, std::memory_order_relaxed);
}
std::map<uint64_t, double> SstSampler::HeatSnapshot(
    const std::set<uint64_t>& live) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t now = clock_();
  const double decay =
      std::exp2(-double(now - last_) / (options_.heat_half_life_ms * 1000.0));
  for (auto it = heat_.begin(); it != heat_.end();) {
    if (!live.count(it->first))
      it = heat_.erase(it);
    else {
      it->second *= decay;
      ++it;
    }
  }
  for (uint64_t number : pending_) {
    if (live.count(number)) heat_[number] += 1;
  }
  pending_.clear();
  last_ = now;
  return heat_;
}
void SstSampler::AddStats(SstTieringStats* stats) const {
  stats->sampled_reads = samples_.load(std::memory_order_relaxed);
  stats->dropped_samples = drops_.load(std::memory_order_relaxed);
  stats->scan_reads = scans_.load(std::memory_order_relaxed);
}
SstPolicyResult PlanSstPlacement(const SstTieringOptions& o,
                                 const std::vector<SstFileState>& files,
                                 uint64_t charged, uint64_t now) {
  SstPolicyResult result;
  const uint64_t budget = o.ssd_capacity_bytes -
                          o.ssd_capacity_bytes / 100 * o.reserve_percent -
                          o.ssd_capacity_bytes % 100 * o.reserve_percent / 100;
  uint64_t available = budget;
  std::vector<const SstFileState*> ranked;
  for (const auto& file : files) {
    if (!file.protected_copy) {
      if (file.hot) available -= std::min(available, file.size);
      continue;
    }
    if (file.size > budget) {
      ++result.oversized;
      continue;
    }
    ranked.push_back(&file);
  }
  auto score = [&](const SstFileState* f) {
    return f->heat / std::max<uint64_t>(1, f->size) /
           (f->hot ? 1.0 : 1.0 + o.replacement_margin);
  };
  std::sort(ranked.begin(), ranked.end(), [&](const auto* a, const auto* b) {
    if (score(a) != score(b)) return score(a) > score(b);
    if (a->hot != b->hot) return a->hot;
    return a->number < b->number;
  });
  for (const auto* file : ranked) {
    if (file->size <= available && (file->hot || file->heat > 0)) {
      result.target.insert(file->number);
      available -= file->size;
    }
  }
  const bool pressure = charged > budget;
  for (const auto& file : files) {
    if (!file.protected_copy) continue;
    const bool target = result.target.count(file.number) != 0;
    if (target == file.hot) continue;
    const bool resident =
        now >= file.changed_micros &&
        now - file.changed_micros >= o.min_residency_ms * 1000;
    if (!target &&
        (pressure || (resident && file.out_rounds >= o.demote_rounds - 1)))
      result.migrations.push_back({file.number, false});
    if (target && resident && file.in_rounds >= o.promote_rounds - 1)
      result.migrations.push_back({file.number, true});
  }
  // Release space before reserving promotion destinations.
  std::stable_sort(result.migrations.begin(), result.migrations.end(),
                   [](const auto& a, const auto& b) { return a.hot < b.hot; });
  return result;
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
