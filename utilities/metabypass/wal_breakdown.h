//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifdef ROCKSDB_MB_WAL_BREAKDOWN
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE::metabypass::wal_probe {
enum Stage {
  operations_wait,
  event_build,
  reserve_wait,
  reserve_bookkeeping,
  capacity_wait,
  payload_copy,
  primary_append,
  finish_wait,
  finish_bookkeeping,
  batch_validation,
  error_check,
  completed_wait,
  completed_bookkeeping,
  stage_count
};

inline uint64_t Clock() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

enum BackgroundStage {
  mirror_selection_hold,
  mirror_completion_hold,
  background_stage_count
};

class BackgroundSample {
 public:
  struct Snapshot {
    uint64_t count = 0;
    uint64_t total_ns = 0;
    uint64_t max_ns = 0;
  };

  void Record(uint64_t elapsed) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++sample_.count;
    sample_.total_ns += elapsed;
    if (elapsed > sample_.max_ns) sample_.max_ns = elapsed;
  }

  Snapshot Read() {
    std::lock_guard<std::mutex> lock(mutex_);
    return sample_;
  }

 private:
  std::mutex mutex_;
  Snapshot sample_;
};

inline std::array<BackgroundSample, background_stage_count> background_samples;

class BackgroundSpan {
 public:
  explicit BackgroundSpan(BackgroundStage stage)
      : stage_(stage), begin_(Clock()) {}

  void End() { background_samples[stage_].Record(Clock() - begin_); }

 private:
  BackgroundStage stage_;
  uint64_t begin_;
};

// Only the sampled writer thread owns this buffer. Bound memory in the locked
// path and reject an overflowing sample in the driver after the API returns.
struct Trace {
  struct Interval {
    Stage stage;
    uint64_t begin, end, bytes;
  };
  Interval intervals[512];
  size_t size;
  bool overflow;
};

extern thread_local Trace* active;
extern thread_local bool mirror_append;

class SampleScope {
 public:
  explicit SampleScope(Trace* trace) : trace_(trace) {
    if (trace_) {
      trace_->size = 0;
      trace_->overflow = false;
      active = trace_;
    }
  }
  void End() {
    if (trace_) {
      active = nullptr;
      trace_ = nullptr;
    }
  }
  ~SampleScope() { End(); }

 private:
  Trace* trace_;
};

class Span {
 public:
  Span(Stage stage, bool enabled) {
    if (active && enabled) {
      if (active->size == 512) {
        active->overflow = true;
      } else {
        interval_ = &active->intervals[active->size++];
        *interval_ = {stage, Clock(), 0, 0};
      }
    }
  }
  void End(uint64_t bytes = 0) {
    if (interval_) {
      interval_->end = Clock();
      interval_->bytes = bytes;
      interval_ = nullptr;
    }
  }
  ~Span() { End(); }

 private:
  Trace::Interval* interval_ = nullptr;
};

struct MirrorAppend {
  explicit MirrorAppend(bool wal) : previous(mirror_append) {
    mirror_append = wal;
  }
  ~MirrorAppend() { mirror_append = previous; }
  bool previous;
};
}  // namespace ROCKSDB_NAMESPACE::metabypass::wal_probe
#define MB_WAL_SPAN(name, stage, enabled)              \
  ROCKSDB_NAMESPACE::metabypass::wal_probe::Span name( \
      ROCKSDB_NAMESPACE::metabypass::wal_probe::stage, enabled)
#define MB_WAL_END(name) name.End()
#define MB_WAL_BG_SPAN(name, stage)                              \
  ROCKSDB_NAMESPACE::metabypass::wal_probe::BackgroundSpan name( \
      ROCKSDB_NAMESPACE::metabypass::wal_probe::stage)
#else
#define MB_WAL_SPAN(name, stage, enabled) ((void)0)
#define MB_WAL_END(name) ((void)0)
#define MB_WAL_BG_SPAN(name, stage) ((void)0)
#endif
