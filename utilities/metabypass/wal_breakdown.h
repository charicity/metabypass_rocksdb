//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifdef ROCKSDB_MB_WAL_BREAKDOWN
#include <chrono>
#include <cstddef>
#include <cstdint>

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
  write_fence_wait,
  completed_wait,
  completed_bookkeeping,
  stage_count
};

inline uint64_t Clock() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

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
#else
#define MB_WAL_SPAN(name, stage, enabled) ((void)0)
#define MB_WAL_END(name) ((void)0)
#endif
