#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
"""Compile the actual pure selector and driver Verify against a counted fake DB."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


class VerificationTests(unittest.TestCase):
    def test_actual_selector_and_verify_without_engine_build(self):
        compiler = shutil.which("clang++-14") or shutil.which("g++")
        if not compiler:
            self.skipTest("a C++17 compiler is required for the pure verification test")
        source = Path(os.environ.get("MB_SAMPLE_DRIVER_SOURCE", HERE / "driver.cc")).read_text()
        config = source[source.index("struct Config {"):source.index("// Fault gates")]
        values = source[source.index("uint64_t Hash("):source.index("struct Histogram {")]
        verify = source[source.index("uint64_t Verify("):source.index("\nvoid Work(")]
        harness = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include "verification_sample.h"
using Clock = std::chrono::steady_clock;
uint64_t Us(Clock::time_point) { return 1; }
std::string evidence;
void Phase(const std::string&, Clock::time_point) {}
void Emit(const std::string&, const std::string& fields) { evidence = fields; }
struct Status {
  bool found = true;
  bool IsNotFound() const { return !found; }
};
void Check(const Status& status) {
  if (!status.found) throw std::runtime_error("missing live value");
}
struct Database {
  std::map<std::string, std::string> values;
  uint64_t reads = 0, iterators = 0;
  Status Get(const std::string& key, std::string* output) {
    ++reads;
    auto item = values.find(key);
    if (item == values.end()) return {false};
    *output = item->second;
    return {};
  }
  struct Iterator {
    uint64_t size, position = 0;
    void SeekToFirst() { position = 0; }
    bool Valid() { return position < size; }
    void Next() { ++position; }
    Status status() { return {}; }
  };
  auto Iter() {
    ++iterators;
    return std::make_unique<Iterator>(Iterator{values.size()});
  }
};
'''
        harness = harness.replace("#include <map>", "#include <map>\n#include <memory>")
        main = r'''
template <class F> void Reject(F operation) {
  bool rejected = false;
  try { operation(); } catch (const std::runtime_error&) { rejected = true; }
  assert(rejected);
}
int main() {
  std::vector<uint64_t> versions(128, 0);
  for (uint64_t worker = 0; worker < 4; ++worker) {
    versions[worker * 32 + 1] = 1;
    versions[worker * 32 + 2] = mb_native::kDeletedVersion | 1;
  }
  auto keys = mb_native::VerificationKeys(versions, 4, 32, 17);
  assert(keys.size() == 32 && std::is_sorted(keys.begin(), keys.end()));
  assert(std::set<uint64_t>(keys.begin(), keys.end()).size() == 32);
  assert(keys == mb_native::VerificationKeys(versions, 4, 32, 17));
  for (uint64_t worker = 0; worker < 4; ++worker)
    for (auto offset : {0, 1, 2, 31})
      assert(std::binary_search(keys.begin(), keys.end(), worker * 32 + offset));
  assert(mb_native::VerificationSeed(mb_native::kVerificationSeedSalt) != 0);
  Reject([&] { mb_native::VerificationKeys(versions, 4, 1, 17); });
  Reject([&] { mb_native::VerificationKeys({}, 1, 1, 17); });
  Reject([&] { mb_native::VerificationKeys(versions, 0, 1, 17); });
  auto all = mb_native::VerificationKeys(versions, 4, 4096, 17);
  assert(all.size() == 128 && all.front() == 0 && all.back() == 127);
  std::vector<std::string> args = {"test", "--index=/tmp/sample-index",
      "--data=/tmp/sample-data", "--backup=/tmp/sample-backup",
      "--state=/tmp/sample-state", "--keys=16", "--value-size=64",
      "--threads=2", "--verify-samples=8"};
  std::vector<char*> argv;
  for (auto& arg : args) argv.push_back(arg.data());
  Config config(argv.size(), argv.data());
  State state(config, true);
  state.version.assign(16, 1);
  Database db;
  for (uint64_t key = 0; key < 16; ++key) db.values[Key(key)] = Value(key, 1, 64);
  assert(Verify(db, config, state) == 8 && db.reads == 8 && db.iterators == 0);
  auto before_restore = evidence;
  db.reads = 0;
  assert(Verify(db, config, state) == 8 && evidence == before_restore);
  // Append-reopen's updated key zero is forced into every sample.
  state.version[0] = 2;
  db.values[Key(0)] = Value(0, 2, 64);
  assert(Verify(db, config, state) == 8);
  db.values[Key(0)] = Value(0, 1, 64);
  Reject([&] { Verify(db, config, state); });
  db.values[Key(0)] = Value(0, 2, 64);
  db.values[Key(0)].back() ^= 1;
  Reject([&] { Verify(db, config, state); });
  db.values[Key(0)] = Value(0, 2, 64).substr(1);
  Reject([&] { Verify(db, config, state); });
  db.values.erase(Key(0));
  Reject([&] { Verify(db, config, state); });
  db.values[Key(0)] = Value(0, 2, 64);
  state.version[2] = kDeleted | 2;
  Reject([&] { Verify(db, config, state); });
  db.values.erase(Key(2));
  assert(Verify(db, config, state) == 8);
  assert(evidence.find("\"deleted_checked\":1") != std::string::npos);
  config.flags["verify-samples"] = "4096";
  db.iterators = 0;
  assert(Verify(db, config, state) == 16 && db.iterators == 0);
  config.flags["verify-samples"] = "0";
  assert(Verify(db, config, state) == 16 && db.iterators == 1);
  db.values[Key(99)] = Value(99, 1, 64);
  Reject([&] { Verify(db, config, state); });
  args.push_back("--verify-acks=/tmp/sample-acks");
  argv.clear();
  for (auto& arg : args) argv.push_back(arg.data());
  Reject([&] { Config invalid(argv.size(), argv.data()); });
}
'''
        with tempfile.TemporaryDirectory(prefix="mb-sample-unit-") as temporary:
            root = Path(temporary)
            test = root / "test.cc"
            test.write_text(harness + config + values + verify + main)
            binary = root / "test"
            subprocess.run([compiler, "-std=c++17", "-Wall", "-Wextra", "-Werror", "-fno-rtti",
                            "-I", str(HERE), str(test), "-o", str(binary)], check=True,
                           capture_output=True, text=True, timeout=60)
            subprocess.run([str(binary)], check=True, capture_output=True, text=True, timeout=60)


if __name__ == "__main__":
    unittest.main()
