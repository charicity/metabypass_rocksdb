// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include "dataset.h"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/db.h"

namespace {
void Require(bool ok, const std::string& message) {
  if (!ok) throw std::runtime_error(message);
}
void Check(const ROCKSDB_NAMESPACE::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}
template <typename DB>
void Open(const ROCKSDB_NAMESPACE::Options& options, const std::string& path,
          std::unique_ptr<DB>* db) {
  if constexpr (requires { DB::Open(options, path, db); }) {
    Check(DB::Open(options, path, db));
  } else {
    DB* raw = nullptr;
    Check(DB::Open(options, path, &raw));
    db->reset(raw);
  }
}
struct Temp {
  std::filesystem::path path;
  Temp() {
    auto pattern =
        (std::filesystem::temp_directory_path() / "mb-ycsb-dataset-test-XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    char* result = mkdtemp(buffer.data());
    if (!result) throw std::runtime_error("mkdtemp failed");
    path = result;
  }
  ~Temp() {
    // Only the unique directory created above belongs to this test.
    std::error_code error;
    std::filesystem::remove_all(path, error);
  }
};
void ExpectFailure(const std::function<void()>& action,
                   const std::string& fragment) {
  try {
    action();
  } catch (const std::runtime_error& error) {
    Require(std::string(error.what()).find(fragment) != std::string::npos,
            "unexpected failure: " + std::string(error.what()));
    return;
  }
  throw std::runtime_error("expected failure containing: " + fragment);
}
void Run(const std::string& name) {
  Require(name == "valid" || name == "missing" || name == "extra" ||
              name == "value" || name == "corrupt",
          "unknown case");
  Temp temp;
  const auto file = (temp.path / "dataset.bin").string();
  mb_ycsb::GenerateDataset(file, 32, 128);
  mb_ycsb::Dataset dataset(file);
  Require(dataset.metadata().record_count == 32 &&
              dataset.metadata().value_size == 128,
          "metadata dimensions");
  Require(dataset.metadata().file_sha256.size() == 64 &&
              dataset.metadata().logical_sha256.size() == 64,
          "metadata SHA256 encoding");
  if (name == "corrupt") {
    std::fstream stream(file, std::ios::binary | std::ios::in | std::ios::out);
    Require(stream.is_open(), "open fixture for corruption");
    // Header is 24 bytes, followed by a 24-byte key and the first value.
    stream.seekg(48);
    char byte = 0;
    stream.read(&byte, 1);
    Require(bool(stream), "read corruption byte");
    byte ^= 1;
    stream.seekp(48);
    stream.write(&byte, 1);
    stream.close();
    Require(!stream.fail(), "write corruption byte");
    ExpectFailure([&] { mb_ycsb::Dataset corrupt(file); }, "SHA256 mismatch");
    return;
  }
  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = true;
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> db;
  Open(options, (temp.path / "db").string(), &db);
  uint64_t replayed = 0;
  dataset.Replay([&](const std::string& key, const std::string& value) {
    auto record = dataset.ReadRecord(replayed++);
    Require(record.first == key && record.second == value,
            "load order differs");
    Check(db->Put(ROCKSDB_NAMESPACE::WriteOptions(), key, value));
  });
  Require(replayed == 32, "replay count");
  auto first = dataset.ReadRecord(0);
  if (name == "missing") {
    Check(db->Delete(ROCKSDB_NAMESPACE::WriteOptions(), first.first));
  } else if (name == "extra") {
    Check(db->Put(ROCKSDB_NAMESPACE::WriteOptions(), "zz-extra", "extra"));
  } else if (name == "value") {
    first.second[47] ^= 1;
    Check(
        db->Put(ROCKSDB_NAMESPACE::WriteOptions(), first.first, first.second));
  }
  {
    std::unique_ptr<ROCKSDB_NAMESPACE::Iterator> it(
        db->NewIterator(ROCKSDB_NAMESPACE::ReadOptions()));
    if (name == "valid") {
      mb_ycsb::Dataset samples(file, mb_ycsb::Dataset::Access::kSamplesOnly);
      Require(samples.metadata().file_sha256.empty() &&
                  samples.metadata().logical_sha256.empty(),
              "sample mode hashed fixture");
      Require(samples.ReadRecord(17) == dataset.ReadRecord(17),
              "sample record differs");
      ExpectFailure([&] { samples.Verify(*it); }, "requires full");
      ExpectFailure([&] { samples.Replay([](const auto&, const auto&) {}); },
                    "requires full");
      const auto actual = dataset.Verify(*it);
      Require(actual.logical_sha256 == dataset.metadata().logical_sha256,
              "actual DB digest differs");
      Require(actual.record_count == replayed, "verified count differs");
      // An attempted duplicate generation must preserve the existing fixture.
      ExpectFailure([&] { mb_ycsb::GenerateDataset(file, 8, 16); }, "dataset:");
      mb_ycsb::Dataset reopened(file);
      Require(reopened.metadata().file_sha256 == dataset.metadata().file_sha256,
              "exclusive generation changed fixture");
    } else {
      ExpectFailure([&] { dataset.Verify(*it); },
                    name == "missing" ? "missing key"
                    : name == "extra" ? "extra key"
                                      : "value mismatch");
    }
  }
  Check(db->Close());
}
}  // namespace
int main(int argc, char** argv) {
  try {
    Require(argc == 2, "usage: dataset_test valid|missing|extra|value|corrupt");
    Run(argv[1]);
    std::cout << "PASS " << argv[1] << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL: " << error.what() << '\n';
    return 1;
  }
}
