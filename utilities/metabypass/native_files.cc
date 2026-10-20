//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/native_files.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <limits>
#include <set>

#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
Status Read(FileSystem* fs, const std::string& path, std::string* data) {
  Status s = fs->FileExists(path, IOOptions(), nullptr);
  if (!s.ok()) return s;
  std::unique_ptr<FSSequentialFile> f;
  s = fs->NewSequentialFile(path, FileOptions(), &f, nullptr);
  data->clear();
  std::array<char, 65536> buf;
  while (s.ok()) {
    Slice part;
    s = f->Read(buf.size(), IOOptions(), &part, buf.data(), nullptr);
    if (!s.ok() || part.empty()) break;
    data->append(part.data(), part.size());
  }
  return s;
}
Status Write(FileSystem* fs, const std::string& path, const Slice& data) {
  std::unique_ptr<FSWritableFile> f;
  Status s = fs->NewWritableFile(path, FileOptions(), &f, nullptr);
  if (!s.ok()) return s;
  s = f->Append(data, IOOptions(), nullptr);
  if (s.ok()) s = f->Fsync(IOOptions(), nullptr);
  s.UpdateIfOk(f->Close(IOOptions(), nullptr));
  return s;
}
Status EnsureDir(FileSystem* fs, const std::string& path) {
  Status s = fs->CreateDirIfMissing(path, IOOptions(), nullptr);
  if (s.ok())
    s = SyncDir(fs, std::filesystem::path(path).parent_path().string());
  return s;
}
Status SyncDir(FileSystem* fs, const std::string& path) {
  std::unique_ptr<FSDirectory> d;
  Status s = fs->NewDirectory(path, IOOptions(), &d, nullptr);
  if (s.ok()) s = d->Fsync(IOOptions(), nullptr);
  return s;
}
Status Copy(FileSystem* fs, const std::string& from, const std::string& to,
            uint64_t length) {
  std::unique_ptr<FSSequentialFile> in;
  std::unique_ptr<FSWritableFile> out;
  Status s = fs->NewSequentialFile(from, FileOptions(), &in, nullptr);
  if (s.ok()) s = fs->NewWritableFile(to, FileOptions(), &out, nullptr);
  if (!s.ok()) return s;
  std::array<char, 65536> buf;
  uint32_t expected = 0;
  const uint64_t original_length = length;
  while (s.ok() && length) {
    Slice part;
    s = in->Read(std::min<uint64_t>(length, buf.size()), IOOptions(), &part,
                 buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) {
      s = Status::Corruption("short file", from);
      break;
    }
    expected = crc32c::Extend(expected, part.data(), part.size());
    s = out->Append(part, IOOptions(), nullptr);
    length -= part.size();
  }
  if (s.ok()) s = out->Fsync(IOOptions(), nullptr);
  s.UpdateIfOk(out->Close(IOOptions(), nullptr));
  if (s.ok()) {
    uint32_t actual = 0;
    s = Digest(fs, to, original_length, &actual);
    if (s.ok() && actual != expected)
      s = Status::Corruption("copied file checksum mismatch", to);
  }
  return s;
}
Status Digest(FileSystem* fs, const std::string& path, uint64_t length,
              uint32_t* crc) {
  *crc = 0;
  return ExtendDigest(fs, path, 0, length, crc);
}
Status ExtendDigest(FileSystem* fs, const std::string& path, uint64_t begin,
                    uint64_t end, uint32_t* crc) {
  if (begin > end) return Status::Corruption("digest prefix shrank", path);
  uint64_t size;
  Status s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
  if (!s.ok()) return s;
  if (size < end) return Status::Corruption("short file", path);
  if (begin == end) return Status::OK();
  std::unique_ptr<FSRandomAccessFile> in;
  s = fs->NewRandomAccessFile(path, FileOptions(), &in, nullptr);
  std::array<char, 65536> buf;
  while (s.ok() && begin < end) {
    Slice part;
    s = in->Read(begin, std::min<uint64_t>(end - begin, buf.size()),
                 IOOptions(), &part, buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) return Status::Corruption("short file", path);
    *crc = crc32c::Extend(*crc, part.data(), part.size());
    begin += part.size();
  }
  return s;
}
Status RemoveDir(FileSystem* fs, const std::string& path) {
  std::vector<std::string> files;
  Status s = fs->GetChildren(path, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  for (const auto& f : files) {
    if (f == "." || f == "..") continue;
    s = fs->DeleteFile(path + "/" + f, IOOptions(), nullptr);
    if (!s.ok()) return s;
  }
  return fs->DeleteDir(path, IOOptions(), nullptr);
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
