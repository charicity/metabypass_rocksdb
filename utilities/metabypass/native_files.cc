//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/native_files.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <limits>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "file/writable_file_writer.h"
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
  while (s.ok() && length) {
    Slice part;
    s = in->Read(std::min<uint64_t>(length, buf.size()), IOOptions(), &part,
                 buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) {
      s = Status::Corruption("short file", from);
      break;
    }
    s = out->Append(part, IOOptions(), nullptr);
    length -= part.size();
  }
  if (s.ok()) s = out->Fsync(IOOptions(), nullptr);
  s.UpdateIfOk(out->Close(IOOptions(), nullptr));
  return s;
}
Status Digest(FileSystem* fs, const std::string& path, uint64_t length,
              uint32_t* crc) {
  std::unique_ptr<FSSequentialFile> in;
  Status s = fs->NewSequentialFile(path, FileOptions(), &in, nullptr);
  std::array<char, 65536> buf;
  *crc = 0;
  while (s.ok() && length) {
    Slice part;
    s = in->Read(std::min<uint64_t>(length, buf.size()), IOOptions(), &part,
                 buf.data(), nullptr);
    if (!s.ok()) break;
    if (part.empty()) return Status::Corruption("short file", path);
    *crc = crc32c::Extend(*crc, part.data(), part.size());
    length -= part.size();
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
namespace {
class Reporter : public log::Reader::Reporter {
 public:
  Status status;
  void Corruption(size_t, const Status& s, uint64_t) override {
    if (status.ok()) status = s;
  }
};
}  // namespace
Status ReadLog(FileSystem* fs, const std::string& path, uint64_t number,
               const std::function<Status(const Slice&)>& visit,
               uint64_t* end) {
  Status s = fs->FileExists(path, IOOptions(), nullptr);
  if (!s.ok()) return s;
  std::unique_ptr<FSSequentialFile> f;
  s = fs->NewSequentialFile(path, FileOptions(), &f, nullptr);
  if (!s.ok()) return s;
  Reporter reporter;
  log::Reader reader(nullptr,
                     std::make_unique<SequentialFileReader>(std::move(f), path),
                     &reporter, true, number);
  Slice record;
  std::string scratch;
  *end = 0;
  while (reader.ReadRecord(&record, &scratch)) {
    if (!reporter.status.ok()) return reporter.status;
    s = visit(record);
    if (!s.ok()) return s;
    *end = reader.LastRecordEnd();
  }
  return reporter.status;
}
Status Inspect(FileSystem* fs, const std::string& dir, NativeState* state) {
  *state = NativeState();
  Status s = Read(fs, dir + "/CURRENT", &state->manifest);
  if (!s.ok()) return s;
  if (state->manifest.empty() || state->manifest.back() != '\n')
    return Status::Incomplete("CURRENT not complete");
  state->manifest.pop_back();
  if (state->manifest.find('/') != std::string::npos ||
      state->manifest.compare(0, 9, "MANIFEST-") != 0)
    return Status::Corruption("invalid CURRENT");
  uint32_t remaining = 0;
  bool group = false;
  s = ReadLog(
      fs, dir + "/" + state->manifest, 0,
      [&](const Slice& record) {
        VersionEdit edit;
        Status decoded = edit.DecodeFrom(record);
        if (!decoded.ok()) return decoded;
        if (edit.GetColumnFamily() != 0 || edit.IsColumnFamilyAdd() ||
            edit.IsColumnFamilyDrop())
          return Status::NotSupported("Metabypass requires one column family");
        if (group && (!edit.IsInAtomicGroup() ||
                      edit.GetRemainingEntries() + 1 != remaining))
          return Status::Corruption("invalid MANIFEST atomic group");
        remaining = edit.IsInAtomicGroup() ? edit.GetRemainingEntries() : 0;
        group = remaining != 0;
        state->edits.emplace_back(record.data(), record.size());
        if (edit.HasLogNumber()) state->log_number = edit.GetLogNumber();
        if (edit.HasNextFile()) state->next_file = edit.GetNextFile();
        for (const auto& f : edit.GetDeletedFiles())
          state->tables.erase(f.second);
        for (const auto& f : edit.GetNewFiles())
          state->tables[f.second.fd.GetNumber()] = f.second.fd.GetFileSize();
        for (const auto& b : edit.GetBlobFileAdditions())
          state->blobs.emplace(b.GetBlobFileNumber(), b);
        return Status::OK();
      },
      &state->manifest_end);
  if (s.ok() && group)
    return Status::Incomplete("MANIFEST atomic group pending");
  return s;
}
Status RewriteManifest(FileSystem* fs, const std::string& dir,
                       const NativeState& state, const VersionEdit& extra) {
  const std::string temp = dir + "/METABYPASS-MANIFEST.tmp";
  std::unique_ptr<FSWritableFile> f;
  Status s = fs->NewWritableFile(temp, FileOptions(), &f, nullptr);
  if (!s.ok()) return s;
  log::Writer writer(
      std::make_unique<WritableFileWriter>(std::move(f), temp, FileOptions()),
      0, false);
  for (const auto& record : state.edits) {
    s = writer.AddRecord(WriteOptions(), record);
    if (!s.ok()) break;
  }
  std::string record;
  if (s.ok() && !extra.EncodeTo(&record)) s = Status::Corruption("VersionEdit");
  if (s.ok()) s = writer.AddRecord(WriteOptions(), record);
  if (s.ok()) s = writer.file()->Sync(IOOptions(), true);
  s.UpdateIfOk(writer.Close(WriteOptions()));
  if (s.ok())
    s = fs->RenameFile(temp, dir + "/" + state.manifest, IOOptions(), nullptr);
  if (s.ok()) s = SyncDir(fs, dir);
  return s;
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
