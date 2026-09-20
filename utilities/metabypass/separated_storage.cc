//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/metabypass/separated_storage.h"

#include <algorithm>
#include <array>
#include <limits>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "rocksdb/sst_file_reader.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace metabypass {
SeparatedStorage::SeparatedStorage(std::shared_ptr<FileSystem> fs,
                                   std::string index, std::string data)
    : FileSystemWrapper(fs), index_(std::move(index)), data_(std::move(data)) {}
SeparatedStorage::~SeparatedStorage() {
  if (lock_)
    target()->UnlockFile(lock_, IOOptions(), nullptr).PermitUncheckedError();
}
Status SeparatedStorage::Lock() {
  Status s = EnsureDir(target(), data_);
  if (s.ok())
    s = target()->LockFile(data_ + "/METABYPASS-LOCK", IOOptions(), &lock_,
                           nullptr);
  return s;
}
std::string SeparatedStorage::BlobPath(uint64_t n) const {
  return BlobFileName(data_, n);
}
bool SeparatedStorage::IsBlob(const std::string& path) const {
  if (path.compare(0, index_.size() + 1, index_ + "/") != 0) return false;
  uint64_t n;
  FileType type;
  return ParseFileName(path.substr(index_.size() + 1), &n, &type) &&
         type == kBlobFile;
}
std::string SeparatedStorage::Map(const std::string& path) const {
  return IsBlob(path) ? data_ + path.substr(index_.size()) : path;
}
#define MB_OPEN(method, type)                                               \
  IOStatus SeparatedStorage::method(                                        \
      const std::string& p, const FileOptions& o, std::unique_ptr<type>* r, \
      IODebugContext* d) {                                                  \
    return target()->method(Map(p), o, r, d);                               \
  }
MB_OPEN(NewSequentialFile, FSSequentialFile)
MB_OPEN(NewRandomAccessFile, FSRandomAccessFile)
MB_OPEN(NewWritableFile, FSWritableFile)
MB_OPEN(ReopenWritableFile, FSWritableFile)
#undef MB_OPEN
IOStatus SeparatedStorage::FileExists(const std::string& p, const IOOptions& o,
                                      IODebugContext* d) {
  return target()->FileExists(Map(p), o, d);
}
IOStatus SeparatedStorage::GetFileSize(const std::string& p, const IOOptions& o,
                                       uint64_t* r, IODebugContext* d) {
  return target()->GetFileSize(Map(p), o, r, d);
}
IOStatus SeparatedStorage::DeleteFile(const std::string& p, const IOOptions& o,
                                      IODebugContext* d) {
  // Retention is intentional, including failed-write and obsolete files.
  return IsBlob(p) ? IOStatus::OK() : target()->DeleteFile(p, o, d);
}
IOStatus SeparatedStorage::SyncFile(const std::string& p, const FileOptions& f,
                                    const IOOptions& o, bool full,
                                    IODebugContext* d) {
  return target()->SyncFile(Map(p), f, o, full, d);
}
IOStatus SeparatedStorage::GetChildren(const std::string& p, const IOOptions& o,
                                       std::vector<std::string>* r,
                                       IODebugContext* d) {
  IOStatus s = target()->GetChildren(p, o, r, d);
  if (!s.ok() || p != index_) return s;
  std::vector<std::string> blobs;
  s = target()->GetChildren(data_, o, &blobs, d);
  if (!s.ok()) return s;
  for (const auto& b : blobs)
    if (IsBlob(index_ + "/" + b)) r->push_back(b);
  return s;
}
namespace {
class References : public WriteBatch::Handler {
 public:
  References(FileSystem* fs, const SeparatedStorage* storage,
             std::map<uint64_t, uint64_t>* lengths)
      : fs_(fs), storage_(storage), lengths_(lengths) {}
  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    return Status::Corruption("inline value in Metabypass WAL");
  }
  Status DeleteCF(uint32_t cf, const Slice&) override {
    return cf == 0 ? Status::OK() : Status::Corruption("non-default CF");
  }
  Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                        const Slice& value) override {
    if (cf != 0 || value.empty())
      return Status::Corruption("invalid blob reference");
    BlobIndex idx;
    Status s = idx.DecodeFrom(value);
    if (!s.ok()) return s;
    if (idx.IsInlined() || idx.HasTTL() || idx.IsSameFile() ||
        idx.offset() < BlobLogRecord::kHeaderSize + key.size() ||
        idx.size() > std::numeric_limits<uint64_t>::max() - idx.offset())
      return Status::Corruption("unsupported blob reference");
    std::unique_ptr<FSRandomAccessFile> f;
    s = fs_->NewRandomAccessFile(storage_->BlobPath(idx.file_number()),
                                 FileOptions(), &f, nullptr);
    if (!s.ok()) return s;
    auto compression = header_compressions_.find(idx.file_number());
    if (compression == header_compressions_.end()) {
      char header_bytes[BlobLogHeader::kSize];
      Slice header_slice;
      s = f->Read(0, sizeof(header_bytes), IOOptions(), &header_slice,
                  header_bytes, nullptr);
      if (!s.ok()) return s;
      BlobLogHeader header;
      s = header.DecodeFrom(header_slice);
      if (!s.ok()) return s;
      if (header.column_family_id != cf || header.has_ttl)
        return Status::Corruption("blob header does not match reference");
      compression =
          header_compressions_.emplace(idx.file_number(), header.compression)
              .first;
    }
    if (compression->second != idx.compression())
      return Status::Corruption("blob compression does not match reference");
    const uint64_t start =
        idx.offset() - key.size() - BlobLogRecord::kHeaderSize;
    char header[BlobLogRecord::kHeaderSize];
    Slice result;
    s = f->Read(start, sizeof(header), IOOptions(), &result, header, nullptr);
    if (!s.ok()) return s;
    BlobLogRecord record;
    s = record.DecodeHeaderFrom(result);
    if (!s.ok()) return s;
    if (record.key_size != key.size() || record.value_size != idx.size())
      return Status::Corruption("blob reference length mismatch");
    std::array<char, 65536> buffer;
    uint64_t position = 0;
    const uint64_t length = key.size() + idx.size();
    uint32_t crc = 0;
    while (position < length) {
      const size_t n = std::min<uint64_t>(length - position, buffer.size());
      s = f->Read(start + sizeof(header) + position, n, IOOptions(), &result,
                  buffer.data(), nullptr);
      if (!s.ok()) return s;
      if (result.size() != n) return Status::Corruption("short blob");
      if (position < key.size()) {
        const size_t key_bytes = std::min<uint64_t>(key.size() - position, n);
        if (Slice(result.data(), key_bytes) !=
            Slice(key.data() + position, key_bytes))
          return Status::Corruption("blob key mismatch");
      }
      crc = crc32c::Extend(crc, result.data(), result.size());
      position += n;
    }
    if (crc32c::Mask(crc) != record.blob_crc)
      return Status::Corruption("blob CRC mismatch");
    (*lengths_)[idx.file_number()] =
        std::max((*lengths_)[idx.file_number()], idx.offset() + idx.size());
    return s;
  }

 private:
  FileSystem* fs_;
  const SeparatedStorage* storage_;
  std::map<uint64_t, uint64_t>* lengths_;
  std::map<uint64_t, CompressionType> header_compressions_;
};
// Scan before mutating anything. A bad/unwritten tail beyond the required
// prefix can be discarded during exclusive recovery; a referenced byte cannot.
Status Scan(FileSystem* fs, const std::string& path, uint64_t required,
            uint64_t* end, uint64_t* count, bool* sealed) {
  uint64_t size;
  Status s = fs->GetFileSize(path, IOOptions(), &size, nullptr);
  if (!s.ok()) return s;
  if (size < BlobLogHeader::kSize || size < required)
    return Status::Corruption("short blob", path);
  std::unique_ptr<FSRandomAccessFile> file;
  s = fs->NewRandomAccessFile(path, FileOptions(), &file, nullptr);
  if (!s.ok()) return s;
  std::array<char, 65536> buffer;
  auto read = [&](uint64_t offset, size_t length, Slice* result) -> Status {
    Status io =
        file->Read(offset, length, IOOptions(), result, buffer.data(), nullptr);
    if (!io.ok()) return io;
    if (result->size() != length)
      return Status::Corruption("short blob read", path);
    return Status::OK();
  };
  Slice part;
  s = read(0, BlobLogHeader::kSize, &part);
  if (!s.ok()) return s;
  BlobLogHeader header;
  s = header.DecodeFrom(part);
  if (!s.ok()) return s;
  if (header.has_ttl || header.column_family_id != 0)
    return Status::Corruption("unsupported blob header", path);
  *end = BlobLogHeader::kSize;
  *count = 0;
  *sealed = false;
  while (*end < size) {
    const uint64_t left = size - *end;
    if (left < BlobLogRecord::kHeaderSize) break;
    s = read(*end, BlobLogRecord::kHeaderSize, &part);
    if (!s.ok()) return s;
    if (left == BlobLogFooter::kSize) {
      BlobLogFooter footer;
      s = footer.DecodeFrom(part);
      if (s.ok() && footer.blob_count == *count &&
          *end + BlobLogFooter::kSize >= required) {
        *sealed = true;
        return Status::OK();
      }
      s.PermitUncheckedError();
    }
    BlobLogRecord record;
    s = record.DecodeHeaderFrom(part);
    if (!s.ok()) {
      s.PermitUncheckedError();
      break;
    }
    if (record.key_size > left - BlobLogRecord::kHeaderSize ||
        record.value_size > left - BlobLogRecord::kHeaderSize - record.key_size)
      break;
    uint64_t offset = *end + BlobLogRecord::kHeaderSize;
    uint64_t remaining = record.key_size + record.value_size;
    uint32_t crc = 0;
    while (remaining) {
      const size_t n = std::min<uint64_t>(remaining, buffer.size());
      s = read(offset, n, &part);
      if (!s.ok()) return s;
      crc = crc32c::Extend(crc, part.data(), part.size());
      offset += n;
      remaining -= n;
    }
    if (crc32c::Mask(crc) != record.blob_crc) break;
    *end = offset;
    ++*count;
  }
  return *end >= required ? Status::OK()
                          : Status::Corruption("damaged referenced blob", path);
}
}  // namespace
Status SeparatedStorage::Dependencies(const std::string& index,
                                      const NativeState& state,
                                      std::map<uint64_t, uint64_t>* lengths,
                                      WalValidationCache* cache) {
  lengths->clear();
  for (const auto& b : state.blobs) {
    if (b.second.GetTotalBlobBytes() > std::numeric_limits<uint64_t>::max() -
                                           BlobLogHeader::kSize -
                                           BlobLogFooter::kSize)
      return Status::Corruption("blob length overflow");
    // All additions are retained, including obsolete historical additions.
    (*lengths)[b.first] = BlobLogHeader::kSize + b.second.GetTotalBlobBytes() +
                          BlobLogFooter::kSize;
  }
  std::vector<std::string> files;
  Status s = target()->GetChildren(index, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  for (const auto& f : files) {
    uint64_t number;
    FileType type;
    if (!ParseFileName(f, &number, &type) || type != kWalFile ||
        number < state.log_number)
      continue;
    WalValidation local;
    WalValidation& validated = cache ? (*cache)[f] : local;
    uint64_t size;
    s = target()->GetFileSize(index + "/" + f, IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size < validated.end) validated = WalValidation();
    References references(target(), this, &validated.lengths);
    uint64_t records = 0, end;
    s = ReadLog(
        target(), index + "/" + f, number,
        [&](const Slice& record) {
          if (++records <= validated.records) return Status::OK();
          TEST_SYNC_POINT("MetaBypass::WalRecordValidated");
          WriteBatch batch;
          Status decoded = WriteBatchInternal::SetContents(&batch, record);
          if (decoded.ok()) decoded = batch.Iterate(&references);
          return decoded;
        },
        &end);
    if (!s.ok()) return s;
    if (records < validated.records)
      return Status::Corruption("cached WAL prefix changed");
    validated.records = records;
    validated.end = end;
    for (const auto& blob : validated.lengths)
      (*lengths)[blob.first] = std::max((*lengths)[blob.first], blob.second);
  }
  return Status::OK();
}
Status SeparatedStorage::ValidateTable(const std::string& path,
                                       const Options& options,
                                       std::map<uint64_t, uint64_t>* lengths) {
  TEST_SYNC_POINT("MetaBypass::TableValidated");
  SstFileReader reader(options);
  Status s = reader.Open(path);
  if (s.ok()) s = reader.VerifyChecksum();
  if (!s.ok()) return s;
  auto it = reader.NewTableIterator();
  References references(target(), this, lengths);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ParsedInternalKey key;
    s = ParseInternalKey(it->key(), &key, false);
    if (!s.ok()) return s;
    if (key.type == kTypeBlobIndex)
      s = references.PutBlobIndexCF(0, key.user_key, it->value());
    else if (key.type != kTypeDeletion)
      return Status::Corruption("unsupported Metabypass SST entry");
    if (!s.ok()) return s;
  }
  return it->status();
}
Status SeparatedStorage::Persist(const std::map<uint64_t, uint64_t>& lengths) {
  for (const auto& b : lengths) {
    uint64_t end, count;
    bool sealed;
    Status s =
        Scan(target(), BlobPath(b.first), b.second, &end, &count, &sealed);
    if (!s.ok()) return s;
    uint64_t size;
    s = target()->GetFileSize(BlobPath(b.first), IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size < b.second)
      return Status::Corruption("blob dependency is incomplete");
    s = target()->SyncFile(BlobPath(b.first), FileOptions(), IOOptions(), true,
                           nullptr);
    if (!s.ok()) return s;
  }
  return SyncDir(target(), data_);
}
Status SeparatedStorage::PersistIncremental(
    const std::map<uint64_t, uint64_t>& lengths, BlobValidationCache* cache,
    uint64_t* scanned) {
  *scanned = 0;
  for (const auto& dependency : lengths) {
    const std::string path = BlobPath(dependency.first);
    const uint64_t required = dependency.second;
    uint64_t size;
    Status s = target()->GetFileSize(path, IOOptions(), &size, nullptr);
    if (!s.ok()) return s;
    if (size < required) return Status::Corruption("short blob", path);
    auto& saved = (*cache)[dependency.first];
    if (size < saved.length)
      return Status::Corruption("cached blob shrank", path);
    BlobValidation next = saved;
    // A smaller dependency cannot use a CRC of a longer prefix.
    if (required < next.length) next = BlobValidation();
    std::unique_ptr<FSRandomAccessFile> file;
    if (next.length < required) {
      s = target()->NewRandomAccessFile(path, FileOptions(), &file, nullptr);
      if (!s.ok()) return s;
    }
    std::array<char, 65536> buffer;
    auto read = [&](uint64_t offset, size_t count, Slice* bytes) -> Status {
      Status io =
          file->Read(offset, count, IOOptions(), bytes, buffer.data(), nullptr);
      if (!io.ok()) return io;
      if (bytes->size() != count) return Status::Corruption("short blob", path);
      *scanned += count;
      return Status::OK();
    };
    Slice bytes;
    if (next.length == 0) {
      if (required < BlobLogHeader::kSize)
        return Status::Corruption("invalid blob dependency", path);
      s = read(0, BlobLogHeader::kSize, &bytes);
      if (!s.ok()) return s;
      BlobLogHeader header;
      s = header.DecodeFrom(bytes);
      if (!s.ok()) return s;
      if (header.has_ttl || header.column_family_id != 0)
        return Status::Corruption("unsupported blob header", path);
      next.crc = crc32c::Value(bytes.data(), bytes.size());
      next.length = bytes.size();
    }
    while (next.length < required) {
      if (next.sealed) return Status::Corruption("sealed blob extended", path);
      const uint64_t left = required - next.length;
      if (left < BlobLogRecord::kHeaderSize)
        return Status::Corruption("partial blob record", path);
      s = read(next.length, BlobLogRecord::kHeaderSize, &bytes);
      if (!s.ok()) return s;
      if (left == BlobLogFooter::kSize) {
        BlobLogFooter footer;
        Status footer_status = footer.DecodeFrom(bytes);
        if (footer_status.ok()) {
          if (footer.blob_count != next.records)
            return Status::Corruption("blob footer count", path);
          next.crc = crc32c::Extend(next.crc, bytes.data(), bytes.size());
          next.length += bytes.size();
          next.sealed = true;
          break;
        }
      }
      BlobLogRecord record;
      s = record.DecodeHeaderFrom(bytes);
      if (!s.ok()) return s;
      if (record.key_size > left - bytes.size() ||
          record.value_size > left - bytes.size() - record.key_size)
        return Status::Corruption("partial blob payload", path);
      next.crc = crc32c::Extend(next.crc, bytes.data(), bytes.size());
      next.length += bytes.size();
      uint64_t remaining = record.key_size + record.value_size;
      uint32_t payload_crc = 0;
      while (remaining) {
        s = read(next.length, std::min<uint64_t>(remaining, buffer.size()),
                 &bytes);
        if (!s.ok()) return s;
        next.crc = crc32c::Extend(next.crc, bytes.data(), bytes.size());
        payload_crc = crc32c::Extend(payload_crc, bytes.data(), bytes.size());
        next.length += bytes.size();
        remaining -= bytes.size();
      }
      if (crc32c::Mask(payload_crc) != record.blob_crc)
        return Status::Corruption("blob CRC mismatch", path);
      ++next.records;
    }
    s = target()->SyncFile(path, FileOptions(), IOOptions(), true, nullptr);
    if (!s.ok()) return s;
    saved = next;
  }
  return SyncDir(target(), data_);
}
Status SeparatedStorage::PrepareRecovery(const std::string& index) {
  NativeState state;
  Status s = Inspect(target(), index, &state);
  if (!s.ok()) return s;
  std::map<uint64_t, uint64_t> lengths;
  s = Dependencies(index, state, &lengths);
  if (!s.ok()) return s;
  // Validate every required file before the first mutation. Retried recovery
  // recognizes already sealed files and atomically replaces the MANIFEST.
  for (const auto& b : lengths) {
    uint64_t end, count;
    bool sealed;
    s = Scan(target(), BlobPath(b.first), b.second, &end, &count, &sealed);
    if (!s.ok()) return s;
  }
  TEST_SYNC_POINT_CALLBACK("MetaBypass::RecoveryValidated", &s);
  if (!s.ok()) return s;
  VersionEdit extra;
  uint64_t next = state.next_file;
  std::vector<std::string> files;
  s = target()->GetChildren(data_, IOOptions(), &files, nullptr);
  if (!s.ok()) return s;
  for (const auto& f : files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      if (number == std::numeric_limits<uint64_t>::max())
        return Status::Corruption("blob file number exhausted");
      next = std::max(next, number + 1);
    }
  }
  for (const auto& b : lengths) {
    uint64_t end, count;
    bool sealed;
    s = Scan(target(), BlobPath(b.first), b.second, &end, &count, &sealed);
    if (!s.ok()) return s;
    if (!sealed) {
      BlobLogFooter footer;
      footer.blob_count = count;
      std::string encoded_footer;
      footer.EncodeTo(&encoded_footer);
      const std::string temp = BlobPath(b.first) + ".recover";
      s = Copy(target(), BlobPath(b.first), temp, end);
      if (!s.ok()) return s;
      std::unique_ptr<FSWritableFile> writer;
      s = target()->ReopenWritableFile(temp, FileOptions(), &writer, nullptr);
      if (!s.ok()) return s;
      s = writer->Append(encoded_footer, IOOptions(), nullptr);
      if (s.ok()) s = writer->Fsync(IOOptions(), nullptr);
      s.UpdateIfOk(writer->Close(IOOptions(), nullptr));
      if (s.ok())
        s = target()->RenameFile(temp, BlobPath(b.first), IOOptions(), nullptr);
      if (!s.ok()) return s;
    }
    TEST_SYNC_POINT_CALLBACK("MetaBypass::RecoveryBlobSealed", &s);
    if (!s.ok()) return s;
    if (state.blobs.count(b.first) == 0)
      extra.AddBlobFile(b.first, count, end - BlobLogHeader::kSize, "", "");
  }
  s = SyncDir(target(), data_);
  extra.SetNextFile(next);
  if (s.ok()) s = RewriteManifest(target(), index, state, extra);
  return s;
}
}  // namespace metabypass
}  // namespace ROCKSDB_NAMESPACE
