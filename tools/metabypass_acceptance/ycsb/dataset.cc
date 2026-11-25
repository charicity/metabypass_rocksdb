// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include "dataset.h"

#include <fcntl.h>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/core_workload.h"

namespace mb_ycsb {
namespace {
constexpr uint64_t kHeader = 24, kKey = 24, kTrailer = 64;
constexpr char kMagic[] = "MBYCSB01";
[[noreturn]] void Fail(const std::string& text) {
  throw std::runtime_error("dataset: " + text);
}
struct Fd {
  int n;
  explicit Fd(int value) : n(value) {
    if (n < 0) Fail(std::strerror(errno));
  }
  ~Fd() {
    if (n >= 0) close(n);
  }
};
struct Hash {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  Hash() {
    if (!ctx || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
      EVP_MD_CTX_free(ctx);
      Fail("SHA256 init");
    }
  }
  ~Hash() { EVP_MD_CTX_free(ctx); }
  void Add(const void* p, size_t n) {
    if (EVP_DigestUpdate(ctx, p, n) != 1) Fail("SHA256 update");
  }
  std::string Finish() {
    unsigned char bytes[EVP_MAX_MD_SIZE];
    unsigned n = 0;
    if (EVP_DigestFinal_ex(ctx, bytes, &n) != 1 || n != 32)
      Fail("SHA256 final");
    std::string out;
    for (unsigned i = 0; i < n; ++i) {
      out += "0123456789abcdef"[bytes[i] >> 4];
      out += "0123456789abcdef"[bytes[i] & 15];
    }
    return out;
  }
};
std::string Number(uint64_t n) {
  std::string out(8, '\0');
  for (int i = 0; i < 8; ++i) {
    out[i] = char(n & 255);
    n >>= 8;
  }
  return out;
}
uint64_t Decode(const char* p) {
  uint64_t n = 0;
  for (int i = 7; i >= 0; --i) {
    unsigned char byte;
    std::memcpy(&byte, p + i, 1);
    n = (n << 8) | byte;
  }
  return n;
}
void Write(int fd, const std::string& s) {
  size_t done = 0;
  while (done < s.size()) {
    auto n = write(fd, s.data() + done, s.size() - done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) Fail("write failed");
    done += n;
  }
}
std::string Read(int fd, uint64_t offset, size_t size) {
  std::string out(size, '\0');
  size_t done = 0;
  while (done < size) {
    auto n = pread(fd, &out[done], size - done, offset + done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) Fail("short or failed read");
    done += n;
  }
  return out;
}
void Logical(Hash& h, const std::string& k, const std::string& v) {
  auto n = Number(k.size());
  h.Add(n.data(), n.size());
  h.Add(k.data(), k.size());
  n = Number(v.size());
  h.Add(n.data(), n.size());
  h.Add(v.data(), v.size());
}
class Capture final : public ycsbc::DB {
 public:
  Capture(int fd, Hash& hash, uint32_t size)
      : fd_(fd), hash_(hash), size_(size) {}
  Status Insert(const std::string& table, const std::string& key,
                std::vector<Field>& values) override {
    if (table != "usertable" || key.size() != kKey || values.size() != 1 ||
        values[0].name != "field0" || values[0].value.size() != size_)
      Fail("unexpected upstream record schema");
    Write(fd_, key);
    hash_.Add(key.data(), key.size());
    Write(fd_, values[0].value);
    hash_.Add(values[0].value.data(), values[0].value.size());
    return kOK;
  }
  Status Read(const std::string&, const std::string&,
              const std::vector<std::string>*, std::vector<Field>&) override {
    return kNotImplemented;
  }
  Status Scan(const std::string&, const std::string&, int,
              const std::vector<std::string>*,
              std::vector<std::vector<Field>>&) override {
    return kNotImplemented;
  }
  Status Update(const std::string&, const std::string&,
                std::vector<Field>&) override {
    return kNotImplemented;
  }
  Status Delete(const std::string&, const std::string&) override {
    return kNotImplemented;
  }

 private:
  int fd_;
  Hash& hash_;
  uint32_t size_;
};
}  // namespace
struct Dataset::Impl {
  Fd fd;
  Metadata meta;
  bool full = false;
  struct Entry {
    std::array<char, kKey> key;
    uint64_t ordinal;
  };
  std::vector<Entry> sorted;
  explicit Impl(const std::string& path)
      : fd(open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW)) {}
};
Dataset::Dataset(const std::string& path, Access access)
    : impl_(new Impl(path)) {
  auto& x = *impl_;
  struct stat st;
  if (fstat(x.fd.n, &st) || !S_ISREG(st.st_mode))
    Fail("fixture is not a regular file");
  auto head = Read(x.fd.n, 0, kHeader);
  if (head.compare(0, 8, kMagic, 8) != 0) Fail("unknown fixture format");
  x.meta.record_count = Decode(head.data() + 8);
  auto size = Decode(head.data() + 16);
  if (!x.meta.record_count || x.meta.record_count > INT_MAX || !size ||
      size > INT_MAX)
    Fail("invalid dimensions");
  x.meta.value_size = size;
  auto expected = kHeader + x.meta.record_count * (kKey + size) + kTrailer;
  if (uint64_t(st.st_size) != expected) Fail("fixture length mismatch");
  if (access == Access::kSamplesOnly) return;
  x.full = true;
  Hash body, whole;
  body.Add(head.data(), head.size());
  whole.Add(head.data(), head.size());
  x.sorted.reserve(x.meta.record_count);
  for (uint64_t i = 0; i < x.meta.record_count; ++i) {
    auto record = ReadRecord(i);
    if (record.first.compare(0, 4, "user") ||
        record.first.find_first_not_of("0123456789", 4) != std::string::npos)
      Fail("invalid key encoding");
    Impl::Entry e;
    std::copy(record.first.begin(), record.first.end(), e.key.begin());
    e.ordinal = i;
    x.sorted.push_back(e);
    for (const std::string* part : {&record.first, &record.second}) {
      const auto& s = *part;
      body.Add(s.data(), s.size());
      whole.Add(s.data(), s.size());
    }
  }
  auto trailer = Read(x.fd.n, expected - kTrailer, kTrailer);
  if (body.Finish() != trailer) Fail("fixture SHA256 mismatch");
  whole.Add(trailer.data(), trailer.size());
  x.meta.file_sha256 = whole.Finish();
  std::sort(x.sorted.begin(), x.sorted.end(),
            [](const auto& a, const auto& b) { return a.key < b.key; });
  Hash logical;
  for (size_t i = 0; i < x.sorted.size(); ++i) {
    if (i && x.sorted[i - 1].key == x.sorted[i].key) Fail("duplicate key");
    auto r = ReadRecord(x.sorted[i].ordinal);
    Logical(logical, r.first, r.second);
  }
  x.meta.logical_sha256 = logical.Finish();
}
Dataset::~Dataset() = default;
const Metadata& Dataset::metadata() const { return impl_->meta; }
std::pair<std::string, std::string> Dataset::ReadRecord(
    uint64_t ordinal) const {
  if (ordinal >= impl_->meta.record_count) Fail("record ordinal out of range");
  auto offset = kHeader + ordinal * (kKey + impl_->meta.value_size);
  return {Read(impl_->fd.n, offset, kKey),
          Read(impl_->fd.n, offset + kKey, impl_->meta.value_size)};
}
void Dataset::Replay(
    const std::function<void(const std::string&, const std::string&)>& apply)
    const {
  if (!impl_->full) Fail("Replay requires full fixture validation");
  for (uint64_t i = 0; i < impl_->meta.record_count; ++i) {
    auto r = ReadRecord(i);
    apply(r.first, r.second);
  }
}
Metadata Dataset::Verify(ROCKSDB_NAMESPACE::Iterator& it) const {
  if (!impl_->full) Fail("Verify requires full fixture validation");
  Hash hash;
  it.SeekToFirst();
  for (const auto& e : impl_->sorted) {
    if (!it.Valid()) {
      auto s = it.status();
      if (!s.ok()) Fail(s.ToString());
      Fail("missing key at ordinal " + std::to_string(e.ordinal));
    }
    auto r = ReadRecord(e.ordinal);
    auto key = it.key().ToString();
    if (key != r.first)
      Fail((key < r.first ? "unexpected key: " : "missing key: ") + r.first);
    auto value = it.value().ToString();
    if (value != r.second) Fail("value mismatch: " + r.first);
    Logical(hash, key, value);
    it.Next();
  }
  auto s = it.status();
  if (!s.ok()) Fail(s.ToString());
  if (it.Valid()) Fail("extra key: " + it.key().ToString());
  auto out = impl_->meta;
  out.logical_sha256 = hash.Finish();
  if (out.logical_sha256 != impl_->meta.logical_sha256)
    Fail("logical digest mismatch");
  return out;
}
void SetDatasetProperties(ycsbc::utils::Properties& props, uint64_t count,
                          uint32_t size) {
  for (const auto& p : std::vector<std::pair<std::string, std::string>>{
           {"recordcount", std::to_string(count)},
           {"fieldcount", "1"},
           {"fieldlength", std::to_string(size)},
           {"field_len_dist", "constant"},
           {"insertorder", "hashed"},
           {"insertstart", "0"},
           {"keyprefix", "user"},
           {"zeropadding", "20"},
           {"table", "usertable"},
           {"fieldnameprefix", "field"}})
    props.SetProperty(p.first, p.second);
}
void GenerateDataset(const std::string& path, uint64_t count, uint32_t size) {
  if (!count || count > INT_MAX || !size || size > INT_MAX)
    Fail("dimensions must fit positive int");
  ycsbc::utils::Properties props;
  SetDatasetProperties(props, count, size);
  props.SetProperty("operationcount", "0");
  ycsbc::CoreWorkload workload;
  workload.Init(props);
  Fd fd(open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
  try {
    Hash hash;
    std::string head(kMagic, 8);
    head += Number(count);
    head += Number(size);
    Write(fd.n, head);
    hash.Add(head.data(), head.size());
    Capture capture(fd.n, hash, size);
    for (uint64_t i = 0; i < count; ++i)
      if (!workload.DoInsert(capture)) Fail("upstream insertion failed");
    Write(fd.n, hash.Finish());
    if (fsync(fd.n)) Fail("fixture fsync failed");
  } catch (...) {
    unlink(path.c_str());
    throw;
  }
}
}  // namespace mb_ycsb
