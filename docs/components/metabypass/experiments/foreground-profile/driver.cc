// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include <dlfcn.h>
#include <sys/resource.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>

#include "rocksdb/utilities/metabypass.h"
using namespace rocksdb;
static long us(timeval t) { return t.tv_sec * 1000000 + t.tv_usec; }
int main(int argc, char** argv) {
  if (argc < 3) return 2;
  Options o;
  o.create_if_missing = true;
  o.allow_concurrent_memtable_write = false;
  o.enable_blob_files = true;
  o.enable_blob_direct_write = true;
  o.min_blob_size = 0;
  o.compression = kNoCompression;
  MetaBypassOptions b;
  std::string root = argv[1];
  b.data_dir = root + "/data";
  b.backup_dir = root + "/backup";
  if (argc > 3) b.batch_bytes = std::stoull(argv[3]);
  std::unique_ptr<MetaBypassDB> db;
  auto s = MetaBypassDB::Open(o, b, root + "/index", &db);
  if (!s.ok()) {
    puts(s.ToString().c_str());
    return 1;
  }
  auto phase = (void (*)(int))dlsym(RTLD_DEFAULT, "mb_phase");
  if (phase) phase(1);
  rusage a{}, z{};
  getrusage(RUSAGE_THREAD, &a);
  auto start = std::chrono::steady_clock::now();
  std::string value(1024, 'v');
  for (int i = 0; s.ok() && i < std::atoi(argv[2]); ++i)
    s = db->Put(WriteOptions(), std::to_string(i), value);
  auto end = std::chrono::steady_clock::now();
  getrusage(RUSAGE_THREAD, &z);
  if (phase) phase(0);
  printf(
      "foreground_us=%ld user_us=%ld system_us=%ld voluntary=%ld "
      "involuntary=%ld\n",
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count(),
      us(z.ru_utime) - us(a.ru_utime), us(z.ru_stime) - us(a.ru_stime),
      z.ru_nvcsw - a.ru_nvcsw, z.ru_nivcsw - a.ru_nivcsw);
  fflush(stdout);
  if (s.ok()) s = db->SyncBackup();
  if (s.ok()) s = db->Close();
  printf("status=%s\n", s.ToString().c_str());
  return s.ok() ? 0 : 1;
}
