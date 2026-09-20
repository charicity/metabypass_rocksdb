// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
static int (*ml)(pthread_mutex_t*);
static int (*cb)(pthread_cond_t*);
static ssize_t (*wr)(int, const void*, size_t);
static ssize_t (*pw)(int, const void*, size_t, off_t);
static ssize_t (*pr)(int, void*, size_t, off_t);
static __thread int active;
struct row {
  void* site;
  uint64_t ns, count;
  int kind;
};
static __thread struct row rows[2048];
static uint64_t now(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1000000000ULL + t.tv_nsec;
}
static void add(void* site, int kind, uint64_t ns) {
  unsigned i = (((uintptr_t)site >> 3) + kind * 137) % 2048;
  while (rows[i].site && (rows[i].site != site || rows[i].kind != kind))
    i = (i + 1) % 2048;
  rows[i].site = site;
  rows[i].kind = kind;
  rows[i].ns += ns;
  rows[i].count++;
}
__attribute__((constructor)) static void init(void) {
  ml = dlsym(RTLD_NEXT, "pthread_mutex_lock");
  cb = dlvsym(RTLD_NEXT, "pthread_cond_broadcast", "GLIBC_2.3.2");
  wr = dlsym(RTLD_NEXT, "write");
  pw = dlsym(RTLD_NEXT, "pwrite");
  pr = dlsym(RTLD_NEXT, "pread");
}
void mb_phase(int on) {
  active = on;
  if (!on) {
    for (int i = 0; i < 2048; i++)
      if (rows[i].site) {
        Dl_info d;
        dladdr(rows[i].site, &d);
        fprintf(stderr, "PROFILE kind=%d site=%p offset=%lx ns=%lu count=%lu\n",
                rows[i].kind, rows[i].site,
                (uintptr_t)rows[i].site - (uintptr_t)d.dli_fbase, rows[i].ns,
                rows[i].count);
      }
  }
}
int pthread_mutex_lock(pthread_mutex_t* p) {
  if (!ml) ml = dlsym(RTLD_NEXT, "pthread_mutex_lock");
  if (!active) return ml(p);
  uint64_t t = now();
  int r = ml(p);
  add(__builtin_return_address(0), 0, now() - t);
  return r;
}
int pthread_cond_broadcast(pthread_cond_t* p) {
  if (!cb) cb = dlvsym(RTLD_NEXT, "pthread_cond_broadcast", "GLIBC_2.3.2");
  if (!active) return cb(p);
  uint64_t t = now();
  int r = cb(p);
  add(__builtin_return_address(0), 1, now() - t);
  return r;
}
ssize_t write(int f, const void* b, size_t n) {
  if (!wr) wr = dlsym(RTLD_NEXT, "write");
  if (!active) return wr(f, b, n);
  uint64_t t = now();
  ssize_t r = wr(f, b, n);
  add(__builtin_return_address(0), 2, now() - t);
  return r;
}
ssize_t pwrite(int f, const void* b, size_t n, off_t o) {
  if (!pw) pw = dlsym(RTLD_NEXT, "pwrite");
  if (!active) return pw(f, b, n, o);
  uint64_t t = now();
  ssize_t r = pw(f, b, n, o);
  add(__builtin_return_address(0), 3, now() - t);
  return r;
}
ssize_t pread(int f, void* b, size_t n, off_t o) {
  if (!pr) pr = dlsym(RTLD_NEXT, "pread");
  if (!active) return pr(f, b, n, o);
  uint64_t t = now();
  ssize_t r = pr(f, b, n, o);
  add(__builtin_return_address(0), 4, now() - t);
  return r;
}
