# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

cmake_minimum_required(VERSION 3.12)
project(metabypass_acceptance LANGUAGES C CXX ASM)

option(ROCKSDB_MB_WAL_BREAKDOWN "Build foreground and background WAL probes" OFF)
set(WITH_TESTS OFF CACHE BOOL "Build RocksDB tests")
set(WITH_TOOLS OFF CACHE BOOL "Build RocksDB tools")
set(WITH_CORE_TOOLS OFF CACHE BOOL "Build RocksDB core tools")
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "Build RocksDB benchmarks")
set(WITH_TRACE_TOOLS OFF CACHE BOOL "Build RocksDB trace tools")
set(WITH_GFLAGS OFF CACHE BOOL "Build RocksDB with gflags")
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "Build shared RocksDB")
set(ASSERT_STATUS_CHECKED ON CACHE BOOL "Check RocksDB Status handling")

if(ASSERT_STATUS_CHECKED)
  add_compile_definitions(ROCKSDB_ASSERT_STATUS_CHECKED)
endif()
if(ROCKSDB_MB_WAL_BREAKDOWN)
  add_compile_definitions(ROCKSDB_MB_WAL_BREAKDOWN)
endif()

get_filename_component(ROCKSDB_SOURCE_DIR "../.." ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
add_subdirectory("${ROCKSDB_SOURCE_DIR}" rocksdb EXCLUDE_FROM_ALL)
add_executable(metabypass_acceptance driver.cc)
target_compile_features(metabypass_acceptance PRIVATE cxx_std_20)
if(MSVC)
  target_compile_options(metabypass_acceptance PRIVATE /GR-)
else()
  target_compile_options(metabypass_acceptance PRIVATE -fno-rtti)
endif()
target_include_directories(metabypass_acceptance PRIVATE "${ROCKSDB_SOURCE_DIR}")
target_link_libraries(metabypass_acceptance PRIVATE rocksdb${ARTIFACT_SUFFIX})
