# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).
cmake_minimum_required(VERSION 3.16)
project(metabypass_quick LANGUAGES C CXX)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "This standalone research experiment requires Linux")
endif()
if(NOT ROCKSDB_SOURCE)
  message(FATAL_ERROR "Set ROCKSDB_SOURCE to the library source directory")
endif()
option(MB_UPSTREAM "Build the ordinary RocksDB adapter" OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
foreach(flag WITH_TESTS WITH_BENCHMARK_TOOLS WITH_CORE_TOOLS WITH_TOOLS
    WITH_TRACE_TOOLS WITH_EXAMPLES WITH_GFLAGS WITH_LIBURING WITH_JEMALLOC
    WITH_SNAPPY WITH_LZ4 WITH_ZLIB WITH_ZSTD WITH_BZ2 WITH_NUMA)
  set(${flag} OFF CACHE BOOL "" FORCE)
endforeach()
set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
add_subdirectory("${ROCKSDB_SOURCE}" rocksdb)
add_executable(mb_quick driver.cc)
target_include_directories(mb_quick PRIVATE "${ROCKSDB_SOURCE}" "${ROCKSDB_SOURCE}/include")
target_compile_options(mb_quick PRIVATE -Wall -Wextra -fno-rtti)
get_directory_property(ROCKSDB_DEFINITIONS DIRECTORY "${ROCKSDB_SOURCE}"
  COMPILE_DEFINITIONS)
target_compile_definitions(mb_quick PRIVATE ${ROCKSDB_DEFINITIONS})
target_link_libraries(mb_quick PRIVATE rocksdb)
if(MB_UPSTREAM)
  target_compile_definitions(mb_quick PRIVATE MB_UPSTREAM)
endif()
