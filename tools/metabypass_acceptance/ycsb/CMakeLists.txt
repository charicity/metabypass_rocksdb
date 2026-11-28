# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

cmake_minimum_required(VERSION 3.18)
project(metabypass_ycsb LANGUAGES C CXX ASM)

# The experiment controller uses Linux process and cgroup interfaces.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "The MetaBypass YCSB experiment requires Linux")
endif()

include(FetchContent)
find_package(Git REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
find_package(Threads REQUIRED)
find_program(PATCH_EXECUTABLE patch REQUIRED)

set(YCSB_COMMIT 6c94fdb52350de66321ef5a8b529cf1b685e2338)
FetchContent_Declare(ycsb_cpp
  GIT_REPOSITORY https://github.com/ls4154/YCSB-cpp.git
  GIT_TAG ${YCSB_COMMIT}
  GIT_SUBMODULES ""
  # Build just the workload sources below.
  SOURCE_SUBDIR core)
FetchContent_MakeAvailable(ycsb_cpp)
execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${ycsb_cpp_SOURCE_DIR}" rev-parse HEAD
  OUTPUT_VARIABLE ycsb_checkout OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY)
execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${ycsb_cpp_SOURCE_DIR}" status --porcelain --untracked-files=no
  OUTPUT_VARIABLE ycsb_changes OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY)
if(NOT ycsb_checkout STREQUAL YCSB_COMMIT OR ycsb_changes)
  message(FATAL_ERROR "Use a clean YCSB-cpp checkout at ${YCSB_COMMIT}")
endif()

set(latest_patch "${CMAKE_CURRENT_SOURCE_DIR}/latest-generator.patch")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${latest_patch}")
file(SHA256 "${latest_patch}" ycsb_patch_sha)
set(ycsb_source "${CMAKE_CURRENT_BINARY_DIR}/ycsb-source")
file(REMOVE_RECURSE "${ycsb_source}")
file(COPY "${ycsb_cpp_SOURCE_DIR}/core" "${ycsb_cpp_SOURCE_DIR}/utils"
  DESTINATION "${ycsb_source}")
execute_process(COMMAND "${PATCH_EXECUTABLE}" -p1 --batch --fuzz=0 -i "${latest_patch}"
  WORKING_DIRECTORY "${ycsb_source}" COMMAND_ERROR_IS_FATAL ANY)

get_filename_component(default_core "../../.." ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(ROCKSDB_SOURCE_DIR "${default_core}" CACHE PATH "RocksDB core checkout or source archive")
set(ROCKSDB_LIBRARY "" CACHE FILEPATH "Optional matching prebuilt strict-Status shared library")
set(ROCKSDB_CORE_COMMIT "" CACHE STRING "Full core commit for a source archive without .git")
if(EXISTS "${ROCKSDB_SOURCE_DIR}/.git")
  execute_process(COMMAND "${GIT_EXECUTABLE}" -C "${ROCKSDB_SOURCE_DIR}" rev-parse HEAD
    OUTPUT_VARIABLE core_commit OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
  if(ROCKSDB_CORE_COMMIT AND NOT core_commit STREQUAL ROCKSDB_CORE_COMMIT)
    message(FATAL_ERROR "ROCKSDB_CORE_COMMIT differs from the checkout")
  endif()
else()
  set(core_commit "${ROCKSDB_CORE_COMMIT}")
endif()
string(LENGTH "${core_commit}" core_commit_length)
if(NOT core_commit_length EQUAL 40 OR NOT core_commit MATCHES "^[0-9a-f]+$")
  message(FATAL_ERROR "A full RocksDB core commit is required")
endif()

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
if(ROCKSDB_LIBRARY)
  add_library(ycsb_rocksdb SHARED IMPORTED)
  set_target_properties(ycsb_rocksdb PROPERTIES IMPORTED_LOCATION "${ROCKSDB_LIBRARY}")
  file(SHA256 "${ROCKSDB_LIBRARY}" core_library_sha)
  set(core_target ycsb_rocksdb)
else()
  set(WITH_TESTS OFF CACHE BOOL "Build RocksDB tests")
  set(WITH_TOOLS OFF CACHE BOOL "Build RocksDB tools")
  set(WITH_CORE_TOOLS OFF CACHE BOOL "Build RocksDB core tools")
  set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "Build RocksDB benchmarks")
  set(WITH_TRACE_TOOLS OFF CACHE BOOL "Build RocksDB trace tools")
  set(WITH_GFLAGS OFF CACHE BOOL "Build RocksDB with gflags")
  set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "Build shared RocksDB")
  set(ASSERT_STATUS_CHECKED ON CACHE BOOL "Check RocksDB Status handling")
  set(PORTABLE ON CACHE STRING "Use the baseline CPU architecture")
  set(USE_RTTI OFF CACHE STRING "Enable RTTI")
  if(NOT ASSERT_STATUS_CHECKED)
    message(FATAL_ERROR "YCSB experiments require a strict-Status core")
  endif()
  add_subdirectory("${ROCKSDB_SOURCE_DIR}" rocksdb EXCLUDE_FROM_ALL)
  set(core_target rocksdb${ARTIFACT_SUFFIX})
endif()

add_library(mb_ycsb_dataset STATIC
  dataset.cc
  "${ycsb_source}/core/acknowledged_counter_generator.cc"
  "${ycsb_source}/core/core_workload.cc")
target_compile_features(mb_ycsb_dataset PUBLIC cxx_std_20)
target_compile_options(mb_ycsb_dataset PUBLIC -fno-rtti)
target_compile_definitions(mb_ycsb_dataset PUBLIC ROCKSDB_ASSERT_STATUS_CHECKED)
target_include_directories(mb_ycsb_dataset PUBLIC
  "${ROCKSDB_SOURCE_DIR}/include" "${ROCKSDB_SOURCE_DIR}" "${ycsb_source}")
target_link_libraries(mb_ycsb_dataset PUBLIC
  ${core_target} OpenSSL::Crypto Threads::Threads)

add_executable(metabypass_ycsb driver.cc bepp.cc wal_operation_mirror.cc)
target_link_libraries(metabypass_ycsb PRIVATE mb_ycsb_dataset)
set(adapter_sources driver.cc dataset.h dataset.cc bepp.h bepp.cc
  wal_operation_mirror.h wal_operation_mirror.cc)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${adapter_sources})
foreach(source IN LISTS adapter_sources)
  file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/${source}" source_sha)
  string(APPEND adapter_hashes "${source_sha}")
endforeach()
string(SHA256 adapter_sha "${adapter_hashes}")
target_compile_definitions(metabypass_ycsb PRIVATE
  MB_YCSB_CORE_REVISION="${core_commit}"
  MB_YCSB_CORE_LIBRARY_SHA256="${core_library_sha}"
  MB_YCSB_UPSTREAM_REVISION="${YCSB_COMMIT}"
  MB_YCSB_PATCH_SHA256="${ycsb_patch_sha}"
  MB_YCSB_ADAPTER_SHA256="${adapter_sha}")

include(CTest)
if(BUILD_TESTING)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_executable(mb_ycsb_dataset_test dataset_test.cc)
  target_link_libraries(mb_ycsb_dataset_test PRIVATE mb_ycsb_dataset)
  foreach(case valid missing extra value corrupt)
    add_test(NAME "dataset_${case}" COMMAND mb_ycsb_dataset_test "${case}")
    set_tests_properties("dataset_${case}" PROPERTIES TIMEOUT 60)
  endforeach()
  foreach(case matching_initial_contents_across_cores unverified_contents
      independent_fixture_rejected workload_and_observation_must_match
      sequence_bounds sst_async_options checkpoint_timing_mode_must_match
      checkpoint_timing_intervals checkpoint_timing_p99_minimum_and_truncated_trace
      checkpoint_timing_capture_intervals checkpoint_timing_rejects_invalid_capture)
    add_test(NAME "report_${case}" COMMAND "${Python3_EXECUTABLE}" -B
      "${CMAKE_CURRENT_SOURCE_DIR}/run_test.py" "ReportTest.test_${case}")
    set_tests_properties("report_${case}" PROPERTIES TIMEOUT 60)
  endforeach()
  foreach(case same_number_move dump_and_inventory_lengths transient_files confirmations_and_phase
      measurement_end_confirms_point_and_earliest_snapshot_wins
      measurement_counter_deltas_and_sampled_queue_peak
      workloadd_capture_forwards_cgroup_and_archives_d_logs
      offline_summary_reads_d_and_legacy_a_archives invalid_recovery_boundary)
    add_test(NAME "sst_study_${case}" COMMAND "${Python3_EXECUTABLE}" -B
      "${CMAKE_CURRENT_SOURCE_DIR}/sst_checkpoint_study_test.py" "CheckpointStudyTest.test_${case}")
    set_tests_properties("sst_study_${case}" PROPERTIES TIMEOUT 60)
  endforeach()
endif()
