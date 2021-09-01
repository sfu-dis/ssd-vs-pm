// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define GLOG_NO_ABBREVIATED_SEVERITIES

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <vector>
#include <string>
#include <iomanip>
#include <ios>
#include "allocator_internal.h"
#include "environment_linux.h"
#include "auto_ptr.h"
#include "macros.h"

namespace pmwcas {

LinuxEnvironment::LinuxEnvironment()
{}

uint64_t LinuxEnvironment::NowMicros() {
  struct timespec ts;
  memset(&ts, 0, sizeof(ts));
  clock_gettime(CLOCK_REALTIME, &ts);
  uint64_t ns = ts.tv_sec * 1000000000LL + ts.tv_nsec;
  return ns / 1000 + (ns % 1000 >= 500);
}

uint64_t LinuxEnvironment::NowNanos() {
  struct timespec ts;
  memset(&ts, 0, sizeof(ts));
  clock_gettime(CLOCK_REALTIME, &ts);
  uint64_t ns = ts.tv_sec * 1000000000LL + ts.tv_nsec;
  return ns / 1000 + (ns % 1000 >= 500);
}

/// Returns the core count on the test machine
uint32_t LinuxEnvironment::GetCoreCount() {
  return std::thread::hardware_concurrency();
}

void LinuxEnvironment::Sleep(uint32_t ms_to_sleep) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms_to_sleep));
}

Status LinuxEnvironment::NewRandomReadWriteAsyncFile(const std::string& filename,
    const FileOptions& options, ThreadPool* threadpool,
    RandomReadWriteAsyncFile** file, bool* exists) {
  return Status::NotSupported("Not implemented");
}

Status LinuxEnvironment::NewThreadPool(uint32_t max_threads,
    ThreadPool** pool) {
  return Status::NotSupported("Not implemented");
}

Status LinuxEnvironment::GetWorkingDirectory(std::string& directory) {
  char cwd[4096];
  memset(cwd, 0, 4096);
  char *c = getcwd(cwd, sizeof(cwd));
  directory = std::string(cwd);
  return Status::OK();
}

Status LinuxEnvironment::GetExecutableDirectory(std::string& directory) {
  // FIXME(tzwang): assuming same as working dir
  GetWorkingDirectory(directory);
  return Status::OK();
}

Status LinuxEnvironment::SetThreadAffinity(uint64_t core,
    AffinityPattern affinity_pattern) {
  pthread_t thread_handle = pthread_self();
  return SetThreadAffinity(thread_handle, core, affinity_pattern);
}

Status LinuxEnvironment::SetThreadAffinity(pthread_t thread, uint64_t core,
    AffinityPattern affinity_pattern) {
  if(affinity_pattern == AffinityPattern::BalanceNumaNodes) {
    // For now, assume that we're running on a 4-node NUMA system, where the
    // cores are numbered with the first n cores on node 0, the next n cores on
    // node 1, ... and the last n cores on node 3.
    const uint32_t numa_node_count = 4;
    CHECK_EQ(GetCoreCount() % numa_node_count, 0) <<
        "Unexpected system configuration!";
    uint32_t logical_core_count = GetCoreCount() / numa_node_count;
    // Assume 2 logical cores per physical core.
    CHECK_EQ(logical_core_count % 2, 0) << "Unexpected system configuration!";
    uint32_t physical_core_count = logical_core_count / 2;

    uint32_t numa_node = core % numa_node_count;
    uint32_t numa_core = core / numa_node_count;

    if(numa_core < physical_core_count) {
      numa_core = numa_core * 2;
    } else {
      numa_core = (numa_core - physical_core_count) * 2 + 1;
    }
    core = (numa_node * logical_core_count) + numa_core;
  } else if(affinity_pattern == 4) {
    // crossfire
    switch(core) {
      case 0: core = 0; break;
      case 1: core = 4; break;
      case 2: core = 8; break;
      case 3: core = 12; break;
      case 4: core = 16; break;
      case 5: core = 20; break;
      case 6: core = 1; break;
      case 7: core = 5; break;
      case 8: core = 9; break;
      case 9: core = 13; break;
      case 10: core = 17; break;
      case 11: core = 21; break;
      case 12: core = 2; break;
      case 13: core = 6; break;
      case 14: core = 10; break;
      case 15: core = 14; break;
      case 16: core = 18; break;
      case 17: core = 22; break;
      case 18: core = 3; break;
      case 19: core = 7; break;
      case 20: core = 11; break;
      case 21: core = 15; break;
      case 22: core = 19; break;
      case 23: core = 23; break;
      defautl: RAW_CHECK(false, "wrong core"); break;
    }
  } else if(affinity_pattern == 5) {
    // spread on c153
    uint64_t orig_core = core;
    if(core >= 32) {
      core -= 32;
    }
    uint32_t nodes = 4;
    uint32_t total_cores = 32;
    uint32_t cores_per_node = 8;
    uint32_t pcore = (core % nodes) * cores_per_node + core / nodes;
    if(orig_core >= 32) {
      pcore += 32;
    }
    LOG(INFO) << "Core " << orig_core << " -> " << pcore;
    core = pcore;
  } else {
    // Assuming 0-n are all the physical cores
  }

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int result = pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);

  if(result != 0) {
    return Status::Aborted("Failed to set thread affinity.", strerror(errno));
  } else {
    return Status::OK();
  }

  return Status::OK();
}

} // namespace pmwcas
