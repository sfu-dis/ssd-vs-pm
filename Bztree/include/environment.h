// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "status.h"
#include "slice.h"
#include "async.h"
#include "allocator.h"
#include "auto_ptr.h"
#include "allocator_internal.h"

namespace pmwcas {

/// Thread affinity used by the benchmark driver:
///   * OSScheduled: let the OS schedule the threads.
///   * PhysicalCoresFirst: schedule 1 thread per physical core first, then use
////    hyperthread cores.
///   * LogicalCoresFirst: schedule 1 thread per logical core (including
///     hyperthread cores) first.
///   * BalanceNumaNodes: spread threads evenly across all NUMA nodes; within
///     each NUMA node, schedule physical cores first.
enum AffinityPattern : int {
  OSScheduled = 0,
  PhysicalCoresFirst = 1,
  LogicalCoresFirst = 2,
  BalanceNumaNodes = 3
};

/// Interface for file wrapper on the target OS.
class File {
 public:
  virtual uint64_t GetFileIdentifier() const = 0;
};

/// Interface to handle async I/O on the target OS. Used to schedule reads and
/// writes against a file.
class AsyncIOHandler {
 public:
  typedef void(*AsyncCallback)(IAsyncContext* context, Status result,
      size_t bytes_transferred);

  virtual Status ScheduleRead(uint8_t* buffer, size_t offset, uint32_t length,
      AsyncIOHandler::AsyncCallback callback, IAsyncContext* context) = 0;

  virtual Status ScheduleWrite(uint8_t* buffer, size_t offset, uint32_t length,
      AsyncIOHandler::AsyncCallback callback, IAsyncContext* context) = 0;
};

/// Schedling priority for threads in the ThreadPool class.
enum class ThreadPoolPriority : uint8_t {
  Low = 0,
  Medium,
  High,

  Last
};

/// Interface to abstract away environment/platform specific threadpool
/// implementations. Used for tasks like performaing asyncronous IO,
/// continuation of async operations, and scheduling tasks.
class ThreadPool {
 public:
  /// Type of functions that can be scheduled for asynchronous work via
  /// ScheduleTask();
  typedef Status(*Task)(void* arguments);

  virtual Status Schedule(ThreadPoolPriority priority, Task task,
                          void* task_argument) = 0;

  virtual Status ScheduleTimer(ThreadPoolPriority priority, Task task,
      void* task_argument, uint32_t ms_period, void** timer_handle) = 0;

  virtual Status CreateAsyncIOHandler(ThreadPoolPriority priority,
      const File& file, unique_ptr_t<AsyncIOHandler>& async_io) = 0;
};

/// Options for opening a file. Keep these as generic and OS agnostic as
/// possible.
struct FileOptions {
 public:
  FileOptions()
    : async{ false }
    , direct_io{ false }
    , truncate_if_exists{ false } {
  }

  bool async;
  bool direct_io;
  bool truncate_if_exists;
};

/// Interface for implementing a async file capable of random read/write IOs.
class RandomReadWriteAsyncFile : public File {
 public:
  RandomReadWriteAsyncFile() {}

  virtual ~RandomReadWriteAsyncFile();

  virtual bool DirectIO() = 0;

  virtual size_t GetAlignment() = 0;

  virtual Status Open(const std::string& filename, const FileOptions& options,
                      ThreadPool* threadpool) = 0;

  virtual Status Close() = 0;

  virtual Status Delete() = 0;

  typedef void (*AsyncCallback)(IAsyncContext* context, Status result,
      size_t bytes_transferred);

  virtual Status Read(size_t offset, uint32_t length, uint8_t* buffer,
      const IAsyncContext& context, AsyncCallback callback) = 0;

  virtual Status Write(size_t offset, uint32_t length, uint8_t* buffer,
      const IAsyncContext& context, AsyncCallback callback) = 0;

  unique_ptr_t<RandomReadWriteAsyncFile> make_unique_ptr_t(
    RandomReadWriteAsyncFile* p);
};

/// Interface for producing a shared memory segment on the target OS. Used for
/// mapping memory segment to NVRAM or simulated NVRAM memory.
class SharedMemorySegment {
 public:
  SharedMemorySegment() {}

  virtual ~SharedMemorySegment();

  virtual Status Initialize(const std::string& segname, uint64_t size,
      bool open_existing) = 0 ;

  virtual Status Attach(void* base_address = nullptr) = 0;

  virtual Status Detach() = 0;

  virtual void* GetMapAddress() = 0;
};

/// Abstract away the OS specific calls for the library. This keeps the PMwCAS
/// library OS agnostics and allows for cross/OS compilation.
class IEnvironment {
 public:
  IEnvironment() {}

  virtual ~IEnvironment() {};

  /// Returns the number of micro-seconds since some fixed point in time. Only
  /// useful for computing deltas of time.
  /// However, it is often used as system time such as in GenericRateLimiter
  /// and other places so a port needs to return system time in order to work.
  virtual uint64_t NowMicros() = 0;

  /// Returns the number of nano-seconds since some fixed point in time. Only
  /// useful for computing deltas of time in one run.
  /// Default implementation simply relies on NowMicros
  virtual uint64_t NowNanos() {
    return NowMicros() * 1000;
  }

  /// Return the unique id of the caller thread.
  uint64_t GetThreadId() {
    return pthread_self();
  }

  /// Return the number of cores (plus hyperthreads, if enabled). Return value
  /// of 0 implies error.
  virtual uint32_t GetCoreCount() = 0;

  /// Put the caller thread to sleep for /a ms_to_sleep microseconds.
  virtual void Sleep(uint32_t ms_to_sleep) = 0;

  /// Produce a new async ready/write file for the target OS.
  virtual Status NewRandomReadWriteAsyncFile(const std::string& filename,
      const FileOptions& options, ThreadPool* threadpool,
      RandomReadWriteAsyncFile** file, bool* exists = nullptr) = 0;


  /// Produce a new threadpool for the target OS.
  virtual Status NewThreadPool(uint32_t max_threads, ThreadPool** pool) = 0;

  /// Affinitize the active thread to the specified physical or logical core.
  virtual Status SetThreadAffinity(uint64_t core,
                                   AffinityPattern affinity_pattern) = 0;

  /// Return the working directory
  virtual Status GetWorkingDirectory(std::string& directory) = 0;

  /// Return the directory of where the executable resides
  virtual Status GetExecutableDirectory(std::string& directory) = 0;

};

} //namespace pmwcas

#include "environment_linux.h"
