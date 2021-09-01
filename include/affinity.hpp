#ifndef AFFINITY_HPP
#define AFFINITY_HPP

#include <glog/logging.h>
#include <pthread.h>
#include <string.h>
#include <numa.h>

// TODO: add support for NUMA at runtime
class AffinityManager {
 private:
  const int stride{2};
  const int offset{0};

 public:
  AffinityManager() = default;
  AffinityManager(const AffinityManager &) = default;
  AffinityManager(int stride, int offset) : stride{stride}, offset{offset} {}

  // Prevent unintentional copies
  AffinityManager &operator=(const AffinityManager &) = delete;
  AffinityManager &operator=(AffinityManager &&) = delete;

  void setAffinity(int my_thread_num) const {
    char err_buf[64]{0};
    auto assigned_cpu = getAssignedCpu(my_thread_num);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(assigned_cpu, &cpuset);
    auto ret =
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    cpu_set_t actual;
    CPU_ZERO(&actual);
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &actual);
    LOG_IF(WARNING, !CPU_ISSET(assigned_cpu, &actual))
        << "Could not set assigned CPU #" << assigned_cpu << " for thread#"
        << my_thread_num;
    LOG_IF(WARNING, CPU_COUNT(&actual) != 1)
        << "Multiple CPUs were found on thread#" << my_thread_num;
    CHECK(ret == 0) << "Error setting affinity for thread #" << my_thread_num
                    << " to CPU #" << assigned_cpu << "("
                    << strerror_r(ret, err_buf, sizeof(err_buf)) << ")";
  }

 private:
  int getAssignedCpu(int thread_num) const noexcept {
    auto assigned_cpu = thread_num * stride + offset;
    if (assigned_cpu >= numa_num_configured_cpus()) {
      if (offset == 0) {
        assigned_cpu = assigned_cpu - numa_num_configured_cpus() + 1; 
      } else {
        assigned_cpu = assigned_cpu - numa_num_configured_cpus() - 1; 
      }
    }
    return assigned_cpu;
  }
};

#endif  // AFFINITY_HPP
