//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <atomic>
#include <cstring>
#include <future>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "core/client.h"
#include "core/core_workload.h"
#include "core/db.h"
#include "core/timer.h"
#include "core/utils.h"
#include "db/db_factory.h"
#include "buffer_manager.h"
#include <buildinfo.h>
#include "../include/affinity.hpp"

using namespace std;

std::atomic<bool> shutdown(false);
std::atomic<uint64_t> shutdown_barrier(0);
std::atomic<uint64_t> start_barrier(0);

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void PrintInfo(utils::Properties &props);

struct ClientStats {
  uint64_t oks{};
  uint64_t inserts{};
  uint64_t reads{};
  std::vector<std::chrono::high_resolution_clock::time_point> latencies{};
  // reducing reserve can cause allocations during benchmark
  ClientStats(double latency_sample) {
    if (latency_sample > 0.0)  // 1 Billion samples / sample rate
      latencies.reserve((1024 * 1024 * 1024) * latency_sample);
  }
};

enum Phase {load, ramp, run};

ClientStats DelegateClient(int thread_id, ycsbc::Client *client, ycsbc::DB *db,
                           uint64_t num_ops, Phase phase,
                           double latency_sample,
                           const AffinityManager &aff_mgr) {
  aff_mgr.setAffinity(thread_id);
  utils::RandomBool random_bool{latency_sample};
  db->thread_init(thread_id);

  ClientStats stats{latency_sample};

  if (phase == load) {
    for (uint64_t i = 0; i < num_ops; ++i) {
      stats.oks += client->DoInsert();
    }
    LOG_IF(WARNING, stats.oks != num_ops) << stats.oks << " Ok != num_ops " << num_ops;
  } else if (phase == ramp) {
    --start_barrier;
    while (start_barrier);
    while (!shutdown.load(std::memory_order_relaxed)) {
      stats.oks += client->DoRead();
    }
    --shutdown_barrier;
  } else {
    --start_barrier;
    while (start_barrier);
    if(latency_sample <= 0.0)
      while (!shutdown.load(std::memory_order_relaxed))
        stats.oks += client->DoTransaction();
    else
      while (!shutdown.load(std::memory_order_relaxed)) {
        if (!random_bool()) {
          stats.oks += client->DoTransaction();
        } else {
          auto start = std::chrono::high_resolution_clock::now();
          stats.oks += client->DoTransaction();
          auto end = std::chrono::high_resolution_clock::now();
          stats.latencies.emplace_back(start); // defer saving latency number
          stats.latencies.emplace_back(end);
        }
      }
    --shutdown_barrier;
    LOG_IF(WARNING, stats.oks != client->GetOps())
        << stats.oks << " Oks != expected: " << client->GetOps()
        << " num failed: " << client->GetOps() - stats.oks
        << " tid: " << thread_id;
  }
  db->thread_deinit(thread_id);
  stats.inserts = client->GetInsert();
  stats.reads = client->GetRead();
  db->Close();
  return stats;
}

int main(const int argc, const char *argv[]) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  LOG(INFO) << "Compiler " << CMAKE_CXX_COMPILER;
  LOG(INFO) << "Build type " << CMAKE_BUILD_TYPE;
  LOG(INFO) << "Compile Definitions " << CMAKE_COMPILE_DEFINITIONS;
  LOG(INFO) << "Compile Flags " << CMAKE_CXX_FLAGS;
  #ifdef VERIFY_VALUE
  LOG(INFO) << "verify on";
  #endif
  utils::Properties props;
  props.SetProperty(ycsbc::CoreWorkload::RAMP_UP_PROPERTY, "0");
  props.SetProperty(ycsbc::CoreWorkload::BENCHMARK_SECONDS_PROPERTY, "20");
  ParseCommandLine(argc, argv, props);

  const bool load = utils::StrToBool(props.GetProperty("load", "false"));
  const bool ramp = stoull(props[ycsbc::CoreWorkload::RAMP_UP_PROPERTY]) > 0 ? true : false;
  const bool run = utils::StrToBool(props.GetProperty("run", "false"));
  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  double latency_sample = stod(props.GetProperty("latency_sample", "0.0"));
  if (latency_sample > 1.0) {
    latency_sample = latency_sample / 100.0;
  } else if (latency_sample < 0) {
    latency_sample = 0;
  }

  vector<ycsbc::DB *> connections;
  vector<future<ClientStats>> workers;
  vector<ycsbc::CoreWorkload> workloads;
  vector<ycsbc::CoreWorkload> ramp_workloads;

  utils::Timer<double> timer;

  int total_ops = 0;

  // Initialize a connection pool and workloads.
  if (props.GetProperty("tree") == "btree") {
    string path = props.GetProperty("path", "");
    if (path == "") {
      cerr << "\"-path\" is required. For example, \"-path /mnt/nvme0n1p1\"." << endl;
      exit(0);
    }

    auto insert_start = props.GetProperty("insertstart");
    for (int i = 0; i < num_threads; ++i) {
      props.SetProperty("btree_file", path + "/btree_" + std::to_string(i));
      props.SetProperty("thread_id", std::to_string(i));

      connections.emplace_back(ycsbc::DBFactory::CreateDB(props));

      props.SetProperty("insertstart", insert_start);
      workloads.emplace_back(props);

      props.SetProperty("insertstart", "0");
      ramp_workloads.emplace_back(props);
    }
  } else if (props.GetProperty("tree") == "hashtable") {
    string path = props.GetProperty("path", "");
    if (path == "") {
      cerr << "\"-path\" is required. For example, \"-path /mnt/nvme0n1p1\"." << endl;
      exit(0);
    }

    for (int i = 0; i < num_threads; ++i) {
      props.SetProperty("hashtable_file", path + "/hashtable_" + std::to_string(i));
      props.SetProperty("thread_id", std::to_string(i));

      connections.emplace_back(ycsbc::DBFactory::CreateDB(props));

      workloads.emplace_back(props);
    }
  } else if (props.GetProperty("tree") == "btree_rdev") {
    string path = props.GetProperty("path", "");
    if (path == "") {
      cerr << "\"-path\" is required. For example, \"-path /dev/nvme0n1\"." << endl;
      exit(0);
    }
    // Device size in bytes.
    const long device_size = stol(props.GetProperty("device_size", "0")) * 1024 * 1024 * 1024;
    if (device_size == 0) {
      cerr << "Invalid \"-device_size\". For example, \"-device_size 300\". Size is in Gigabytes." << endl;
      exit(0);
    }
    const long index_size = device_size / 3;
    for (int i = 0; i < num_threads; ++i) {
      props.SetProperty("btree_file", path);
      props.SetProperty("thread_id", std::to_string(i));

      long index_start = (index_size / num_threads) * i;
      long aligned_index_start = ((index_start + ALIGNMENT -1) / ALIGNMENT) * ALIGNMENT;

      long data_start = index_size + (index_size * 2 / num_threads) * i;
      long aligned_data_start = ((data_start + ALIGNMENT -1) / ALIGNMENT) * ALIGNMENT;

      cout << "thread: " << i << endl;
      cout << "index_start: " << index_start << endl;
      cout << "data_start: " << data_start << endl << endl;

      props.SetProperty("index_start", std::to_string(aligned_index_start));
      props.SetProperty("data_start", std::to_string(aligned_data_start));

      connections.emplace_back(ycsbc::DBFactory::CreateDB(props));

      workloads.emplace_back(props);
    }
  } else if (props.GetProperty("tree") == "pibench") {
    string path = props.GetProperty("path", "");
    if (path == "") {
      cerr << "\"-path\" is required. For example, \"-path /mnt/pmem0/darieni/pool\".\n";
      exit(0);
    }
    string wrapper = props.GetProperty("wrapper", "");
    if (wrapper == "") {
      cerr << "\"wrapper\" is required. For example, \"-wrapper "
              "/home/darieni/libfptree_wrapper.so\"."
           << endl;
      exit(0);
    }

    auto *db = ycsbc::DBFactory::CreateDB(props);
    for (int i = 0; i < num_threads; ++i) {
      connections.emplace_back(db);
      props.SetProperty("thread_id", std::to_string(i));
      workloads.emplace_back(props);
    }
  } else if (props.GetProperty("tree") == "dash" || props.GetProperty("tree") == "bztree") {
    string path = props.GetProperty("path", "");
    if (path == "") {
      cerr << "\"-path\" is required. For example, \"-path "
              "/mnt/pmem0/darieni/pool\".\n";
      exit(0);
    }
    auto *db = ycsbc::DBFactory::CreateDB(props);
    for (int i = 0; i < num_threads; ++i) {
      connections.emplace_back(db);
      props.SetProperty("thread_id", std::to_string(i));
      workloads.emplace_back(props);
    }
  } else {
    cerr << "Invalid option \"-tree\", choose from btree, btree_rdev, dash, "
            "and pibench.\n";
    exit(0);
  }

  PrintInfo(props);

  AffinityManager aff_mgr(stoi(props.GetProperty("stride", "2")),
                          stoi(props.GetProperty("starting_cpu", "0")));

  // Loads data
  if (load) {
    total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    timer.Start();

    std::vector<ycsbc::Client *> clients;

    // Start threads.
    for (int i = 0; i < num_threads; ++i) {
      // the call to async() synchronizes with the call to DelegateClient()
      ycsbc::Client *client = new ycsbc::Client(*connections[i], workloads[i]);
      clients.push_back(client);

      workers.emplace_back(async(launch::async, DelegateClient, i, client,
                                 connections[i], total_ops / num_threads, Phase::load,
                                 latency_sample, aff_mgr));
    }

    assert((int)workers.size() == num_threads);

    // Wait for threads to finish.
    uint64_t sum = 0;
    for (auto &n : workers) {
      assert(n.valid());
      sum += n.get().oks;  // future.get() is a blocking method
    }

    for (int i = 0; i < num_threads; ++i) {
      delete clients[i];
    }

    double use_time = timer.End();

    cout << "********** load result **********" << endl;
    cout << "loading records: " << sum << ", use time: " << use_time
         << " s, qps: " << sum / use_time << " ops/sec" << endl;
    cout << "*********************************" << endl;
  }

  if (run && ramp) {
    workers.clear();
    auto ramp_sec = stoull(props[ycsbc::CoreWorkload::RAMP_UP_PROPERTY]);

    shutdown_barrier = num_threads;
    start_barrier = num_threads;
    std::vector<ycsbc::Client *> clients;

    for (int i = 0; i < num_threads; ++i) {
      ycsbc::Client *client = new ycsbc::Client(*connections[i], ramp_workloads[i]);
      clients.push_back(client);

      workers.emplace_back(async(launch::async, DelegateClient, i, client,
                                 connections[i], 0, Phase::ramp, latency_sample,
                                 aff_mgr));
    }
    assert((int)workers.size() == num_threads);

    // Print some results every second
    uint64_t slept = 0;
    uint64_t last_ops = 0;

    auto gather_stats = [&]() {
      sleep(1); // XXX(darieni): is it safe to ignore this return value?
      uint64_t sec_ops = 0;
      for (int i = 0; i < num_threads; ++i) {
        sec_ops += clients[i]->GetStats();
      }
      sec_ops -= last_ops;
      last_ops += sec_ops;

      if (sec_ops > 0) {
        printf("%lu,%lu\n", slept + 1, sec_ops);
      }

      slept++;
    };

    printf("=== ramp-up ===\n");
    printf("Seconds,Operations\n");
    while (start_barrier);

    while (slept < ramp_sec) {
      gather_stats();
    }

    shutdown = true;
    while (shutdown_barrier.load(std::memory_order_acquire));

    for (int i = 0; i < num_threads; ++i) {
      delete clients[i];
    }
  }

  // Peforms transactions
  if (run) {
    workers.clear();
    shutdown = false;
    //total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    auto seconds = stoull(props[ycsbc::CoreWorkload::BENCHMARK_SECONDS_PROPERTY]);

    shutdown_barrier = num_threads;
    start_barrier = num_threads;
    std::vector<ycsbc::Client *> clients;

    for (int i = 0; i < num_threads; ++i) {
      ycsbc::Client *client = new ycsbc::Client(*connections[i], workloads[i]);
      clients.push_back(client);

      workers.emplace_back(async(launch::async, DelegateClient, i, client,
                                 connections[i], 0, Phase::run, latency_sample,
                                 aff_mgr));
    }
    assert((int)workers.size() == num_threads);

    // Print some results every second
    uint64_t slept = 0;
    uint64_t last_ops = 0;

    auto gather_stats = [&]() {
      sleep(1); // XXX(darieni): is it safe to ignore this return value?
      uint64_t sec_ops = 0;
      for (int i = 0; i < num_threads; ++i) {
        sec_ops += clients[i]->GetStats();
      }
      sec_ops -= last_ops;
      last_ops += sec_ops;

      if (sec_ops > 0) {
        printf("%lu,%lu\n", slept + 1, sec_ops);
      }

      slept++;
    };

    printf("=== run ===\n");
    printf("Seconds,Operations\n");
    while (start_barrier);

    timer.Start();

    while (slept < seconds) {
      gather_stats();
    }

    shutdown = true;
    while (shutdown_barrier.load(std::memory_order_acquire));
    double duration = timer.End();

    std::vector<double> global_latencies{};
    global_latencies.reserve(1024*1024);
    uint64_t total_ops = 0;
    for (auto &f : workers) {
      auto stats = f.get();
      total_ops += stats.inserts + stats.reads;
      for (unsigned int i = 0; i < stats.latencies.size(); i = i + 2) {
        auto s = std::chrono::nanoseconds(stats.latencies[i + 1] -
                                          stats.latencies[i]).count();
        if (props.GetProperty("tree") == "pibench")
          global_latencies.push_back(s);
        else
          global_latencies.push_back((double) s / 1000.0);
      }
    }

    for (int i = 0; i < num_threads; ++i) {
      delete clients[i];
    }

    cout << "********** run result **********" << endl;
    cout << "operations: " << total_ops << ", duration: " << duration
         << " s,  qps: " << total_ops / duration << " ops/s" << endl;

    if(latency_sample != 0){
      std::sort(global_latencies.begin(), global_latencies.end());
      auto observed = global_latencies.size();
      if (!observed) global_latencies.emplace_back(0);
      std::cout << "Latencies in "
                << ((props.GetProperty("tree") == "pibench")? "ns" : "us") << " ("
                << observed << " operations observed):\n"
                << std::fixed << std::setprecision(2)
                << "     min: " << global_latencies[0] << '\n'
                << "     50%: " << global_latencies[0.5 * observed] << '\n'
                << "     90%: " << global_latencies[0.9 * observed] << '\n'
                << "     99%: " << global_latencies[0.99 * observed] << '\n'
                << "   99.9%: " << global_latencies[0.999 * observed] << '\n'
                << "  99.99%: " << global_latencies[0.9999 * observed] << '\n'
                << " 99.999%: " << global_latencies[0.99999 * observed] << '\n'
                << "     max: " << (observed ? global_latencies[observed - 1] : 0)
                << std::endl;
    }
  }
  if (props.GetProperty("tree") != "pibench" && props.GetProperty("tree") != "dash" &&
      props.GetProperty("tree") != "bztree") {
    for (auto &db: connections) {
      delete db;
    }
  } else {
    delete connections.front();
  }
}

string ParseCommandLine(int argc, const char *argv[],
                        utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-ramp_up") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("ramp_up", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-benchmarkseconds") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("benchmarkseconds", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-buffer_page") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("buffer_page", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-falloc_index") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("falloc_index", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-falloc_data") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("falloc_data", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-device_size") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("device_size", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-path") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("path", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-tree") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("tree", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-load") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("load", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("run", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-latency_sample") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("latency_sample", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-wrapper") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("wrapper", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-poolsize") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("poolsize", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-stride") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("stride", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-starting_cpu") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("starting_cpu", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-epoch") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("epoch", argv[argindex]);
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [flags]" << R"foo(
Required Flags:
  path pathname : Path to the file, folder, or device used for the DB.
  tree treename : Tree type, choose from [btree btree_rdev pibench].
Optional Flags:
  ramp_up n: Ramp-up time. Default is 0.
  benchmarkseconds n: Duration of test. Default is 20
  latency_sample n: Measure latency with this probability. Default is 0.0
                    1.0 means sample every operation, 0.0 means no sampling.
  threads n: execute using n threads (default: 1)
  p propertyfile: load properties from the given file. Multiple files can be
                  specified, and will be processed in the order specified.
  load <true|false>: if set true, the existing files will be truncated. Default is false.
  run <true|false>: if set true, run with the workload defined in the property file. Default is false.
  stride n: The stride for CPU pinning. Must be greater than 0. Default is 2.
  starting_cpu n: The first CPU # to use. Default is 0.
Tree Dependent Flags:
btree:
  buffer_page n: the number of pages for the buffer pool.
  falloc_index n: the size of the pre-allocated index files in n bytes.
  falloc_data n: the size of the pre-allocated data files in n bytes.
btree_rdev:
  device_size n: the size of the raw device in n GB, required.
pibench:
  wrapper wrapperfile.so: Use a PiBench wrapper file, required.
  poolsize n: The size to give the pibench wrapper in bytes, depending on the
              wrapper this may be optional.
dash:
  poolsize n: The size in bytes.
  epoch n: The number of operations per epoch. Default 1024.

)foo"; // Ensure there is an empty newline before )foo
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void PrintInfo(utils::Properties &props) {
  printf("----------------------------------------\n");
  printf("%s", props.DebugString().c_str());
  printf("----------------------------------------\n");
  fflush(stdout);
}
