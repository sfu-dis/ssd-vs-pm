#pragma once
#include <string>

#include "../core/db.h"
#include "../../Bztree/bztree.h"

namespace ycsbc {

class DbBztree : public DB {
 public:
  DbBztree(const std::string &pool_name, uint64_t pool_size, int num_threads);
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) override;
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;
  int Scan(const std::string &table, const std::string &key, int record_count,
           const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) override;
  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;
  int Delete(const std::string &table, const std::string &key) override;
  ~DbBztree();

  // Optimized Path
  int Read(const std::string &table, uint64_t key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) override;
  int Insert(const std::string &table, uint64_t key,
             std::vector<KVPair> &values) override;

  void thread_init(int thread_id) override;
  void thread_deinit(int thread_id) override;

 private:
  const std::string pool_name;
  const size_t pool_size;
  bztree::BzTree *tree;
};
}  // namespace ycsbc
