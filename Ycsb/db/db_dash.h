#pragma once
#include <string>

#include "../core/db.h"

// Forward Declarations
template <class T>
class Hash;

namespace ycsbc {

class DbDash : public DB {
 public:
  DbDash(const std::string &pool_name, uint64_t pool_size, uint64_t epoch_size);
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
  ~DbDash();

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
  Hash<uint64_t> *hash_table;
  const uint64_t epoch_size;
};
}  // namespace ycsbc
