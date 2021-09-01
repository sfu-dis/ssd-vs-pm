#include "../core/db.h"
#include "../../include/tree_api.hpp"

namespace ycsbc {

class DbPiBench : public DB {
 public:
  DbPiBench(std::string wrapper_path, std::string pool_path,
            size_t num_threads = 1, size_t pool_size = 0, size_t key_size = 8,
            size_t value_size = 8);
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
  ~DbPiBench();

 private:
  void *handle{nullptr};
  tree_api *tree_{nullptr};
  const size_t key_size;
  const size_t value_size;
};
}  // namespace ycsbc
