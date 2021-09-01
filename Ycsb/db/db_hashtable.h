#include "../../HashTable/HashTable.h"
#include "../core/db.h"

namespace ycsbc {
class DbHashTable : public DB {
  public:
    DbHashTable(std::string filename, const bool load, uint32_t buffer_page);
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
  
    // Optimized Path
    int Read(const std::string &table, uint64_t key,
            const std::vector<std::string> *fields,
            std::vector<KVPair> &result) override;
    int Insert(const std::string &table, uint64_t key,
              std::vector<KVPair> &values) override;
  private:
    HashTable ht;
};
}