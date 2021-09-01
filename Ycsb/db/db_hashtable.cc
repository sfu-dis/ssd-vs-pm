#include "db_hashtable.h"

namespace ycsbc {
DbHashTable::DbHashTable(std::string filename, const bool load, uint32_t buffer_page) 
                        : ht(filename, 100000, buffer_page, load) {}

int DbHashTable::Read(const std::string &table, uint64_t key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
  uint64_t output = 0;
  bool success = ht.Search(key, output);
  if (success && key == output) {
    return DB::kOK;
  } else {
    return DB::kErrorNoData;
  }
}

int DbHashTable::Insert(const std::string &table, uint64_t key,
                    std::vector<KVPair> &values) {
  bool success = ht.Insert(key, key);
  if (success) {
    return DB::kOK;
  } else {
    return DB::kErrorNoData;
  }
}

int DbHashTable::Scan(const std::string &table, const std::string &key,
                  int record_count, const std::vector<std::string> *fields,
                  std::vector<std::vector<KVPair>> &result) {
  return DB::kOK;
}

int DbHashTable::Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
  return DB::kOK;
}

int DbHashTable::Delete(const std::string &table, const std::string &key) {
  return DB::kOK;
}

int DbHashTable::Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
  return Read(table, strtoull(key.c_str(), NULL, 10), fields, result);
}

int DbHashTable::Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
  return Insert(table, strtoull(key.c_str(), NULL, 10), values);
}
}
