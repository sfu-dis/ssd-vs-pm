#include "btree.h"
#include "buffer_manager.h"
#include "../core/db.h"

namespace ycsbc {

struct alignas(ALIGNMENT) Record {
  constexpr static auto value_len = ALIGNMENT - sizeof(uint64_t);
  char value[value_len];
  uint64_t key;

  Record () = default;

  Record (uint64_t k, const std::string_view v) {
    key = k;
    std::memcpy(value, v.data(), std::min<size_t>(v.length(), value_len));
  }

  std::string getValue() {
    std::string s{};
    s.resize(value_len);
    std::memcpy(s.data(), value, value_len);
    return s;
  }
};

static_assert(sizeof(Record) == ALIGNMENT);

class DbBtree : public DB {
 public:
  DbBtree(std::string filename, const off_t index_len, const off_t data_len, const bool load, uint32_t buffer_page);
  DbBtree(std::string filename, const bool load, const long index_start, const long data_start);
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
  std::shared_ptr<File> file;
  Btree<Pair> index;
  File data;
  record_t num_records = 0;
  inline constexpr bool valid_record_number(record_t n) { return n <= num_records; }
};
}  // namespace ycsbc
