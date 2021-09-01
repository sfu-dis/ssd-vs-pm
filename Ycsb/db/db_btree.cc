#include "db_btree.h"
#include <thread>

static std::once_flag init{};
namespace ycsbc {
DbBtree::DbBtree(std::string filename, const off_t index_len,
                 const off_t data_len, const bool load,
                 uint32_t buffer_page) :
      file(new File(filename + ".index", index_len, load, PAGE_SIZE)),
      index(file, (buffer_page <= 0 ? buffer_page = 1000 : buffer_page)),
      data(filename + ".data", data_len, load, sizeof(Record)) {
  std::call_once(init, [&]() {
    LOG(INFO) << "BTREE_ORDER=" << BTREE_ORDER << " PAGE_SIZE=" << PAGE_SIZE
              << " sizeof(Record)=" << sizeof(Record)
              << " sizeof(Node)=" << sizeof(Node<Pair>)
              << " sizeof(Page.page_data)=" << sizeof(decltype(std::declval<Page>().page_data))
              << " sizeof(Page)=" << sizeof(Page)
              << " sizeof(Metadata)=" << sizeof(Metadata)
              << " ALIGNMENT=" << ALIGNMENT
              << " falloc INDEX_SIZE=" << index_len
              << " falloc DATA_SIZE=" << data_len
              << " #buffer pages=" << buffer_page
              << " truncate files LOAD=" << load;
  });
  if(!load){ // todo: recovery
    num_records = index.get_record_count();
  }
}

DbBtree::DbBtree(std::string filename, const bool load, const long index_start, const long data_start)
  : file(new File(filename, load, index_start, PAGE_SIZE)),
    index(file, 0), data(filename, load, data_start, sizeof(Record)) {}

int DbBtree::Read(const std::string &table, uint64_t key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
  auto iter = index.find(Pair{key});
  if(!iter){
    return DB::kErrorNoData;
  }
  auto pair = *iter;

#if defined(CLUSTERED)
  if (valid_record_number(pair.record_number)) {
    return DB::kOK;
  } else {
    return DB::kErrorNoData;
  }
#endif

  Record r; // todo: refactor out
  data.load(pair.record_number, r);
  result[0].second = r.getValue();
  if (key == r.key) {
    return DB::kOK;
  }
  return DB::kErrorNoData;
}

int DbBtree::Insert(const std::string &table, uint64_t key,
                    std::vector<KVPair> &values) {
#ifndef CLUSTERED
  Record r{key, values.at(0).second};
  data.flush(num_records, r);
#endif

  index.insert(Pair{key, num_records});

  num_records++;
  return DB::kOK;
}

int DbBtree::Scan(const std::string &table, const std::string &key,
                  int record_count, const std::vector<std::string> *fields,
                  std::vector<std::vector<KVPair>> &result) {
  return DB::kOK;
}

int DbBtree::Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
  return DB::kOK;
}

int DbBtree::Delete(const std::string &table, const std::string &key) {
  return DB::kOK;
}

int DbBtree::Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
  return Read(table, strtoull(key.c_str(), NULL, 10), fields, result);
}

int DbBtree::Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
  return Insert(table, strtoull(key.c_str(), NULL, 10), values);
}

}  // namespace ycsbc
