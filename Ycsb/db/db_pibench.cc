#include "db_pibench.h"
#include <glog/logging.h>
#include <dlfcn.h>

namespace ycsbc {

DbPiBench::DbPiBench(std::string wrapper_path, std::string pool_path,
                     size_t num_threads, size_t pool_size, size_t key_size,
                     size_t value_size) : key_size{key_size}, value_size{value_size} {
  handle = dlopen(wrapper_path.c_str(), RTLD_NOW);
  if (handle == nullptr)
    LOG(FATAL) << "Error in dlopen(): " << dlerror();
  dlerror();
  auto create_fn_ =
      (tree_api * (*)(const tree_options_t &)) dlsym(handle, "create_tree");
  if (create_fn_ == nullptr)
    LOG(FATAL) << "Error when finding 'create()' in dlsym(): " << dlerror();

  tree_options_t opts{};
  opts.key_size = key_size;
  opts.value_size = value_size;
  opts.pool_path = std::move(pool_path);
  opts.pool_size = pool_size;
  opts.num_threads = num_threads;
  tree_ = create_fn_(opts);
  if (tree_ == nullptr)
    LOG(FATAL) << "Error when creating tree";
}

int DbPiBench::Read(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {
  thread_local char buf[4096];
  if (tree_->find(key.c_str(), key.size(), buf)) {
  #ifdef VERIFY_VALUE
    result[0].second = std::string(buf, value_size); // very expensive
  #endif
    return DB::kOK;
  } else {
    return DB::kErrorNoData;
  }
}

int DbPiBench::Insert(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
  return tree_->insert(key.c_str(), key.size(), values.front().second.c_str(),
                       values.front().second.size())
             ? DB::kOK
             : DB::kErrorNoData;
}

int DbPiBench::Scan(const std::string &table, const std::string &key,
                    int record_count, const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {
  return DB::kOK;
}

int DbPiBench::Update(const std::string &table, const std::string &key,
                      std::vector<KVPair> &values) {
  return tree_->update(key.c_str(), key.size(), values.front().second.c_str(),
                       values.front().second.size())
             ? DB::kOK
             : DB::kErrorNoData;
}

int DbPiBench::Delete(const std::string &table, const std::string &key) {
  return tree_->remove(key.c_str(), key.size() ? DB::kOK : DB::kErrorNoData);
}

DbPiBench::~DbPiBench() {
  if (handle != nullptr)
    if (dlclose(handle) != 0)
      LOG(FATAL) << "Error in dlclose(): " << dlerror();
}
}  // namespace ycsbc