#include "db_bztree.h"
#define TEST_LAYOUT_NAME "bztree_layout"
static constexpr uint32_t kDescriptorsPerThread = 1024;

static bool FileExists(const char *pool_path) {
  struct stat buffer;
  return (stat(pool_path, &buffer) == 0);
}

bztree::BzTree *create_new_tree(const std::string &pool_name,
                                uint64_t pool_size, int _num_threads) {
  bztree::BzTree::ParameterSet param(1024, 512, 1024);
  uint32_t num_threads = _num_threads + 1;  // account for the loading thread
  uint32_t desc_pool_size = kDescriptorsPerThread * num_threads;

  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          pool_name.c_str(), TEST_LAYOUT_NAME, pool_size),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto *bztree = reinterpret_cast<bztree::BzTree *>(
      pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  pmdk_allocator->Allocate((void **)&bztree->pmwcas_pool,
                           sizeof(pmwcas::DescriptorPool));
  new (bztree->pmwcas_pool)
      pmwcas::DescriptorPool(desc_pool_size, num_threads, false);

  new (bztree)
      bztree::BzTree(param, bztree->pmwcas_pool,
                     reinterpret_cast<uint64_t>(pmdk_allocator->GetPool()));

  return bztree;
}

bztree::BzTree *recovery_from_pool(const std::string &pool_name,
                                   uint64_t pool_size, int _num_threads) {
  uint32_t num_threads = _num_threads + 1;  // account for the loading thread
  uint32_t desc_pool_size = kDescriptorsPerThread * num_threads;

  pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create(
                          pool_name.c_str(), TEST_LAYOUT_NAME, pool_size),
                      pmwcas::PMDKAllocator::Destroy,
                      pmwcas::LinuxEnvironment::Create,
                      pmwcas::LinuxEnvironment::Destroy);
  auto pmdk_allocator =
      reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  bztree::Allocator::Init(pmdk_allocator);

  auto tree = reinterpret_cast<bztree::BzTree *>(
      pmdk_allocator->GetRoot(sizeof(bztree::BzTree)));
  tree->Recovery(num_threads);

  pmdk_allocator->Allocate((void **)&tree->pmwcas_pool,
                           sizeof(pmwcas::DescriptorPool));
  new (tree->pmwcas_pool)
      pmwcas::DescriptorPool(desc_pool_size, num_threads, false);

  tree->SetPMWCASPool(tree->pmwcas_pool);

  return tree;
}

namespace ycsbc {

DbBztree::DbBztree(const std::string &pool_name, uint64_t pool_size,
                   int _num_threads)
    : pool_name(pool_name), pool_size{pool_size} {
  if (FileExists(pool_name.c_str())) {
    std::cout << "recovery from existing pool." << std::endl;
    tree = recovery_from_pool(pool_name, pool_size, _num_threads);
  } else {
    tree = create_new_tree(pool_name, pool_size, _num_threads);
  }
}

DbBztree::~DbBztree() {
  pmwcas::Thread::ClearRegistry();
  // tree->~BzTree();
  // auto pmdk_allocator =
  //     reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
  // auto *pool = tree->GetPMWCASPool();
  // pool->~DescriptorPool();
}

// Optimized Path
int DbBztree::Read(const std::string &table, uint64_t key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) {
  // uint64_t k = __builtin_bswap64(key);
  return tree->Read(reinterpret_cast<const char *>(&key), 8,
                    (uint64_t *)result[0].second.data())
                 .IsOk()
             ? DB::kOK
             : DB::kErrorNoData;
}
int DbBztree::Insert(const std::string &table, uint64_t key,
                     std::vector<KVPair> &values) {
  uint64_t v = reinterpret_cast<const uint64_t>(values[0].second.c_str());
  auto rv = tree->Insert(reinterpret_cast<const char *>(&key), 8, 0);
  return rv.IsOk() ? DB::kOK : DB::kErrorConflict;
}

int DbBztree::Read(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) {
  return Read(table, strtoull(key.c_str(), NULL, 10), fields, result);
}

int DbBztree::Insert(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) {
  return Insert(table, strtoull(key.c_str(), NULL, 10), values);
}
int DbBztree::Scan(const std::string &table, const std::string &key,
                   int record_count, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) {
  return 0;
}
int DbBztree::Update(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) {
  return 0;
}
int DbBztree::Delete(const std::string &table, const std::string &key) {
  return 0;
}

void DbBztree::thread_init(int thread_id) {}

void DbBztree::thread_deinit(int thread_id) {}
}  // namespace ycsbc
