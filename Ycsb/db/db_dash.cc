#include "db_dash.h"

#include "Hash.h"
#include "allocator.h"
#include "ex_finger.h"

thread_local uint64_t ops_this_epoch = 0;
thread_local EpochGuard guard = EpochGuard{nullptr, false};

namespace ycsbc {

DbDash::DbDash(const std::string &pool_name, uint64_t pool_size,
               uint64_t epoch_size)
    : pool_name(pool_name), pool_size{pool_size}, epoch_size{epoch_size} {
  // Step 1: create (if not exist) and open the pool
  bool file_exist = false;
  if (FileExists(pool_name.c_str())) file_exist = true;
  Allocator::Initialize(pool_name.c_str(), pool_size);

  // Step 2: Allocate the initial space for the hash table on PM and get the
  // root; we use Dash-EH in this case.
  hash_table = reinterpret_cast<Hash<uint64_t> *>(
      Allocator::GetRoot(sizeof(extendible::Finger_EH<uint64_t>)));

  // Step 3: Initialize the hash table
  if (!file_exist) {
    // During initialization phase, allocate 64 segments for Dash-EH
    size_t segment_number = 64;
    new (hash_table) extendible::Finger_EH<uint64_t>(
        segment_number, Allocator::Get()->pm_pool_);
  } else {
    new (hash_table) extendible::Finger_EH<uint64_t>();
  }
}

DbDash::~DbDash() {
  // Allocator::Close_pool();
}

// Optimized Path
int DbDash::Read(const std::string &table, uint64_t key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result) {
  ops_this_epoch++;
  auto ret = hash_table->Get(key, true);
  if (ops_this_epoch == epoch_size) {
    guard = Allocator::AquireEpochGuard();
    ops_this_epoch = 0;
  }
  return ret == NONE ? DB::kErrorNoData : DB::kOK;
}
int DbDash::Insert(const std::string &table, uint64_t key,
                   std::vector<KVPair> &values) {
  ops_this_epoch++;
  auto ret = hash_table->Insert(key, DEFAULT, true);
  if (ops_this_epoch == epoch_size) {
    guard = Allocator::AquireEpochGuard();
    ops_this_epoch = 0;
  }
  return ret == -1 ? DB::kErrorConflict : DB::kOK;
}

int DbDash::Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result) {
  return Read(table, strtoull(key.c_str(), NULL, 10), fields, result);
}

int DbDash::Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values) {
  return Insert(table, strtoull(key.c_str(), NULL, 10), values);
}
int DbDash::Scan(const std::string &table, const std::string &key,
                 int record_count, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result) {
  return 0;
}
int DbDash::Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values) {
  return 0;
}
int DbDash::Delete(const std::string &table, const std::string &key) {
  return 0;
}

void DbDash::thread_init(int thread_id) {
  ops_this_epoch = 0;
  guard = Allocator::AquireEpochGuard();
}

void DbDash::thread_deinit(int thread_id) {
  ops_this_epoch = 0;
  guard = EpochGuard{nullptr, false};
}
}  // namespace ycsbc
