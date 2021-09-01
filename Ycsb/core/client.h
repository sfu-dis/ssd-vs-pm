//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include <atomic>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

extern uint64_t get_now_micros();

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { }

  bool DoInsert();
  bool DoRead();
  bool DoTransaction();

  __attribute__((no_sanitize("thread"))) uint64_t GetStats() { return read_cnt + ins_cnt; }
  uint64_t GetRead() { return read_cnt; }
  uint64_t GetInsert() { return ins_cnt; }
  auto GetOps() const noexcept { return op_cnt; }
 protected:
  
  int TransactionRead();
  int TransactionReadModifyWrite();
  int TransactionScan();
  int TransactionUpdate();
  int TransactionInsert();
  
  void ReadVerify(uint64_t, const std::vector<DB::KVPair> &);

  DB &db_;
  CoreWorkload &workload_;

  // Stats
  uint64_t read_cnt{};
  uint64_t ins_cnt{};
  uint64_t op_cnt{};
};

const static std::string table = "";

inline bool Client::DoInsert() {
  return (TransactionInsert() == DB::kOK);
}

inline bool Client::DoRead() {
  int status = -1;
  status = TransactionRead();
  read_cnt += (status == DB::kOK);
  assert(status >= 0);
  return (status == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  op_cnt++;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      read_cnt += (status == DB::kOK);
      break;
    // case UPDATE:
    //   status = TransactionUpdate();
    //   break;
    case INSERT:
      status = TransactionInsert();
      ins_cnt += (status == DB::kOK);
      break;
    // case SCAN:
    //   status = TransactionScan();
    //   break;
    // case READMODIFYWRITE:
    //   status = TransactionReadModifyWrite();
    //   break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  // XXX(khuang): currently, we have each thread work on one tree,
  // so we don't need to call workload_.NextTable().
  auto key = workload_.NextTransactionKey();

  // XXX(darieni): right now we only use 1 field, so we omit NextFieldName()
  thread_local std::vector<DB::KVPair> result(1);
  auto ret = db_.Read(table, key, NULL, result);

  #ifdef VERIFY_VALUE
  #ifdef CLUSTERED
  #error Cannot verify clustered mode
  #endif
  ReadVerify(key, result);
  #endif
  return ret;
}

inline int Client::TransactionReadModifyWrite() {
  // const std::string &table = workload_.NextTable();
  // const std::string &key = workload_.NextTransactionKey();
  // std::vector<DB::KVPair> result;

  // if (!workload_.read_all_fields()) {
  //   std::vector<std::string> fields;
  //   fields.push_back("field" + workload_.NextFieldName());
  //   db_.Read(table, key, &fields, result);
  // } else {
  //   db_.Read(table, key, NULL, result);
  // }

  // std::vector<DB::KVPair> values;
  // if (workload_.write_all_fields()) {
  //   workload_.BuildValues(values);
  // } else {
  //   workload_.BuildUpdate(values);
  // }
  // return db_.Update(table, key, values);
  return 0;
}

inline int Client::TransactionScan() {
  // const std::string &table = workload_.NextTable();
  // const std::string &key = workload_.NextTransactionKey();
  // int len = workload_.NextScanLength();
  // std::vector<std::vector<DB::KVPair>> result;
  // if (!workload_.read_all_fields()) {
  //   std::vector<std::string> fields;
  //   fields.push_back("field" + workload_.NextFieldName());
  //   return db_.Scan(table, key, len, &fields, result);
  // } else {
  //   return db_.Scan(table, key, len, NULL, result);
  // }
  return 0;
}

inline int Client::TransactionUpdate() {
  // const std::string &table = workload_.NextTable();
  // const std::string &key = workload_.NextTransactionKey();
  // std::vector<DB::KVPair> values;
  // if (workload_.write_all_fields()) {
  //   workload_.BuildValues(values);
  // } else {
  //   workload_.BuildUpdate(values);
  // }
  // return db_.Update(table, key, values);
  return 0;
}

inline int Client::TransactionInsert() {
  // XXX(khuang): currently, we have each thread work on one tree,
  // so we don't need to call workload_.NextTable().
  auto key = workload_.NextSequenceKey();
  thread_local std::vector<DB::KVPair> values{};
  values.clear(); // reuse vector
  workload_.BuildValues(key, values);
  return db_.Insert(table, key, values);
}

inline void Client::ReadVerify(uint64_t key,
                               const std::vector<DB::KVPair> &result) {
  thread_local std::vector<DB::KVPair> values{};
  values.clear();  // reuse vector
  workload_.BuildValues(key, values);

  thread_local uint64_t count = 0;
  if (!std::equal(values[0].second.cbegin(), values[0].second.cend(),
                  result[0].second.cbegin())) {
    LOG(WARNING) << "Key: " << key << " thread " << workload_.thread_id
                 << " insert start/recordcount " << workload_.insert_start
                 << "/" << workload_.record_count_
                 << "successful verifys: " << count << "\n";
    std::cout << std::hex << std::setfill('0');
    utils::printBytes(
        reinterpret_cast<const unsigned char *>(values[0].second.data()));
    std::cout << "\nresult:";
    utils::printBytes(
        reinterpret_cast<const unsigned char *>(result[0].second.data()));
    exit(1);
  }
  count++;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
