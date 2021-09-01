//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"
#include "db_btree.h"
#include "db_hashtable.h"
#include "db_pibench.h"
#include "db_dash.h"
#include "db_bztree.h"
#include <string>

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB *DBFactory::CreateDB(utils::Properties &props) {
  if (props["tree"] == "btree") {
    std::string btree_file = props.GetProperty("btree_file", "btree");
    const bool load = utils::StrToBool(props.GetProperty("load", "false"));
    const off_t index_len = stol(props.GetProperty("falloc_index", "0"));
    const off_t data_len = stol(props.GetProperty("falloc_data", "0"));
    const long buffer_page = stol(props.GetProperty("buffer_page", "0"));
    return new DbBtree(btree_file, index_len, data_len, load, buffer_page);
  } else if (props["tree"] == "hashtable") {
    std::string hashtable_file = props.GetProperty("hashtable_file", "hashtable");
    const bool load = utils::StrToBool(props.GetProperty("load", "false"));
    const long buffer_page = stol(props.GetProperty("buffer_page", "1000"));
    return new DbHashTable(hashtable_file, load, buffer_page);
  } else if (props["tree"] == "btree_rdev") {
    std::string btree_file = props.GetProperty("btree_file", "/dev/nvme0n1");
    const bool load = utils::StrToBool(props.GetProperty("load", "false"));

    long index_start = std::stol(props.GetProperty("index_start", "0"));
    long data_start = std::stol(props.GetProperty("data_start", "0"));
    return new DbBtree(btree_file, load, index_start, data_start);
  } else if (props["tree"] == "pibench") {
    std::string pool_file = props.GetProperty("path", "");
    auto num_threads = stoi(props.GetProperty("threadcount", "1"));
    auto wrapper = props.GetProperty("wrapper", "");
    auto pool_size = stoll(props.GetProperty("poolsize", "0"));
    auto key_size = stoi(props.GetProperty("keylength", "8"));
    auto value_size = stoi(props.GetProperty("fieldlength", "8"));
    return new ycsbc::DbPiBench(wrapper, pool_file, num_threads, pool_size, key_size, value_size);
  } else if (props["tree"] == "dash") {
    std::string pool_file = props.GetProperty("path", "/tmp/pool");
    auto pool_size = stoull(props.GetProperty("poolsize", "10737418240"));
    auto epoch = stoi(props.GetProperty("epoch", "1024"));
    return new ycsbc::DbDash(pool_file, pool_size, epoch);
  } else if (props["tree"] == "bztree") {
    std::string pool_file = props.GetProperty("path", "/tmp/pool");
    auto pool_size = stoull(props.GetProperty("poolsize", "10737418240"));
    auto num_threads = stoi(props.GetProperty("threadcount", "1"));
    return new ycsbc::DbBztree(pool_file, pool_size, num_threads);
  } else {
    return NULL;
  }
}
