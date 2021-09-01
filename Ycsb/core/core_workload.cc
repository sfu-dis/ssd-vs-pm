//
//  core_workload.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"

#include <string>
#include <glog/logging.h>

using ycsbc::CoreWorkload;
using std::string;

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "1";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
    "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "8";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
    "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
    "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::ZIPFIAN_SKEW_FACTOR_PROPERTY = "zipfianskewfactor";
const string CoreWorkload::ZIPFIAN_SKEW_FACTOR_DEFAULT = "0.99";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
    "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const string CoreWorkload::BENCHMARK_SECONDS_PROPERTY = "benchmarkseconds";
const string CoreWorkload::RAMP_UP_PROPERTY = "ramp_up";

CoreWorkload::CoreWorkload(const utils::Properties &p)
    : thread_id{std::stoi(p.GetProperty("thread_id", "0"))} {
  table_name_ = p.GetProperty(TABLENAME_PROPERTY,TABLENAME_DEFAULT);
  
  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY,
                                         FIELD_COUNT_DEFAULT));
  LOG_IF(WARNING, field_count_ > 1) << "Fieldcount is more than 1\n";

  string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                        FIELD_LENGTH_DISTRIBUTION_DEFAULT);
  int field_len =
      std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
  if (field_len_dist == "constant") {
    field_len_generator_ = std::make_unique<ConstGenerator>(field_len);
  } else if (field_len_dist == "uniform") {
    field_len_generator_ = std::make_unique<UniformGenerator>(1, field_len);
  } else if (field_len_dist == "zipfian") {
    field_len_generator_ = std::make_unique<ZipfianGenerator>(1, field_len);
  } else {
    throw utils::Exception("Unknown field length distribution: " +
                           field_len_dist);
  }

  double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                   READ_PROPORTION_DEFAULT));
  double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                     UPDATE_PROPORTION_DEFAULT));
  double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                     INSERT_PROPORTION_DEFAULT));
  double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                   SCAN_PROPORTION_DEFAULT));
  double readmodifywrite_proportion = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));
  
  record_count_ = std::stoull(p.GetProperty(RECORD_COUNT_PROPERTY)) / stoi(p.GetProperty("threadcount", "1"));

  std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                           REQUEST_DISTRIBUTION_DEFAULT);

  double zipfian_skew_factor = std::stod(p.GetProperty(ZIPFIAN_SKEW_FACTOR_PROPERTY,
                                                        ZIPFIAN_SKEW_FACTOR_DEFAULT));

  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY,
                                             MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  insert_start =
      std::stoull(p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT)) +
      record_count_ * thread_id;

  read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                    READ_ALL_FIELDS_DEFAULT));
  write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                     WRITE_ALL_FIELDS_DEFAULT));
  
  if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    ordered_inserts_ = false;
  } else {
    ordered_inserts_ = true;
  }
  
  sequence_key_generator_ = std::make_unique<CounterGenerator>(insert_start);
  
  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }
  
  insert_key_sequence_.Set(insert_start + record_count_);
  
  if (request_dist == "uniform") {
    // XXX(khuang): the keys are partitioned by the # of DBs, thus no conflict at all.
    // key_chooser_ = new UniformGenerator(0, record_count_ - 1);
    LOG(INFO) << "tid:" << thread_id << " start:" << insert_start << " end:" << insert_start + record_count_ - 1;
    key_chooser_ = std::make_unique<UniformGenerator>(insert_start, insert_start + record_count_ - 1);
  } else if (request_dist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    uint64_t op_count = std::stoull(p.GetProperty(OPERATION_COUNT_PROPERTY)) / stoull(p.GetProperty("threadcount", "1"));
    uint64_t new_keys = op_count * insert_proportion * 2; // a fudge factor
    // XXX(khuang): the keys are partitioned by the # of DBs, thus no conflict at all.
    // key_chooser_ = new ScrambledZipfianGenerator(record_count_ + new_keys);
    key_chooser_ = std::make_unique<ScrambledZipfianGenerator>(insert_start, insert_start + record_count_ + new_keys, zipfian_skew_factor);
  } else if (request_dist == "latest") {
    key_chooser_ = std::make_unique<SkewedLatestGenerator>(insert_key_sequence_);
  } else {
    throw utils::Exception("Unknown request distribution: " + request_dist);
  }

  field_chooser_ = std::make_unique<UniformGenerator>(0, field_count_ - 1);

  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = std::make_unique<UniformGenerator>(1, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = std::make_unique<ZipfianGenerator>(1, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
        scan_len_dist);
  }
}

void CoreWorkload::BuildValues(uint64_t key, std::vector<ycsbc::DB::KVPair> &values) {
  for (int i = 0; i < field_count_; ++i) {
    ycsbc::DB::KVPair pair{"", std::to_string(i)};
    values.emplace_back(pair);
  }
}

void CoreWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update) {
  ycsbc::DB::KVPair pair;
  pair.first.append(NextFieldName());
  pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
  update.push_back(pair);
}

