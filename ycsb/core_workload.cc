//
//  core_workload.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//
//  Modified by Yongping Luo on 3/4/22
//  Copyright (c) 2022 Yongping Luo <ypluo18@qq.com>

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "core_workload.h"

#include <string>

using ycsbc::CoreWorkload;
using std::string;

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY ="readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY ="scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

void CoreWorkload::Init(const utils::Properties &p) {
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
  
  record_count_ = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY));
  std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                           REQUEST_DISTRIBUTION_DEFAULT);
  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY,
                                             MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  
  key_generator_ = new CounterGenerator();
  
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

  if (request_dist == "uniform") {
    key_chooser_ = new UniformGenerator(0, record_count_ - 1, rand());
    
  } else if (request_dist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    int new_keys = (int)(op_count * insert_proportion * 2); // a fudge factor
    key_chooser_ = new ScrambledZipfianGenerator(record_count_ + new_keys);
    
  } else if (request_dist == "latest") {
    key_chooser_ = new SkewedLatestGenerator(insert_key_sequence_);
    
  } else {
    throw utils::Exception("Unknown request distribution: " + request_dist);
  }
  
  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(1, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(1, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
        scan_len_dist);
  }

  // if we read dateset from given file
  if(from_file_) {
    keys_.reserve(KEYSET_SCALE_DEFAULT);
    
    std::ifstream infile_load(filename_.c_str());
    max_seq_id_ = 0;
    _key_t key;
    while (true) {
      infile_load >> key;
      if(!infile_load.good()) break;
      max_seq_id_++;
      keys_.push_back(key);
    }
  }
}