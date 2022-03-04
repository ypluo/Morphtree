//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modified by Yongping Luo on 3/4/22
//  Copyright (c) 2022 Yongping Luo <ypluo18@qq.com>
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "properties.h"
#include "utils.h"
#include "basic_db.h"

namespace ycsbc {

typedef uint64_t _key_t;

enum Operation {
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE
};

class CoreWorkload {
 public:
   /// 
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;
  
  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;

  /// 
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;
  
  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);
  
  virtual std::string NextSequenceKey(); /// Used for loading data
  virtual std::string NextTransactionKey(); /// Used for transactions
  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }

  CoreWorkload(std::string filename = "") :
    key_generator_(NULL), key_chooser_(NULL), 
    scan_len_chooser_(NULL), insert_key_sequence_(),
    record_count_(0) {
    if(filename.size() == 0) {
      from_file_ = false;
      return ;
    } else {
      from_file_ = true;
      max_key_num_ = 0;
      keys_.reserve(64 * 1024 * 1024);

      std::ifstream infile_load(filename.c_str());
      if(!infile_load) {
        fprintf(stderr, "dataset file %s is not found\n", filename.c_str());
        exit(-1);
      }

      _key_t key;
      while (true) {
        infile_load >> key;
        if(!infile_load.good()) break;
        max_key_num_++;
      }
    }
  }
  
  virtual ~CoreWorkload() {
    if (key_generator_) delete key_generator_;
    if (key_chooser_) delete key_chooser_;
    if (scan_len_chooser_) delete scan_len_chooser_;
  }
  
 protected:
  std::string BuildKeyName(uint64_t key_num);

  Generator<uint64_t> *key_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  size_t record_count_;
  bool from_file_;
  int max_key_num_;
  std::vector<_key_t> keys_;
};

inline std::string CoreWorkload::NextSequenceKey() {
  uint64_t key_num = key_generator_->Next();
  
  if(from_file_) {
    assert(key_num < max_key_num_);
    key_num = keys_[key_num];
  } else {
    key_num = utils::Hash(key_num);
  }

  std::string key_num_str = std::to_string(key_num);
  return key_num_str;
}

inline std::string CoreWorkload::NextTransactionKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  
  if(from_file_) {
    assert(key_num < max_key_num_);
    key_num = keys_[key_num];
  } else {
    key_num = utils::Hash(key_num);
  }

  std::string key_num_str = std::to_string(key_num);
  return key_num_str;
}
  
} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
