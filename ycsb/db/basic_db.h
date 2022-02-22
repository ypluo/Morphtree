//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <mutex>
#include "core/properties.h"

using std::cout;
using std::endl;

namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init() {
    // std::lock_guard<std::mutex> lock(mutex_);
    //cout << "A new thread begins working." << endl;
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    // std::lock_guard<std::mutex> lock(mutex_);
    cout << "READ " << key << endl;
    // cout << "READ " << table << ' ' << key;
    // if (fields) {
    //   cout << " [ ";
    //   for (auto f : *fields) {
    //     cout << f << ' ';
    //   }
    //   cout << ']' << endl;
    // } else {
    //   cout  << " < all fields >" << endl;
    // }
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    // std::lock_guard<std::mutex> lock(mutex_);
    cout << "SCAN " << key << " " << len << endl;
    // cout << "SCAN " << table << ' ' << key << " " << len ;
    // if (fields) {
    //   cout << " [ ";
    //   for (auto f : *fields) {
    //     cout << f << ' ';
    //   }
    //   cout << ']' << endl;
    // } else {
    //   cout  << " < all fields >" << endl;
    // }
    return 0;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    // std::lock_guard<std::mutex> lock(mutex_);
    // for (auto v : values) {
    //   //cout << v.first << '=' << v.second << ' ';
    //   cout << v.second << ' ';
    // }
    //cout << ']' << endl;
    cout << "UPDATE " << key << " " << endl;
    return 0;
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    // std::lock_guard<std::mutex> lock(mutex_);
    // cout << "INSERT " << table << ' ' << key << " [ ";
    // for (auto v : values) {
    //   //cout << v.first << '=' << v.second << ' ';
    //   cout << v.second << ' ';
    // }
    //cout << ']' << endl;
    cout << "INSERT " << key << endl;
    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    // std::lock_guard<std::mutex> lock(mutex_);
    cout << "DELETE " << key << endl;
    return 0; 
  }

 private:
  std::mutex mutex_;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

