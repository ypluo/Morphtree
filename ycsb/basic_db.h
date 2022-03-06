//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  
//  Modified by Yongping Luo on 3/4/22
//  Copyright (c) 2022 Yongping Luo <ypluo18@qq.com>
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include <iostream>
#include <string>
#include <vector>

using std::cout;
using std::endl;

namespace ycsbc {

typedef std::pair<std::string, std::string> KVPair;

class BasicDB{
 public:
  int Read(const std::string &key, std::vector<KVPair> &result) {
    cout << "READ " << key << endl;
    return 0;
  }

  int Scan(const std::string &key, int len, std::vector<std::vector<KVPair>> &result) {
    cout << "SCAN " << key << " " << len << endl;
    return 0;
  }

  int Update(const std::string &key,
             std::vector<KVPair> &values) {
    cout << "UPDATE " << key << " " << endl;
    return 0;
  }

  int Insert(const std::string &key,
             std::vector<KVPair> &values) {
    cout << "INSERT " << key << endl;
    return 0;
  }

  int Delete(const std::string &key) {
    cout << "DELETE " << key << endl;
    return 0; 
  }
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

