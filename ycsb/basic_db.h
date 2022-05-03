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

#include <fstream>
#include <string>
#include <vector>
#include <cassert>
#include <iomanip>

using std::string;
using std::ofstream;
using std::endl;

namespace ycsbc {

typedef std::pair<std::string, std::string> KVPair;

class BasicDB {
 public:
    BasicDB(const string & filename) {
      fout.open(filename.c_str(), std::ios::out);
      assert(fout.good());
    }

    ~BasicDB() {
      fout.close();
    }

 public:
  int Read(const std::string &key, std::vector<KVPair> &result) {
    fout << "READ " << key << endl;
    return 0;
  }

  int Scan(const std::string &key, int len, std::vector<std::vector<KVPair>> &result) {
    fout << "SCAN " << key << " " << len << endl;
    return 0;
  }

  int Update(const std::string &key,
             std::vector<KVPair> &values) {
    fout << "UPDATE " << key << " " << endl;
    return 0;
  }

  int Insert(const std::string &key,
             std::vector<KVPair> &values) {
    fout << "INSERT " << key << endl;
    return 0;
  }

  int Delete(const std::string &key) {
    fout << "DELETE " << key << endl;
    return 0; 
  }

private:
  ofstream fout;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

