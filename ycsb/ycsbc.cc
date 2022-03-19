//
//    ycsbc.cc
//    YCSB-C
//
//    Created by Jinglei Ren on 12/19/14.
//    Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//    Modified by Yongping Luo on 3/4/22
//    Copyright (c) 2022 Yongping Luo <ypluo18@qq.com>
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include "utils.h"
#include "basic_db.h"
#include "core_workload.h"

using namespace std;
using namespace ycsbc;

void UsageMessage(const char *command)  {
    cout << "Usage: " << command << " [options]" << endl;
    cout << "Options:" << endl;
    cout << "-P propertyfile: load properties from the given file. Multiple files can" << endl;
    cout << "                 be specified, and will be processed in the order specified" << endl;
    cout << "-F datasetfile:  using keys from a given file" << endl;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int argindex = 1;
    while (argindex < argc) {
        if (strcmp(argv[argindex], "-P") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            ifstream input(argv[argindex]);
            try {
                props.Load(input);
            } catch (const string &message) {
                cout << message << endl;
                exit(0);
            }
            input.close();
            argindex++;
        } 
        else if (strcmp(argv[argindex], "-F") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("dataset_file", argv[argindex]);
            argindex++;
        } 
        else {
            cout << "Unknown option '" << argv[argindex] << "'" << endl;
            exit(0);
        }
    }

    if (argindex == 1 || argindex != argc) {
        UsageMessage(argv[0]);
        exit(0);
    }
}

int main(const int argc, const char *argv[]) {
    utils::Properties props;
    ParseCommandLine(argc, argv, props);

    const string filename = props.GetProperty("dataset_file", "");
    const int recordcount = stoi(props.GetProperty("recordcount", "1000")); 
    const int operationcount = stoi(props.GetProperty("operationcount", "0"));
    const float insert_ratio = stof(props.GetProperty(CoreWorkload::INSERT_PROPORTION_PROPERTY, "0"));

    ycsbc::CoreWorkload wl(filename, recordcount + operationcount * insert_ratio);
    ycsbc::BasicDB db;

    wl.Init(props);

    std::string key;
    std::vector<KVPair> result;
    std::vector<KVPair> values;
    std::vector<std::vector<KVPair>> scanresult;
    int len;
    for(int i = 0; i < operationcount; i++) {
        switch(wl.NextOperation()) {
            case READ:
                key = wl.NextTransactionKey();
                db.Read(key, result);
                break;
            case UPDATE:
                key = wl.NextTransactionKey();
                db.Update(key, values);
                break;
            case INSERT:
                key = wl.NextSequenceKey();
                db.Insert(key, values);
                break;
            case SCAN:
                key = wl.NextTransactionKey();
                len = wl.NextScanLength();
                db.Scan(key, len, scanresult);
                break;
            case READMODIFYWRITE:
                key = wl.NextTransactionKey();
                db.Read(key, result);
                db.Update(key, values);
                break;
            default:
                throw utils::Exception("Operation request is not recognized!");
        }
    }
}