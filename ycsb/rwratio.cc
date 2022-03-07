#include <cstring>
#include <cstdlib>
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
    cout << "-S stage count:  specify the stage # of dynamic workload" << endl;
    cout << "-W stage width:  specify the width of each stage" << endl;
    cout << "-P propertyfile: load properties from the given file. Multiple files can" << endl;
    cout << "                 be specified, and will be processed in the order specified" << endl;
    cout << "-F datasetfile:  using keys from a given file" << endl;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int argindex = 1;
    while (argindex < argc) {
        if (strcmp(argv[argindex], "-S") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("stage_count", argv[argindex]);
            argindex++;
        } 
        else if (strcmp(argv[argindex], "-W") == 0) {
            argindex++;
            if (argindex >= argc) {
                UsageMessage(argv[0]);
                exit(0);
            }
            props.SetProperty("stage_width", argv[argindex]);
            argindex++;
        } 

        else if (strcmp(argv[argindex], "-P") == 0) {
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

void Stage(ycsbc::CoreWorkload & wl, ycsbc::BasicDB & db, int opcount) {
    std::string key;
    std::vector<KVPair> result;
    std::vector<KVPair> values;
    std::vector<std::vector<KVPair>> scanresult;
    int len;
    for(int i = 0; i < opcount; i++) {
        switch(wl.NextOperation()) {
            case READ:
                key = wl.NextTransactionKey();
                db.Read(key, result);
                break;
            case INSERT:
                key = wl.NextSequenceKey();
                db.Insert(key, values);
                break;
            default:
                throw utils::Exception("Operation request is not recognized!");
        }
    }
}

int main(int argc, const char *argv[]) {
    utils::Properties props;
    ParseCommandLine(argc, argv, props);

    // basic workload and basic db
    ycsbc::CoreWorkload wl(props.GetProperty("dataset_file", ""));
    ycsbc::BasicDB db;

    // for each stage, generate a piece of workloads
    int stage_count = stoi(props.GetProperty("stage_count", "6"));
    int stage_width = stoi(props.GetProperty("stage_width", "1000"));
    int initial_record_count = stoi(props.GetProperty("recordcount", "1000"));
    for(int i = 0; i < stage_count; i++) { 
        // change the read and write properties
        float write_portion = 1.0 / (stage_count - 1) * i;
        float read_portion = 1 - write_portion;
        props.SetProperty(CoreWorkload::INSERT_START_PROPERTY, to_string(initial_record_count));
        props.SetProperty(CoreWorkload::READ_PROPORTION_PROPERTY, to_string(read_portion));
        props.SetProperty(CoreWorkload::INSERT_PROPORTION_PROPERTY, to_string(write_portion));
        wl.Init(props);

        // fire a stage
        Stage(wl, db, stage_width);

        // update inital_record_count
        initial_record_count += stage_width * write_portion;

        printf("----------------------------------------------------\n\n");
    }
}