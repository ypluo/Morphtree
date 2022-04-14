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
}

void Stage(ycsbc::CoreWorkload & wl, ycsbc::BasicDB & db, int opcount) {
    std::string key;
    std::vector<KVPair> result;
    std::vector<KVPair> values;
    std::vector<std::vector<KVPair>> scanresult;
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
    const int INITIAL_SIZE_DEFAULT = 16000000;
    const int SATGE_WIDTH_DEFAULT  = 32000000;
    const int STAGE_COUNT_DEFAULT  = 6;
    
    utils::Properties props;
    props.SetProperty(CoreWorkload::READ_PROPORTION_PROPERTY, to_string(0));
    props.SetProperty(CoreWorkload::INSERT_PROPORTION_PROPERTY, to_string(0));
    props.SetProperty(CoreWorkload::UPDATE_PROPORTION_PROPERTY, to_string(0));
    props.SetProperty(CoreWorkload::SCAN_PROPORTION_PROPERTY, to_string(0));
    ParseCommandLine(argc, argv, props);

    // for each stage, generate a piece of workloads
    const string filename = props.GetProperty("dataset_file", "");
    int stage_count = stoi(props.GetProperty("stage_count", to_string(STAGE_COUNT_DEFAULT)));
    int stage_width = stoi(props.GetProperty("stage_width", to_string(SATGE_WIDTH_DEFAULT)));
    int max_record_count = INITIAL_SIZE_DEFAULT + (stage_count * stage_width) / 2;

    // basic workload and basic db
    ycsbc::CoreWorkload wl(filename, max_record_count);
    
    // generate load
    BasicDB db_load("dataset.dat");
    std::string key;
    std::vector<KVPair> values;
    props.SetProperty(CoreWorkload::INSERT_START_PROPERTY, to_string(0));
    props.SetProperty(CoreWorkload::RECORD_COUNT_PROPERTY, to_string(INITIAL_SIZE_DEFAULT));
    wl.Init(props);
    for(int i = 0; i < INITIAL_SIZE_DEFAULT; i++) {
        key = wl.NextSequenceKey();
        db_load.Insert(key, values);
    }

    uint32_t cur_start = INITIAL_SIZE_DEFAULT;
    ycsbc::BasicDB db_txn("query.dat");
    for(int i = 1; i <= stage_count; i++) { 
        // change the read and write properties
        float read_portion = 1.0 / stage_count * i;
        float write_portion = 1 - read_portion;
        props.SetProperty(CoreWorkload::INSERT_START_PROPERTY, to_string(cur_start));
        props.SetProperty(CoreWorkload::RECORD_COUNT_PROPERTY, to_string(0));
        props.SetProperty(CoreWorkload::OPERATION_COUNT_PROPERTY, to_string(stage_width));
        props.SetProperty(CoreWorkload::READ_PROPORTION_PROPERTY, to_string(read_portion));
        props.SetProperty(CoreWorkload::INSERT_PROPORTION_PROPERTY, to_string(write_portion));
        wl.Init(props);

        // fire a stage
        Stage(wl, db_txn, stage_width);

        // update inital_record_count
        cur_start += stage_width * write_portion;
    }
}