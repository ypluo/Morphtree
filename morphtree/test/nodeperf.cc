/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "../src/node.h"

using std::cout;
using std::endl;
using namespace morphtree;

// The fanout of a node when bulk loading them: do NOT be larger than 3000
static constexpr uint64_t LFARY = GLOBAL_LEAF_SIZE / 2;

template<typename TestNodeType>
double ReadTest(int fd, uint64_t scale) {
    // read keys from dataset file
    _key_t * tmp = new _key_t[scale];
    auto discard = read(fd, tmp, sizeof(_key_t) * scale);
    std::sort(tmp, tmp + scale);

    // prepare records
    Record * recs = new Record[scale];
    for(int i = 0; i < scale; i++) {
        recs[i].key = tmp[i];
        recs[i].val = _val_t(tmp[i] & 0xffffffffffff);
    }
    delete tmp;
    
    uint64_t load_num = scale;
    uint64_t test_num = scale / 4;

    // do bulkloading 
    int node_num = load_num / LFARY + (load_num % LFARY == 0 ? 0 : 1);
    std::vector<TestNodeType *> nodes;
    uint64_t STEP = LFARY * (INT64_MAX / scale);
    _key_t end_k = STEP;
    int last_i = 0;
    nodes.reserve(node_num);
    for(int i = 0; i < load_num; i++) {
        if(recs[i].key < end_k && i < load_num - 1) {
            continue;
        } else { // meets the end of an partition
            auto new_node = new TestNodeType(&recs[last_i], i - last_i);
            nodes.push_back(new_node);
            // next node
            last_i = i;
            end_k += STEP;
        }
    }

    // prepare the lookup workload
    std::mt19937_64 gen(1007);
    std::shuffle(recs, recs + scale, gen);

    // test lookup
    uint64_t notfound = 0;
    _val_t val;

    auto start = seconds();
    for(int i = 0; i < test_num; i++) {
        int id = (int64_t) recs[i].key / STEP;
        TestNodeType * cur_node = nodes[id];

        if(!cur_node->Lookup(recs[i].key, val)) {
            notfound++;
        }
    }
    auto end = seconds();
    
    // tell the compile to not optimize out lookup
    if(notfound > 1000) 
        cout << test_num << " Not Found Number " << notfound << endl;

    // free memory resource
    for(int i = 0; i < nodes.size(); i++) {
        delete nodes[i];
    }
    delete recs;

    return end - start;
}

template<typename TestNodeType>
double WriteTest(int fd, uint64_t scale) {
    // read keys from dataset file
    _key_t * tmp = new _key_t[scale];
    auto discard = read(fd, tmp, sizeof(_key_t) * scale);

    // prepare records
    Record * recs = new Record[scale];
    for(int i = 0; i < scale; i++) {
        recs[i].key = tmp[i];
        recs[i].val = _val_t(tmp[i] & 0xffffffffffff);
    }
    delete tmp;

    uint64_t test_num = scale / 4;
    uint64_t load_num = scale - test_num;
    std::sort(recs, recs + load_num);
    
    // do bulkloading 
    int node_num = load_num / LFARY + (load_num % LFARY == 0 ? 0 : 1);
    std::vector<TestNodeType *> nodes;
    uint64_t STEP = LFARY * (INT64_MAX / load_num);
    _key_t end_k = STEP;
    int last_i = 0;
    nodes.reserve(node_num * 2);
    for(int i = 0; i < load_num; i++) {
        if(recs[i].key < end_k && i < load_num - 1) {
            continue;
        } else { // meets the end of an partition
            auto new_node = new TestNodeType(&recs[last_i], i - last_i);
            nodes.push_back(new_node);
            // next node
            last_i = i;
            end_k += STEP;
        }
    }

    // do insertion
    _key_t split_k;
    TestNodeType * split_node;
    auto start = seconds();
    for(int i = load_num; i < scale; i++) {
        int id = (int64_t) recs[i].key / STEP;
        TestNodeType * cur_node = nodes[id];

        if(cur_node->Store(recs[i].key, recs[i].val, &split_k, &split_node)) {
            nodes.push_back(split_node);
        }
    }
    auto end = seconds();

    // free memory resource
    for(int i = 0; i < nodes.size(); i++) {
        delete nodes[i];
    }
    delete recs;

    return end - start;
}

int main(int argc, char ** argv) {
    std::string workload_fname = "keyset.dat";
    const uint64_t SCALE = 32 * MILLION;
    const char * node_type_str[] = {"ROLeaf\0", "RWLeaf\0", "WOLeaf\0"};

    // default options
    bool opt_read = true;
    int opt_nodeid = 2;

    // retrieve options from arguments
    static const char * optstr = "t:wh";
    char op;
    while((op = getopt(argc, argv, optstr)) != -1) {
        switch(op) {
        case 't': 
            if(atoi(optarg) >= 1 && atoi(optarg) <= 3)
                opt_nodeid = atoi(optarg);
            break;
        case 'w':
            opt_read = false;
            break;
        case 'h':
        case '?':
            cout << "Usage: " << argv[0] << "[option]" << endl;
            cout << "-t: Node Type (1:ROLeaf, 2: RWLeaf, 3: WOLeaf, default 2)" << endl;
            cout << "-w: Do Write Test" << endl;
            exit(-1);
        default: exit(-1);
        }
    }
    
    cout << "Test Type: " << (opt_read ? "Read " : "Write ") << endl;
    cout << "Node Type: " << node_type_str[opt_nodeid - 1] << endl;

    // open data file
    int fd = open(workload_fname.c_str(), O_RDONLY);
    if(fd <= 2) {
        cout << "File Open Error" << endl;
        exit(-1);
    }

    do_morphing = false;

    // start test 
    double elasped_time = 0;
    if(opt_read) {
        switch (opt_nodeid) {
        case 1:
            elasped_time = ReadTest<ROLeaf>(fd, SCALE);
            break;
        case 2: 
            elasped_time = ReadTest<RWLeaf>(fd, SCALE);
            break;
        case 3:
            elasped_time = ReadTest<WOLeaf>(fd, SCALE);
            break;
        default: break;
        }
    } else {
        switch (opt_nodeid) {
        case 1:
            elasped_time = WriteTest<ROLeaf>(fd, SCALE);
            break;
        case 2: 
            elasped_time = WriteTest<RWLeaf>(fd, SCALE);
            break;
        case 3:
            elasped_time = WriteTest<WOLeaf>(fd, SCALE);
            break;
        default: break;
        }
    }
    cout << (double) (SCALE / 4) / MILLION / elasped_time << endl;

    close(fd);
    return 0;
}