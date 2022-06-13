/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <algorithm>
#include <vector>
#include <cstring>
#include <cmath>

#include "node.h"

namespace newtree {
    
WOLeaf::WOLeaf() {
    node_type = NodeType::WOLEAF;
    stats = WOSTATS;

    recs = new Record[NODE_SIZE];
    insert_count = 0;
    inital_count = 0;
    sort_count = 0;
}

WOLeaf::WOLeaf(Record * recs_in, int num) {
    node_type = NodeType::WOLEAF;
    stats = WOSTATS;

    recs = new Record[NODE_SIZE];
    memcpy(recs, recs_in, sizeof(Record) * num);
    inital_count = num;
    insert_count = 0;
    sort_count = 0;
}

WOLeaf::~WOLeaf() {
    delete [] recs;
}

bool WOLeaf::Store(_key_t k, _val_t v, _key_t * split_key, WOLeaf ** split_node) {
    recs[inital_count + insert_count++] = {k, v};

    if(insert_count % PIECE_SIZE == 0) {
        int16_t start = insert_count - PIECE_SIZE;
        std::sort(recs + inital_count + start, recs + inital_count + insert_count);
        sort_count = insert_count;
    }
    
    if(inital_count + insert_count == GLOBAL_LEAF_SIZE) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool WOLeaf::Lookup(_key_t k, _val_t &v) {
    // do binary search in all sorted runs
    if(BinSearch(recs, inital_count, k, v)) {
        return true;
    }

    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    for(int i = inital_count; i < inital_count + bin_end; i += PIECE_SIZE) {
        if(BinSearch(recs + i, PIECE_SIZE, k, v)) {
            return true;
        }
    }

    // bound the unsorted region into 1KB
    if(insert_count - sort_count > 64) {
        std::sort(recs + inital_count + bin_end, recs + inital_count + insert_count);
        sort_count = insert_count;
    }
    if(BinSearch(recs + inital_count + bin_end, sort_count - bin_end, k, v)) {
        return true;
    }
    
    // do scan in unsorted runs
    for(int i = inital_count + sort_count; i < inital_count + insert_count; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            return true;
        }
    }

    return false;
}

void WOLeaf::Dump(std::vector<Record> & out) {
    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / PIECE_SIZE;
    Record * sort_runs[MAX_RUN_NUM];
    int lens[MAX_RUN_NUM];

    int16_t total_count = inital_count + insert_count;
    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    if(bin_end < insert_count) {
        std::sort(recs + inital_count + bin_end, recs + total_count);
    }

    int run_cnt = 0;
    if(inital_count > 0) {
        sort_runs[0] = recs;
        lens[0] = inital_count;
        run_cnt += 1;
    }
    for(int i = inital_count; i < total_count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = recs + i;
        lens[run_cnt] = (i + PIECE_SIZE <= total_count) ? PIECE_SIZE : total_count - i;
        run_cnt += 1;
    }

    KWayMerge(sort_runs, lens, run_cnt, out);
    return ;
}

void WOLeaf::DoSplit(_key_t * split_key, WOLeaf ** split_node) {
    std::vector<Record> data;
    data.reserve(inital_count + insert_count);
    Dump(data);

    int pid = getSubOptimalSplitkey(data, inital_count + insert_count);
    // creat two new nodes
    WOLeaf * left = new WOLeaf(data.data(), pid);
    WOLeaf * right = new WOLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    left->split_k = data[pid].key;
    right->sibling = sibling;
    right->split_k = this->split_k;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);
    delete left;
}

void WOLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d, %d)[", prefix.c_str(), node_type, inital_count + insert_count);
    for(int i = 0; i < out.size(); i++) {
        printf("%12.8lf, ", out[i].key);
    }
    printf("]\n");
}

} // namespace newtree