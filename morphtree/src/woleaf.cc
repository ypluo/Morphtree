/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <algorithm>
#include <vector>
#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {
    
WOLeaf::WOLeaf() {
    node_type = NodeType::WOLEAF;
    stats = WOSTATS;

    recs = new Record[NODE_SIZE];
    count = 0;
    sorted_count = 0;
}

WOLeaf::WOLeaf(Record * recs_in, int num) {
    node_type = NodeType::WOLEAF;
    stats = WOSTATS;

    recs = new Record[NODE_SIZE];
    memcpy(recs, recs_in, sizeof(Record) * num);
    count = num;
    sorted_count = num;
}

WOLeaf::~WOLeaf() {
    delete [] recs;
}

bool WOLeaf::Store(_key_t k, _val_t v, _key_t * split_key, WOLeaf ** split_node) {
    recs[count++] = {k, v};

    if(count % PIECE_SIZE == 0) {
        int32_t start = count - PIECE_SIZE;
        std::sort(recs + start, recs + count);
        sorted_count = count;
    }
    
    if(count < GLOBAL_LEAF_SIZE) {
        return false;
    } else {
        // find global median number
        std::vector<_key_t> medians;
        for(int i = 0; i < count; i += PIECE_SIZE) {
            medians.push_back(recs[i + PIECE_SIZE / 3].key);
            medians.push_back(recs[i + PIECE_SIZE * 2 / 3].key);
        }
        *split_key = GetMedian(medians);
        
        // create two new nodes
        WOLeaf * left_node = new WOLeaf;
        *split_node = new WOLeaf;

        // split the data
        for(int i = 0; i < count; i++) {
            if(recs[i].key < *split_key)
                left_node->Store(recs[i].key, recs[i].val, nullptr, nullptr);
            else
                (*split_node)->Store(recs[i].key, recs[i].val, nullptr, nullptr);
        }

        // update siblings
        left_node->sibling = *split_node;
        (*split_node)->sibling = this->sibling;

        // Swap the old left node and the new
        SwapNode(this, left_node);
        delete left_node;
        
        return true;
    }
}

bool WOLeaf::Lookup(_key_t k, _val_t &v) {
    // do binary search in all sorted runs
    int32_t bin_end = count / PIECE_SIZE * PIECE_SIZE;
    for(int i = 0; i < bin_end; i += PIECE_SIZE) {
        if(BinSearch(recs + i, PIECE_SIZE, k, v)) {
            return true;
        }
    }
    if(BinSearch(recs + bin_end, sorted_count - bin_end, k, v)) {
        return true;
    }

    // do scan in unsorted runs
    for(int i = sorted_count; i < count; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            // sort the unsorted runs if it is big
            if(count - sorted_count > PIECE_SIZE / 4) {
                std::sort(recs + bin_end, recs + count);
                sorted_count = count;
            }
            return true;
        }
    }

    // sort the unsorted runs if it is big
    if(count - sorted_count > PIECE_SIZE / 4) {
        std::sort(recs + bin_end, recs + count);
        sorted_count = count;
    }
    return false;
}

void WOLeaf::Dump(std::vector<Record> & out) {
    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / PIECE_SIZE;
    static Record * sort_runs[MAX_RUN_NUM];
    static int lens[MAX_RUN_NUM];

    if(sorted_count != count) {
        int32_t bin_end = count / PIECE_SIZE * PIECE_SIZE;
        std::sort(recs + bin_end, recs + count);
    }

    int run_cnt = 0;
    for(int i = 0; i < count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = recs + i;
        lens[run_cnt] = (i + PIECE_SIZE <= count ? PIECE_SIZE : count - i);
        run_cnt += 1;
    }

    KWayMerge(sort_runs, lens, run_cnt, out);
    return ;
}

void WOLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d)[", prefix.c_str(), node_type);
    for(int i = 0; i < out.size(); i++) {
        printf("%lf, ", out[i].key);
    }
    printf("]\n");
}

} // namespace morphtree