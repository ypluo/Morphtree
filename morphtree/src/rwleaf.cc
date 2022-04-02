/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>

#include "node.h"

namespace morphtree {

RWLeaf::RWLeaf() {
    node_type = NodeType::RWLEAF;
    stats = RWSTATS;
    count = 0;
    buf_count = 0;
    sorted_count = 0;

    recs = new Record[NODE_SIZE];
    buffer = new Record[BUFFER_SIZE];
    slope = 0;
    intercept = 0;
}

RWLeaf::RWLeaf(Record * recs_in, int num) {
    node_type = NodeType::RWLEAF;
    stats = RWSTATS;
    count = num;
    buf_count = 0;
    sorted_count = 0;

    recs = new Record[NODE_SIZE];
    buffer = new Record[BUFFER_SIZE];
    LinearModelBuilder model;
    for(int i = 0; i < num; i++) {
        recs[i] = recs_in[i];
        model.add(recs_in[i].key, i);
    }
    model.build();

    // caculate the linear model
    slope = model.a_ ;
    intercept = model.b_;
}

RWLeaf::~RWLeaf() {
    delete [] recs;
    delete [] buffer;
}

bool RWLeaf::Store(_key_t k, _val_t v, _key_t * split_key, RWLeaf ** split_node) {
    buffer[buf_count++] = {k, v};
    count += 1;

    if(buf_count % PIECE_SIZE == 0) {
        std::sort(buffer + sorted_count, buffer + buf_count);
        sorted_count = buf_count;
    }

    if(count == GLOBAL_LEAF_SIZE) { // to do split
        DoSplit(split_key, split_node);
        return true;
    } else if (buf_count == BUFFER_SIZE) { // buffer is full, merge it with recs
        std::vector<Record> data;
        data.reserve(count);
        Dump(data);

        RWLeaf * new_node = new RWLeaf(data.data(), data.size());
        SwapNode(this, new_node);
        delete new_node;

        return false;
    } else {
        return false;
    }
}

bool RWLeaf::Lookup(_key_t k, _val_t & v) {
    int predict = Predict(k);
    if(ExpSearch(recs, count - buf_count, predict, k, v)) {
        // search in the learned records
        return true;
    } else {
        // search in the buffer
        for(int i = 0; i < sorted_count; i += PIECE_SIZE) {
            if(BinSearch(buffer + i, PIECE_SIZE, k, v)) {
                return true;
            }
        }

        // second part of the buffer
        for(int i = sorted_count; i < buf_count; i++) {
            if(buffer[i].key == k) {
                v = buffer[i].val;
                return true;
            }
        }
        return false;
    }
}

void RWLeaf::Dump(std::vector<Record> & out) {
    static const int MAX_RUN_NUM = BUFFER_SIZE / PIECE_SIZE;
    static Record * sort_runs[MAX_RUN_NUM + 1];
    static int lens[MAX_RUN_NUM + 1];

    std::sort(buffer + sorted_count, buffer + buf_count);

    // prepare three sorted runs
    sort_runs[0] = recs;
    lens[0] = count - buf_count;

    int run_cnt = 1;
    for(int i = 0; i < buf_count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = buffer + i;
        lens[run_cnt] = (i + PIECE_SIZE <= buf_count ? PIECE_SIZE : buf_count - i);
        run_cnt += 1;
    }

    KWayMerge(sort_runs, lens, run_cnt, out);
}

void RWLeaf::Print(string prefix) {
    std::vector<Record> out;
    out.reserve(count);
    Dump(out);

    printf("%s(%d)[", prefix.c_str(), node_type);
    for(int i = 0; i < out.size(); i++) {
        printf("%lu, ", out[i].key);
    }
    printf("]\n");
}

void RWLeaf::DoSplit(_key_t * split_key, RWLeaf ** split_node) {
    std::vector<Record> data;
    data.reserve(count);
    Dump(data);

    // creat two new nodes
    RWLeaf * left = new RWLeaf(data.data(), count / 2);
    RWLeaf * right = new RWLeaf(data.data() + count / 2, count - count / 2);
    left->sibling = right;
    right->sibling = sibling;

    // update splitting info
    *split_key = data[count / 2].key;
    *split_node = right;

    SwapNode(this, left);
    delete left;
}

} // namespace morphtree