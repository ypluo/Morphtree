/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"

namespace newtree {

ROLeaf::ROLeaf() {
    node_type = ROLEAF;
    stats = ROSTATS;
    count = 0;
    of_count = 0;

    recs = new Record[NODE_SIZE + OVERFLOW_SIZE * sizeof(oftree_type *) / sizeof(Record)];
    oftree_type ** overflow = (oftree_type **)(recs + NODE_SIZE);
    for(int i = 0; i < OVERFLOW_SIZE; i++)
        overflow[i] = new oftree_type;

    slope = ((double)NODE_SIZE - 1) / MAX_KEY;
    intercept = 0;
}

ROLeaf::ROLeaf(Record * recs_in, int num) {
    node_type = ROLEAF;
    stats = ROSTATS;
    count = 0;
    of_count = 0;

    LinearModelBuilder model;
    for(int i = PROBE_SIZE; i < num - PROBE_SIZE; i++) {
        model.add(recs_in[i].key, i);
    }
    model.build();

    // caculate the linear model
    slope = model.a_ * NODE_SIZE / num;
    intercept = model.b_ * NODE_SIZE / num;     
    recs = new Record[NODE_SIZE + OVERFLOW_SIZE * sizeof(oftree_type *) / sizeof(Record)];
    oftree_type ** overflow = (oftree_type **)(recs + NODE_SIZE);
    for(int i = 0; i < OVERFLOW_SIZE; i++)
        overflow[i] = new oftree_type;

    for(int i = 0; i < num; i++) {
        this->Store(recs_in[i].key, recs_in[i].val, nullptr, nullptr);
    }
}

ROLeaf::~ROLeaf() {
    oftree_type ** overflow = (oftree_type **)(recs + NODE_SIZE);
    for(int i = 0; i < OVERFLOW_SIZE; i++) {
        delete overflow[i];
    }
    delete recs;
}

bool ROLeaf::Store(_key_t k, _val_t v, _key_t * split_key, ROLeaf ** split_node) {
    int predict = Predict(k);
    int start = predict / PROBE_SIZE * PROBE_SIZE;

    int i;
    for (i = start; i < start + PROBE_SIZE; i++) {
        if(recs[i].key > k)
            break;
    }

    // try to find a empty slot
    Record last_one = recs[start + PROBE_SIZE - 1];
    if(last_one.key == MAX_KEY) { // there is an empty slot
        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (start + PROBE_SIZE - 1 - i));
        recs[i] = Record(k, v);
        count += 1;
    } else {
        if(last_one.key > k) { // kick the last record into overflow node
            memmove(&recs[i + 1], &recs[i], sizeof(Record) * (start + PROBE_SIZE - 1 - i));
            recs[i] = Record(k, v); 
            k = last_one.key;
            v = last_one.val;
        }

        int overflow_idx = start / PROBE_SIZE / SHARING;
        oftree_type ** overflow = (oftree_type **)(recs + NODE_SIZE);
        overflow[overflow_idx]->insert(k, v);
        count += 1;
        of_count += 1;
    }

    // finish insertion
    if(ShouldSplit() && split_key != nullptr) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool ROLeaf::Lookup(_key_t k, _val_t &v) {
    int predict = Predict(k);
    int start = predict & ~(PROBE_SIZE - 1);

    int i;
    for (i = start; i < start + PROBE_SIZE; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            return true;
        } else if(recs[i].key > k) {
            return false;
        }
    }
    
    int overflow_idx = start / PROBE_SIZE / SHARING;
    oftree_type ** overflow = (oftree_type **)(recs + NODE_SIZE);
    auto iter = overflow[overflow_idx]->find(k);
    if(iter != overflow[overflow_idx]->end()) {
        v = iter->second;
        return true;
    } else {
        return false;
    }
}

void ROLeaf::Dump(std::vector<Record> & out) {
    for(int i = 0; i < OVERFLOW_SIZE; i++) {
        oftree_type ** overflow = (oftree_type **)(recs + NODE_SIZE);
        auto iter = overflow[i]->begin();
        auto end = overflow[i]->end();
        int start = i * PROBE_SIZE * SHARING;
        int base_end = start + PROBE_SIZE * SHARING;

        // find the fisrt valid record in recs
        while(recs[start].key == MAX_KEY && start < base_end) {
            start = start / PROBE_SIZE * PROBE_SIZE + PROBE_SIZE;
        }
        // do two way merge
        while(start < base_end && iter != end) {
            if(recs[start].key < iter->first) {
                out.push_back(recs[start]);
                start += 1;
                while(recs[start].key == MAX_KEY && start < base_end) {
                    start = start / PROBE_SIZE * PROBE_SIZE + PROBE_SIZE;
                }
            } else {
                out.push_back({iter->first, iter->second});
                iter++;
            }
        }

        while(start < base_end) {
            out.push_back(recs[start]);
            start += 1;
            while(recs[start].key == MAX_KEY && start < base_end) {
                start = start / PROBE_SIZE * PROBE_SIZE + PROBE_SIZE;
            }
        }

        while(iter != end) {
            out.push_back({iter->first, iter->second});
            iter++;
        }
    }
}

void ROLeaf::Print(string prefix) {
    std::vector<Record> out;
    out.reserve(count);
    Dump(out);

    printf("%s(%d %5.4lf)[", prefix.c_str(), count, (double)of_count/count);
    // for(int i = 0; i < out.size(); i++) {
    //     printf("%lf, ", out[i].key);
    // }
    printf("]\n");
}

void ROLeaf::DoSplit(_key_t * split_key, ROLeaf ** split_node) {
    std::vector<Record> data;
    data.reserve(count);
    Dump(data);
    assert(data.size() == count);

    int pid = getSubOptimalSplitkey(data, data.size());
    // creat two new nodes
    ROLeaf * left = new ROLeaf(data.data(), pid);
    ROLeaf * right = new ROLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    left->split_k = data[pid].key;
    right->sibling = this->sibling;
    right->split_k = this->split_k;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);
    delete left;
}

} // namespace newtree