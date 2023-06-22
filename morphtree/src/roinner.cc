/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {

static const int MARGIN = ROInner::PROBE_SIZE;

ROInner::ROInner(Record * recs_in, int num) {
    node_type = NodeType::ROINNER;
    count = num;
    of_count = 0;
    recs = nullptr;

    if(num < BNODE_SIZE) {
        // Use Btree Node
        capacity = BNODE_SIZE;
        recs = new Record[capacity];
        memcpy(recs, recs_in, num * sizeof(Record));
        return ;
    } else {
        capacity = (num * 3 + PROBE_SIZE - 1) / PROBE_SIZE * PROBE_SIZE;
        recs = new Record[capacity];
    }

    // train a model
    LinearModelBuilder model;
    for(int i = num / 8; i < num * 7 / 8; i++) {
        model.add(recs_in[i].key, i);
    }
    model.build();

    // store the model 
    slope = model.a_ * (capacity - 2 * MARGIN) / num;
    intercept = model.b_ * (capacity - 2 * MARGIN) / num + MARGIN;

    // populate the node with records
    int i, last_i = 0, cid = Predict(recs_in[0].key) / PROBE_SIZE;
    for(int j = 0; j < cid; j++) {
        recs[j * PROBE_SIZE].key = MIN_KEY;
    }
    Record last;
    for(i = 1; i < num; i++) {
        int predict = Predict(recs_in[i].key);
        int predict_cid = predict / PROBE_SIZE;
        if(predict_cid != cid) {
            int c = i - last_i;

            if(c <= PROBE_SIZE) {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
            } else {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1].key = recs_in[last_i + PROBE_SIZE - 1].key;
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1].val = uint64_t(new ROInner(&recs_in[last_i + PROBE_SIZE - 1], c - PROBE_SIZE + 1));
                of_count += c - PROBE_SIZE + 1;
            }

            // fill with pivot
            last = recs_in[i - 1];
            for(int j = cid + 1; j < predict_cid; j++) {
                recs[j * PROBE_SIZE] = last;
            }
            // go next cacheline
            last_i = i;
            cid = predict_cid;
        }
    }

    int c = i - last_i;
    if(c <= PROBE_SIZE) {
        memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
    } else {
        memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
        recs[cid * PROBE_SIZE + PROBE_SIZE - 1].key = recs_in[last_i + PROBE_SIZE - 1].key;
        recs[cid * PROBE_SIZE + PROBE_SIZE - 1].val = uint64_t(new ROInner(&recs_in[last_i + PROBE_SIZE - 1], c - PROBE_SIZE + 1));
        of_count += c - PROBE_SIZE + 1;
    }

    // fill with pivot
    last = recs_in[i - 1];
    for(int j = cid + 1; j < capacity / PROBE_SIZE; j++) {
        recs[j * PROBE_SIZE] = last;
    }
}

ROInner::~ROInner() {
    for(int i = PROBE_SIZE - 1; i < capacity; i += PROBE_SIZE) {
        uint64_t vv = recs[i].val;
        BaseNode * child = (ROInner *) vv;
        if(child != nullptr && !child->Leaf()) {
            ((ROInner *)child)->Clear();
            delete child;
        }
    }

    delete [] recs;
}

void ROInner::Print(string prefix) {
    printf("%s(%d, %d %d)[", prefix.c_str(), node_type, count, capacity);
    for(int i = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY){
            printf("{%10.8lf %lu}, ", recs[i].key, recs[i].val);
        }
        if(i % PROBE_SIZE == PROBE_SIZE - 1) {
            printf("><");
        }
    }
    printf("]\n");
    for(int i = 0; i < capacity; i++) {
        if(recs[i].key == MAX_KEY || (recs[i].key == MIN_KEY && recs[i].val == 0)) {
            continue;
        } else {
            uint64_t vv = recs[i].val;
            BaseNode * child = (BaseNode *) vv;
            child->Print(prefix + "\t");
        }
    }
}

bool ROInner::Store(const _key_t & k, uint64_t v, _key_t * split_key, ROInner ** split_node) {
    if(count < BNODE_SIZE) {
        int i;
        for(i = 0; i < count; i++) {
            if(recs[i].key > k) {
                break;
            }
        }

        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (count - i));
        recs[i] = {k, v};
        count += 1;

        if(count == BNODE_SIZE) { // create a new inner node
            ROInner * new_inner = new ROInner(recs, count);
            SwapNode(new_inner, this);
            new_inner->Clear();
            delete new_inner;
        }
    } else {
        int predict = Predict(k);
        predict = (predict / PROBE_SIZE) * PROBE_SIZE;
        
        // check if there is a pivot
        int next_pivot = predict + PROBE_SIZE;
        while(next_pivot < capacity && recs[next_pivot].key < k) {
            recs[next_pivot] = {k, v}; // replace all pivots in following buckets
            next_pivot += PROBE_SIZE;
        }
        if(recs[predict].key < k && Predict(recs[predict].key) < predict) {
            count += 1;
            recs[predict] = {k, v}; // replace the pivot in current bucket
            return false;
        }

        int i;
        for (i = predict; i < predict + PROBE_SIZE; i++) {
            if(recs[i].key > k)
                break;
        }

        Record last_one = recs[predict + PROBE_SIZE - 1];
        if(last_one.key == MAX_KEY) { // there is an empty slot
            memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 1 - i));
            recs[i] = Record(k, v);
            count += 1;
        } else {
            uint64_t vv = last_one.val;
            BaseNode * rightmost = (BaseNode *)vv;
            if(rightmost->Leaf()) { // has no overflow inner node
                // copy records in this bucket into a tmp array
                Record tmp[PROBE_SIZE + 1];
                memcpy(tmp, &recs[predict], sizeof(Record) * PROBE_SIZE);
                memmove(&tmp[i + 1 - predict], &tmp[i - predict], sizeof(Record) * (predict + PROBE_SIZE - i));
                tmp[i - predict] = Record(k, v);
                
                // rearange the records in this bucket
                memcpy(&recs[predict], tmp, sizeof(Record) * (PROBE_SIZE - 1));
                ROInner * new_inner = new ROInner(&tmp[PROBE_SIZE - 1], 2);
                recs[predict + PROBE_SIZE - 1].key = tmp[PROBE_SIZE - 1].key;
                recs[predict + PROBE_SIZE - 1].val = (uint64_t)new_inner;
                of_count += 2;
                count += 1;
            } else { // has a overflow inner node
                if(i < predict + PROBE_SIZE - 1) {
                    _key_t new_k = recs[predict + PROBE_SIZE - 2].key;
                    uint64_t new_v = recs[predict + PROBE_SIZE - 2].val;
                    memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 2 - i));
                    recs[i] = Record(k, v);
                    
                    rightmost->Store(new_k, new_v, nullptr, nullptr);
                    recs[predict + PROBE_SIZE - 1].key = new_k; // update the split key of bucket and overflow node
                    of_count += 1;
                    count += 1;
                } else if(i == predict + PROBE_SIZE - 1) { 
                    rightmost->Store(k, v, nullptr, nullptr);
                    recs[predict + PROBE_SIZE - 1].key = k; // update the split key of bucket and overflow node
                    of_count += 1;
                    count += 1;
                } else {
                    rightmost->Store(k, v, nullptr, nullptr);
                }
            }
        }

        if(ShouldRebuild()) {
            RebuildSubTree();
        }
    }

    return false;
}

bool ROInner::Lookup(const _key_t & k, uint64_t &v) {
    if(count < BNODE_SIZE) {
        int i;
        for(i = 0; i < BNODE_SIZE; i++) {
            if(recs[i].key > k) {
                break;
            }
        }
        v = recs[i - 1].val;
        return true;
    } else {
        int predict = Predict(k);
        predict = (predict / PROBE_SIZE) * PROBE_SIZE;

        // probe left: if k is less than the minimal key in current bucket
        if(recs[predict].key > k) {
            predict -= PROBE_SIZE;
        }

        // probe right by 1 to find a proper index record
        int i = predict + 1;
        for (; i < predict + PROBE_SIZE; i++) {
            if(recs[i].key > k) {
                v = recs[i - 1].val;
                return true;
            }
        }

        // this bucket is probed
        v = recs[i - 1].val;
        return true;
    }
}

void ROInner::RebuildSubTree() {
    std::vector<Record> all_record;
    all_record.reserve(count * 2);
    this->Dump(all_record);

    ROInner * new_inner = new ROInner(all_record.data(), all_record.size());
    SwapNode(new_inner, this);
    delete new_inner;
}

void ROInner::Dump(std::vector<Record> & out) {
    for(int i = 0; i < capacity; i += PROBE_SIZE) {
        for(int j = 0; j < PROBE_SIZE; j++) {
            if(recs[i + j].key == MAX_KEY) {
                break;
            } else if(recs[i + j].val == 0) {
                continue;
            } else if(j == 0 && i > 0 && recs[i + j].key == out.back().key) {
                // pivots
                break;
            } else if (j == PROBE_SIZE - 1) {
                // overflow nodes
                uint64_t vv = recs[i + j].val;
                BaseNode * node = (BaseNode *) vv;
                if(!node->Leaf()) {
                    ((ROInner *)node)->Dump(out);
                } else {
                    out.push_back(recs[i + j]);
                }
            } else {
                out.push_back(recs[i + j]);
            }
        }
    }
}

} // namespace morphtree