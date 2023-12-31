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
    for(i = 1; i < num; i++) {
        int predict = Predict(recs_in[i].key);
        if((predict / PROBE_SIZE) != cid) {
            int c = i - last_i;

            if(c <= PROBE_SIZE) {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
            } else {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1].key = recs_in[last_i + PROBE_SIZE - 1].key;
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1].val = new ROInner(&recs_in[last_i + PROBE_SIZE - 1], c - PROBE_SIZE + 1);
                of_count += c - PROBE_SIZE + 1;
            }

            // go next cacheline
            last_i = i;
            cid = predict / PROBE_SIZE;
        }
    }
    int c = i - last_i;
    if(c <= PROBE_SIZE) {
        memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
    } else {
        memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
        recs[cid * PROBE_SIZE + PROBE_SIZE - 1].key = recs_in[last_i + PROBE_SIZE - 1].key;
        recs[cid * PROBE_SIZE + PROBE_SIZE - 1].val = new ROInner(&recs_in[last_i + PROBE_SIZE - 1], c - PROBE_SIZE + 1);
        of_count += c - PROBE_SIZE + 1;
    }
}

ROInner::~ROInner() {
    for(int i = PROBE_SIZE - 1; i < capacity; i += PROBE_SIZE) {
        BaseNode * child = (ROInner *) recs[i].val;
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
        if(i % PROBE_SIZE == PROBE_SIZE - 1) {
            printf("><");
        }
        if(recs[i].key != MAX_KEY)
            printf("%lf, ", recs[i].key);
    }
    printf("]\n");
    for(int i = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY) {
            BaseNode * child = (BaseNode *) recs[i].val;
            child->Print(prefix + "\t");
        }
    }
}

bool ROInner::Store(_key_t k, _val_t v, _key_t * split_key, ROInner ** split_node) {
    if(count < BNODE_SIZE) {
        int i;
        for(i = 0; i < count; i++) {
            if(recs[i].key > k) {
                break;
            }
        }

        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (count - i));
        recs[i] = {k, (char *) v};
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

        int i;
        for (i = predict; i < predict + PROBE_SIZE; i++) {
            if(recs[i].key > k)
                break;
        }

        Record last_one = recs[predict + PROBE_SIZE - 1];
        if(last_one.key == MAX_KEY) { // there is an empty slot
            memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 1 - i));
            recs[i] = Record(k, v);
        } else {
            BaseNode * rightmost = (BaseNode *)last_one.val;
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
                recs[predict + PROBE_SIZE - 1].val = new_inner;
                of_count += 2;
            } else { // has a overflow inner node
                if(i < predict + PROBE_SIZE - 1) {
                    _key_t new_k = recs[predict + PROBE_SIZE - 2].key;
                    _val_t new_v = recs[predict + PROBE_SIZE - 2].val;
                    memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 2 - i));
                    recs[i] = Record(k, v);
                    
                    rightmost->Store(new_k, new_v, nullptr, nullptr);
                    recs[predict + PROBE_SIZE - 1].key = new_k; // update the split key of bucket and overflow node
                } else if(i == predict + PROBE_SIZE - 1) { 
                    rightmost->Store(k, v, nullptr, nullptr);
                    recs[predict + PROBE_SIZE - 1].key = k; // update the split key of bucket and overflow node
                } else {
                    rightmost->Store(k, v, nullptr, nullptr);
                }
                of_count += 1;
            }
        }
        count += 1;

        if(shouldRebuild()) {
            RebuildSubTree();
        }
    }

    return false;
}

bool ROInner::Lookup(_key_t k, _val_t &v) {
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
        while(recs[predict].key > k) {
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
    rebuild_times += 1;
    std::vector<Record> all_record;
    all_record.reserve(count);

    // gather all the records start with this node
    for(int i = 0; i < capacity; i += PROBE_SIZE) {
        for(int j = 0; j < PROBE_SIZE; j++) {
            if(recs[i + j].key == MAX_KEY) {
                break;
            } else if(j == PROBE_SIZE - 1) {
                BaseNode * node = (BaseNode *) recs[i + j].val;
                if(!node->Leaf()) {
                    ((ROInner *)node)->Dump(all_record);
                } else {
                    all_record.push_back(recs[i + j]);
                }
            } else {
                all_record.push_back(recs[i + j]);
            }
        }
    }

    ROInner * new_inner = new ROInner(all_record.data(), all_record.size());
    SwapNode(new_inner, this);
    delete new_inner;
}

void ROInner::Dump(std::vector<Record> & out) {
    for(int i = 0; i < capacity; i += PROBE_SIZE) {
        for(int j = 0; j < PROBE_SIZE; j++) {
            if(recs[i + j].key == MAX_KEY) {
                break;
            } else if(j == PROBE_SIZE - 1) {
                BaseNode * node = (BaseNode *) recs[i + j].val;
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