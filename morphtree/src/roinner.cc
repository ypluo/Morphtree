/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>
#include <thread>
#include <iomanip>
#include <glog/logging.h>
#include "node.h"

namespace morphtree {

const uint64_t LOCK_MARK    = 0xffff000000000000;
const uint64_t POINTER_MARK = 0x0000ffffffffffff;
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
    int i, last_i = 0, cid = Predict(recs_in[0].key, slope, intercept) / PROBE_SIZE;
    for(i = 1; i < num; i++) {
        int predict = Predict(recs_in[i].key, slope, intercept);
        if((predict / PROBE_SIZE) != cid) {
            int c = i - last_i;

            if(c <= PROBE_SIZE) {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
            } else {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1].key = recs_in[last_i + PROBE_SIZE - 1].key;
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1].val = uint64_t(new ROInner(&recs_in[last_i + PROBE_SIZE - 1], c - PROBE_SIZE + 1));
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
        recs[cid * PROBE_SIZE + PROBE_SIZE - 1].val = uint64_t(new ROInner(&recs_in[last_i + PROBE_SIZE - 1], c - PROBE_SIZE + 1));
        of_count += c - PROBE_SIZE + 1;
    }
}

ROInner::~ROInner() {
    for(int i = PROBE_SIZE - 1; i < capacity; i += PROBE_SIZE) {
        BaseNode * child = (ROInner *) (recs[i].val & POINTER_MARK);
        if(child != nullptr && !child->Leaf()) {
            child->DeleteNode();
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
            printf("(%lf,%lu), ", recs[i].key, recs[i].val);
    }
    printf("]\n");
    for(int i = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY) {
            BaseNode * child = (BaseNode *) (recs[i].val & POINTER_MARK);
            child->Print(prefix + "\t");
        }
    }
}

bool ROInner::Store(const _key_t & k, uint64_t v, _key_t * split_key, ROInner ** split_node) {
    if(count < BNODE_SIZE) {
        nodelock.Lock();
        int i;
        for(i = 0; i < count; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (count - i));
        if(recs[i].key == k) {
            recs[i].val = v;
        } else {
            recs[i] = {k, v};
            count += 1;
        }

        if(count == BNODE_SIZE) { // create a new inner node
            ROInner * new_inner = new ROInner(recs, count);
            new_inner->nodelock.Lock();
            new_inner->headerlock.WLock();
            headerlock.WLock();
            SwapNode(this, new_inner);
            headerlock.UnLock();
        }

        nodelock.UnLock();

        return false;
    } else {
        roinner_store_retry:
        
        headerlock.RLock();
        
        int predict = Predict(k, slope, intercept);
        predict = (predict / PROBE_SIZE) * PROBE_SIZE;
        int i;
        for (i = predict; i < predict + PROBE_SIZE; i++) {
            if(recs[i].key >= k)
                break;
        }

        // lock the coresponding bucket
        ExtVersionLock::Lock(&recs[predict].val);
        if(recs[i].key == k) { // reinsert from retry
            recs[i].val = v;
            ExtVersionLock::UnLock(&recs[predict].val);
            if(nodelock.IsLocked()) {
                headerlock.UnLock();
                std::this_thread::yield();
                goto roinner_store_retry;
            } else {
                headerlock.UnLock();
                return false;
            }
        } 

        Record last_one = recs[predict + PROBE_SIZE - 1];
        if(i == predict) { // the insert record will rewrite the bucket lock
            v = (recs[predict].val & LOCK_MARK) + v;
        }
        bool not_overflow = false;
        if(last_one.key == MAX_KEY) { // there is an empty slot
            memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 1 - i));
            recs[i] = Record(k, v);
        } else {
            not_overflow = true;
            BaseNode * rightmost = (BaseNode *)(last_one.val & POINTER_MARK);
            if(rightmost->Leaf()) { // has no overflow inner node
                // copy records in this bucket into a tmp array
                Record tmp[PROBE_SIZE + 1];
                memcpy(tmp, &recs[predict], sizeof(Record) * PROBE_SIZE);
                memmove(&tmp[i + 1 - predict], &tmp[i - predict], sizeof(Record) * (predict + PROBE_SIZE - i));
                tmp[i - predict] = Record(k, v);
                
                // rearange the records in this bucket
                memcpy(&recs[predict], tmp, sizeof(Record) * (PROBE_SIZE - 1));
                tmp[PROBE_SIZE - 1].val &= POINTER_MARK;
                tmp[PROBE_SIZE].val &= POINTER_MARK;
                ROInner * new_inner = new ROInner(&tmp[PROBE_SIZE - 1], 2);
                recs[predict + PROBE_SIZE - 1].key = tmp[PROBE_SIZE - 1].key;
                recs[predict + PROBE_SIZE - 1].val = (uint64_t)new_inner;
            } else { // has a overflow inner node
                if(i < predict + PROBE_SIZE - 1) {
                    _key_t new_k = recs[predict + PROBE_SIZE - 2].key;
                    uint64_t new_v = (recs[predict + PROBE_SIZE - 2].val & POINTER_MARK);
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
            }
            __atomic_fetch_add(&of_count, 1, __ATOMIC_RELAXED);
        }
        __atomic_fetch_add(&count, 1, __ATOMIC_RELAXED);

        ExtVersionLock::UnLock(&recs[predict].val);

        if(nodelock.IsLocked()) {
            headerlock.UnLock();
            std::this_thread::yield();
            __atomic_fetch_sub(&count, 1, __ATOMIC_RELAXED);
            if(not_overflow) __atomic_fetch_sub(&of_count, 1, __ATOMIC_RELAXED);
            goto roinner_store_retry;
        }
        headerlock.UnLock();

        if(ShouldRebuild()) {
            RebuildSubTree();
            return true;
        }

        return false;
    }
}

bool ROInner::Lookup(const _key_t & k, uint64_t &v) {
    roinner_lookup_retry:
    if(count < BNODE_SIZE) {
        auto v1 = nodelock.Version();
        int i;
        for(i = 0; i < BNODE_SIZE; i++) {
            if(recs[i].key > k) {
                break;
            }
        }
        v = (recs[i - 1].val & POINTER_MARK);
        if(nodelock.IsLocked() || v1 != nodelock.Version()) goto roinner_lookup_retry;

        return true;
    } else {
        auto v1 = headerlock.Version();
            Record * recs_snapshot = recs;
            double slope_st = slope;
            double intercept_st = intercept;
        auto v2 = headerlock.Version();
        if(!(v1 == v2 && v1 % 2 == 0)) goto roinner_lookup_retry;

        int predict = Predict(k, slope_st, intercept_st);
        predict = (predict / PROBE_SIZE) * PROBE_SIZE;

        // probe left: if k is less than the minimal key in current bucket
        while(recs_snapshot[predict].key > k) {
            predict -= PROBE_SIZE;
        }

        roinner_lookup_retry2:
        auto v3 = ExtVersionLock::Version(recs_snapshot[predict].val);
        // probe right by 1 to find a proper index record
        int i = predict + 1;
        for (; i < predict + PROBE_SIZE; i++) {
            if(recs_snapshot[i].key > k) {
                v = (recs_snapshot[i - 1].val & POINTER_MARK);
                if(ExtVersionLock::IsLocked(recs_snapshot[predict].val) || v3 != ExtVersionLock::Version(recs_snapshot[predict].val)) goto roinner_lookup_retry2;

                DLOG_IF(FATAL, v == 0) << "The child pointer is invalid " << " Key " << k << " at slot " << predict;
                return true;
            }
        }

        // this bucket is probed
        v = (recs_snapshot[i - 1].val & POINTER_MARK);
        if(ExtVersionLock::IsLocked(recs_snapshot[predict].val) || v3 != ExtVersionLock::Version(recs_snapshot[predict].val)) goto roinner_lookup_retry2;
        
        DLOG_IF(FATAL, v == 0) << "The child pointer is invalid " << " Key " << k << " at slot " << predict;
        return true;
    }
}

void ROInner::RebuildSubTree() {
    nodelock.Lock();
    std::vector<Record> all_record;
    all_record.reserve(count);
    Dump(all_record);
    ROInner * new_inner = new ROInner(all_record.data(), all_record.size());
    new_inner->nodelock.Lock();
    new_inner->headerlock.WLock();
    this->headerlock.WLock();
    SwapNode(this, new_inner);
    this->headerlock.UnLock();
    nodelock.UnLock();
}

void ROInner::Dump(std::vector<Record> & out) {
    if(count < BNODE_SIZE) {
        for(int i = 0; i < count; i++) {
            out.emplace_back(recs[i].key, recs[i].val & POINTER_MARK);
        }
    } else {
        for(int i = 0; i < capacity; i += PROBE_SIZE) {
        ExtVersionLock::Lock(&recs[i].val);
        for(int j = 0; j < PROBE_SIZE; j++) {
            if(recs[i + j].key == MAX_KEY) {
                break;
            } else if(j == PROBE_SIZE - 1) {
                BaseNode * node = (BaseNode *) (recs[i + j].val & POINTER_MARK);
                if(!node->Leaf()) {
                    ((ROInner *)node)->Dump(out);
                } else {
                    out.emplace_back(recs[i + j].key, recs[i + j].val & POINTER_MARK);
                }
            } else {
                out.emplace_back(recs[i + j].key, recs[i + j].val & POINTER_MARK);
            }
        }
        ExtVersionLock::UnLock(&recs[i].val);
    }
}

}
} // namespace morphtree