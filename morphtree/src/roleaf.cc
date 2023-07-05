/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"
#include "epoch.h"

namespace morphtree {

// Overflow node
struct OFNode {
    uint16_t len;
    uint16_t cap;
    uint32_t unused;
    Record recs_[0];
    
    OFNode(): len(0), cap(0) {}

    bool Store(const _key_t & k, uint64_t v) {
        // this function can be slow
        if(len < cap) {
            uint16_t i;
            for(i = 0; i < len; i++) {
                if(recs_[i].key >= k) {
                    break;
                }
            }

            if(recs_[i].key == k) { // upsert 
                recs_[i].val = v;
                return true;
            }

            memmove(&recs_[i + 1], &recs_[i], sizeof(Record) * (len - i));
            recs_[i] = Record(k, v);
            len += 1;

            return true;
        } else {
            return false;
        }
    }

    bool Lookup(const _key_t & k, uint64_t & v) {
        if(len < 64) {
            // scan search
            uint16_t i;
            for(i = 0; i < len; i++) {
                if(recs_[i].key >= k) {
                    break;
                }
            }

            if (i < len && recs_[i].key == k) {
                v = recs_[i].val;
                return true;
            } else {
                return false;
            }
        } else {
            return BinSearch(recs_, len, k, v);
        }
    }

    bool update(const _key_t & k, uint64_t v) {
        if(len < 64) {
            // scan search
            uint16_t i;
            for(i = 0; i < len; i++) {
                if(recs_[i].key >= k) {
                    break;
                }
            }

            if (recs_[i].key == k) {
                recs_[i].val = v;
                return true;
            } else {
                return false;
            }
        } else {
            auto binary_update = [&v](Record & r) {
                r.val = v;
                return true;
            };
            return BinSearch_CallBack(recs_, len, k, binary_update);
        }
    }

    bool remove(const _key_t & k) {
        // this function might be slow
        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs_[i].key >= k) {
                break;
            }
        }

        if(i < len && recs_[i].key == k) {
            memmove(&recs_[i], &recs_[i + 1], (len - 1 - i) * sizeof(Record));
            len -= 1;
            return true;
        } else {
            return false;
        }
    }
};

ROLeaf::ROLeaf() {
    node_type = NodeType::ROLEAF;
    next_node_type = NodeType::ROLEAF;
    stats = ROSTATS;
    count = 0;
    sibling = nullptr;
    shadow = nullptr;
    mysplitkey = MAX_KEY;

    slope = (double)(NODE_SIZE - 1) / MAX_KEY;
    intercept = 0;
    recs = new Record[NODE_SIZE];
}

ROLeaf::ROLeaf(Record * recs_in, int num) {
    node_type = NodeType::ROLEAF;
    next_node_type = NodeType::ROLEAF;
    stats = ROSTATS;
    count = num;
    sibling = nullptr;
    shadow = nullptr;
    mysplitkey = MAX_KEY;

    // caculate the linear model
    LinearModelBuilder model;
    for(int i = num / 8; i < num * 7 / 8; i++) {
        model.add(recs_in[i].key, i);
    }
    model.build();

    // caculate the linear model
    slope = model.a_ * NODE_SIZE / num;
    intercept = model.b_ * NODE_SIZE / num;
    recs = new Record[NODE_SIZE];

    for(int i = 0; i < num; i++) {
        int predict = Predict(recs_in[i].key, slope, intercept) / PROBE_SIZE * PROBE_SIZE;
        this->Append(recs_in[i].key, recs_in[i].val, predict);
    }
}

ROLeaf::ROLeaf(double a, double b) {
    node_type = NodeType::ROLEAF;
    next_node_type = NodeType::ROLEAF;
    stats = ROSTATS;
    count = 0;
    sibling = nullptr;
    shadow = nullptr;
    mysplitkey = MAX_KEY;

    slope = a;
    intercept = b;
    recs = new Record[NODE_SIZE];
}

ROLeaf::~ROLeaf() {
    for(int i = 0; i < NODE_SIZE; i += PROBE_SIZE) {
        OFNode * ofnode = (OFNode *) (recs[i + PROBE_SIZE - 1].val & POINTER_MARK);
        if(ofnode != nullptr){
            delete (char *)(recs[i + PROBE_SIZE - 1].val & POINTER_MARK);
        }
    }

    delete recs;
}

void ROLeaf::Append(const _key_t k, const uint64_t v, int predict) {
    int bucket_end = predict + PROBE_SIZE - 1;
    _key_t kk = k;
    uint64_t vv = v;
    int i = 0;
    for (i = predict; i < bucket_end; i++) {
        if(recs[i].key > kk)
            break;
    }

    if(i < bucket_end && recs[i].key == kk) { // upsert
        recs[i].val = vv;
        return ;
    }

    // try to find a empty slot
    Record last_one = recs[predict + PROBE_SIZE - 2];
    if(last_one.key == MAX_KEY) { 
        // there is an empty slot
        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 2 - i));
        recs[i] = Record(kk, vv);
    } else {
        // no empty slot found
        OFNode * ofnode = (OFNode *) (recs[bucket_end].val & POINTER_MARK);
        if (ofnode == nullptr) {
            ofnode = (OFNode *) new char[sizeof(OFNode) + 8 * sizeof(Record)];
            Record * _discard = new(ofnode->recs_) Record[8]; // just for initializition

            ofnode->cap = 8;
            ofnode->len = 0;
            recs[bucket_end].val = (recs[bucket_end].val & LOCK_MARK) + (uint64_t) ofnode;
        }

        if(last_one.key > kk) { // kick the last record into overflow node
            memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 2 - i));
            recs[i] = Record(kk, vv); 
            kk = last_one.key;
            vv = last_one.val;
        }

        // store the target record into overflow node
        if(!ofnode->Store(kk, vv)) {
            // the overflow node is full
            OFNode * old_ofnode = ofnode;
            
            // create a new overflow node, two times the formal one
            int16_t newcap = old_ofnode->cap * 2;
            ofnode = (OFNode *) new char[sizeof(OFNode) + newcap * sizeof(Record)];
            Record * _discard = new(ofnode->recs_) Record[newcap]; // just for initializition
            
            // copy records into it
            ofnode->cap = newcap;
            ofnode->len = old_ofnode->len;
            memcpy(ofnode->recs_, old_ofnode->recs_, sizeof(Record) * ofnode->len);
            ofnode->Store(kk, vv);
            recs[bucket_end].val = (recs[bucket_end].val & LOCK_MARK) + (uint64_t) ofnode;

            ebr->scheduleForDeletion(ReclaimEle{old_ofnode, true});
        }
    }
}

bool ROLeaf::DeAppend(const _key_t k, int predict) {
    int bucket_end = predict + PROBE_SIZE - 1;
    int i;
    for (i = predict; i < bucket_end; i++) {
        if(recs[i].key >= k)
            break;
    }

    OFNode * ofnode = (OFNode *) (recs[bucket_end].val & POINTER_MARK);
    if(i < bucket_end) {
        if(recs[i].key > k) 
            return false;

        // found the record in inline bucket
        memmove(&recs[i], &recs[i + 1], (predict + PROBE_SIZE - 2 - i) * sizeof(Record));
        recs[predict + PROBE_SIZE - 2] = Record();

        if(ofnode != nullptr) {
            // shift one record from ofnode into inline bucket
            recs[predict + PROBE_SIZE - 2] = ofnode->recs_[0];
            ofnode->remove(ofnode->recs_[0].key);
            if(ofnode->len == 0) { // delete the ofnode if necessary
                recs[bucket_end].val &= LOCK_MARK;
                ebr->scheduleForDeletion(ReclaimEle{ofnode, true});
            }
            return true;
        }

        return true;
    } else if (ofnode != nullptr) {  
        bool foundIf = ofnode->remove(k);
        if(ofnode->len == 0) { // delete the ofnode if necessary
            recs[bucket_end].val &= LOCK_MARK;
            ebr->scheduleForDeletion(ReclaimEle{ofnode, true});
        }
        return foundIf;
    } else {
        return false;
    }
}

bool ROLeaf::Store(const _key_t & k, uint64_t v, _key_t * split_key, ROLeaf ** split_node) {
    auto shadow_st = shadow;
    if(shadow_st != nullptr) { // this node is under morphing
        return ((WOLeaf *)shadow_st)->Store(k, v, split_key, (WOLeaf **)split_node);
    }

    roleaf_store_retry:
    // critical section, write and read to header are safe
    headerlock.RLock();
        if(node_type != ROLEAF) {
            // the node is changed to another type
            headerlock.UnLock();
            return ((WOLeaf *)this)->Store(k, v, split_key, (WOLeaf **)split_node);
        }

        if(k >= mysplitkey) {
            headerlock.UnLock();
            return sibling->Store(k, v, split_key, (BaseNode **)split_node);
        }

        uint32_t cur_count;
        if(count >= GLOBAL_LEAF_SIZE) {
            headerlock.UnLock();
            std::this_thread::yield();
            goto roleaf_store_retry;
        } else {
            int predict = Predict(k, slope, intercept);
            predict = predict / PROBE_SIZE * PROBE_SIZE;
            int bucket_end = predict + PROBE_SIZE - 1;
            
            ExtVersionLock::Lock(&recs[bucket_end].val);
            this->Append(k, v, predict);
            if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                // a better way is to retry and insert the record into its shadow node
                this->DeAppend(k, predict);
                ExtVersionLock::UnLock(&recs[bucket_end].val);
                headerlock.UnLock();
                goto roleaf_store_retry;
            } else {
                ExtVersionLock::UnLock(&recs[bucket_end].val);
                cur_count = __atomic_fetch_add(&count, 1, __ATOMIC_RELEASE);
            }
        }
    headerlock.UnLock();
    
    if(cur_count + 1 == GLOBAL_LEAF_SIZE) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }    
}

bool ROLeaf::Lookup(const _key_t & k, uint64_t &v) {
    // critical section: read an valid header
    roleaf_lookup_retry:
    if(node_type != ROLEAF) {
        // the node is changed to another type
        return ((WOLeaf *)this)->Lookup(k, v);
    }
    auto v1 = headerlock.Version();
        barrier();
        auto recs_st = recs;
        auto slope_st = slope;
        auto intercept_st = intercept;
        auto splitkey_st = mysplitkey;
        auto sibling_st = sibling;
        auto shadow_st = shadow;
        barrier();
    auto v2 = headerlock.Version();
    if(!(v1 == v2 && v1 % 2 == 0)) goto roleaf_lookup_retry;

     // check its sibling node
    if (k >= splitkey_st) {
        return sibling_st->Lookup(k, v);
    }
    
    int predict = Predict(k, slope_st, intercept_st);
    predict = predict / PROBE_SIZE * PROBE_SIZE;

    int bucket_end = predict + PROBE_SIZE - 1;
    auto v3 = ExtVersionLock::Version(recs_st[bucket_end].val);
    barrier();
    int i;
    for (i = predict; i < bucket_end; i++) {
        if(recs_st[i].key == k) {
            v = recs_st[i].val;
            barrier();
            if(ExtVersionLock::IsLocked(recs_st[bucket_end].val) || v3 != ExtVersionLock::Version(recs_st[bucket_end].val)) 
                goto roleaf_lookup_retry;
            return true;
        } else if(recs_st[i].key > k) {
            break; // not found here, try its shadow node
        }
    }
    
    bool found = false;
    OFNode * ofnode = (OFNode *) (recs_st[bucket_end].val & POINTER_MARK);
    if(ofnode != nullptr && i == bucket_end) {
        found = ofnode->Lookup(k, v);
    } 
    
    barrier();
    if(ExtVersionLock::IsLocked(recs_st[bucket_end].val) || v3 != ExtVersionLock::Version(recs_st[bucket_end].val)) 
        goto roleaf_lookup_retry;
    
    if(found == true) {
        return true;
    } else {
        if(shadow_st != nullptr)
            return ((WOLeaf *)shadow_st)->Lookup(k, v);
        else
            return false;
    }
}

bool ROLeaf::Update(const _key_t & k, uint64_t v) {
    roleaf_update_retry:
    auto shadow_st = shadow;
    if(shadow_st != nullptr) { // this node is under morphing
        return ((WOLeaf *)shadow_st)->Update(k, v);
    }

    // critical section, write and read to header are safe
    headerlock.RLock();
        if(node_type != ROLEAF) {
            // the node is changed to another type
            headerlock.UnLock();
            return ((WOLeaf *)this)->Update(k, v);
        }

        if(k >= mysplitkey) {
            headerlock.UnLock();
            return sibling->Update(k, v);
        }

        int predict = Predict(k, slope, intercept);
        predict = predict / PROBE_SIZE * PROBE_SIZE;
        int bucket_end = predict + PROBE_SIZE - 1;

        ExtVersionLock::Lock(&recs[bucket_end].val);
        int i;
        for (i = predict; i < predict + PROBE_SIZE - 1; i++) {
            if(recs[i].key == k) {
                recs[i].val = v;

                ExtVersionLock::UnLock(&recs[bucket_end].val);
                if(nodelock.IsLocked()) {
                    headerlock.UnLock();
                    goto roleaf_update_retry;
                }
                return true;
            } else if(recs[i].key > k) {
                break; // not found;
            }
        }

        bool found = false;
        OFNode * ofnode = (OFNode *) (recs[bucket_end].val & POINTER_MARK);
        if(ofnode != nullptr && i == bucket_end) {  
            found = ofnode->update(k, v);
        }

        ExtVersionLock::UnLock(&recs[bucket_end].val);
        if(nodelock.IsLocked()) {
            headerlock.UnLock();
            goto roleaf_update_retry;
        }
    headerlock.UnLock();

    return found;
}

bool ROLeaf::Remove(const _key_t & k) {
    roleaf_remove_retry:
    auto shadow_st = shadow;
    if(shadow_st != nullptr) { // this node is under morphing
        return ((WOLeaf *)shadow_st)->Remove(k);
    }

    headerlock.RLock();
        if(node_type != ROLEAF) {
            // the node is changed to another type
            headerlock.UnLock();
            return ((WOLeaf *)this)->Remove(k);
        }

        if(k >= mysplitkey) {
            headerlock.UnLock();
            return sibling->Remove(k);
        }

        int predict = Predict(k, slope, intercept);
        predict = predict / PROBE_SIZE * PROBE_SIZE;
        int bucket_end = predict + PROBE_SIZE - 1;

        ExtVersionLock::Lock(&recs[bucket_end].val);
        bool found = this->DeAppend(k, predict);
        ExtVersionLock::UnLock(&recs[bucket_end].val);
        
        if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
            // a better way is to retry and insert the record into its shadow node
            headerlock.UnLock();
            goto roleaf_remove_retry;
        }

        if(found) {
            __atomic_fetch_sub(&count, 1, __ATOMIC_RELEASE);
        }
    headerlock.UnLock();

    return found;
}

int ROLeaf::Scan(const _key_t &startKey, int len, Record *result) {
    return 0;
}

void ROLeaf::Dump(std::vector<Record> & out) {
    // retrieve records from this node
    for(int i = 0; i < NODE_SIZE; i += PROBE_SIZE) {
        ExtVersionLock::Lock(&recs[i + PROBE_SIZE - 1].val);
        for(int j = 0; j < PROBE_SIZE; j++) {
            if(recs[i + j].key != MAX_KEY)
                out.push_back(recs[i + j]);
            else if(j == PROBE_SIZE - 1) {
                OFNode * ofnode = (OFNode *) (recs[i + j].val & POINTER_MARK);
                if(ofnode != nullptr)
                    out.insert(out.end(), ofnode->recs_, ofnode->recs_ + ofnode->len);
            } else 
                break;
        }
        ExtVersionLock::UnLock(&recs[i + PROBE_SIZE - 1].val);
    }
}

int ROLeaf::Dump(int i, Record * out) {
    int base = i * PROBE_SIZE;
    uint16_t total_len = 0;
    ExtVersionLock::Lock(&recs[base + PROBE_SIZE - 1].val);
    for(int j = 0; j < PROBE_SIZE; j++) {
        if(recs[base + j].key != MAX_KEY) {
            out[total_len] = recs[base + j];
            total_len += 1;
        }
        else if(j == PROBE_SIZE - 1) {
            OFNode * ofnode = (OFNode *) (recs[base + j].val & POINTER_MARK);
            if(ofnode != nullptr) {
                memcpy(out + total_len, ofnode->recs_, sizeof(Record) * ofnode->len);
                total_len += ofnode->len;
            }
        } else { 
            break;
        }
    }
    
    ExtVersionLock::UnLock(&recs[base + PROBE_SIZE - 1].val);
    return total_len;
}

void ROLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d, (%d))[", prefix.c_str(), node_type, count);
    for(int i = 0; i < out.size(); i++) {
        printf("(%12.8lf %lu), ", out[i].key, out[i].val);
    }
    printf("]\n");
}

void ROLeaf::DoSplit(_key_t * split_key, ROLeaf ** split_node) {
    // lock-based split operation
    nodelock.Lock();
    
    std::vector<Record> data;
    data.reserve(count);
    Dump(data);
    
    int pid = getSubOptimalSplitkey(data.data(), data.size());
    // creat two new nodes
    ROLeaf * left = new ROLeaf(data.data(), pid);
    
    ROLeaf * right = new ROLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    right->sibling = sibling;
    left->mysplitkey = data[pid].key;
    right->mysplitkey = this->mysplitkey;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    left->nodelock.Lock();
    left->headerlock.WLock();
    headerlock.WLock();
    SwapNode(this, left);
    headerlock.UnWLock();
    nodelock.UnLock();
}

} // namespace morphtree