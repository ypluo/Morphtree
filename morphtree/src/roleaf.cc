/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>
#include <thread>

#include "node.h"

namespace morphtree {

struct Bucket {
    // bucket header
    VersionLock lock;
    uint16_t unused;
    uint16_t cap;
    uint16_t len;
    Record * recs;

    Bucket() {
        cap = ROLeaf::PROBE_SIZE;
        len = 0;
        recs = new Record[cap];
    }

    ~Bucket() {
        delete [] recs;
    }

    bool Append(const _key_t & k, uint64_t v) {
        // without any lock on bucket
        if(len >= cap) { // no more space
            uint16_t new_cap = cap * 3 / 2;
            Record * new_recs = new Record[new_cap];
            memcpy(new_recs, recs, sizeof(Record) * len);
            std::swap(new_recs, recs);
            cap = new_cap;
            delete [] new_recs;
        } 

        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key > k) {
                break;
            }
        }
        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (len - i));
        recs[i] = Record(k, v);

        return (++len) > ROLeaf::PROBE_SIZE;
    }

    bool Store(const _key_t & k, uint64_t v) {
        // lock-based insertion
        lock.Lock();
        if(len >= cap) { // no more space
            uint16_t new_cap = cap * 3 / 2;
            Record * new_recs = new Record[new_cap];
            memcpy(new_recs, recs, sizeof(Record) * len);
            std::swap(new_recs, recs);
            cap = new_cap;
            delete [] new_recs;
        } 

        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        if(recs[i].key == k) { // an update operation
            recs[i].val = v;
            lock.UnLock();
            return false;
        }

        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (len - i));
        recs[i] = Record(k, v);

        lock.UnLock();
        return (++len) > ROLeaf::PROBE_SIZE;
    }

    bool Lookup(const _key_t & k, uint64_t &v) {
        // lock-free lookup operation
        retry_lookup:
        auto v1 = lock.Version();
        if(len > 64) {
            bool found = BinSearch(recs, len, k, v);
            if(lock.IsLocked() || v1 != lock.Version()) goto retry_lookup;

            return found;
        }

        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }
        
        uint64_t get_val = recs[i].val;
        if(lock.IsLocked() || v1 != lock.Version()) goto retry_lookup;

        if (recs[i].key == k) {
            v = get_val;
            return true;
        } else {
            return false;
        }
    }

    bool Update(const _key_t & k, uint64_t v) {
        // lock-based update operation
        lock.Lock();
        if(len > 64) {
            auto binary_update = [&v](Record & r) {
                r.val = v;
                return true;
            };
            bool found = BinSearch_CallBack(recs, len, k, binary_update);

            lock.UnLock();
            return found;
        }

        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        if (recs[i].key == k) {
            recs[i].val = v;

            lock.UnLock();
            return true;
        } else {
            lock.UnLock();
            return false;
        }
    }

    bool Remove(const _key_t & k) {
        // lock-based deletion
        lock.Lock();
        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        if(recs[i].key == k) {
            memmove(&recs[i], &recs[i + 1], sizeof(Record) * (len - i));
            len--;

            lock.UnLock();
            return true;
        } else {
            lock.UnLock();
            return false;
        }
    }

    int Scan(const _key_t & k, int out_len, Record *result) {
        // lock-free scan operation
        retry_scan:
        auto v1 = lock.Version();
        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        int no = 0;
        for(; i < len && no < out_len; i++) {
            result[no++] = recs[i];
        }

        if(lock.IsLocked() || v1 != lock.Version()) goto retry_scan;

        return no;
    }

    void Dump(std::vector<Record> & out) {
        retry_dump:
        auto v1 = lock.Version();
        auto old_size = out.size();
        out.insert(out.end(), recs, recs + len);
        if(lock.IsLocked() || v1 != lock.Version()) {
            out.resize(old_size);
            goto retry_dump;
        }
        return;
    }
};

ROLeaf::ROLeaf() {
    node_type = ROLEAF;
    stats = ROSTATS;
    of_count = 0;
    count = 0;
    sibling = nullptr;
    shadow = nullptr;

    slope = (double)(NODE_SIZE - 1) / MAX_KEY;
    intercept = 0;
    buckets = new Bucket[NODE_SIZE / PROBE_SIZE];
}

ROLeaf::ROLeaf(Record * recs_in, int num) {
    node_type = ROLEAF;
    stats = ROSTATS;
    of_count = 0;
    count = 0;
    sibling = nullptr;
    shadow = nullptr;

    // caculate the linear model
    LinearModelBuilder model;
    for(int i = num / 8; i < num * 7 / 8; i++) {
        model.add(recs_in[i].key, i);
    }
    model.build();

    // caculate the linear model
    slope = model.a_ * NODE_SIZE / num;
    intercept = model.b_ * NODE_SIZE / num;
    buckets = new Bucket[NODE_SIZE / PROBE_SIZE];

    for(int i = 0; i < num; i++) {
        this->Append(recs_in[i].key, recs_in[i].val);
    }
}

ROLeaf::~ROLeaf() {
    delete [] buckets;
}

bool ROLeaf::Append(const _key_t & k, uint64_t v) {
    int bucket_no = Predict(k) / PROBE_SIZE;
    bool overflow = buckets[bucket_no].Append(k, v);
    if (overflow)
        of_count += 1;
    return false;
}

bool ROLeaf::Store(const _key_t & k, uint64_t v, _key_t * split_key, ROLeaf ** split_node) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Store(k, v, split_key, (BaseNode **)split_node);
    }

    roleaf_store_retry:
    auto v1 = nodelock.Version();
    int bucket_no = Predict(k) / PROBE_SIZE;
    bool overflow = buckets[bucket_no].Store(k, v);
    if(nodelock.IsLocked() || v1 != nodelock.Version()) goto roleaf_store_retry;

    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        cur_shadow->Store(k, v, split_key, (BaseNode **)split_node);
    }

    if(ShouldSplit()) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool ROLeaf::Lookup(const _key_t & k, uint64_t &v) {
    // critical section
    int bucket_no = Predict(k) / PROBE_SIZE; 
    bool found = buckets[bucket_no].Lookup(k, v);

    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Lookup(k, v);
    }
    // In the critical section, current node does not split
    return found;
}

bool ROLeaf::Update(const _key_t & k, uint64_t v) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Update(k, v);
    }

    roleaf_update_retry:
    auto v1 = nodelock.Version();
    // critical section
    int bucket_no = Predict(k) / PROBE_SIZE;    
    bool found = buckets[bucket_no].Update(k, v);
    if(nodelock.IsLocked() || v1 != nodelock.Version()) goto roleaf_update_retry;

    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Update(k, v);
    }

    // In the critical section, current node does not split
    return found;
}

bool ROLeaf::Remove(const _key_t & k) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Remove(k);
    }

    roleaf_remove_retry:
    auto v1 = nodelock.Version();
    // critical section
    int bucket_no = Predict(k) / PROBE_SIZE;
    bool found = buckets[bucket_no].Remove(k);
    if(nodelock.IsLocked() || v1 != nodelock.Version()) goto roleaf_remove_retry;

    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Remove(k);
    }

    return found;
}

int ROLeaf::Scan(const _key_t &startKey, int len, Record *result) {
    roleaf_scan_retry:
    if(shadow != nullptr) { // this node is under morphing
        std::this_thread::yield();
        goto roleaf_scan_retry;
    }

    auto v1 = nodelock.Version();
    int bucket_no = Predict(startKey) / PROBE_SIZE;
    int new_len = len;
    while(new_len > 0 && bucket_no < (NODE_SIZE / PROBE_SIZE)) {
        int no = buckets[bucket_no].Scan(startKey, new_len, result);
        new_len -= no; 
        result += no;
        bucket_no += 1;
    }

    if(nodelock.IsLocked() || v1 != nodelock.Version()) goto roleaf_scan_retry;

    if(new_len > 0) {  
        return (len - new_len) + sibling->Scan(startKey, new_len, result);
    } else {
        return len;
    }
}

void ROLeaf::Dump(std::vector<Record> & out) {
    for(int i = 0; i < (NODE_SIZE / PROBE_SIZE); i++) {
        buckets[i].Dump(out);
    }
}

void ROLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d)[(%f)", prefix.c_str(), node_type, (float)of_count / count);
    for(int i = 0; i < out.size(); i++) {
        printf("%lf, ", out[i].key);
    }
    printf("]\n");
}

void ROLeaf::DoSplit(_key_t * split_key, ROLeaf ** split_node) {
    // lock-based split operation
    morphlock.Lock();
    nodelock.Lock();
    
    std::vector<Record> data;
    data.reserve(count);
    Dump(data);
    
    int pid = getSubOptimalSplitkey(data.data(), data.size());
    // creat two new nodes
    ROLeaf * left = new ROLeaf(data.data(), pid);
    left->morphlock.Lock();
    left->nodelock.Lock();
    ROLeaf * right = new ROLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    right->sibling = sibling;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);

    morphlock.UnLock();
    nodelock.UnLock();
}

} // namespace morphtree