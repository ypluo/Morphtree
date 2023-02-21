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
        
        Record get_rec = recs[i];
        if(lock.IsLocked() || v1 != lock.Version()) goto retry_lookup;

        if (get_rec.key == k) {
            v = get_rec.val;
            return true;
        } else {
            return false;
        }
    }

    bool Update(const _key_t & k, uint64_t v) {
        if(len > 64) {
            auto binary_update = [&v](Record & r) {
                r.val = v;
                return true;
            };
            bool found = BinSearch_CallBack(recs, len, k, binary_update);

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

            return true;
        } else {
            return false;
        }
    }

    bool Remove(const _key_t & k, uint64_t &v) {
        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        if(recs[i].key == k) {
            v = recs[i].val;
            memmove(&recs[i], &recs[i + 1], sizeof(Record) * (len - i));
            len--;
            return true;
        } else {
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

    int Dump(std::vector<Record> & out) {
        lock.Lock();
        int num = len;
        out.insert(out.end(), recs, recs + len);
        lock.UnLock();
        return num;
    }

    int Dump(Record * out) {
        lock.Lock();
        int num = len;
        memcpy(out, recs, sizeof(Record) * len);
        lock.UnLock();
        return num;
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
    buckets = new Bucket[NODE_SIZE / PROBE_SIZE];
}

ROLeaf::ROLeaf(Record * recs_in, int num) {
    node_type = NodeType::ROLEAF;
    next_node_type = NodeType::ROLEAF;
    stats = ROSTATS;
    count = 0;
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
    buckets = new Bucket[NODE_SIZE / PROBE_SIZE];

    for(int i = 0; i < num; i++) {
        this->Append(recs_in[i].key, recs_in[i].val);
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
    buckets = new Bucket[NODE_SIZE / PROBE_SIZE];
}

ROLeaf::~ROLeaf() {
    delete [] buckets;
}

bool ROLeaf::Append(const _key_t & k, uint64_t v) {
    int bucket_no = Predict(k) / PROBE_SIZE;
    bool overflow = buckets[bucket_no].Append(k, v);
    count += 1;
    return false;
}

bool ROLeaf::Store(const _key_t & k, uint64_t v, _key_t * split_key, ROLeaf ** split_node) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { 
        return cur_shadow->Store(k, v, split_key, (BaseNode **)split_node); // case 1: under morphing before we try to store
    }

    roleaf_store_retry:
    if(k >= mysplitkey) {
        return sibling->Store(k, v, split_key, (BaseNode **)split_node);
    }

    auto v1 = nodelock.Version();
    auto v2 = morphlock.Version();
    auto buckets_snapshot = buckets;
    int bucket_no = Predict(k) / PROBE_SIZE;
    
    buckets_snapshot[bucket_no].lock.Lock();
    bool overflow = buckets_snapshot[bucket_no].Append(k, v);
    if(nodelock.IsLocked() || v1 != nodelock.Version()) {
        buckets_snapshot[bucket_no].lock.UnLock();
        goto roleaf_store_retry;
    }
    
    cur_shadow = shadow;
    if(cur_shadow != nullptr) { 
        // case 2: start morphing after we store a record, not finished
        uint64_t tmp_v;
        buckets_snapshot[bucket_no].Remove(k, tmp_v); // reverse the store operation
        buckets_snapshot[bucket_no].lock.UnLock();
        return cur_shadow->Store(k, v, split_key, (BaseNode **)split_node);
    } else {
        buckets_snapshot[bucket_no].lock.UnLock();
        auto v3 = morphlock.Version();
        if(v2 != v3 && v3 % 2 == 0) 
            goto roleaf_store_retry; // case 3: start morphing after we store a record, finished
        __atomic_fetch_add(&count, 1, __ATOMIC_RELAXED); // case 4: common case, not overlaped with morphing
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
    auto buckets_snapshot = buckets;
    int bucket_no = Predict(k) / PROBE_SIZE; 
    bool found = buckets_snapshot[bucket_no].Lookup(k, v);

    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Lookup(k, v);
    }

    // check its sibling node
    if(found == false && k >= mysplitkey) {
        return sibling->Lookup(k, v);
    }
    return found;
}

bool ROLeaf::Update(const _key_t & k, uint64_t v) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Update(k, v);
    }

    roleaf_update_retry:
    if(k >= mysplitkey) {
        return sibling->Update(k, v);
    }

    auto v1 = nodelock.Version();
    auto v2 = morphlock.Version();
    // critical section
    auto buckets_snapshot = buckets;
    int bucket_no = Predict(k) / PROBE_SIZE;
    buckets_snapshot[bucket_no].lock.Lock();
    bool found = buckets_snapshot[bucket_no].Update(k, v);
    buckets_snapshot[bucket_no].lock.UnLock();
    if(nodelock.IsLocked() || v1 != nodelock.Version()) goto roleaf_update_retry;

    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Update(k, v);
    } else {
        auto v3 = morphlock.Version();
        if(v2 != v3 && v3 % 2 == 0) 
            goto roleaf_update_retry; // case 3: start morphing after we store a record, finished
        return found;
    }
}

bool ROLeaf::Remove(const _key_t & k) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Remove(k);
    }

    roleaf_remove_retry:
    if(k >= mysplitkey) {
        return sibling->Remove(k);
    }
    auto v1 = nodelock.Version();
    auto v2 = morphlock.Version();
    // critical section
    auto buckets_snapshot = buckets;
    int bucket_no = Predict(k) / PROBE_SIZE;
    buckets_snapshot[bucket_no].lock.Lock();
    uint64_t v;
    bool found = buckets_snapshot[bucket_no].Remove(k, v);
    
    if(nodelock.IsLocked() || v1 != nodelock.Version()) {
        buckets_snapshot[bucket_no].lock.UnLock();
        goto roleaf_remove_retry;
    }
    
    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        if (found) buckets_snapshot[bucket_no].Append(k, v); // reverse the remove operation
        buckets_snapshot[bucket_no].lock.UnLock();
        return cur_shadow->Remove(k);
    } else {
        buckets_snapshot[bucket_no].lock.UnLock();
        auto v3 = morphlock.Version();
        if(v2 != v3 && v3 % 2 == 0) 
            goto roleaf_remove_retry; // case 3: start morphing after we store a record, finished
        if(found) __atomic_fetch_sub(&count, 1, __ATOMIC_RELAXED);
    }

    return found;
}

int ROLeaf::Scan(const _key_t &startKey, int len, Record *result) {
    roleaf_scan_retry:
    if(shadow != nullptr) { // this node is under morphing
        std::this_thread::yield();
        goto roleaf_scan_retry;
    }

    if(startKey >= mysplitkey) {
        return sibling->Scan(startKey, len, result);
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

int ROLeaf::Dump(int i, Record * out) {
    return buckets[i].Dump(out);
}

void ROLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d, (%d))[", prefix.c_str(), node_type, count);
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
    left->mysplitkey = data[pid].key;
    right->mysplitkey = this->mysplitkey;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);

    morphlock.UnLock();
    nodelock.UnLock();
}

} // namespace morphtree