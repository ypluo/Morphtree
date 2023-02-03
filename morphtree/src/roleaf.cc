/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {

struct Bucket {
    // bucket header
    uint32_t versioned_lock;
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

    bool Store(const _key_t & k, uint64_t v) {
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
        if(len > 64) {
            return BinSearch(recs, len, k, v);
        }

        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        if (recs[i].key == k) {
            v = recs[i].val;
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
            return BinSearch_CallBack(recs, len, k, binary_update);
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

    bool Remove(const _key_t & k) {
        uint16_t i;
        for(i = 0; i < len; i++) {
            if(recs[i].key >= k) {
                break;
            }
        }

        if(recs[i].key == k) {
            memmove(&recs[i], &recs[i + 1], sizeof(Record) * (len - i));
            len--;
            return true;
        } else {
            return false;
        }
    }

    int Scan(const _key_t & k, int out_len, Record *result) {
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
        return no;
    }
};

ROLeaf::ROLeaf() {
    node_type = ROLEAF;
    stats = ROSTATS;
    of_count = 0;
    count = 0;
    sibling = nullptr;

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
        this->Store(recs_in[i].key, recs_in[i].val, nullptr, nullptr);
    }
}

ROLeaf::~ROLeaf() {
    delete [] buckets;
}

bool ROLeaf::Store(const _key_t & k, uint64_t v, _key_t * split_key, ROLeaf ** split_node) {
    int bucket_no = Predict(k) / PROBE_SIZE;

    bool overflow = buckets[bucket_no].Store(k, v);
    if (overflow)
        of_count += 1;

    if(split_node != nullptr && ShouldSplit()) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool ROLeaf::Lookup(const _key_t & k, uint64_t &v) {
    int bucket_no = Predict(k) / PROBE_SIZE;
    return buckets[bucket_no].Lookup(k, v);
}

bool ROLeaf::Update(const _key_t & k, uint64_t v) {
    int bucket_no = Predict(k) / PROBE_SIZE;
    return buckets[bucket_no].Update(k, v);
}

bool ROLeaf::Remove(const _key_t & k) {
    int bucket_no = Predict(k) / PROBE_SIZE;
    return buckets[bucket_no].Remove(k);
}

int ROLeaf::Scan(const _key_t &startKey, int len, Record *result) {
    int bucket_no = Predict(startKey) / PROBE_SIZE;
    int new_len = len;
    while(new_len > 0 && bucket_no < (NODE_SIZE / PROBE_SIZE)) {
        int no = buckets[bucket_no].Scan(startKey, new_len, result);
        new_len -= no; 
        result += no;
        bucket_no += 1;
    }

    if(new_len > 0) {
        return (len - new_len) + sibling->Scan(startKey, new_len, result);
    } else {
        return len;
    }
}

void ROLeaf::Dump(std::vector<Record> & out) {
    for(int i = 0; i < (NODE_SIZE / PROBE_SIZE); i++) {
        out.insert(out.end(), buckets[i].recs, buckets[i].recs + buckets[i].len);
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
    std::vector<Record> data;
    data.reserve(count);
    Dump(data);
    
    int pid = getSubOptimalSplitkey(data.data(), data.size());
    // creat two new nodes
    ROLeaf * left = new ROLeaf(data.data(), pid);
    ROLeaf * right = new ROLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    right->sibling = sibling;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);
    delete left;
}

} // namespace morphtree