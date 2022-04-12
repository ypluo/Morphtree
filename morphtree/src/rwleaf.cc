/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>

#include "node.h"

namespace morphtree {

static const int MARGIN = ROLeaf::PROBE_SIZE * 2;

struct OFNode {
    uint16_t len;
    uint8_t lock;
    char dummy[5];

    Record recs_[0];
    
    OFNode(): len(0) {}

    bool Store(_key_t k, _val_t v) {
        if(recs_[len - 1].key == MAX_KEY) {
            uint16_t i;
            for(i = 0; i < len; i++) {
                if(recs_[i].key > k) {
                    break;
                }
            }

            memmove(&recs_[i + 1], &recs_[i], sizeof(Record) * (len - 1 - i));
            recs_[i] = Record(k, v);

            return true;
        } else {
            return false;
        }
    }

    bool Lookup(_key_t k, _val_t & v) {
        if(len < 64) {
            // scan search
            uint16_t i;
            for(i = 0; i < len; i++) {
                if(recs_[i].key >= k) {
                    break;
                }
            }

            if (recs_[i].key == k) {
                v = recs_[i].val;
                return true;
            } else {
                return false;
            }
        } else {
            return BinSearch(recs_, len, k, v);
        }
    }
};

RWLeaf::RWLeaf() {
    node_type = NodeType::RWLEAF;
    stats = RWSTATS;
    initial_count = 0;
    buf_count = 0;
    of_count = 0;
    sorted_count = 0;

    slope = 0;
    intercept = 0;

    recs = new Record[NODE_SIZE];
    buffer = new Record[BUFFER_SIZE];
}

RWLeaf::RWLeaf(Record * recs_in, int num) {
    node_type = RWLEAF;
    stats = RWSTATS;
    initial_count = 0;
    buf_count = 0;
    of_count = 0;
    sorted_count = 0;

    LinearModelBuilder model;
    for(int i = 0; i < num; i++) {
        model.add(recs_in[i].key, MARGIN + i);
    }
    model.build();

    // caculate the linear model
    slope = model.a_ * (NODE_SIZE - 2 * MARGIN) / num;
    intercept = model.b_ * (NODE_SIZE - 2 * MARGIN) / num + MARGIN;
    recs = new Record[NODE_SIZE];
    buffer = new Record[BUFFER_SIZE];

    for(int i = 0; i < num; i++) {
        this->Populate(recs_in[i].key, recs_in[i].val);
    }
}

RWLeaf::~RWLeaf() {
    for(int i = 0; i < NODE_SIZE / PROBE_SIZE; i++) {
        if(recs[PROBE_SIZE * i + 7].val != nullptr){
            delete (char *)recs[PROBE_SIZE * i + 7].val;
        }
    }
    delete [] recs;
    delete [] buffer;
}

void RWLeaf::Populate(_key_t k, _val_t v) {
    int predict = Predict(k);
    predict = predict & ~(PROBE_SIZE - 1);

    int i;
    for (i = predict; i < predict + PROBE_SIZE - 1; i++) {
        if(recs[i].key > k)
            break;
    }

    // try to find a empty slot
    Record last_one = recs[predict + PROBE_SIZE - 2];
    if(last_one.key == MAX_KEY) { 
        // there is an empty slot
        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 2 - i));
        recs[i] = Record(k, v);
    } else {
        // no empty slot found
        OFNode * ofnode = (OFNode *) recs[predict + PROBE_SIZE - 1].val;
        if (ofnode == nullptr) {
            ofnode = (OFNode *) new char[sizeof(OFNode) + 8 * sizeof(Record)];
            Record * _discard = new(ofnode->recs_) Record[8]; // just for initializition

            ofnode->len = 8;
            recs[predict + PROBE_SIZE - 1].val = (_val_t) ofnode;
        }

        if(last_one.key > k) { // kick the last record into overflow node
            memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 2 - i));
            recs[i] = Record(k, v); 
            k = last_one.key;
            v = last_one.val;
        }

        // store the target record into overflow node
        if(!ofnode->Store(k, v)) {
            // the overflow node is full
            OFNode * old_ofnode = ofnode;
            
            // create a new overflow node, two times the formal one
            int16_t newlen = old_ofnode->len * 3 / 2;
            ofnode = (OFNode *) new char[sizeof(OFNode) + newlen * sizeof(Record)];
            Record * _discard = new(ofnode->recs_) Record[newlen]; // just for initializition
            
            // copy records into it
            ofnode->len = newlen;
            memcpy(ofnode->recs_, old_ofnode->recs_, sizeof(Record) * old_ofnode->len);
            ofnode->Store(k, v);
            recs[predict + PROBE_SIZE - 1].val = (_val_t) ofnode;

            delete old_ofnode;
        }
        of_count += 1;
    }
    initial_count += 1;
}

bool RWLeaf::Store(_key_t k, _val_t v, _key_t * split_key, RWLeaf ** split_node) {
    buffer[buf_count++] = {k, v};

    if(buf_count % PIECE_SIZE == 0) {
        int32_t start = buf_count - PIECE_SIZE;
        std::sort(buffer + start, buffer + buf_count);
        sorted_count = buf_count;
    }
    
    if(ShouldSplit()) {
        DoSplit(split_key, split_node);
        return true;
    }
    else 
        return false;
}

bool RWLeaf::Lookup(_key_t k, _val_t & v) {
    // search in the sorted node
    int predict = Predict(k);
    predict = predict & ~(PROBE_SIZE - 1);

    int i;
    for (i = predict; i < predict + PROBE_SIZE - 1; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            return true;
        } else if(recs[i].key > k) {
            break;
        }
    }

    OFNode * ofnode = (OFNode *) recs[predict + PROBE_SIZE - 1].val;
    if(ofnode != nullptr && ofnode->Lookup(k, v)) {  
        return true;
    }
    
    // search in the buffer
    // do binary search in all sorted runs
    int32_t bin_end = buf_count / PIECE_SIZE * PIECE_SIZE;
    for(int i = 0; i < bin_end; i += PIECE_SIZE) {
        if(BinSearch(buffer + i, PIECE_SIZE, k, v)) {
            Populate(k, v);
            return true;
        }
    }
    if(BinSearch(buffer + bin_end, sorted_count - bin_end, k, v)) {
        Populate(k, v);
        return true;
    }

    // do scan in unsorted runs
    for(int i = sorted_count; i < buf_count; i++) {
        if(buffer[i].key == k) {
            v = buffer[i].val;
            Populate(k, v);
            return true;
        }
    }

    return false;
}

void RWLeaf::Dump(std::vector<Record> & out) {
    std::vector<Record> out1;
    out1.reserve(initial_count);

    for(int i = 0; i < NODE_SIZE; i += PROBE_SIZE) {
        int k = 0;
        for(; k < PROBE_SIZE - 1; k++) {
            if(recs[i + k].key != MAX_KEY) {
                out1.push_back(recs[i + k]);
            } else {
                break;
            }
        }
        if(k == PROBE_SIZE - 1 && recs[i + k].val != nullptr) {
            OFNode * ofnode = (OFNode *) recs[i + k].val;
            for(int j = 0; j < ofnode->len && ofnode->recs_[j].key != MAX_KEY; j++) {
                out1.push_back(ofnode->recs_[j]);
            }
        }
    }

    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / 2 / PIECE_SIZE + 1;
    static Record * sort_runs[MAX_RUN_NUM + 1];
    static int lens[MAX_RUN_NUM + 1];

    if(sorted_count != buf_count) {
        int32_t bin_end = buf_count / PIECE_SIZE * PIECE_SIZE;
        std::sort(buffer + bin_end, buffer + buf_count);
    }

    sort_runs[0] = out1.data();
    lens[0] = initial_count;
    int run_cnt = 1;
    for(int i = 0; i < buf_count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = buffer + i;
        lens[run_cnt] = (i + PIECE_SIZE <= buf_count ? PIECE_SIZE : buf_count - i);
        run_cnt += 1;
    }

    KWayMerge_nodup(sort_runs, lens, run_cnt, out);
    return ;
}

void RWLeaf::Print(string prefix) {
    std::vector<Record> out;
    out.reserve(initial_count + buf_count + GLOBAL_LEAF_SIZE / 4);
    Dump(out);

    printf("%s(%d)[", prefix.c_str(), node_type);
    for(int i = 0; i < out.size(); i++) {
        printf("%lf, ", out[i].key);
    }
    printf("]\n");
}

void RWLeaf::DoSplit(_key_t * split_key, RWLeaf ** split_node) {
    std::vector<Record> data;
    data.reserve(initial_count + buf_count + GLOBAL_LEAF_SIZE / 4);
    Dump(data);

    int16_t count = data.size();
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