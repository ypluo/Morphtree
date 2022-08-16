/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {

static const int MARGIN = ROLeaf::PROBE_SIZE;

// Overflow node
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

    bool update(_key_t k, _val_t v) {
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
            return BinSearch_update(recs_, len, k, v);
        }
    }
};

ROLeaf::ROLeaf() {
    node_type = ROLEAF;
    stats = ROSTATS;
    of_count = 0;
    count = 0;

    slope = (double)(NODE_SIZE - 1) / MAX_KEY;
    intercept = 0;
    recs = new Record[NODE_SIZE];
}

ROLeaf::ROLeaf(Record * recs_in, int num) {
    node_type = ROLEAF;
    stats = ROSTATS;
    of_count = 0;
    count = 0;

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
        this->Store(recs_in[i].key, recs_in[i].val, nullptr, nullptr);
    }
}

ROLeaf::~ROLeaf() {
    for(int i = 0; i < NODE_SIZE / PROBE_SIZE; i++) {
        if(recs[PROBE_SIZE * i + PROBE_SIZE - 1].val != nullptr){
            delete (char *)recs[PROBE_SIZE * i + PROBE_SIZE - 1].val;
        }
    }

    delete recs;
}

bool ROLeaf::Store(_key_t k, _val_t v, _key_t * split_key, ROLeaf ** split_node) {
    int predict = Predict(k);
    predict = predict / PROBE_SIZE * PROBE_SIZE;

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
            int16_t newlen = old_ofnode->len * 2;
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
    count += 1;

    if(split_node != nullptr && ShouldSplit()) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool ROLeaf::Lookup(_key_t k, _val_t &v) {
    int predict = Predict(k);
    predict = predict / PROBE_SIZE * PROBE_SIZE;

    int i;
    for (i = predict; i < predict + PROBE_SIZE - 1; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            return true;
        } else if(recs[i].key > k) {
            return false;
        }
    }

    OFNode * ofnode = (OFNode *) recs[predict + PROBE_SIZE - 1].val;
    if(ofnode != nullptr) {  
        return ofnode->Lookup(k, v);
    } else {
        return false;
    }
}

bool ROLeaf::Update(_key_t k, _val_t v) {
    int predict = Predict(k);
    predict = predict / PROBE_SIZE * PROBE_SIZE;

    int i;
    for (i = predict; i < predict + PROBE_SIZE - 1; i++) {
        if(recs[i].key == k) {
            recs[i].val = v;
            return true;
        } else if(recs[i].key > k) {
            return false;
        }
    }

    OFNode * ofnode = (OFNode *) recs[predict + PROBE_SIZE - 1].val;
    if(ofnode != nullptr) {  
        return ofnode->update(k, v);
    } else {
        return false;
    }
}

bool ROLeaf::Remove(_key_t k) {
    return true;
}

void ROLeaf::Dump(std::vector<Record> & out) {
    // retrieve records from this node
    for(int i = 0; i < NODE_SIZE; i++) {
        if(recs[i].key != MAX_KEY) {
            out.push_back(recs[i]);
        } else if(recs[i].val != nullptr) {
            OFNode * ofnode = (OFNode *) recs[i].val;
            for(int j = 0; j < ofnode->len && ofnode->recs_[j].key != MAX_KEY; j++) {
                out.push_back(ofnode->recs_[j]);
            }
        }
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
    
    int pid = getSubOptimalSplitkey(data, data.size());
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