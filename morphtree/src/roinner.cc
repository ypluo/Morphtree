/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {

static const int MARGIN = ROInner::PROBE_SIZE;

struct OFNode {
    uint32_t len;
    char dummy[4];
    Record recs_[0];

    bool Lookup(_key_t k, _val_t last_v, _val_t & v) {
        uint16_t i;
        if(recs_[0].key > k) {
            v = last_v;
            return true;
        }

        for(i = 1; i < len; i++) {
            if(recs_[i].key > k) {
                v = recs_[i - 1].val;
                return true;
            }
        }

        v = recs_[i - 1].val;
        return true;
    }
};

ROInner::ROInner(Record * recs_in, int num, int expand) {
    node_type = NodeType::ROINNER;
    count = 0;
    of_count= 0;
    recs = nullptr;

    LinearModelBuilder model;
    for(int i = 0; i < num; i++) {
        model.add(recs_in[i].key, MARGIN + i);
    }
    model.build();

    // caculate the linear model
    capacity = (num * expand) / PROBE_SIZE * PROBE_SIZE + PROBE_SIZE;
    slope = model.a_ * capacity / (2 * MARGIN + num);
    intercept = model.b_ * capacity / (2 * MARGIN + num);
    recs = new Record[capacity];

    int i, last_i = 0, cid = Predict(recs_in[0].key) / PROBE_SIZE;
    // populate the node with records
    for(i = 1; i < num; i++) {
        int predict = Predict(recs_in[i].key);
        if((predict / PROBE_SIZE) != cid) {
            // copy records in [last_i, i) into cid-th cacheline
            int c = i - last_i;
            if(c <= PROBE_SIZE - 1) {
                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
            } else {
                // add an overflow node
                int oc = c - PROBE_SIZE + 1;
                of_count += oc;
                OFNode * ofnode = (OFNode *) new char[sizeof(OFNode) + oc * sizeof(Record)];
                Record * _discard = new(ofnode->recs_) Record[oc]; // just for initializition
                recs[cid * PROBE_SIZE + PROBE_SIZE - 1] = {recs_in[last_i + PROBE_SIZE - 1].key, ofnode};
                ofnode->len = oc;

                memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
                memcpy(&ofnode->recs_[0], &recs_in[last_i + PROBE_SIZE - 1], oc * sizeof(Record));
            }

            // go next cacheline
            last_i = i;
            cid = predict / PROBE_SIZE;
        }
    }
    {
        int c = i - last_i;
        if(c <= PROBE_SIZE - 1) {
            memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], c * sizeof(Record));
        } else {
            // add an overflow node
            int oc = c - PROBE_SIZE + 1;
            of_count += oc;
            OFNode * ofnode = (OFNode *) new char[sizeof(OFNode) + oc * sizeof(Record)];
            Record * _discard = new(ofnode->recs_) Record[oc]; // just for initializition
            recs[cid * PROBE_SIZE + PROBE_SIZE - 1] = {recs_in[last_i + PROBE_SIZE - 1].key, ofnode};
            ofnode->len = oc;
            
            memcpy(&recs[cid * PROBE_SIZE], &recs_in[last_i], (PROBE_SIZE - 1) * sizeof(Record));
            memcpy(&ofnode->recs_[0], &recs_in[last_i + PROBE_SIZE - 1], oc * sizeof(Record));
        }
    }

    count = num;
}

void ROInner::Clear() {
    // free all overflow records
    for(int i = 0; i < capacity; i += PROBE_SIZE) {
        if(recs[i + PROBE_SIZE - 1].val != nullptr) {
            delete [] (char *) recs[i + PROBE_SIZE - 1].val;
        }
    }
    capacity = 0;
}

static void free_child(BaseNode * child) {
    /* it will invalid the children of current nodes
        In some cases, we do not want that happen
    */
    switch(child->node_type) {
    case NodeType::ROINNER:
        delete reinterpret_cast<ROInner *>(child);
    case NodeType::ROLEAF: 
        delete reinterpret_cast<ROLeaf *>(child);
    case NodeType::RWLEAF:
        delete reinterpret_cast<RWLeaf *>(child);
    case NodeType::WOLEAF:
        delete reinterpret_cast<WOLeaf *>(child);
    }
}

ROInner::~ROInner() {
    for (int i = 0; i < capacity; i++) {
        if(i % PROBE_SIZE == PROBE_SIZE - 1 && recs[i].val != nullptr) {
            OFNode * ofnode = (OFNode *) recs[i].val;
            for(int i = 0; i < ofnode->len; i++) {
                BaseNode * child = (BaseNode *)ofnode->recs_[i].val;
                child->DeleteNode();
            }
            delete ofnode;
        } else if(recs[i].key != MAX_KEY) {
            BaseNode * child = (BaseNode *)recs[i].val;
            child->DeleteNode();
        }
    }

    delete [] recs;
}

void ROInner::Print(string prefix) {
    printf("%s[(%d)", prefix.c_str(), of_count);
    for(int i = 0; i < capacity; i++) {
        if(i % PROBE_SIZE == 0) {
            printf("||");
        }

        if(i % PROBE_SIZE == PROBE_SIZE - 1 && recs[i].val != nullptr) {
            OFNode * ofnode = (OFNode *) recs[i].val;
            for(int i = 0; i < ofnode->len; i++) {
                printf("(%lu, 0x%lx) ", ofnode->recs_[i].key, (uint64_t)ofnode->recs_[i].val);
            }
        }
        if(recs[i].key != MAX_KEY) {
            printf("(%lu, 0x%lx) ", recs[i].key, (uint64_t)recs[i].val);
        }
    }
    printf("]\n");
    
    for(int i = 0; i < capacity; i++) {
        if(i % PROBE_SIZE == PROBE_SIZE - 1 && recs[i].val != nullptr) {
            OFNode * ofnode = (OFNode *) recs[i].val;
            for(int i = 0; i < ofnode->len; i++) {
                BaseNode * child = (BaseNode *)ofnode->recs_[i].val;
                child->Print("");
            }
        } else if(recs[i].key != MAX_KEY) {
            BaseNode * child = (BaseNode *)recs[i].val;
            child->Print("");
        }
    }
}

bool ROInner::Insert(_key_t k, _val_t v) {
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
        count += 1;
        return true;
    } else {
        return false;
    }
}

bool ROInner::Store(_key_t k, _val_t v, _key_t * split_key, ROInner ** split_node) {
    if(Insert(k, v))
        return false;

    // there is no empty slot
    if(ShouldExpand()) {
        Expand(k, v);
        return false;
    } else {
        Split(k, v, split_key, split_node);
        return true;
    }
}

bool ROInner::Lookup(_key_t k, _val_t &v) {
    int predict = Predict(k);
    predict = (predict / PROBE_SIZE) * PROBE_SIZE;

    // probe left by PROBE_SIZE: keys less than the first key should be direct to the previous index record 
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

    OFNode * ofnode = (OFNode *) recs[i - 1].val;
    ofnode->Lookup(k, recs[i - 2].val, v);
    return true;
}

void ROInner::Split(_key_t k, _val_t v, _key_t * split_key, ROInner ** split_node) {
    std::vector<Record> records;
    records.reserve(count + 1);

    bool merged = false;
    for(int i = 0; i < capacity; i++) {
        if(i % PROBE_SIZE == PROBE_SIZE - 1 && recs[i].val != nullptr) {
            OFNode * ofnode = (OFNode *)recs[i].val;
            for(int j = 0; j < ofnode->len; j++) {
                if(!merged && ofnode->recs_[j].key > k) {
                    records.push_back(Record(k, v));
                    merged = true;
                }
                records.push_back(ofnode->recs_[j]);
            }
        } else if(recs[i].key != MAX_KEY) {
            if(!merged && recs[i].key > k) {
                records.push_back(Record(k, v));
                merged = true;
            }
            records.push_back(recs[i]);
        }
    }
    if(!merged) {
        records.push_back(Record(k, v));
    }

    int new_count = count + 1;
    ROInner * left = new ROInner(records.data(), new_count / 2);
    ROInner * right = new ROInner(records.data() + new_count / 2, new_count - new_count / 2);
    left->sibling = right;
    right->sibling = sibling;

    *split_key = records[new_count / 2].key;
    *split_node = right;

    SwapNode(this, left);

    left->Clear(); // delete left node without clear it incurs the danger of free valid child nodes
    delete left;
}

void ROInner::Expand(_key_t k, _val_t v) {
    std::vector<Record> records;
    records.reserve(count + 1);
    
    bool merged = false;
    for(int i = 0; i < capacity; i++) {
        if(i % PROBE_SIZE == PROBE_SIZE - 1 && recs[i].val != nullptr) {
            OFNode * ofnode = (OFNode *)recs[i].val;
            for(int j = 0; j < ofnode->len; j++) {
                if(!merged && ofnode->recs_[j].key > k) {
                    records.push_back(Record(k, v));
                    merged = true;
                }
                records.push_back(ofnode->recs_[j]);
            }
        } else if(recs[i].key != MAX_KEY) {
            if(!merged && recs[i].key > k) {
                records.push_back(Record(k, v));
                merged = true;
            }
            records.push_back(recs[i]);
        }
    }
    if(!merged) {
        records.push_back(Record(k, v));
    }

    ROInner * new_inner = new ROInner(records.data(), count + 1, 4);
    SwapNode(this, new_inner);

    new_inner->Clear();
    delete new_inner;
}

} // namespace morphtree