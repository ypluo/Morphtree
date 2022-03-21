/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>

#include "node.h"

namespace morphtree {

// B+ tree Node, used by RWLeaf
struct BNode {
    // header
    uint16_t version;
    uint8_t lock;
    uint8_t count;
    char dummy[4];
    BNode * sibling;
    
    // records
    Record recs[BNODE_SIZE + 1];

    BNode() : count(0), sibling(nullptr) {}
    
    void Append(const Record & r) {
        recs[count++] = r;
    }

    bool Store(_key_t k, _val_t v, _key_t * split_key, BNode ** split_node) {
        uint8_t i;
        for(i = 0; i < count; i++) {
            if(recs[i].key > k) {
                break;
            }
        }

        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (count - i));
        recs[i] = Record(k, v);
        count += 1;

        if(count > BNODE_SIZE) {
            // copy half of records into new RWLeaf
            uint8_t m = count / 2, copy_count = count - m;

            *split_key = recs[m].key;
            *split_node = new BNode;
            
            (*split_node)->count = copy_count;
            memcpy((*split_node)->recs, &(recs[m]), sizeof(Record) * copy_count);
            
            // update the count of old RWLeaf
            count = m;

            return true;
        } else {
            return false;
        }
    }

    bool Lookup(_key_t k, _val_t & v) {
        uint8_t i;
        for(i = 0; i < count; i++) {
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

    void Print(string prefix) {
        printf("%s[", prefix.c_str());
        for(int i = 0; i < count; i++) {
            printf("(%lu, %lu) ", recs[i].key, (uint64_t)recs[i].val);
        }
        printf("]\n");
    }
};

RWLeaf::RWLeaf() {
    node_type = NodeType::RWLEAF;
    stats = RWSTATS;

    child_num = 1;
    sibling = nullptr;
    keys = new _key_t[INNER_SIZE + 1];
    vals = new _val_t[INNER_SIZE + 1];
    keys[0] = MIN_KEY;
    vals[0] = new BNode;
}

RWLeaf::RWLeaf(Record * recs, int num) {
    node_type = NodeType::RWLEAF;

    static const int lfary = BNODE_SIZE;
    child_num = num / lfary + ((num % lfary) == 0 ? 0 : 1);
    keys = new _key_t[INNER_SIZE + 1];
    vals = new _val_t[INNER_SIZE + 1];
    //assert(child_num < INNER_SIZE + 1);
    
    for(int i = 0; i < child_num; i++) {
        BNode * leaf = new BNode;
        if (i == child_num - 1) {
            memcpy(leaf->recs, &recs[i * lfary], sizeof(Record) * (num - i * lfary));
            leaf->count = num - i * lfary;
        } else {
            memcpy(leaf->recs, &recs[i * lfary], sizeof(Record) * lfary);
            leaf->count = lfary;
        }
        
        keys[i] = recs[i * lfary].key;
        vals[i] = leaf;
    }
    keys[0] = MIN_KEY;
}

RWLeaf::~RWLeaf() {
    for(int i = 0; i < child_num; i++) {
        delete (BNode *)vals[i];
    }
    delete keys;
    delete vals;
}

bool RWLeaf::Store(_key_t k, _val_t v, _key_t * split_key, RWLeaf ** split_node) {
    //assert(child_num < INNER_SIZE + 1);

    uint8_t i;
    for (i = 0; i < child_num; i++) {
        if(keys[i] > k) {
            break;
        }
    }

    BNode * child = (BNode *)vals[i - 1];
    _key_t split_k_child;
    BNode * split_n_child;
    if(child->Store(k, v, &split_k_child, &split_n_child)) {
        memmove(&keys[i + 1], &keys[i], sizeof(_key_t) * (child_num - i));
        memmove(&vals[i + 1], &vals[i], sizeof(_val_t) * (child_num - i));
        keys[i] = split_k_child;
        vals[i] = split_n_child;
        child_num += 1;

        if(child_num > INNER_SIZE) {
            // copy half of records into new RWLeaf
            uint8_t m = child_num / 2, copy_count = child_num - m;

            *split_key = keys[m];
            *split_node = new RWLeaf;
            
            (*split_node)->child_num = copy_count;
            memcpy((*split_node)->keys, &(keys[m]), sizeof(_key_t) * copy_count);
            memcpy((*split_node)->vals, &(vals[m]), sizeof(_val_t) * copy_count);
            
            // update the count of old RWLeaf
            child_num = m;

            return true;
        } else {
            return false;
        }
    }

    return false;
}

bool RWLeaf::Lookup(_key_t k, _val_t & v) {
    uint8_t i;
    for(i = 0; i < child_num; i++) {
        if(keys[i] > k) {
            break;
        }
    }

    return ((BNode *)vals[i - 1])->Lookup(k, v);
}

void RWLeaf::Dump(std::vector<Record> & out) {
    for(int i = 0; i < child_num; i++){
        BNode * child = (BNode *) vals[i];

        for(int j = 0; j < child->count; j++) {
            out.push_back(child->recs[j]);
        }
    }
}

void RWLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d)[", prefix.c_str(), node_type);
    // for(int i = 0; i < out.size(); i++) {
    //     printf("%lu, ", out[i].key);
    // }
    printf("]\n");
}

} // namespace morphtree