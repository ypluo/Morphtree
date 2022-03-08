/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#ifndef __BASENODE__
#define __BASENODE__

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include "../include/util.h"

namespace morphtree {
using std::string;
// Node types: all non-leaf nodes are of type ROLEAF
enum NodeType {ROINNER = 0, ROLEAF, RWLEAF, WOLEAF};

// hyper parameters of Morphtree
const uint64_t ROSTATS = 0x0000000000000000; // default statistic of RONode
const uint64_t RWSTATS = 0x5555555555555555; // default statistic of RWNode
const uint64_t WOSTATS = 0xFFFFFFFFFFFFFFFF; // default statistic of WONode
const NodeType INIT_NODE_TYPE = WOLEAF; // the initial type of a leaf node
const int GLOBAL_LEAF_SIZE    = 4096;    // the maximum node size of a leaf node
const int MORPH_FREQ          = 8;      // FREQ must be power of 2
const uint8_t RO_RW_LINE      = 40;     // read times that distinguishs RONode and RWNode
const uint8_t RW_WO_LINE      = 16;     // read times that distinguishs RWNode and WONode

// We do NOT use virtual function here, 
// as it brings extra overhead of searching virtual table
class BaseNode {
public:
    BaseNode() { stats = RWSTATS; }

public:
    bool Store(_key_t k, _val_t v, _key_t * split_key, BaseNode ** split_node);

    bool Lookup(_key_t k, _val_t & v);

    void Dump(std::vector<Record> & out);

    inline bool Leaf() { return node_type != ROINNER; }

    void Print(string prefix);

public:
    // Node header
    uint8_t node_type;
    uint8_t lock;
    char padding[6];
    uint64_t stats;
};

// Node structure with high read performance
class ROInner : public BaseNode {
public:
    ROInner();

    ROInner(Record * recs_in, int num);

    ~ROInner();

    inline void Clear() {
        capacity = 0;
    }

    bool Store(_key_t k, _val_t v, _key_t * split_key, ROInner ** split_node);

    bool Lookup(_key_t k, _val_t &v);

    void Print(string prefix) {
        printf("%s[", prefix.c_str());
        for(int i = 0; i < capacity; i++) {
            if(recs[i].key != MAX_KEY) {
                printf("(%lu, 0x%lx) ", recs[i].key, (uint64_t)recs[i].val);
            }
        }
        printf("]\n");
        for(int i = 0; i < capacity; i++) {
            if(recs[i].key != MAX_KEY) {
                BaseNode * child = (BaseNode *) recs[i].val;
                child->Print(prefix + "\t");
            }
        }
    }

private:
    bool Insert(_key_t k, _val_t v);

    void Expand();

    void Split(_key_t * split_key, ROInner ** split_node);

    inline int Predict(_key_t k) {
        return std::min(std::max(0, int(slope * k + intercept)), capacity - 1);
    }

    inline bool ShouldExpand() {
        // fill factor > 0.33
        // fill factor > 0.125 && count < EXPAND_THRESHOLD
        return count > (capacity / 2) || ((count > capacity >> 4) && count < EXPAND_THRESHOLD);
    }
    
public:
    static const int EXPAND_THRESHOLD = 1024;
    static const int PROBE_SIZE       = 8;

    int32_t capacity;
    int32_t count;
    ROInner *sibling;
    
    // model
    double slope;
    double intercept;
    // data
    Record *recs;
    char dummy[8];
};

// Node structure with high read performance
class ROLeaf : public BaseNode {
public:
    ROLeaf() = delete;

    ROLeaf(Record * recs_in, int num);

    ~ROLeaf();

    bool Store(_key_t k, _val_t v, _key_t * split_key, ROLeaf ** split_node);

    bool Lookup(_key_t k, _val_t &v);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

private:
    void DoSplit(_key_t * split_key, ROLeaf ** split_node);

    inline bool ShouldSplit() {
        return (of_count > (NODE_SIZE >> 3)) || (count == NODE_CAP);
    }

    inline int Predict(_key_t k) {
        return std::min(std::max(0, int(slope * k + intercept)), NODE_SIZE - 1);
    }

public:
    static const int PROBE_SIZE = 8;
    static const int NODE_CAP  = GLOBAL_LEAF_SIZE; 
    static const int NODE_SIZE = ((GLOBAL_LEAF_SIZE / 7 * 8) & (~(PROBE_SIZE - 1))) + PROBE_SIZE; // without overflow, it can hold roughly 4096 Records

    int32_t of_count;
    int32_t count;
    ROLeaf *sibling;
    
    // model
    double slope;
    double intercept;
    // data
    Record *recs;
    char dummy[8];
};

// Node structure with balanced read and write performance
const int BNODE_SIZE = 64;
class RWLeaf : public BaseNode {
public:
    RWLeaf();
    
    RWLeaf(Record * recs, int num);

    ~RWLeaf();

    bool Store(_key_t k, _val_t v, _key_t * split_key, RWLeaf ** split_node);

    bool Lookup(_key_t k, _val_t & v);

    void Dump(std::vector<Record> &out);

    void Print(string prefix);

private:
    static const int INNER_SIZE = GLOBAL_LEAF_SIZE / BNODE_SIZE;

    int64_t child_num;
    // the sibling of the RWLeaf
    RWLeaf * sibling;
    // key value are seperated for better search performance in a larger node
    _key_t * keys;
    _val_t * vals;
    char dummy[16]; // placeholder
};

// Node structure with high write performance
struct SortedRun;
struct Buffer;
const uint16_t BUFFER_SIZE = 256;
class WOLeaf : public BaseNode {
public:
    WOLeaf(bool isLeaf = true);

    WOLeaf(Record * recs_in, int num);

    ~WOLeaf();

    bool Store(_key_t k, _val_t v, _key_t * split_key, WOLeaf ** split_node);

    bool Lookup(_key_t k, _val_t &v);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

public:
    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / BUFFER_SIZE; 

    int64_t sorted_num;
    WOLeaf * sibling;
    // data
    Buffer * insert_buf;
    SortedRun * head;
    char dummy[16]; // placeholder
};

// Swap the metadata of two nodes
inline void SwapNode(BaseNode * a, BaseNode *b) {
    static const int COMMON_SIZE = 64;
    char tmp[COMMON_SIZE];
    memcpy(tmp, a, COMMON_SIZE);
    memcpy(a, b, COMMON_SIZE);
    memcpy(b, tmp, COMMON_SIZE);
}

extern uint64_t access_count;

extern void MorphNode(BaseNode * leaf, NodeType from, NodeType to);

} // namespace morphtree

#endif // __BASENODE__