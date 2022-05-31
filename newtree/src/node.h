/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#ifndef __NEW_TREE_BASENODE__
#define __NEW_TREE_BASENODE__

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include "../include/util.h"
#include "btree.h"

namespace newtree {
using std::string;
// Node types: all non-leaf nodes are of type ROLEAF
enum NodeType {ROLEAF, WOLEAF};

// hyper parameters of Morphtree
const uint64_t ROSTATS = 0x0000000000000000; // default statistic of RONode
const uint64_t WOSTATS = 0xFFFFFFFFFFFFFFFF; // default statistic of WONode
const int GLOBAL_LEAF_SIZE = 10240;            // the maximum node size of a leaf node

// We do NOT use virtual function here, 
// as it brings extra overhead of searching virtual table
class BaseNode {
public:
    BaseNode() = default;
    
    void DeleteNode();

    void TypeManager(bool isWrite);

public:
    bool Store(_key_t k, _val_t v, _key_t * split_key, BaseNode ** split_node);

    bool Lookup(_key_t k, _val_t & v);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

public:
    // Node header
    uint8_t node_type;
    uint8_t lock;
    char padding[6];
    uint64_t stats;
    BaseNode * sibling;
    _key_t split_k;
};

// read optimized leaf nodes
class ROLeaf : public BaseNode {
public:
    ROLeaf();

    ROLeaf(Record * recs_in, int num);

    ~ROLeaf();

    bool Store(_key_t k, _val_t v, _key_t * split_key, ROLeaf ** split_node);

    bool Lookup(_key_t k, _val_t &v);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

private:
    void DoSplit(_key_t * split_key, ROLeaf ** split_node);

    inline bool ShouldSplit() {
        return count == GLOBAL_LEAF_SIZE;
    }

    inline int Predict(_key_t k) {
        return std::min(std::max(0.0, slope * k + intercept), NODE_SIZE - 1.0);
    }

public:
    static const int PROBE_SIZE = 8;
    static const int NODE_SIZE  = GLOBAL_LEAF_SIZE;
    static const int SHARING    = 16;
    static const int OVERFLOW_SIZE = GLOBAL_LEAF_SIZE / PROBE_SIZE / SHARING;
    typedef stx::btree<_key_t, _val_t> oftree_type;

    // meta data
    double slope;
    double intercept;
    int32_t of_count;
    int32_t count;
    Record *recs;
};

// write optimzied leaf nodes
class WOLeaf : public BaseNode {
public:
    WOLeaf();

    WOLeaf(Record * recs_in, int num);

    ~WOLeaf();

    bool Store(_key_t k, _val_t v, _key_t * split_key, WOLeaf ** split_node);

    bool Lookup(_key_t k, _val_t &v);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

private:
    void DoSplit(_key_t * split_key, WOLeaf ** split_node);

    static const int NODE_SIZE = GLOBAL_LEAF_SIZE;
    static const int PIECE_SIZE = 1024;

    // meta data
    Record * recs; 
    int16_t inital_count;
    int16_t insert_count;
    int16_t sort_count;
    int16_t unused;
    char dummy[16];
};

// Swap the metadata of two nodes
inline void SwapNode(BaseNode * a, BaseNode *b) {
    static const int COMMON_SIZE = 64;
    char tmp[COMMON_SIZE];
    memcpy(tmp, a, COMMON_SIZE);
    memcpy(a, b, COMMON_SIZE);
    memcpy(b, tmp, COMMON_SIZE);
}

// Global variables and functions controling the morphing of Morphtree 
extern bool do_morphing;
extern void MorphNode(BaseNode * leaf, NodeType from, NodeType to);
} // namespace newtree

#endif // __NEW_TREE_BASENODE__