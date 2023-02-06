/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#ifndef __MORPHTREE_BASENODE__
#define __MORPHTREE_BASENODE__

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <atomic>

#include "versionlock.h"
#include "../include/util.h"

namespace morphtree {
using std::string;
// Node types: all non-leaf nodes are of type ROLEAF
enum NodeType {ROINNER = 0, ROLEAF, WOLEAF};

// hyper parameters of Morphtree
const uint64_t ROSTATS = 0x0000000000000000; // default statistic of RONode
const uint64_t WOSTATS = 0xFFFFFFFFFFFFFFFF; // default statistic of WONode
const int GLOBAL_LEAF_SIZE   = 10240;    // the maximum node size of a leaf node

// We do NOT use virtual function here, 
// as it brings extra overhead of searching virtual table
class BaseNode {
public:
    BaseNode() = default;
    
    void DeleteNode();

    void TypeManager(bool isWrite);

public:
    bool Store(const _key_t & k, uint64_t v, _key_t * split_key, BaseNode ** split_node);

    bool Lookup(const _key_t & k, uint64_t & v);

    bool Update(const _key_t & k, uint64_t v);

    bool Remove(const _key_t & k);

    int Scan(const _key_t &startKey, int len, Record *result);

    void Dump(std::vector<Record> & out);

    inline bool Leaf() { return node_type != ROINNER; }

    void Print(string prefix);

public:
    // Node header: 32 bytes
    uint8_t node_type;
    VersionLock nodelock;
    uint16_t unused;
    uint32_t count;
    uint64_t stats;
    BaseNode * sibling;
    BaseNode * shadow;
};

// Inner node structures
class ROInner : public BaseNode {
public:
    ROInner() = delete;

    ROInner(Record * recs_in, int num);

    ~ROInner();

    void Clear() {capacity = 0;}

    bool Store(const _key_t & k, uint64_t v, _key_t * split_key, ROInner ** split_node);

    bool Lookup(const _key_t & k, uint64_t &v);

    void Print(string prefix);

private:
    inline int Predict(const _key_t & k) {
        return std::min(std::max(0.0, slope * k + intercept), capacity - 1.0);
    }

    inline bool ShouldRebuild() {
        // more than 67% index records are overflowed
        return of_count > (count << 1) / 3;
    }
    
    void RebuildSubTree();

    void Dump(std::vector<Record> & out);

public:
    static const int PROBE_SIZE       = 4;
    static const int BNODE_SIZE       = 12;
    // model: 16 Bytes
    double slope;
    double intercept;
    // data
    Record *recs;
    uint32_t capacity;
    uint32_t of_count;
};

// read optimized leaf nodes
struct Bucket;
class ROLeaf : public BaseNode {
public:
    ROLeaf();

    ROLeaf(Record * recs_in, int num);

    ~ROLeaf();

    bool Store(const _key_t & k, uint64_t v, _key_t * split_key, ROLeaf ** split_node);

    bool Lookup(const _key_t & k, uint64_t &v);

    bool Update(const _key_t & k, uint64_t v);

    bool Remove(const _key_t & k);

    int Scan(const _key_t &startKey, int len, Record *result);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

private:
    bool Append(const _key_t & k, uint64_t v);

    void DoSplit(_key_t * split_key, ROLeaf ** split_node);

    inline bool ShouldSplit() {
        return count >= GLOBAL_LEAF_SIZE || of_count > (GLOBAL_LEAF_SIZE / 4);
    }

    inline int Predict(const _key_t & k) {
        return std::min(std::max(0.0, slope * k + intercept), NODE_SIZE - 1.0);
    }

public:
    static const int PROBE_SIZE = 16;
    static const int NODE_SIZE = GLOBAL_LEAF_SIZE;

    // model
    double slope;
    double intercept;

    // data
    Bucket *buckets;
    uint32_t of_count;
    char dummy[4];
};

// write optimzied leaf nodes
class WOLeaf : public BaseNode {
public:
    WOLeaf();

    WOLeaf(Record * recs_in, int num);

    ~WOLeaf();

    bool Store(const _key_t & k, uint64_t v, _key_t * split_key, WOLeaf ** split_node);

    bool Lookup(const _key_t & k, uint64_t &v);

    bool Update(const _key_t & k, uint64_t v);

    bool Remove(const _key_t & k);

    int Scan(const _key_t &startKey, int len, Record *result);

    void Dump(std::vector<Record> & out);

    void Print(string prefix);

private:
    void DoSplit(_key_t * split_key, WOLeaf ** split_node);

    static const int NODE_SIZE = GLOBAL_LEAF_SIZE;
    static const int PIECE_SIZE = GLOBAL_LEAF_SIZE / 10;

    // data
    Record * recs; 
    uint32_t readonly_count;
    uint32_t readable_count;
    std::atomic<uint32_t> alloc_count;
    VersionLock sortlock;
    VersionLock mutex;
    char dummy[10];
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
extern uint64_t global_stats;
extern void MorphNode(BaseNode * leaf, NodeType from, NodeType to);
} // namespace morphtree

#endif // __MORPHTREE_BASENODE__