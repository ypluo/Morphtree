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
#include <list>

#include "versionlock.h"
#include "../include/util.h"

namespace morphtree {
using std::string;
// Node types: all non-leaf nodes are of type ROLEAF
enum NodeType {ROINNER = 0, ROLEAF, WOLEAF};

// hyper parameters of Morphtree
const uint64_t ROSTATS = 0x0000000000000000; // default statistic of RONode
const uint64_t WOSTATS = 0xFFFFFFFFFFFFFFFF; // default statistic of WONode
const int GLOBAL_LEAF_SIZE   = 1280;    // the maximum node size of a leaf node

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
    VersionLock morphlock;
    uint8_t unused;
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

    // void Clear() {capacity = 0;}

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
        return count >= GLOBAL_LEAF_SIZE;
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
    _key_t mysplitkey;
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
    VersionLock writelock;
    VersionLock sortlock;
    VersionLock mutex;
    char dummy[5];
    _key_t mysplitkey;
};

class NodeReclaimer {
    typedef std::tuple<BaseNode *, bool, int> ReclaimEle;
    std::list<ReclaimEle> list;

public:
    void Add(BaseNode * node, bool header_only, int epoch = 1) {
        list.emplace(list.end(), node, header_only, epoch);
    }

    void Reclaim(int safe_epoch = 0) {
        for(auto iter = list.begin(); iter != list.end(); iter++) {
            if(std::get<2>(*iter) <= safe_epoch) { 
                // this node is safe to reclaim
                BaseNode * tmp = std::get<0>(*iter);
                if(std::get<1>(*iter) == false) 
                    tmp->DeleteNode(); // should also reclaim its data
                else 
                    delete (char *)tmp; // reclaim its header only
            }
        }
    }
};

// Global variables and functions controling the morphing of Morphtree 
extern bool do_morphing;
extern uint64_t global_stats;
extern NodeReclaimer reclaimer;
extern void MorphNode(BaseNode * leaf, NodeType from, NodeType to);
extern void SwapNode(BaseNode * oldone, BaseNode *newone);

} // namespace morphtree

#endif // __MORPHTREE_BASENODE__