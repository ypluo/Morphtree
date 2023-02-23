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
#include <thread>
#include <queue>

#include <iomanip>
#include <glog/logging.h>

#include "lock/versionlock.h"
#include "lock/rwlock.h"
#include "lock/spinlock.h"
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

    void MorphJudge(bool isWrite);

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
    uint8_t node_type      : 2;
    uint8_t next_node_type : 2;
    uint8_t lsn            : 4;
    VersionLock nodelock;
    RWLock headerlock;
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
    inline int Predict(const _key_t & k, double a, double b) {
        return std::min(std::max(0.0, a * k + b), capacity - 1.0);
    }

    inline bool ShouldRebuild() {
        // more than 67% index records are overflowed
        return of_count > (count << 1) / 3 || count >= capacity;
    }

    void RebuildSubTree();

    void Dump(std::vector<Record> & out);

public:
    static const int PROBE_SIZE       = 4;
    static const int BNODE_SIZE       = 16;
    // model: 16 Bytes
    double intercept;
    double slope;
    
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

    ROLeaf(double a, double b);

    ~ROLeaf();

    bool Store(const _key_t & k, uint64_t v, _key_t * split_key, ROLeaf ** split_node);

    bool Lookup(const _key_t & k, uint64_t &v);

    bool Update(const _key_t & k, uint64_t v);

    bool Remove(const _key_t & k);

    int Scan(const _key_t &startKey, int len, Record *result);

    void Dump(std::vector<Record> & out);

    int Dump(int i, Record * out);

    void Print(string prefix);

private:
    bool Append(const _key_t & k, uint64_t v);

    void DoSplit(_key_t * split_key, ROLeaf ** split_node);

    static inline int Predict(const _key_t & k, double a, double b) {
        return std::min(std::max(0.0, a * k + b), NODE_SIZE - 1.0);
    }

public:
    static const int PROBE_SIZE = 16;
    static const int NODE_SIZE = GLOBAL_LEAF_SIZE;

    // model
    double intercept;
    double slope;
    
    // data
    Bucket *buckets;
    _key_t mysplitkey;
};

// write optimzied leaf nodes
class WOLeaf : public BaseNode {
public:
    WOLeaf(uint32_t initial_num = 0);

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

public:
    // data
    Record * recs; 
    _key_t mysplitkey;

    uint32_t readonly_count;
    uint32_t readable_count;
    SpinLock sortlock;
    SpinLock writelock;
};

// background threads
class MorphLogger {
    typedef std::tuple<BaseNode *, uint16_t, uint8_t> MorphRecord;
    std::queue<MorphRecord> que;
    VersionLock mtx;
    uint32_t size;

public:
    MorphLogger() : size(0) {
        #ifdef BG_MORPH
            std::thread t(MorphLogger::Run, this);
            t.detach();
        #endif
    }

    void Add(BaseNode * node, uint16_t lsn, uint8_t node_type) {
        mtx.Lock();
        que.emplace(node, lsn, node_type);
        size += 1;
        mtx.UnLock();
    }

    static void MorphOneNode(BaseNode * leaf, uint16_t lsn, uint8_t to);

    void Run();
};

typedef std::pair<BaseNode *, bool> ReclaimEle;

// Global variables and functions controling the morphing of Morphtree 
extern bool do_morphing;
extern MorphLogger *morph_log;
extern uint64_t gacn;
extern void MorphNode(BaseNode * leaf, uint8_t lsn, NodeType to);
extern void SwapNode(BaseNode * oldone, BaseNode *newone);

} // namespace morphtree

#endif // __MORPHTREE_BASENODE__