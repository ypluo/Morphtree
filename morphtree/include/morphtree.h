#ifndef __MORPHTREE_H__
#define __MORPHTREE_H__

#include "../src/morphtree_impl.h"
#include "../src/node.h"

#include "util.h"

using namespace morphtree;

class Morphtree {
private:
    MorphtreeImpl<NodeType::WOLEAF, true> *mt_;

public:
    Morphtree() {
        mt_ = new MorphtreeImpl<NodeType::WOLEAF, true>();
    }

    Morphtree(std::vector<Record> initial_recs) {
        mt_ = new MorphtreeImpl<NodeType::WOLEAF, true>(initial_recs);
    }

    ~Morphtree() {
        delete mt_;
    }

    inline void insert(_key_t key, uint64_t val) {
        mt_->insert(key, val);
    }

    inline bool update(_key_t key, uint64_t val) {
        return mt_->update(key, val);
    }

    inline uint64_t lookup(_key_t key) {
        uint64_t val;
        bool found = mt_->lookup(key, val);

        if (found) 
            return val;
        else 
            return 0;
    }

    inline uint64_t remove(_key_t key) {
        uint64_t val;
        bool found = mt_->remove(key, val);

        if (found) 
            return val;
        else 
            return 0;
    }

    inline int scan(_key_t startKey, int len, Record * buf) {
        return mt_->scan(startKey, len, buf);
    }

    inline void print() {
        mt_->Print();
    }
};

#endif //__MORPHTREE_H__
