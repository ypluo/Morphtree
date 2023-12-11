#ifndef __MORPHTREE_H__
#define __MORPHTREE_H__

#include "../src/morphtree_impl.h"
#include "../src/node.h"

#include "util.h"

namespace morphtree {

class Morphtree {
private:
    MorphtreeImpl<NodeType::WOLEAF, true> *mt_;

public:
    Morphtree() {
        mt_ = new MorphtreeImpl<NodeType::WOLEAF, true>();
    }

    Morphtree(std::vector<Record> initial_recs) {
        // build from initial records
        mt_ = (MorphtreeImpl<NodeType::WOLEAF, true> *) new MorphtreeImpl<NodeType::ROLEAF, true>(initial_recs);
    }

    ~Morphtree() {
        delete mt_;
    }

    inline void insert(_key_t key, _val_t val) {
        mt_->insert(key, val);
    }

    inline bool update(_key_t key, _val_t val) {
        return mt_->update(key, val);
    }

    inline _val_t lookup(_key_t key) {
        _val_t val;
        bool found = mt_->lookup(key, val);

        if (found) 
            return val;
        else 
            return nullptr;
    }

    inline _val_t remove(_key_t key) {
        _val_t val;
        bool found = mt_->remove(key);

        if (found) 
            return val;
        else 
            return nullptr;
    }

    inline void print() {
        mt_->Print();
    }
};

} // morphtree

#endif //__MORPHTREE_H__
