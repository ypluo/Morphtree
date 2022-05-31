#ifndef __NEW_TREE_H__
#define __NEW_TREE_H__

#include "../src/morphtree_impl.h"
#include "../src/node.h"

#include "util.h"

using namespace newtree;

class Morphtree {
private:
    MorphtreeImpl<NodeType::WOLEAF, true> *mt_;

public:
    Morphtree() {
        mt_ = new MorphtreeImpl<NodeType::WOLEAF, true>();
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
        bool found = mt_->remove(key, val);

        if (found) 
            return val;
        else 
            return nullptr;
    }

    inline void print() {
        mt_->Print();
    }
};

#endif //__NEW_TREE_H__
