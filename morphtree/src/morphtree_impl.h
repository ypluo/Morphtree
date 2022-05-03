#ifndef __MORPHTREE_IMPL_H__
#define __MORPHTREE_IMPL_H__

#include "node.h"

namespace morphtree {

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF = false>
class MorphtreeImpl {
public:
    explicit MorphtreeImpl();

    ~MorphtreeImpl();
 
    void insert(const _key_t & key, const _val_t val);

    bool update(const _key_t & key, const _val_t val);

    bool remove(const _key_t & key, _val_t & val);

    bool lookup(const _key_t & key, _val_t & v);

    uint64_t scan(const _key_t &startKey, int range, std::vector<_val_t> &result);

    void Print();
    
private:
    bool insert_recursive(BaseNode * n, const _key_t & key, const _val_t val, 
                                _key_t * split_k, BaseNode ** split_n);

    BaseNode * root_;
};

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl() {
    switch(INIT_LEAF_TYPE) {
    case NodeType::ROLEAF:
        root_ = new ROLeaf(); // TODO
        global_stats = ROSTATS;
        break;
    case NodeType::RWLEAF:
        root_ = new RWLeaf();
        global_stats = RWSTATS;
        break;
    case NodeType::WOLEAF:
        root_ = new WOLeaf();
        global_stats = WOSTATS;
        break;
    }

    // global variables assignment
    do_morphing = MORPH_IF;
    global_stats = 0;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::~MorphtreeImpl() {
    delete root_;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::lookup(const _key_t &key, _val_t & val) {
    // global_stats = (global_stats << 1);
    BaseNode * cur = root_;

    _val_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Lookup(key, val);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::insert(const _key_t &key, _val_t val) {
    // global_stats = (global_stats << 1) + 1;

    _key_t split_k;
    BaseNode * split_node;
    bool splitIf = insert_recursive(root_, key, val, &split_k, &split_node);
    
    if(splitIf) {
        Record tmp[2] = {Record(MIN_KEY, root_), Record(split_k, split_node)};
        ROInner * newroot = new ROInner(tmp, 2);
        root_ = newroot;
    }
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::insert_recursive(BaseNode * n, const _key_t & key, const _val_t val, 
                                    _key_t * split_k, BaseNode ** split_n) {
    if(n->Leaf()) {
        return n->Store(key, val, split_k, split_n);
    } else {
        _val_t v; n->Lookup(key, v);
        BaseNode * child = (BaseNode *) v;

        _key_t split_k_child;
        BaseNode * split_n_child;
        bool splitIf = insert_recursive(child, key, val, &split_k_child, &split_n_child);

        if(splitIf) {
            bool found = n->Store(split_k_child, split_n_child, split_k, split_n);

            return found;
        } else {
            return false;
        }
    }
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::Print() {
    root_->Print(string(""));
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::update(const _key_t & key, const _val_t val) {
    // TODO
    return false;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::remove(const _key_t & key, _val_t & val) {
    // TODO
    return false;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
uint64_t MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::scan(const _key_t &startKey, int range, std::vector<_val_t> &result) {
    // TODO
    return 0;
}

} // namespace morphtree

#endif // __MORPHTREE_IMPL_H__