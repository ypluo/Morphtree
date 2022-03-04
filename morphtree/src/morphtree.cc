#include "morphtree_impl.h"

namespace morphtree {

static BaseNode * new_init_node(const NodeType & type) {
    switch(type) {
    case NodeType::RWLEAF:
        return new RWLeaf();
    case NodeType::WOLEAF:
        return new WOLeaf();
    default:
        return new RWLeaf();
    }
}

MorphtreeImpl::MorphtreeImpl() {
    root_ = new_init_node(INIT_NODE_TYPE);

    // global access count to all leaf nodes
    access_count = 0;
}

MorphtreeImpl::~MorphtreeImpl() {
    delete root_;
}

bool MorphtreeImpl::lookup(const _key_t &key, _val_t & val) {
    BaseNode * cur = root_;

    _val_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Lookup(key, val);
}

void MorphtreeImpl::insert(const _key_t &key, _val_t val) {
    BaseNode * cur = root_;

    _key_t split_k;
    BaseNode * split_node;
    bool splitIf = insert_recursive(root_, key, val, &split_k, &split_node);
    
    if(splitIf) {
        Record tmp[2] = {Record(MIN_KEY, root_), Record(split_k, split_node)};
        ROInner * newroot = new ROInner(tmp, 2);
        root_ = newroot;
    }
}

bool MorphtreeImpl::insert_recursive(BaseNode * n, const _key_t & key, const _val_t val, 
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

void MorphtreeImpl::Print() {
    root_->Print(string(""));
}

bool MorphtreeImpl::update(const _key_t & key, const _val_t val) {
    // TODO
    return false;
}

bool MorphtreeImpl::remove(const _key_t & key, _val_t & val) {
    // TODO
    return false;
}

uint64_t MorphtreeImpl::scan(const _key_t &startKey, int range, std::vector<_val_t> &result) {
    // TODO
    return 0;
}

} // namespace morphtree
