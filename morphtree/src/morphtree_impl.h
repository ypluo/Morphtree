#ifndef __MORPHTREE_IMPL_H__
#define __MORPHTREE_IMPL_H__

#include "node.h"

namespace morphtree {

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF = false>
class MorphtreeImpl {
public:
    explicit MorphtreeImpl();

    MorphtreeImpl(std::vector<Record> & initial_recs);

    ~MorphtreeImpl();
 
    void insert(const _key_t & key, const uint64_t val);

    bool update(const _key_t & key, const uint64_t val);

    bool remove(const _key_t & key);

    bool lookup(const _key_t & key, uint64_t & v);

    int scan(const _key_t &startKey, int len, Record *result);

    BaseNode * bulkload(std::vector<Record> & initial_recs);

    void Print();
    
private:
    BaseNode * root_;
};

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl() {
    switch(INIT_LEAF_TYPE) {
    case NodeType::ROLEAF:
        root_ = new ROLeaf(); // TODO
        global_stats = ROSTATS;
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
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl(std::vector<Record> & initial_recs) {
    root_ = bulkload(initial_recs);
    // global variables assignment
    do_morphing = MORPH_IF;
    global_stats = 0;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::~MorphtreeImpl() {
    delete root_;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::lookup(const _key_t &key, uint64_t & val) {
    // global_stats = (global_stats << 1);
    BaseNode * cur = root_;

    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Lookup(key, val);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::insert(const _key_t &key, uint64_t val) {
    // global_stats = (global_stats << 1) + 1;
    BaseNode * cur = root_;

    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }
    _key_t split_k_child;
    BaseNode * split_n_child;
    bool splitIf = cur->Store(key, val, &split_k_child, &split_n_child);
    
    if(splitIf == true) {
        if(root_ == cur) { 
            // no any leaf node
            Record tmp[2] = {Record(MIN_KEY, (uint64_t)root_), 
                                Record(split_k_child, (uint64_t)split_n_child)};
            ROInner * newroot = new ROInner(tmp, 2);
            root_ = newroot;
        } else {
            // insert into the root node
            root_->Store(split_k_child, (uint64_t)split_n_child, nullptr, nullptr);
        }
    }
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::Print() {
    root_->Print(string(""));
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::update(const _key_t & key, const uint64_t val) {
    // global_stats = (global_stats << 1);
    BaseNode * cur = root_;

    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Update(key, val);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::remove(const _key_t & key) {
    // global_stats = (global_stats << 1);
    BaseNode * cur = root_;

    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Remove(key);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
int MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::scan(const _key_t &startKey, int len, Record *result) {
    // the user is reponsible for reserve enough space for saving result
    BaseNode * cur = root_;

    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(startKey, v);
        cur = (BaseNode *) v;
    }

    return cur->Scan(startKey, len, result);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
BaseNode * MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::bulkload(std::vector<Record> & initial_recs) {
    int leafnode_num = (initial_recs.size() / GLOBAL_LEAF_SIZE) * 2 +
                        (initial_recs.size() % GLOBAL_LEAF_SIZE > 0 ? 1 : 0) * 2;
    Record * index_record = new Record[leafnode_num];

    int i = 0;
    for(; i < initial_recs.size() / GLOBAL_LEAF_SIZE; i++) {
        Record * base = initial_recs.data() + i * GLOBAL_LEAF_SIZE;
        int split_pos = getSubOptimalSplitkey(base, GLOBAL_LEAF_SIZE);
        index_record[i * 2].key = base[0].key;
        index_record[i * 2].val = uint64_t(new ROLeaf(base, split_pos));
        index_record[i * 2 + 1].key = base[split_pos].key;
        index_record[i * 2 + 1].val = uint64_t(new ROLeaf(base + split_pos, GLOBAL_LEAF_SIZE - split_pos));
    }

    if(initial_recs.size() % GLOBAL_LEAF_SIZE > 0) {
        int total = initial_recs.size() - i * GLOBAL_LEAF_SIZE; 
        Record * base = initial_recs.data() + i * GLOBAL_LEAF_SIZE;
        int split_pos = getSubOptimalSplitkey(base, total);
        index_record[i * 2].key = base[0].key;
        index_record[i * 2].val = uint64_t(new ROLeaf(base, split_pos));
        index_record[i * 2 + 1].key = base[split_pos].key;
        index_record[i * 2 + 1].val = uint64_t(new ROLeaf(base + split_pos, total - split_pos));
    }

    // link them togather
    for(int i = 0; i < leafnode_num - 1; i++) {
        uint64_t vv1 = index_record[i].val;
        uint64_t vv2 = index_record[i + 1].val;
        ROLeaf * cur = (ROLeaf *) vv1;
        cur->sibling = (ROLeaf *) vv2;
    }

    BaseNode * new_root = new ROInner(index_record, leafnode_num);
    delete [] index_record;
    
    return new_root;
}

} // namespace morphtree

#endif // __MORPHTREE_IMPL_H__