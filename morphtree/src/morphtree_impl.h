#ifndef __MORPHTREE_IMPL_H__
#define __MORPHTREE_IMPL_H__

#include "node.h"

namespace morphtree {

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF = false>
class MorphtreeImpl {
public:
    explicit MorphtreeImpl();

    explicit MorphtreeImpl(std::vector<Record> & initial_recs);

    ~MorphtreeImpl();
 
    void insert(const _key_t & key, const _val_t val);

    bool update(const _key_t & key, const _val_t val);

    bool remove(const _key_t & key);

    bool lookup(const _key_t & key, _val_t & v);

    int scan(const _key_t &startKey, int range, Record *result);

    void Print();

    void bulkload(std::vector<Record> & initial_recs);
    
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
    case NodeType::WOLEAF:
        root_ = new WOLeaf();
        global_stats = WOSTATS;
        break;
    }

    // global variables assignment
    do_morphing = MORPH_IF;
    global_stats = 0;
    rebuild_times = 0;
    morph_times = 0;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl(std::vector<Record> & initial_recs) {
    // global variables assignment
    do_morphing = MORPH_IF;
    rebuild_times = 0;
    morph_times = 0;

    bulkload(initial_recs);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::~MorphtreeImpl() {
    delete root_;
    // fprintf(stderr, "Rebuild times: %d\nMorph Times: %d\n", rebuild_times, morph_times);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::lookup(const _key_t &key, _val_t & val) {
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
    BaseNode * cur = root_;

    _val_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Update(key, val);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::remove(const _key_t & key) {
    BaseNode * cur = root_;

    _val_t v;
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

    _val_t v;
    while(!cur->Leaf()) {
        cur->Lookup(startKey, v);
        cur = (BaseNode *) v;
    }

    return cur->Scan(startKey, len, result);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::bulkload(std::vector<Record> & initial_recs) {
    int leafnode_num = (initial_recs.size() / GLOBAL_LEAF_SIZE) * 2 +
                        (initial_recs.size() % GLOBAL_LEAF_SIZE > 0 ? 1 : 0) * 2;
    int total_num = initial_recs.size();
    Record * index_record = new Record[leafnode_num];
    int i = 0;
    BaseNode *l1, *l2;
    for(; i < initial_recs.size() / GLOBAL_LEAF_SIZE; i++) {
        Record * base = initial_recs.data() + i * GLOBAL_LEAF_SIZE;
        int split_pos = getSubOptimalSplitkey(base, GLOBAL_LEAF_SIZE);
        
        if(INIT_LEAF_TYPE == NodeType::ROLEAF) {
            l1 = new ROLeaf(base, split_pos);
            l2 = new ROLeaf(base + split_pos, GLOBAL_LEAF_SIZE - split_pos);
        } else {
            l1 = new WOLeaf(base, split_pos);
            l2 = new WOLeaf(base + split_pos, GLOBAL_LEAF_SIZE - split_pos);
        }

        index_record[i * 2].key = (i == 0 ? MIN_KEY : base[0].key);
        index_record[i * 2].val = _val_t(l1);
        index_record[i * 2 + 1].key = base[split_pos].key;
        index_record[i * 2 + 1].val = _val_t(l2);
    }

    if(initial_recs.size() % GLOBAL_LEAF_SIZE > 0) {
        int total = initial_recs.size() - i * GLOBAL_LEAF_SIZE; 
        Record * base = initial_recs.data() + i * GLOBAL_LEAF_SIZE;
        int split_pos = getSubOptimalSplitkey(base, total);
        if(INIT_LEAF_TYPE == NodeType::ROLEAF) {
            l1 = new ROLeaf(base, split_pos);
            l2 = new ROLeaf(base + split_pos, total - split_pos);
        } else {
            l1 = new WOLeaf(base, split_pos);
            l2 = new WOLeaf(base + split_pos, total - split_pos);
        }
        
        index_record[i * 2].key = base[0].key;
        index_record[i * 2].val = _val_t(l1);
        index_record[i * 2 + 1].key = base[split_pos].key;
        index_record[i * 2 + 1].val = _val_t(l2);
    }

    // link them togather
    for(int i = 0; i < leafnode_num - 1; i++) {
        _val_t vv1 = index_record[i].val;
        _val_t vv2 = index_record[i + 1].val;
        BaseNode * cur = (BaseNode *) vv1;
        cur->sibling = (BaseNode *) vv2;
    }
    BaseNode * cur = (BaseNode *)index_record[leafnode_num - 1].val;
    cur->sibling = nullptr;

    root_ = new ROInner(index_record, leafnode_num);
    delete [] index_record;
}

} // namespace morphtree

#endif // __MORPHTREE_IMPL_H__