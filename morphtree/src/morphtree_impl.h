#ifndef __MORPHTREE_IMPL_H__
#define __MORPHTREE_IMPL_H__

#include "node.h"
#include "epoch.h"

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

    void bulkload(std::vector<Record> & initial_recs);

    void Print();
    
private:
    BaseNode * root_;
    BaseNode * first_leaf_;
};

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl() {
    switch(INIT_LEAF_TYPE) {
    case NodeType::ROLEAF:
        root_ = new ROLeaf(); // TODO
        break;
    case NodeType::WOLEAF:
        root_ = new WOLeaf();
        break;
    }

    first_leaf_ = root_;

    // global variables assignment
    do_morphing = MORPH_IF;
    morph_log = new MorphLogger();
    ebr = EpochBasedMemoryReclamationStrategy::getInstance();
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl(std::vector<Record> & initial_recs) {
    // global variables assignment
    do_morphing = MORPH_IF;
    morph_log = new MorphLogger();
    ebr = EpochBasedMemoryReclamationStrategy::getInstance();

    bulkload(initial_recs);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::~MorphtreeImpl() {
    ebr->clearAll();
    // delete the inner search tree;
    root_->DeleteNode();
    // delete all leafs
    BaseNode * cur = first_leaf_;
    while(cur != root_ && cur != nullptr) {
        BaseNode * tmp = cur;
        cur = cur->sibling;
        tmp->DeleteNode();
    }
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::insert(const _key_t &key, uint64_t val) {
    EpochGuard guard;

    BaseNode * cur = root_;
    uint64_t v;
    while(!cur->Leaf()) {
        BaseNode * last = cur;
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
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::lookup(const _key_t &key, uint64_t & val) {
    EpochGuard guard;

    BaseNode * cur = root_;
    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Lookup(key, val);
}


template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::Print() {
    root_->Print(string(""));
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::update(const _key_t & key, const uint64_t val) {
    EpochGuard guard;

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
    EpochGuard guard;

    BaseNode * cur = root_;
    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(key, v);
        cur = (BaseNode *) v;
    }

    return cur->Remove(key);
}

// users are reponsible for reserve enough space for saving result
template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
int MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::scan(const _key_t &startKey, int len, Record *result) {
    EpochGuard guard;
    
    BaseNode * cur = root_;
    uint64_t v;
    while(!cur->Leaf()) {
        cur->Lookup(startKey, v);
        cur = (BaseNode *) v;
    }

    return cur->Scan(startKey, len, result);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::bulkload(std::vector<Record> & initial_recs) {
    EpochGuard guard;
    
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
            ((ROLeaf *)l1)->mysplitkey = base[split_pos].key;
            ((ROLeaf *)l2)->mysplitkey = (i * GLOBAL_LEAF_SIZE + GLOBAL_LEAF_SIZE < total_num) ? base[GLOBAL_LEAF_SIZE].key : MAX_KEY;
        } else {
            l1 = new ROLeaf(base, split_pos);
            l2 = new ROLeaf(base + split_pos, GLOBAL_LEAF_SIZE - split_pos);
            ((WOLeaf *)l1)->mysplitkey = base[split_pos].key;
            ((WOLeaf *)l2)->mysplitkey = (i * GLOBAL_LEAF_SIZE + GLOBAL_LEAF_SIZE < total_num) ? base[GLOBAL_LEAF_SIZE].key : MAX_KEY;
        }

        index_record[i * 2].key = (i == 0 ? MIN_KEY : base[0].key);
        index_record[i * 2].val = uint64_t(l1);
        index_record[i * 2 + 1].key = base[split_pos].key;
        index_record[i * 2 + 1].val = uint64_t(l2);
    }

    if(initial_recs.size() % GLOBAL_LEAF_SIZE > 0) {
        int total = initial_recs.size() - i * GLOBAL_LEAF_SIZE; 
        Record * base = initial_recs.data() + i * GLOBAL_LEAF_SIZE;
        int split_pos = getSubOptimalSplitkey(base, total);
        if(INIT_LEAF_TYPE == NodeType::ROLEAF) {
            l1 = new ROLeaf(base, split_pos);
            l2 = new ROLeaf(base + split_pos, total - split_pos);
            ((ROLeaf *)l1)->mysplitkey = base[split_pos].key;
            ((ROLeaf *)l2)->mysplitkey = MAX_KEY;
        } else {
            l1 = new WOLeaf(base, split_pos);
            l2 = new WOLeaf(base + split_pos, total - split_pos);
            ((WOLeaf *)l1)->mysplitkey = base[split_pos].key;
            ((WOLeaf *)l2)->mysplitkey = MAX_KEY;
        }
        
        index_record[i * 2].key = base[0].key;
        index_record[i * 2].val = uint64_t(l1);
        index_record[i * 2 + 1].key = base[split_pos].key;
        index_record[i * 2 + 1].val = uint64_t(l2);
    }

    // link them togather
    for(int i = 0; i < leafnode_num - 1; i++) {
        uint64_t vv1 = index_record[i].val;
        uint64_t vv2 = index_record[i + 1].val;
        BaseNode * cur = (BaseNode *) vv1;
        cur->sibling = (BaseNode *) vv2;
    }
    BaseNode * cur = (BaseNode *)index_record[leafnode_num - 1].val;
    cur->sibling = nullptr;

    root_ = new ROInner(index_record, leafnode_num);
    first_leaf_ = (BaseNode *)index_record[0].val;
    delete [] index_record;
}

} // namespace morphtree

#endif // __MORPHTREE_IMPL_H__