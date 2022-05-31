#ifndef __MORPHTREE2_IMPL_H__
#define __MORPHTREE2_IMPL_H__

#include <vector>
#include <atomic>

#include "fixtree.h"
#include "node.h"

using std::vector;

namespace newtree {

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
    void rebuild_uptree();

private:
    Fixtree * uptree_;
    BaseNode * first_;
    std::vector<Record> * buffer_;
    bool is_rebuilding_;
    const uint32_t MAX_STEP = 2;
};

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::MorphtreeImpl() {
    switch(INIT_LEAF_TYPE) {
    case NodeType::ROLEAF:
        first_ = new ROLeaf();
        break;
    case NodeType::WOLEAF:
        first_ = new WOLeaf();
        break;
    }

    first_->sibling = nullptr;
    Record tmp[] = {Record(MIN_KEY, first_)};
    uptree_ = new Fixtree(tmp, 1);
    buffer_ = new std::vector<Record>;
    buffer_->reserve(0xff);
    is_rebuilding_ = false;

    // global variables assignment
    do_morphing = MORPH_IF;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::~MorphtreeImpl() {
    BaseNode * cur = first_;
    while(cur != nullptr) {
        BaseNode *next = cur->sibling;
        cur->DeleteNode();
        cur = next;
    }

    delete uptree_;
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
bool MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::lookup(const _key_t &key, _val_t & val) {
    BaseNode * leaf = (BaseNode *)uptree_->Lookup(key);

    uint32_t step = 0;
    while (leaf->split_k < key) {
        step += 1;
        leaf = leaf->sibling;
    }
    
    if(step > MAX_STEP)
        rebuild_uptree();

    return leaf->Lookup(key, val);
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::insert(const _key_t &key, _val_t val) {
    BaseNode * leaf = (BaseNode *)uptree_->Lookup(key);

    uint32_t step = 0;
    while (leaf->split_k < key) {
        step += 1;
        leaf = leaf->sibling;
    }

    if(step > MAX_STEP)
        rebuild_uptree();

    _key_t split_key;
    BaseNode * split_node;
    bool splitIf = leaf->Store(key, val, &split_key, &split_node);
    if(splitIf) {
        if(!uptree_->Store(split_key, split_node)) {
            buffer_->push_back({split_key, split_node});
        }
    }
}

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::Print() {
    uptree_->printAll();
    return ;
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

template<NodeType INIT_LEAF_TYPE, bool MORPH_IF>
void MorphtreeImpl<INIT_LEAF_TYPE, MORPH_IF>::rebuild_uptree() {
    vector<Record> * new_buffer = new vector<Record>;
    new_buffer->reserve(buffer_->size());
    vector<Record> * imm_buffer = buffer_;
    buffer_ = new_buffer;

    is_rebuilding_ = true;
        // get all leaf nodes
        std::sort(imm_buffer->begin(), imm_buffer->end());
        std::vector<Record> leafs;
        leafs.reserve(0x2fff);
        uptree_->merge(*imm_buffer, leafs);
        
        // printf("[");
        // for(int i = 0; i < leafs.size(); i++) {
        //     printf("%12.8lf, ", leafs[i]);
        // }
        // printf("]\n");
        // uptree_->printAll();
        // printf("\n\n");

        // rebuild the top index 
        Fixtree * old_tree = uptree_;
        Fixtree * new_tree = new Fixtree(leafs.data(), leafs.size());
        uptree_ = new_tree;
    is_rebuilding_ = false;

    delete old_tree;
    delete imm_buffer;
}

} // namespace newtree

#endif // __MORPHTREE2_IMPL_H__