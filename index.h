#ifndef _INDEX_H
#define _INDEX_H

#include <iostream>
#include "ALEX/src/core/alex.h"
#include "LIPP/src/core/lipp.h"
#include "PGM-index/pgm_index_dynamic.hpp"
#include "FITingTree/inplace_index.h"
#include "morphtree/src/morphtree_impl.h"
#include "FITingTree/btree.h"

template<typename KeyType, typename ValType>
class Index
{
 public:
    virtual bool insert(KeyType key, ValType value) = 0;

    virtual bool find(KeyType key, ValType *v) = 0;

    virtual bool upsert(KeyType key, ValType value) = 0;

    virtual uint64_t scan(KeyType key, int range) = 0;

    virtual int64_t printTree() const = 0;
    
    virtual void bulkload(std::pair<KeyType, ValType> * recs, int len) = 0;

    // Destructor must also be virtual
    virtual ~Index() {}
};

/////////////////////////////////////////////////////////////////////
// ALEX
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class AlexIndex : public Index<KeyType, ValType>
{
public:
    AlexIndex() {
        idx = new alex::Alex<KeyType, uint64_t>;
    }

    ~AlexIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        auto it = idx->find(key);
        if (it != idx->end()) {
            *v = it.payload();
            return true;
        } else {
            return false;
        }
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx->bulk_load(recs, len);
    }

    int64_t printTree() const {return 0;}

private:
    alex::Alex<KeyType, uint64_t> * idx;
};

/////////////////////////////////////////////////////////////////////
// LIPP
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class LippIndex : public Index<KeyType, ValType>
{
public:
    LippIndex() {
        idx = new LIPP<KeyType, ValType>;
    }

    ~LippIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        *v = idx->at(key);
        return (*v) != 0;
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx->bulk_load(recs, len);
    }

    int64_t printTree() const {return 0;}

private:
    LIPP<KeyType, ValType> * idx;
};

/////////////////////////////////////////////////////////////////////
// PGM-index
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class PGMIndex : public Index<KeyType, ValType>
{
public:
    PGMIndex() {
        idx = nullptr;
    }

    ~PGMIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(typename pgm::DynamicPGMIndex<KeyType, ValType>::Item(key, value));
        return true;
    }

    bool find(KeyType key, uint64_t *v) {
        auto iter = idx->find(key);
        if(iter != idx->end()) {
            *v = iter->second;
            return true;
        } else {
            return false;
        }
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx = new pgm::DynamicPGMIndex<KeyType, ValType>(recs, recs + len);
    }

    int64_t printTree() const {return 0;}

private:
    pgm::DynamicPGMIndex<KeyType, ValType> * idx;
};

/////////////////////////////////////////////////////////////////////
// FITingTree
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class FITingTree : public Index<KeyType, ValType>
{
public:
    FITingTree() {}

    ~FITingTree() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->upsert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        *v = idx->find(key);
        return (*v) != 0;
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        KeyType * keys = new KeyType[len];
        ValType * vals = new ValType[len];

        for(int i = 0; i < len; i++) {
            keys[i] = recs[i].first;
            vals[i] = recs[i].second;
        }

        idx = new InplaceIndex<KeyType, ValType>(keys, vals, len);
    }

    int64_t printTree() const {return 0;}

private:
    InplaceIndex<KeyType, ValType> *idx;
};

/////////////////////////////////////////////////////////////////////
// morphtree using write optimized leaf nodes
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class WoIndex : public Index<KeyType, ValType>
{
public:
    WoIndex() {
        idx = new morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, false>();
    }

    ~WoIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, *v);
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        return;
    }

    int64_t printTree() const {
        idx->Print();
        return 0;
    }
    
private:
    morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, false> * idx;
};

/////////////////////////////////////////////////////////////////////
// morphtree using read optimized leaf nodes
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class RoIndex : public Index<KeyType, ValType>
{
public:
    RoIndex() {
        idx = new morphtree::MorphtreeImpl<morphtree::NodeType::ROLEAF, false>();
    }

    ~RoIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, *v);
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        return;
    }

    int64_t printTree() const {
        idx->Print();
        return 0;
    }
    
private:
    morphtree::MorphtreeImpl<morphtree::NodeType::ROLEAF, false> * idx;
};

/////////////////////////////////////////////////////////////////////
// morphtree
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class MorphTree : public Index<KeyType, ValType>
{
public:
    MorphTree() {
        idx = new morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, true>();
    }

    ~MorphTree() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, *v);
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        return;
    }

    int64_t printTree() const {
        idx->Print();
        return 0;
    }
    
private:
    morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, true> * idx;
};

/////////////////////////////////////////////////////////////////////
// stxbtree
/////////////////////////////////////////////////////////////////////
template<typename KeyType, class ValType>
class BtreeIndex : public Index<KeyType, ValType>
{
public:
    BtreeIndex() {
        idx = new stx::btree<KeyType, ValType>;
    }

    ~BtreeIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        auto it = idx->find(key);
        if (it != idx->end()) {
            *v = it.data();
            return true;
        } else {
            return false;
        }
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    int64_t printTree() const {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        return;
    }
    
private:
    stx::btree<KeyType, ValType> * idx;
};

#endif