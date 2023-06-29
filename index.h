#ifndef _INDEX_H
#define _INDEX_H

#include <iostream>
#include <vector>
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

    virtual bool remove(KeyType key) {return true;}

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
        return true;
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
        typename alex::Alex<KeyType, uint64_t>::Iterator it = idx->find(key);
        if (it != idx->end()) {
            it.payload() = value;
            return true;
        }
        return false;
    }

    uint64_t scan(KeyType key, int range) {
        std::vector<std::pair<KeyType, uint64_t>> res(range);
        int cnt = 0;

        auto it = idx->lower_bound(key);
        while(cnt < range && it != idx->end()) {
            res[cnt++] = {it.key(), it.payload()};
            ++it;
        }

        return cnt;
    }

    bool remove(KeyType key) {
        idx->erase(key);
        return true;
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
        return true;
    }

    bool find(KeyType key, uint64_t *v) {
        *v = idx->at(key);
        return (*v) != 0;
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->at(key) = value;
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
        idx->insert_or_assign(key, value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        std::vector<std::pair<KeyType, uint64_t>> res(range);
        int cnt = 0;

        auto it = idx->lower_bound(key);
        while(cnt < range && it != idx->end()) {
            res[cnt++] = {(*it).first, (*it).second};
            ++it;
        }

        return cnt;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx = new pgm::DynamicPGMIndex<KeyType, ValType>(recs, recs + len);
    }

    bool remove(KeyType key) {
        idx->erase(key);
        return true;
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
        return true;
    }

    bool find(KeyType key, uint64_t *v) {
        *v = idx->find(key);
        return (*v) != 0;
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->upsert(key, value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        //TODO
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
        idx->insert(key, (void *)value);
        return true;
    }

    bool find(KeyType key, uint64_t *v) {
        void * res;
        return idx->lookup(key, res);
        *v = (uint64_t)res;
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, (void *)value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        Record buff[1000];
        return idx->scan(key, range, buff);
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        std::vector<Record> tmp(len);
        for(size_t i = 0; i < len; i++) {
            tmp[i] = {recs[i].first, (void *)recs[i].second};
        }
        idx->bulkload(tmp);
    }

    int64_t printTree() const {
        idx->Print();
        return 0;
    }

    bool remove(KeyType key) {
        idx->remove(key);
        return true;
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
        idx->insert(key, (void *)value);
        return true;
    }

    bool find(KeyType key, uint64_t *v) {
        void * res;
        return idx->lookup(key, res);
        *v = (uint64_t)res;
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, (void *)value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        Record buff[1000];
        return idx->scan(key, range, buff);
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        std::vector<Record> tmp(len);
        for(size_t i = 0; i < len; i++) {
            tmp[i] = {recs[i].first, (void *)recs[i].second};
        }
        idx->bulkload(tmp);
    }

    int64_t printTree() const {
        idx->Print();
        return 0;
    }

    bool remove(KeyType key) {
        idx->remove(key);
        return true;
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
        idx->insert(key, (void *)value);
        return true;
    }

    bool find(KeyType key, uint64_t *v) {
        void * res;
        return idx->lookup(key, res);
        *v = (uint64_t)res;
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, (void *)value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        Record buff[1000];
        return idx->scan(key, range, buff);
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        std::vector<Record> tmp(len);
        for(size_t i = 0; i < len; i++) {
            tmp[i] = {recs[i].first, (void *)recs[i].second};
        }
        idx->bulkload(tmp);
    }

    bool remove(KeyType key) {
        idx->remove(key);
        return true;
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
        return true;
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
        auto it = idx->find(key);
        if (it != idx->end()) {
            it.data() = value;
        }
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        std::vector<std::pair<KeyType, uint64_t>> res(range);
        int cnt = 0;

        auto it = idx->lower_bound(key);
        while(cnt < range && it != idx->end()) {
            res[cnt++] = {it.key(), it.data()};
            ++it;
        }

        return cnt;
    }

    int64_t printTree() const {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx->bulk_load(recs, recs + len);
    }

    bool remove(KeyType key) {
        idx->erase(key);
        return true;
    }
    
private:
    stx::btree<KeyType, ValType> * idx;
};

#endif