#ifndef _INDEX_H
#define _INDEX_H

#include <iostream>
#include "ALEX/src/core/alex.h"
#include "stxbtree/btree.h"
#include "morphtree/src/morphtree_impl.h"
#include "lipp/src/core/lipp.h"

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
        }
        return true;
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
        idx = new LIPP<KeyType, uint64_t>;
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
        return true;
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
    LIPP<KeyType, uint64_t> * idx;
};

/////////////////////////////////////////////////////////////////////
// stxbtree
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class BtreeIndex : public Index<KeyType, ValType>
{
public:
    BtreeIndex() {
        idx = new stx::btree<KeyType, uint64_t>;
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
        }
        return true;
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        return;
    }

    int64_t printTree() const {return 0;}
    
private:
    stx::btree<KeyType, uint64_t> * idx;
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
        idx->insert(key, reinterpret_cast<void *>(value));
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, reinterpret_cast<void * &>(*v));
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, reinterpret_cast<void *>(value));
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
// morphtree using read write leaf nodes
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class RwIndex : public Index<KeyType, ValType>
{
public:
    RwIndex() {
        idx = new morphtree::MorphtreeImpl<morphtree::NodeType::RWLEAF, false>();
    }

    ~RwIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, reinterpret_cast<void *>(value));
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, reinterpret_cast<void * &>(*v));
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, reinterpret_cast<void *>(value));
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
    morphtree::MorphtreeImpl<morphtree::NodeType::RWLEAF, false> * idx;
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
        idx->insert(key, reinterpret_cast<void *>(value));
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, reinterpret_cast<void * &>(*v));
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, reinterpret_cast<void *>(value));
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
        idx = new morphtree::MorphtreeImpl<morphtree::NodeType::RWLEAF, true>();
    }

    ~MorphTree() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, reinterpret_cast<void *>(value));
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->lookup(key, reinterpret_cast<void * &>(*v));
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, reinterpret_cast<void *>(value));
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
    morphtree::MorphtreeImpl<morphtree::NodeType::RWLEAF, true> * idx;
};
#endif