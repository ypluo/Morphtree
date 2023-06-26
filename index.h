#ifndef _INDEX_H
#define _INDEX_H

#include <iostream>
#include "ALEX/src/core/alex.h"
#include "LIPP/src/core/lipp.h"
#include "XIndex/xindex_impl.h"
#include "FINEdex/osm/include/function.h"
#include "FINEdex/osm/include/finedex.h"
#include "morphtree/src/morphtree_impl.h"
#include "BTreeOLC/BTreeOLC.h"

extern int worknum;

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
        idx = new alexol::Alex <KeyType, ValType>;
    }

    ~AlexIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        return idx->insert(key, value);
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->get_payload(key, v);
    }

    bool upsert(KeyType key, uint64_t value) {
        return idx->update(key, value);
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    bool remove(KeyType key) {
        idx->erase(key);
        return true;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx->set_max_model_node_size(1 << 24);
        idx->set_max_data_node_size(1 << 19);
        idx->bulk_load(recs, len);
    }

    int64_t printTree() const {return 0;}

private:
    alexol::Alex <KeyType, ValType> *idx;
};

/////////////////////////////////////////////////////////////////////
// LIPP
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class LippIndex : public Index<KeyType, ValType>
{
public:
    LippIndex() {
        idx = new lippolc::LIPP<KeyType, ValType>;
    }

    ~LippIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->insert(key, value);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->at(key, *v);
    }

    bool upsert(KeyType key, uint64_t value) {
        idx->update(key, value);
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        idx->bulk_load(recs, static_cast<int>(len));
    }

    int64_t printTree() const {return 0;}

private:
    lippolc::LIPP<KeyType, ValType> * idx;
};

/////////////////////////////////////////////////////////////////////
// XIndex
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class XindexIndex : public Index<KeyType, ValType>
{

public:
    class ModelKeyType {
        typedef std::array<double, 1> model_key_t;

        public:
        static constexpr size_t model_key_size() { return 1; }
        static ModelKeyType max() {
            static const KeyType MAX_KEY = std::numeric_limits<KeyType>::max();
            static ModelKeyType max_key(MAX_KEY);
            return max_key;
        }
        static ModelKeyType min() {
            static const KeyType MIN_KEY = typeid(KeyType) == typeid(double) || typeid(KeyType) == typeid(float) 
                            ? -1 * MAX_KEY : std::numeric_limits<KeyType>::min();
            static ModelKeyType min_key(MIN_KEY);
            return min_key;
        }

        ModelKeyType() : key(0) {}
        ModelKeyType(KeyType key) : key(key) {}
        ModelKeyType(const ModelKeyType &other) { key = other.key; }
        ModelKeyType &operator=(const ModelKeyType &other) {
            key = other.key;
            return *this;
        }

        model_key_t to_model_key() const {
            model_key_t model_key;
            model_key[0] = key;
            return model_key;
        }

        friend bool operator<(const ModelKeyType &l, const ModelKeyType &r) { return l.key < r.key; }
        friend bool operator>(const ModelKeyType &l, const ModelKeyType &r) { return l.key > r.key; }
        friend bool operator>=(const ModelKeyType &l, const ModelKeyType &r) { return l.key >= r.key; }
        friend bool operator<=(const ModelKeyType &l, const ModelKeyType &r) { return l.key <= r.key; }
        friend bool operator==(const ModelKeyType &l, const ModelKeyType &r) { return l.key == r.key; }
        friend bool operator!=(const ModelKeyType &l, const ModelKeyType &r) { return l.key != r.key; }

        KeyType key;
    };

public:
    XindexIndex() {
        idx = nullptr;
    }

    ~XindexIndex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        idx->put(key, value, 0);
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        return idx->get(key, *v, 0);
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        std::vector<ModelKeyType> keys;
        std::vector<ValType> vals;
        keys.resize(len);
        vals.resize(len);
        
        for(int i = 0; i < len; i++) {
            keys[i] = ModelKeyType(recs[i].first);
            vals[i] = recs[i].second;
        }

        int bg_n = std::max(worknum / 12, 1);
        idx = new xindex::XIndex<ModelKeyType, ValType>(keys, vals, worknum, bg_n);
    }

    int64_t printTree() const {return 0;}

private:
    xindex::XIndex<ModelKeyType, ValType> * idx;
};

/////////////////////////////////////////////////////////////////////
// FINEDex
/////////////////////////////////////////////////////////////////////
template<typename KeyType, typename ValType>
class Finedex : public Index<KeyType, ValType>
{
public:
    Finedex() {
        idx = new finedex::FINEdex<KeyType, ValType>();
    }

    ~Finedex() {
        delete idx;
    }

    bool insert(KeyType key, uint64_t value) {
        finedex::result_t res = idx->insert(key, value);
        if(res == finedex::result_t::ok) {
            return true;
        }
        return false;
    }

    bool find(KeyType key, uint64_t *v) {
        finedex::result_t res = idx->find(key, *v);
        if(res == finedex::result_t::ok) {
            return true;
        }
        return false;
    }

    bool upsert(KeyType key, uint64_t value) {
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        std::vector<KeyType> keys;
        std::vector<ValType> vals;
        keys.resize(len);
        vals.resize(len);
        
        for(int i = 0; i < len; i++) {
            keys[i] = recs[i].first;
            vals[i] = recs[i].second;
        }

        idx->train(keys, vals, 32);
    }

    int64_t printTree() const {return 0;}

private:
    finedex::FINEdex<KeyType, ValType> * idx;
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
        for(size_t i = 0; i < len; i++) {
            idx->insert(recs[i].first, recs[i].second);
        }
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
        for(size_t i = 0; i < len; i++) {
            idx->insert(recs[i].first, recs[i].second);
        }
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
        for(size_t i = 0; i < len; i++) {
            idx->insert(recs[i].first, recs[i].second);
        }
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
        idx = new btreeolc::BTree<KeyType, ValType>;
    }

    ~BtreeIndex() {
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
        return true;
    }

    uint64_t scan(KeyType key, int range) {
        return 0;
    }

    int64_t printTree() const {
        return 0;
    }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        for(size_t i = 0; i < len; i++) {
            idx->insert(recs[i].first, recs[i].second);
        }
    }

    bool remove(KeyType key) {
        return true;
    }
    
private:
    btreeolc::BTree<KeyType, ValType> * idx;
};

#endif