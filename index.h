#ifndef _INDEX_H
#define _INDEX_H

#include <iostream>
#include "ARTOLC/Tree.h"
#include "ALEX/src/core/alex.h"
#include "stxbtree/btree.h"
#include "morphtree/src/morphtree_impl.h"

template<typename KeyType, class KeyComparator>
class Index
{
 public:
    virtual bool insert(KeyType key, uint64_t value) = 0;

    virtual bool find(KeyType key, uint64_t *v) = 0;

    virtual bool upsert(KeyType key, uint64_t value) = 0;

    virtual uint64_t scan(KeyType key, int range) = 0;

    virtual int64_t printTree() const = 0;
    
    virtual void bulkload(std::pair<KeyType, uint64_t> * recs, int len) = 0;

    // Destructor must also be virtual
    virtual ~Index() {}
};

/////////////////////////////////////////////////////////////////////
// ALEX
/////////////////////////////////////////////////////////////////////
template<typename KeyType, class KeyComparator>
class AlexIndex : public Index<KeyType, KeyComparator>
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
// stxbtree
/////////////////////////////////////////////////////////////////////
template<typename KeyType, class KeyComparator>
class BtreeIndex : public Index<KeyType, KeyComparator>
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
template<typename KeyType, class KeyComparator>
class WoIndex : public Index<KeyType, KeyComparator>
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

    int64_t printTree() const {return 0;}
    
private:
    morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, false> * idx;
};

/////////////////////////////////////////////////////////////////////
// morphtree using read optimized leaf nodes
/////////////////////////////////////////////////////////////////////
template<typename KeyType, class KeyComparator>
class RoIndex : public Index<KeyType, KeyComparator>
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
// morphtree using read optimized leaf nodes
/////////////////////////////////////////////////////////////////////
template<typename KeyType, class KeyComparator>
class MorphTree : public Index<KeyType, KeyComparator>
{
public:
    MorphTree() {
        idx = new morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, true>();
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
    morphtree::MorphtreeImpl<morphtree::NodeType::WOLEAF, true> * idx;
};


/////////////////////////////////////////////////////////////////////
// ARTOLC
/////////////////////////////////////////////////////////////////////
template<typename KeyType, class KeyComparator>
class ArtOLCIndex : public Index<KeyType, KeyComparator>
{
 public:
  ArtOLCIndex() {
    if (sizeof(KeyType)==8) {
      idx = new ART_OLC::Tree([](TID tid, Key &key) { key.setInt(*reinterpret_cast<uint64_t*>(tid)); });
      maxKey.setInt(~0ull);
    } else {
      idx = new ART_OLC::Tree([](TID tid, Key &key) { key.set(reinterpret_cast<char*>(tid),31); });
      uint8_t m[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
      maxKey.set((char*)m,31);
    }
  }

  ~ArtOLCIndex() {
    delete idx;
  }

  void UpdateThreadLocal(size_t thread_num) {}
  void AssignGCID(size_t thread_id) {}
  void UnregisterThread(size_t thread_id) {}

  void setKey(Key& k, uint64_t key) { k.setInt(key); }

  bool insert(KeyType key, uint64_t value) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    idx->insert(k, value, t);
    return true;
  }

  bool find(KeyType key, uint64_t *v) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    *v=idx->lookup(k, t);
    return true;
  }

  bool upsert(KeyType key, uint64_t value) {
    std::atomic_thread_fence(std::memory_order_acq_rel);
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    idx->insert(k, value, t);
    return true;
  }

  uint64_t scan(KeyType key, int range) {
    auto t = idx->getThreadInfo();
    Key startKey; setKey(startKey, key);

    TID results[range];
    size_t resultCount;
    Key continueKey;
    idx->lookupRange(startKey, maxKey, continueKey, results, range, resultCount, t);

    return resultCount;
  }

  int64_t printTree() const {
    return 0;
  }

    void bulkload(std::pair<KeyType, uint64_t>* recs, int len) {
        return;
    }

  void merge() {
  }

 private:
  Key maxKey;
  ART_OLC::Tree *idx;
};

#endif