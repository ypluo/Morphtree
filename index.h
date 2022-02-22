#ifndef _INDEX_H
#define _INDEX_H

#include <iostream>
#include "indexkey.h"
#include "ARTOLC/Tree.h"
#include "ALEX/src/core/alex.h"
#include "hydralist/include/HydraList.h"

template<typename KeyType, class KeyComparator>
class Index
{
 public:
  virtual bool insert(KeyType key, uint64_t value) = 0;

  virtual uint64_t find(KeyType key, std::vector<uint64_t> *v) = 0;

  virtual bool upsert(KeyType key, uint64_t value) = 0;

  virtual uint64_t scan(KeyType key, int range) = 0;

  virtual int64_t getMemory() const = 0;

  // This initializes the thread pool
  virtual void UpdateThreadLocal(size_t thread_num) = 0;
  virtual void AssignGCID(size_t thread_id) = 0;
  virtual void UnregisterThread(size_t thread_id) = 0;

  // Destructor must also be virtual
  virtual ~Index() {}
};

/////////////////////////////////////////////////////////////////////
// ARTOLC
/////////////////////////////////////////////////////////////////////

template<typename KeyType, class KeyComparator>
class ArtOLCIndex : public Index<KeyType, KeyComparator>
{
 public:

  ~ArtOLCIndex() {
    delete idx;
  }

  void UpdateThreadLocal(size_t thread_num) {}
  void AssignGCID(size_t thread_id) {}
  void UnregisterThread(size_t thread_id) {}

  void setKey(Key& k, uint64_t key) { k.setInt(key); }
  void setKey(Key& k, GenericKey<31> key) { k.set(key.data,31); }

  bool insert(KeyType key, uint64_t value) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    idx->insert(k, value, t);
    return true;
  }

  uint64_t find(KeyType key, std::vector<uint64_t> *v) {
    auto t = idx->getThreadInfo();
    Key k; setKey(k, key);
    uint64_t result=idx->lookup(k, t);
    v->clear();
    v->push_back(result);
    return 0;
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

  int64_t getMemory() const {
    return 0;
  }

  void merge() {
  }

  ArtOLCIndex(uint64_t kt) {
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

 private:
  Key maxKey;
  ART_OLC::Tree *idx;
};


/////////////////////////////////////////////////////////////////////
// HYDRALIST
/////////////////////////////////////////////////////////////////////

// template<typename KeyType, class KeyComparator>
// class HydraListIndex : public Index<KeyType, KeyComparator>
// {
//  public:

//   ~HydraListIndex() {
//           //idx.~HydraList();
//   }

//   void UpdateThreadLocal(size_t thread_num) {}
//   void AssignGCID(size_t thread_id) {idx.registerThread();}
//   void UnregisterThread(size_t thread_id) {idx.unregisterThread();}

//   bool insert(KeyType key, uint64_t value) {
//     idx.insert(key, value);
//     return true;
//   }

//   uint64_t find(KeyType key, std::vector<uint64_t> *v) {
//     uint64_t result;
//     result = idx.lookup(key);
//     v->clear();
//     v->push_back(result);
//     return 0;
//   }

//   bool upsert(KeyType key, uint64_t value) {
//     idx.update(key, value);
//     return true;
//   }

//   void incKey(uint64_t& key) { key++; };
//   void incKey(GenericKey<31>& key) { key.data[strlen(key.data)-1]++; };

//   uint64_t scan(KeyType key, int range) {
//     std::vector<KeyType> result;
//     uint64_t size = idx.scan(key, range, result);
//     //if(range != size) printf("%d %d\n", range, size);
//     return size;
//   }

//   int64_t getMemory() const {
//     return 0;
//   }

//   void merge() {}

//   HydraListIndex(uint64_t kt) : idx(4){

//   }


//  private:
//  HydraList idx;
// };

#endif