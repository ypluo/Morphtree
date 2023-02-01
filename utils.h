#include "index.h"
#include <sys/time.h>

#ifndef _UTIL_H
#define _UTIL_H

//This enum enumerates index types we support
enum {
  TYPE_ALEX = 0,
  TYPE_LIPP,
  TYPE_PGM,
  TYPE_FITING,
  TYPE_MORPHTREE_WO,
  TYPE_MORPHTREE_RO,
  TYPE_MORPHTREE,
  TYPE_BTREE
};

// These are workload operations
enum {
  OP_INSERT,
  OP_READ,
  OP_UPSERT,
  OP_SCAN,
  OP_DELETE
};

//==============================================================
// GET INSTANCE
//==============================================================
template<typename KeyType, 
         typename ValType>
Index<KeyType, ValType> *getInstance(const int type) {
  if (type == TYPE_ALEX)
    return new AlexIndex<KeyType, ValType>();
  else if (type == TYPE_LIPP) 
    return new LippIndex<KeyType, ValType>();
  else if(type == TYPE_PGM) 
    return new PGMIndex<KeyType, ValType>();
  else if(type == TYPE_FITING) 
    return new FITingTree<KeyType, ValType>();
  else if(type == TYPE_MORPHTREE_WO)
    return new WoIndex<KeyType, ValType>();
  else if(type == TYPE_MORPHTREE_RO)
    return new RoIndex<KeyType, ValType>();
  else if(type == TYPE_MORPHTREE)
    return new MorphTree<KeyType, ValType>();
  else if(type == TYPE_BTREE) 
    return new BtreeIndex<KeyType, ValType>();
  else {
    fprintf(stderr, "Unknown index type 2: %d\n", type);
    exit(1);
  }
  
  return nullptr;
}

inline double randseed() { 
  struct timeval tv; 
  gettimeofday(&tv, 0); 
  return tv.tv_usec; 
}

/*
 * Rdtsc() - This function returns the value of the time stamp counter
 *           on the current core
 */
inline uint64_t Rdtsc()
{
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t) hi << 32) | lo);
}

inline double get_now() { 
  struct timeval tv; 
  gettimeofday(&tv, 0); 
  return tv.tv_sec + tv.tv_usec / 1000000.0; 
} 

/*
 * MemUsage() - Reads memory usage from /proc file system
 */
size_t MemUsage() {
  FILE *fp = fopen("/proc/self/statm", "r");
  if(fp == nullptr) {
    fprintf(stderr, "Could not open /proc/self/statm to read memory usage\n");
    exit(1);
  }

  unsigned long unused;
  unsigned long rss;
  if (fscanf(fp, "%ld %ld %ld %ld %ld %ld %ld", &unused, &rss, &unused, &unused, &unused, &unused, &unused) != 7) {
    perror("");
    exit(1);
  }

  (void)unused;
  fclose(fp);

  return rss * (4096 / 1024); // in KiB (not kB)
}

#endif