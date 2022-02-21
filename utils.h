#include "index.h"
#include <sys/time.h>

#ifndef _UTIL_H
#define _UTIL_H

//This enum enumerates index types we support
enum {
  TYPE_ALEX,
  TYPE_ARTOLC,
  TYPE_HYDRALIST,
  TYPE_NONE,
};

// These are workload operations
enum {
  OP_INSERT,
  OP_READ,
  OP_UPSERT,
  OP_SCAN,
};

// These are YCSB workloads
enum {
  WORKLOAD_A,
  WORKLOAD_B,
  WORKLOAD_C,
  WORKLOAD_E,
  WORKLOAD_F,
  WORKLOAD_D,
};

// These are key types we use for running the benchmark
enum {
  RAND_KEY,
  MONO_KEY,
  RDTSC_KEY,
  EMAIL_KEY,
};

//==============================================================
// GET INSTANCE
//==============================================================
template<typename KeyType, 
         typename KeyComparator=std::less<KeyType>, 
         typename KeyEuqal=std::equal_to<KeyType>, 
         typename KeyHash=std::hash<KeyType>>
Index<KeyType, KeyComparator> *getInstance(const int type, const uint64_t kt) {
  if (type == TYPE_ALEX)
    return nullptr;
  else if (type == TYPE_ARTOLC)
      return new ArtOLCIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_HYDRALIST)
    return new HydraListIndex<KeyType, KeyComparator>(kt);
  else {
    fprintf(stderr, "Unknown index type: %d\n", type);
    exit(1);
  }
  
  return nullptr;
}

inline double randint() { 
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

#endif