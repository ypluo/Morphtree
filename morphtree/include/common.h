#ifndef __COMMON_H__
#define __COMMON_H__

#include <algorithm>
#include <vector>
#include <cstdint>
#include <limits>
#include <sys/time.h>
#include <sys/stat.h>

#define KILO 1024
#define MILLION (KILO * KILO)

typedef uint64_t _key_t;
typedef void * _val_t;

const _key_t MAX_KEY   = UINT64_MAX;
const _key_t MIN_KEY   = 0;

struct Record {
    _key_t key;
    _val_t val;
    
    Record(): key(MAX_KEY), val(nullptr) {}
    Record(_key_t k, _val_t v) : key(k), val(v) {}

    inline bool operator < (const Record & oth) {
        return key < oth.key;
    }

    inline bool operator > (const Record & oth) const {
        return key > oth.key;
    }
};

static inline double seconds()
{
    timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec + now.tv_usec/1000000.0;
}

static inline int getRandom() {
	timeval now;
	gettimeofday(&now, NULL);
	return now.tv_usec;
}

static inline bool fileExist(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
}

#endif // __COMMON_H__