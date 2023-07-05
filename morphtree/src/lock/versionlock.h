#ifndef __VERSION_LOCK__
#define __VERSION_LOCK__

#include <cstdint>

#include <iomanip>
#include <glog/logging.h>

class VersionLock {
private:
    uint8_t lock : 1;
    uint8_t version : 7;

public:
    VersionLock() {
        lock = 0;
        version = 0;
    }

    uint8_t Version() const {return version;}

    bool TryLock() {
        VersionLock expected = *this;
        expected.lock = 0; // we always expect the lock to be free
        VersionLock desired = expected; // the desired value should always be fresh
        desired.lock = 1;
        desired.version = expected.version + 1;
        return __atomic_compare_exchange(this, &expected, &desired, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE);
    }

    void Lock() {
        bool success;
        do {
            VersionLock expected = *this;
            expected.lock = 0; // we always expect the lock to be free
            VersionLock desired = expected; // the desired value should always be fresh
            desired.lock = 1;
            desired.version = expected.version + 1;
            success = __atomic_compare_exchange(this, &expected, &desired, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE);
        } while(success == false);
    }

    void UnLock() {
        VersionLock desired = *this;
        VersionLock discard;
        desired.lock = 0;
        desired.version += 1;
        __atomic_exchange(this, &desired, &discard, __ATOMIC_ACQUIRE);
    }

    bool IsLocked() {
        return lock == 1;
    }
};

class ExtVersionLock {
public:
    static uint8_t Version(uint64_t v) {
        return ((v >> 56) & 0x7f);
    }

    static void Lock(uint64_t *v) {
        bool success;
        do {
            uint64_t expected = (*v & 0x7fffffffffffffff); // we always expect the lock to be free
            uint64_t desired  = (*v | 0x8000000000000000);
            success = __atomic_compare_exchange(v, &expected, &desired, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE);
        } while(success == false);
        // lock first and update verion
        if((*v & 0x7f00000000000000) == 0x7f00000000000000) {
            *v = (*v & 0x80ffffffffffffff);
        } else {
            *v = *v + ((uint64_t)0x1 << 56);
        }
    }

    static void UnLock(uint64_t *v) {
        if((*v & 0x7f00000000000000) == 0x7f00000000000000) {
            *v = (*v & 0x80ffffffffffffff);
        } else {
            *v = *v + ((uint64_t)0x1 << 56);
        }
        // update verion and UnLock
        *v = (*v & 0x7fffffffffffffff);
    }

    static bool IsLocked(uint64_t v) {
        return v >= 0x8000000000000000;
    }
};


#endif // __VERSION_LOCK__