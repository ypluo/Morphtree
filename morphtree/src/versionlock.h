#ifndef __VERSION_LOCK__
#define __VERSION_LOCK__

#include <cstdint>

class VersionLock {
private:
    uint16_t lock : 2;
    uint16_t ver1 : 7;
    uint16_t ver2 : 7;

public:
    VersionLock() {
        lock = 0;
        ver1 = ver2 = 0;
    }

    uint16_t GetVer1() {return ver1;}

    uint16_t GetVer2() {return ver2;}

    void Lock() {
        bool success;
        do {
            VersionLock expected = *this;
            expected.lock = 0; // we always expect the lock to be free
            VersionLock desired = *this; // the desired value should always be fresh
            desired.lock = 1;
            success = __atomic_compare_exchange(this, &expected, &desired, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        } while(success == false);
        // lock first and update verion1
        __asm__ __volatile__("sfence":::"memory");
        ver1 += 1;
    }

    void Unlock() {
        ver2 += 1;
        // update verion2 and unlock
        __asm__ __volatile__("sfence":::"memory");
        lock = 0;
    }
};
#endif // __VERSION_LOCK__

/* test code for version lock */
/* 

#include <iostream>
#include <unistd.h>
#include <thread>
#include <mutex>

std::mutex mtx;
VersionLock lock;
int global_i;

int run1(int pid) {
    lock.Lock();

    for(int i = 0; i < 10; i++) {
        global_i = pid * 100 + i;
    }

    usleep(100);
    mtx.lock();
    std::cout << "writer: " <<  global_i << std::endl;
    mtx.unlock();

    lock.Unlock();

    return 0;
}

int run2(int pid) {
    int i;
    uint16_t v1, v2;
    retry:
    v1 = lock.GetVer1();
    i = global_i;
    v2 = lock.GetVer2();
    if(v1 != v2) 
        goto retry;
    
    mtx.lock();
    std::cout << "=======reader: " << i << std::endl;
    mtx.unlock();
    return 0;
}


int main() {
    int writer = 5;
    int reader = 20;

    std::thread wths[writer];
    std::thread rths[reader];
    
    for(int i = 0; i < writer; i++) {
        wths[i] = std::move(std::thread(run1, i));
    }

    for(int i = 0; i < reader; i++) {
        rths[i] = std::move(std::thread(run2, i));
    }
    
    for(int i = 0; i < writer; i++) {
        wths[i].join();
    }
        
    for(int i = 0; i < reader; i++) {
        rths[i].join();
    }

    return 0;
}

*/