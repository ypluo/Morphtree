#ifndef __SPINLOCK__
#define __SPINLOCK__ 

#include <atomic>

class SpinLock {
public:
std::atomic_bool lock_;
public:
    SpinLock() {
        lock_.store(false);
    }

    ~SpinLock() {
        lock_.store(false);
    }

    void Lock(){
        while(lock_.exchange(true)){}
    }

    bool TryLock() {
        return !lock_.exchange(true);
    }

    void UnLock(){
        while(!lock_.exchange(false)){}
    }

    void Wait(){
        while(lock_.load()){};
    }
};

#endif // __SPINLOCK__