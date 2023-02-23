#ifndef __RW_LOCK__
#define __RW_LOCK__

#include <cstdint>

/*  RWLock: 
        writer locks exclusively while readers share the lock
        writers are more priviledged to own the lock than readers
*/
class RWLock {
private:
    uint8_t lock : 1;  // exclusive bits for 
    uint8_t intent: 1; // intent for writer to lock
    uint8_t version : 6;
    uint8_t reader_num; // no more than 255 readers

public:
    RWLock() {
        lock = 0;
        intent = 0;
        version = 0;
        reader_num = 0;
    }

    uint8_t Version() const {return version;}

    void RLock() {
        bool success;
        do {
            RWLock expected = *this;
            expected.intent = 0;        // the reader waits for writer to clear its intent
            RWLock desired = expected;
            desired.reader_num = expected.reader_num + 1;
            success = __atomic_compare_exchange(this, &expected, &desired, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        } while(success == false);
    }

    void WLock() {
        bool success;
        do {
            this->intent = 1;        // set lock intent
            RWLock expected = *this;
            expected.lock = 0;       // the lock is not locked
            expected.reader_num = 0; // the writer waits for all readers to finish
            RWLock desired = expected;
            desired.lock = 1;        // the writer owns the lock exclusively after WLock
            success = __atomic_compare_exchange(this, &expected, &desired, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        } while(success == false);
        version += 1;
    }

    void UnLock() {
        if(reader_num == 0 && lock == 1) { // writer unlock
            version += 1;
            intent = 0; // release lock intent
            lock = 0;
        } else { // reader unlock
            __atomic_fetch_sub(&reader_num, 1, __ATOMIC_RELAXED);
        }
    }
};

#endif // __RW_LOCK__