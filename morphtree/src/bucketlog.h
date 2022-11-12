#ifndef __MORPH_LOG__
#define __MORPH_LOG__

template<typename LogRecordType>
class BucketLog {
public:
    static const int BATCH_SIZE = 64;
    struct BucketNode {
        LogRecordType batch[BATCH_SIZE];
        BucketNode * next = nullptr;
        int aindex = 0;
        int pindex = 0;
    };

    BucketLog() {
        head = tail = new BucketNode;
    }

    ~BucketLog() {
        while(head != nullptr) {
            BucketNode * old_head = head;
            head = head->next;
            delete old_head;
        }
    }

    void AddLog(const LogRecordType &rec) {
        if(tail->aindex == BATCH_SIZE) {
            tail->next = new BucketNode;
            tail = tail->next;
        }

        tail->batch[tail->aindex++] = rec;

        return ;
    }

    LogRecordType * PopLog() {
        if(head->pindex == BATCH_SIZE) {
            // current bucket node is drained, check next bucket
            BucketNode * old_head = head;
            head = head->next;
            delete old_head;
        }

        if(head == nullptr) {
            // no more bucket node
            head = tail = new BucketLog;
            return nullptr;
        } else if(head->pindex == head->aindex) {
            // only one partial bucket node
            return nullptr;
        } else {
            return &(head->batch[head->pindex++]);
        }
    }

private:
    BucketNode * head;
    BucketNode * tail;
};

#endif // __MORPH_LOG__