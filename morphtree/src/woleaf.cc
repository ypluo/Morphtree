/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <algorithm>
#include <vector>
#include <cstring>

#include "node.h"

namespace morphtree {

// Sorted run
struct SortedRun {
    SortedRun * next;
    Record recs[BUFFER_SIZE];

    bool Lookup(_key_t k, _val_t &v) { // do binary search
        int low = 0;
        int high = BUFFER_SIZE - 1;

        while(low <= high) {
            int mid = low + (high - low) / 2;
            if(recs[mid].key < k) {
                low = mid + 1;
            } else if (recs[mid].key == k){
                v = recs[mid].val;
                return true;
            } else {
                high = mid - 1;
            }
        }
        return false; 
    }

    void Print() {
        printf("[");
        for(int i = 0; i < BUFFER_SIZE; i++) {
            printf("%lu ", recs[i].key);
        }
        printf("]\n");
    }
};

// Insert buffer node
struct Buffer {
    uint16_t count;
    Record recs[BUFFER_SIZE];

    Buffer(): count(0) {}
    
    bool Append(_key_t k, _val_t v) {
        recs[count++] = Record(k, v);
        return count == BUFFER_SIZE;
    }

    bool Lookup(_key_t k, _val_t &v) {
        for(int i = 0; i < count; i++) {
            if(recs[i].key == k) {
                v = recs[i].val;
                return true;
            }
        }
        return false;
    }

    SortedRun * GenRun() {
        std::sort(recs, recs + BUFFER_SIZE);

        SortedRun * run = new SortedRun;
        memcpy(run->recs, recs, sizeof(Record) * BUFFER_SIZE);

        return run;
    }

    void Sort() {
        std::sort(recs, recs + count);
    }

    void Print() {
        std::sort(recs, recs + count);
        printf("[");
        for(int i = 0; i < count; i++) {
            printf("%lu ", recs[i].key);
        }
        printf("]\n");
    }
};

WOLeaf::WOLeaf(bool isLeaf) {
    node_type = NodeType::WOLEAF;
    stats = WOSTATS;

    insert_buf = new Buffer;
    head = nullptr;
    sorted_num = 0;
}

WOLeaf::WOLeaf(Record * recs_in, int num) {
    node_type = NodeType::WOLEAF;
    stats = WOSTATS;
    
    insert_buf = new Buffer;
    head = nullptr;
    sorted_num = num / BUFFER_SIZE;
    //assert(sorted_num <= MAX_RUN_NUM);
    
    for(int i = 0; i < sorted_num; i++) {
        SortedRun * newrun = new SortedRun;
        memcpy(newrun->recs, &recs_in[i * BUFFER_SIZE], sizeof(Record) * BUFFER_SIZE);
        newrun->next = head;
        head = newrun;
    }

    int current_num = sorted_num * BUFFER_SIZE;
    if(current_num < num) {
        memcpy(insert_buf->recs, &recs_in[current_num], sizeof(Record) * (num - current_num));
        insert_buf->count = num - current_num;
    }
}

WOLeaf::~WOLeaf() {
    delete insert_buf;
    SortedRun * cur = head;
    for(int i = 0; i < sorted_num; i++) {
        SortedRun *next = cur->next;
        delete cur;
        cur = next;
    }
}

bool WOLeaf::Store(_key_t k, _val_t v, _key_t * split_key, WOLeaf ** split_node) {
    if(insert_buf->Append(k, v)) {
        // flush the buffer to L0
        SortedRun * newrun = insert_buf->GenRun();
        newrun->next = head;
        head = newrun;
        sorted_num += 1;
        //clear the buffer
        insert_buf->count = 0;
    }

    if(sorted_num == MAX_RUN_NUM) {
        // find global median number
        std::vector<_key_t> medians(MAX_RUN_NUM);
        SortedRun * cur = head;
        for(int i = 0; i < MAX_RUN_NUM; i++) {
            medians[i] = cur->recs[BUFFER_SIZE / 2].key;
            cur = cur->next;
        }
        *split_key = GetMedian(medians);
        
        // create two new nodes
        WOLeaf * left_node = new WOLeaf;
        *split_node = new WOLeaf;
        // update siblings
        left_node->sibling = *split_node;
        (*split_node)->sibling = this->sibling;

        // do splitting: for all sortedruns in L0, scan them and direct to two nodes
        cur = head;
        for(int i = 0; i < MAX_RUN_NUM; i++) {
            for(int j = 0; j < BUFFER_SIZE; j++) {
                if(cur->recs[j].key < *split_key)
                    left_node->Store(cur->recs[j].key, cur->recs[j].val, nullptr, nullptr);
                else 
                    (*split_node)->Store(cur->recs[j].key, cur->recs[j].val, nullptr, nullptr);
            }
            cur = cur->next;
        }
        
        // Swap the old left node and the new
        SwapNode(this, left_node);
        delete left_node;
        
        return true;
    } else {
        return false;
    }
}

void WOLeaf::Dump(std::vector<Record> & out) {
    Record * sort_runs[MAX_RUN_NUM];
    int lens[MAX_RUN_NUM];

    SortedRun * cur = head;
    for(int i = 0; i < sorted_num; i++) {
        sort_runs[i] = cur->recs;
        lens[i] = BUFFER_SIZE;
        cur = cur->next;
    }

    if(insert_buf->count > 0) {
        insert_buf->Sort();
        sort_runs[sorted_num] = insert_buf->recs;
        lens[sorted_num] = insert_buf->count;
    }

    int k = sorted_num + (insert_buf->count > 0 ? 1 : 0);
    KWayMerge(sort_runs, lens, k, out);
    return ;
}

bool WOLeaf::Lookup(_key_t k, _val_t &v) {
    if(!insert_buf->Lookup(k, v)) {
        SortedRun * cur = head;
        for(int i = sorted_num - 1; i >= 0; i--) {
            if(cur->Lookup(k, v)) {
                return true;
            }
            cur = cur->next;
        }
        return false;
    } else {
        return true;
    }
}

void WOLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d)[", prefix.c_str(), node_type);
    for(int i = 0; i < out.size(); i++) {
        printf("%lu, ", out[i].key);
    }
    printf("]\n");
}

} // namespace morphtree