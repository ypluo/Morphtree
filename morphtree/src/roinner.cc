/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {

static const int MARGIN = 16;

ROInner::ROInner() {
    node_type = NodeType::ROINNER;
    count = 0;
    recs = nullptr;
}

ROInner::ROInner(Record * recs_in, int num) : ROInner() {
    _key_t max_key = recs_in[num - 1].key;
    _key_t min_key = recs_in[0].key;
    slope = double(num - 1) / (max_key - min_key);
    intercept = MARGIN - slope * min_key;
    capacity = num + 2 * MARGIN;

    // caculate a proper expand ratio
    int most_crowded = 1;
    int last_i = 0;
    int clid = 0; // current cacheline id
    for(int i = 0; i < num; i++) {
        int predict = Predict(recs_in[i].key);
        if(predict / PROBE_SIZE != clid) { // this is a new cacheline
            if(i - last_i > most_crowded) { // count the population of last cacheline 
                most_crowded = i - last_i;
            }
            last_i = i;
            clid = predict / PROBE_SIZE;
        }
    }
    int expand_ratio = (most_crowded + PROBE_SIZE - 1) / PROBE_SIZE;
    
    // update model information
    capacity = ((capacity * expand_ratio) & (~(PROBE_SIZE - 1))) + PROBE_SIZE;
    intercept = intercept * capacity / (num + 2 * MARGIN);
    slope = slope * capacity / (num + 2 * MARGIN);

    // insert key values
    recs = new Record[capacity];
    for(int i = 0; i < num; i++) {
        assert(Insert(recs_in[i].key, recs_in[i].val));
    }
}

ROInner::~ROInner() {
    for (int i = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY) {
            BaseNode * child = (BaseNode *)recs[i].val;
            /* it will invalid the children of current nodes
                In some cases, we do not want that happen
            */
            switch(child->node_type) {
            case NodeType::ROINNER:
                delete reinterpret_cast<ROInner *>(child);
            case NodeType::ROLEAF: 
                delete reinterpret_cast<ROLeaf *>(child);
            case NodeType::RWLEAF:
                delete reinterpret_cast<RWLeaf *>(child);
            case NodeType::WOLEAF:
                delete reinterpret_cast<WOLeaf *>(child);
            }
        }
    }

    delete [] recs;
}

bool ROInner::Insert(_key_t k, _val_t v) {
    int predict = Predict(k);
    predict = predict & ~(PROBE_SIZE - 1);

    int i;
    for (i = predict; i < predict + PROBE_SIZE; i++) {
        if(recs[i].key > k)
            break;
    }

    // try to find a empty slot
    Record last_one = recs[predict + PROBE_SIZE - 1];
    if(last_one.key == MAX_KEY) { 
        // there is an empty slot
        memmove(&recs[i + 1], &recs[i], sizeof(Record) * (predict + PROBE_SIZE - 1 - i));
        recs[i] = Record(k, v);

        count += 1;
        return true;
    } else {
        return false;
    }
}

bool ROInner::Store(_key_t k, _val_t v, _key_t * split_key, ROInner ** split_node) {
    if(Insert(k, v)) 
        return false;

    // there is no empty slot
    if(ShouldExpand()) {
        Expand();
        assert(this->Insert(k, v));
        return false;
    } else {
        Split(split_key, split_node);
        if(k < *split_key) assert(this->Insert(k, v));
        else assert((*split_node)->Insert(k, v));
        return true;
    }
}

bool ROInner::Lookup(_key_t k, _val_t &v) {
    int predict = Predict(k);
    predict = predict & ~(PROBE_SIZE - 1);

    if(recs[predict].key <= k) {
        for (int i = predict + 1; i < predict + PROBE_SIZE; i++) {
            if(recs[i].key > k) {
                v = recs[i - 1].val;
                return true;
            }
        }

        v = recs[predict + PROBE_SIZE - 1].val;
        return true;
    } else {
        for (int i = predict - 1; i >= 0; i--) {
            if(recs[i].key != MAX_KEY) {
                v = recs[i].val;
                return true;
            }
        }
    }

    __builtin_unreachable();
    return false;
}

void ROInner::Split(_key_t * split_key, ROInner ** split_node) {
    std::vector<Record> records;
    records.reserve(capacity / 2);

    for(int i = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY)
            records.push_back(recs[i]);
    }

    ROInner * left = new ROInner(records.data(), count / 2);
    ROInner * right = new ROInner(records.data() + count / 2, count - count / 2);
    left->sibling = right;
    right->sibling = sibling;

    *split_key = records[count / 2].key;
    *split_node = right;

    SwapNode(this, left);

    left->Clear(); // delete left node without clear it incurs the danger of free valid child nodes
    delete left;
}

void ROInner::Expand() {
    // find max and min key
    _key_t max_key, min_key;
    int i = 0;
    while(recs[i].key == MAX_KEY) {
        i += 1;
    }
    min_key = recs[i].key;
    i = capacity - 1;
    while(recs[i].key == MAX_KEY) {
        i -= 1;
    }
    max_key = recs[i].key;

    // create a new inner node
    ROInner * new_inner = new ROInner();
    new_inner->slope = double(count - 1) / (max_key - min_key);
    new_inner->intercept = MARGIN - slope * min_key;
    new_inner->capacity = count + 2 * MARGIN;

    // caculate a proper expand ratio
    int most_crowded = PROBE_SIZE;
    int last_j = 0;
    int clid = 0; // current cacheline id
    for(int i = 0, j = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY) {
            int predict = new_inner->Predict(recs[i].key);
            if(predict / PROBE_SIZE != clid) { // this is a new cacheline
                if(j - last_j > most_crowded) { // count the population of last cacheline 
                    most_crowded = j - last_j;
                }
                last_j = j;
                clid = predict / PROBE_SIZE;
            }
            j += 1;
        }
    }
    int expand_ratio = (most_crowded + PROBE_SIZE) / PROBE_SIZE;
    
    // update model information
    new_inner->capacity = ((new_inner->capacity * expand_ratio) & (~(PROBE_SIZE - 1))) + PROBE_SIZE;
    new_inner->intercept = new_inner->intercept * capacity / (count + 2 * MARGIN);
    new_inner->slope = new_inner->slope * new_inner->capacity / (count + 2 * MARGIN);

    // insert key values
    new_inner->recs = new Record[new_inner->capacity];
    for(int i = 0; i < capacity; i++) {
        if(recs[i].key != MAX_KEY) {
            new_inner->Insert(recs[i].key, recs[i].val);
        }
    }

    SwapNode(this, new_inner);
    new_inner->Clear();
    delete new_inner;
}

} // namespace morphtree