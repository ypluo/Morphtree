/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <algorithm>
#include <vector>
#include <cstring>
#include <cmath>
#include <thread>

#include "node.h"

namespace morphtree {
    
WOLeaf::WOLeaf(uint32_t initial_num) {
    node_type = NodeType::WOLEAF;
    next_node_type = NodeType::WOLEAF;
    stats = WOSTATS;
    sibling = nullptr;
    shadow = nullptr;
    mysplitkey = MAX_KEY;
    lsn = 0;

    recs = new Record[NODE_SIZE];
    count = initial_num;
    readable_count = readonly_count = count;
}

WOLeaf::WOLeaf(Record * recs_in, int num) {
    node_type = NodeType::WOLEAF;
    next_node_type = NodeType::WOLEAF;
    stats = WOSTATS;
    sibling = nullptr;
    shadow = nullptr;
    mysplitkey = MAX_KEY;
    lsn = 0;

    recs = new Record[NODE_SIZE];
    memcpy(recs, recs_in, sizeof(Record) * num);
    count = num;
    readable_count = readonly_count = count;
}

WOLeaf::~WOLeaf() {
    delete [] recs;
}

bool WOLeaf::Store(const _key_t & k, uint64_t v, _key_t * split_key, WOLeaf ** split_node) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Store(k, v, split_key, (BaseNode **)split_node);
    }

    woleaf_store_retry:
    if(k >= mysplitkey) {
        return sibling->Store(k, v, split_key, (BaseNode **)split_node);
    }

    writelock.Lock();
    uint32_t next_count;
    if(readable_count == GLOBAL_LEAF_SIZE) { // no more empty slot, wait for the node to split
        writelock.UnLock();
        std::this_thread::yield();
        goto woleaf_store_retry;
    } else {
        recs[readable_count] = {k, v};
        next_count = ++readable_count;
        writelock.UnLock();
    }
    
    if(next_count - readonly_count == PIECE_SIZE) { // handle sort
        sortlock.Lock();
        std::sort(&recs[readonly_count], &recs[next_count]);
        readonly_count += PIECE_SIZE;
        sortlock.UnLock();
    }
    
    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        cur_shadow->Store(k, v, split_key, (BaseNode **)split_node);
    }

    // handle splittion
    if(next_count == GLOBAL_LEAF_SIZE) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool WOLeaf::Lookup(const _key_t & k, uint64_t &v) {    
    // do binary search in the initial run
    if(BinSearch(recs, count, k, v)) {
        return true;
    }

    
    // do binary search in sorted pieces
    for(int i = count; i < readonly_count; i += PIECE_SIZE) {
        if(BinSearch(recs + i, PIECE_SIZE, k, v)) {
            return true;
        }
    }
    uint32_t readonly_count_snapshot = readonly_count;

    woleaf_look_retry:
    auto v1 = sortlock.Version();
    // do binary search in the unsorted piece
    for(int i = readonly_count_snapshot; i < readable_count; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            if(sortlock.IsLocked() || v1 != sortlock.Version()) goto woleaf_look_retry;
            return v != 0;
        }
    }
    if(sortlock.IsLocked() || v1 != sortlock.Version()) goto woleaf_look_retry;
    
    // check the shadow node
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { 
        return cur_shadow->Lookup(k, v);
    }

    // check its sibling node
    if (k >= mysplitkey) {
        return sibling->Lookup(k, v);
    }
    return false;
}

bool WOLeaf::Update(const _key_t & k, uint64_t v) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Update(k, v);
    }

    auto binary_update = [&v](Record & r) {
        if(r.val == 0) {
            return false;
        } else {
            r.val = v;
            return true;
        }
    };

    woleaf_update_retry:
    if(k >= mysplitkey) {
        return sibling->Update(k, v);
    }

    auto v1 = nodelock.Version();
    // do binary update in all sorted runs
    if(BinSearch_CallBack(recs, count, k, binary_update)) {
        if(nodelock.IsLocked() || v1 != nodelock.Version()) goto woleaf_update_retry;
        return true;
    }

    for(int i = count; i < readonly_count; i += PIECE_SIZE) {
        if(BinSearch_CallBack(recs + i, PIECE_SIZE, k, binary_update)) {
            if(nodelock.IsLocked() || v1 != nodelock.Version()) goto woleaf_update_retry;
            return true;
        }
    }

    sortlock.Lock();
    // do scan in unsorted runs
    for(int i = readonly_count; i < readable_count; i++) {
        if(recs[i].key == k) {
            recs[i].val = v;
            
            sortlock.UnLock();
            if(nodelock.IsLocked() && v1 != nodelock.Version()) goto woleaf_update_retry;
            return true;
        }
    }

    sortlock.UnLock();
    if(nodelock.IsLocked() && v1 != nodelock.Version()) goto woleaf_update_retry;
    
    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Update(k, v);
    }
    return false;
}

bool WOLeaf::Remove(const _key_t & k) {
    auto cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Remove(k);
    }

    auto binary_remove = [](Record & r) {
        if(r.val == 0) {
            return false;
        } else {
            r.val = 0;
            return true;
        }
    };

    woleaf_remove_retry:
    if(k >= mysplitkey) {
        return sibling->Remove(k);
    }
    auto v1 = nodelock.Version();
    // do binary remove in all sorted runs
    if(BinSearch_CallBack(recs, count, k, binary_remove)) {
        return true;
    }

    for(int i = count; i < readonly_count; i += PIECE_SIZE) {
        if(BinSearch_CallBack(recs + i, PIECE_SIZE, k, binary_remove)) {
            return true;
        }
    }

    sortlock.Lock();
    // do scan in unsorted runs
    for(int i = readonly_count; i < readable_count; i++) {
        if(recs[i].key == k) {
            recs[i].val = 0;
        
            sortlock.UnLock();
            if(nodelock.IsLocked() && v1 != nodelock.Version()) goto woleaf_remove_retry;
            return true;
        }
    }

    sortlock.UnLock();
    if(nodelock.IsLocked() && v1 != nodelock.Version()) goto woleaf_remove_retry;

    cur_shadow = shadow;
    if(cur_shadow != nullptr) { // this node is under morphing
        return cur_shadow->Remove(k);
    }
    return false;
}

int WOLeaf::Scan(const _key_t &startKey, int len, Record *result) {
    woleaf_scan_retry:
    if(shadow != nullptr) { // this node is under morphing
        std::this_thread::yield();
        goto woleaf_scan_retry;
    }

    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / PIECE_SIZE;
    Record * sort_runs[MAX_RUN_NUM + 1];
    int ends[MAX_RUN_NUM + 1];

    uint32_t total_count = readable_count;
    sortlock.Lock();
    std::sort(recs + readonly_count, recs + readable_count);
    sortlock.UnLock();

    int run_cnt = 0;
    if(count > 0) {
        sort_runs[0] = recs;
        ends[0] = count;
        run_cnt += 1;
    }
    for(int i = count; i < total_count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = recs + i;
        ends[run_cnt] = (i + PIECE_SIZE <= total_count) ? PIECE_SIZE : total_count - i;
        run_cnt += 1;
    }

    int cur = KWayScan(sort_runs, ends, run_cnt, startKey, len, result);
    
    if(cur >= len) 
        return len;
    else if(sibling == nullptr) 
        return cur;
    else
        return cur + ((BaseNode *) sibling)->Scan(result[cur - 1].key, len - cur, result + cur);
}

void WOLeaf::Dump(std::vector<Record> & out) {
    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / PIECE_SIZE;
    Record * sort_runs[MAX_RUN_NUM + 1];
    int ends[MAX_RUN_NUM + 1];

    uint32_t total_count = readable_count;
    sortlock.Lock();
    std::sort(&recs[readonly_count], &recs[readable_count]);
    sortlock.UnLock();

    int run_cnt = 0;
    if(count > 0) {
        sort_runs[0] = recs;
        ends[0] = count;
        run_cnt += 1;
    }
    for(int i = count; i < total_count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = recs + i;
        ends[run_cnt] = (i + PIECE_SIZE <= total_count) ? PIECE_SIZE : total_count - i;
        run_cnt += 1;
    }

    KWayMerge(sort_runs, ends, run_cnt, out);
    return ;
}

void WOLeaf::DoSplit(_key_t * split_key, WOLeaf ** split_node) {
    morphlock.Lock();
    nodelock.Lock();

    std::vector<Record> data;
    data.reserve(GLOBAL_LEAF_SIZE);
    Dump(data);

    int pid = GLOBAL_LEAF_SIZE / 2;
    // creat two new nodes
    WOLeaf * left = new WOLeaf(data.data(), pid);
    left->morphlock.Lock();
    left->nodelock.Lock();
    
    WOLeaf * right = new WOLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    right->sibling = sibling;
    left->mysplitkey = data[pid].key;
    right->mysplitkey = this->mysplitkey;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);
    
    nodelock.UnLock();
    morphlock.UnLock();
}

void WOLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d, %d)[", prefix.c_str(), node_type, readable_count);
    for(int i = 0; i < out.size(); i++) {
        printf("%12.8lf, ", out[i].key);
    }
    printf("]\n");
    
    printf("%s(%d, %d)[", prefix.c_str(), node_type, readable_count);
    for(int i = 0; i < readable_count; i++) {
        printf("%12.8lf, ", recs[i].key);
    }
    printf("]\n");
}

} // namespace morphtree
