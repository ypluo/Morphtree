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
    auto shadow_st = shadow;
    if(shadow_st != nullptr) { // this node is under morphing
        return ((ROLeaf *)shadow_st)->Store(k, v, split_key, (ROLeaf **)split_node);
    }

    woleaf_store_retry:
    headerlock.RLock();
        if(node_type != WOLEAF) {
            // the node is changed to another type
            headerlock.UnLock();
            return ((ROLeaf *)this)->Store(k, v, split_key, (ROLeaf **)split_node);
        }

        if(k >= mysplitkey) {
            headerlock.UnLock();
            return sibling->Store(k, v, split_key, (BaseNode **)split_node);
        }

        writelock.Lock();
        uint64_t cur_count;
        if(readable_count >= GLOBAL_LEAF_SIZE) { // no more empty slot, wait for the node to split
            writelock.UnLock();
            headerlock.UnLock();
            std::this_thread::yield();
            goto woleaf_store_retry;
        } else {
            cur_count = readable_count++;
            recs[cur_count] = {k, v};
            
            if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                // a better way is to retry and insert the record into its shadow node
                readable_count--;
                writelock.UnLock();
                headerlock.UnLock();
                goto woleaf_store_retry;
            }
            writelock.UnLock();
        }
        
        uint32_t next_count = cur_count + 1;
        if(next_count - readonly_count == PIECE_SIZE) { // handle sort
            sortlock.Lock();
            std::sort(&recs[readonly_count], &recs[next_count]);
            readonly_count += PIECE_SIZE;
            sortlock.UnLock();
        }
    headerlock.UnLock();
    
    // handle splittion
    if(next_count == GLOBAL_LEAF_SIZE) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool WOLeaf::Lookup(const _key_t & k, uint64_t &v) {
    woleaf_look_retry:
    if(node_type != WOLEAF) {
        // the node is changed to another type
        return ((ROLeaf *)this)->Lookup(k, v);
    }
    auto v1 = headerlock.Version();
        auto recs_st = recs;
        auto count_st = count;
        auto readonly_count_st = readonly_count;
        auto readable_count_st = readable_count;
        auto splitkey_st = mysplitkey;
        auto sibling_st = sibling;
        auto shadow_st = shadow;
    auto v2 = headerlock.Version();
    if(!(v1 == v2 && v1 % 2 == 0)) goto woleaf_look_retry;
    
    // check its sibling node
    if (k >= splitkey_st) {
        return sibling_st->Lookup(k, v);
    }

    // do binary search in the initial run
    if(BinSearch(recs_st, count_st, k, v)) {
        return true;
    }

    // do binary search in sorted pieces
    for(int i = count_st; i < readonly_count_st; i += PIECE_SIZE) {
        if(BinSearch(recs_st + i, PIECE_SIZE, k, v)) {
            return true;
        }
    }

    // do binary search in the unsorted piece
    for(int i = readonly_count_st; i < readable_count_st; i++) {
        if(recs_st[i].key == k) {
            v = recs_st[i].val;
            return v != 0;
        }
    }

    if(shadow_st != nullptr)
        return ((ROLeaf *)shadow_st)->Lookup(k, v);
    else
        return false;
}

bool WOLeaf::Update(const _key_t & k, uint64_t v) {
    auto binary_update = [&v](Record & r) {
        if(r.val == 0) {
            return false;
        } else {
            r.val = v;
            return true;
        }
    };

    woleaf_update_retry:
    auto shadow_st = shadow;
    if(shadow_st != nullptr) { // this node is under morphing
        return ((ROLeaf *)shadow_st)->Update(k, v);
    }

    headerlock.RLock();
        if(node_type != WOLEAF) {
            // the node is changed to another type
            headerlock.UnLock();
            return ((ROLeaf *)this)->Update(k, v);
        }

        if(k >= mysplitkey) {
            headerlock.UnLock();
            return sibling->Update(k, v);
        }
        
        // do binary update in all sorted runs
        if(BinSearch_CallBack(recs, count, k, binary_update)) {
            if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                headerlock.UnLock();
                goto woleaf_update_retry;
            } else {
                headerlock.UnLock();
                return true;
            }
        }

        for(int i = count; i < readonly_count; i += PIECE_SIZE) {
            if(BinSearch_CallBack(recs + i, PIECE_SIZE, k, binary_update)) {
                if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                    headerlock.UnLock();
                    goto woleaf_update_retry;
                } else {
                    headerlock.UnLock();
                    return true;
                }
            }
        }

        sortlock.Lock();
            // do scan in unsorted runs
            for(int i = readonly_count; i < readable_count; i++) {
                if(recs[i].key == k) {
                    recs[i].val = v;
                    sortlock.UnLock();
                    if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                        headerlock.UnLock();
                        goto woleaf_update_retry;
                    } else {
                        headerlock.UnLock();
                        return true;
                    }
                }
            }
        sortlock.UnLock();

        if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
            headerlock.UnLock();
            goto woleaf_update_retry;
        }
    headerlock.UnLock();

    return false;
}

bool WOLeaf::Remove(const _key_t & k) {
    auto binary_remove = [](Record & r) {
        if(r.val == 0) {
            return false;
        } else {
            r.val = 0;
            return true;
        }
    };

    woleaf_remove_retry:
    auto shadow_st = shadow;
    if(shadow_st != nullptr) { // this node is under morphing
        return ((ROLeaf *)shadow_st)->Remove(k);
    }

    headerlock.RLock();
        if(node_type != WOLEAF) {
            // the node is changed to another type
            headerlock.UnLock();
            return ((ROLeaf *)this)->Remove(k);
        }

        if(k >= mysplitkey) {
            headerlock.UnLock();
            return sibling->Remove(k);
        }

        // do binary update in all sorted runs
        if(BinSearch_CallBack(recs, count, k, binary_remove)) {
            if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                headerlock.UnLock();
                goto woleaf_remove_retry;
            } else {
                headerlock.UnLock();
                return true;
            }
        }

        for(int i = count; i < readonly_count; i += PIECE_SIZE) {
            if(BinSearch_CallBack(recs + i, PIECE_SIZE, k, binary_remove)) {
                if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                    headerlock.UnLock();
                    goto woleaf_remove_retry;
                } else {
                    headerlock.UnLock();
                    return true;
                }
            }
        }

        sortlock.Lock();
            // do scan in unsorted runs
            for(int i = readonly_count; i < readable_count; i++) {
                if(recs[i].key == k) {
                    recs[i].val = 0;
                    sortlock.UnLock();
                    if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
                        headerlock.UnLock();
                        goto woleaf_remove_retry;
                    } else {
                        headerlock.UnLock();
                        return true;
                    }
                }
            }
        sortlock.UnLock();

        if(nodelock.IsLocked()) { // CAUTIOUS: the node starts morphing / splitting after we enter this loop
            headerlock.UnLock();
            goto woleaf_remove_retry;
        }
    headerlock.UnLock();

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
    assert(total_count <= GLOBAL_LEAF_SIZE);
    sortlock.Lock();
    std::sort(&recs[readonly_count], &recs[total_count]);
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
    nodelock.Lock();

    std::vector<Record> data;
    data.reserve(GLOBAL_LEAF_SIZE);
    Dump(data);

    int pid = GLOBAL_LEAF_SIZE / 2;
    // creat two new nodes
    WOLeaf * left = new WOLeaf(data.data(), pid);
    
    WOLeaf * right = new WOLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    right->sibling = sibling;
    left->mysplitkey = data[pid].key;
    right->mysplitkey = this->mysplitkey;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    left->nodelock.Lock();
    left->headerlock.WLock();
    headerlock.WLock();
    SwapNode(this, left);
    headerlock.UnWLock();

    nodelock.UnLock();
}

void WOLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d, %d)[", prefix.c_str(), node_type, readable_count);
    for(int i = 0; i < out.size(); i++) {
        printf("(%12.8lf %lu), ", out[i].key, out[i].val);
    }
    printf("]\n");
}

} // namespace morphtree
