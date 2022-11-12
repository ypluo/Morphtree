/*
    Copyright (c) Luo Yongping ypluo18@qq.com
*/

#include <algorithm>
#include <vector>
#include <cstring>
#include <cmath>

#include "node.h"

namespace morphtree {
    
WOLeaf::WOLeaf() {
    node_type = WOLEAF;
    next_node_type = WOLEAF;
    stats = WOSTATS;

    recs = new Record[NODE_SIZE];
    inital_count = 0;
    insert_count = 0;
    swap_pos = inital_count;
}

WOLeaf::WOLeaf(Record * recs_in, int num) {
    node_type = WOLEAF;
    next_node_type = WOLEAF;
    stats = WOSTATS;

    recs = new Record[NODE_SIZE];
    memcpy(recs, recs_in, sizeof(Record) * num);
    inital_count = num;
    insert_count = 0;
    swap_pos = inital_count;
}

WOLeaf::~WOLeaf() {
    delete [] recs;
}

bool WOLeaf::Store(const _key_t & k, uint64_t v, _key_t * split_key, WOLeaf ** split_node) {
    recs[inital_count + insert_count++] = Record(k, v);

    if(insert_count % PIECE_SIZE == 0) {
        int16_t start = insert_count - PIECE_SIZE;
        std::sort(recs + inital_count + start, recs + inital_count + insert_count);
        swap_pos = inital_count + insert_count;
    }
    
    if(inital_count + insert_count == GLOBAL_LEAF_SIZE) {
        DoSplit(split_key, split_node);
        return true;
    } else {
        return false;
    }
}

bool WOLeaf::Lookup(const _key_t & k, uint64_t &v) {
    // do binary search in all sorted runs
    if(BinSearch(recs, inital_count, k, v)) {
        return true;
    }

    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    for(int i = inital_count; i < inital_count + bin_end; i += PIECE_SIZE) {
        if(BinSearch(recs + i, PIECE_SIZE, k, v)) {
            return true;
        }
    }

    // do scan in unsorted runs
    for(int i = inital_count + bin_end; i < inital_count + insert_count; i++) {
        if(recs[i].key == k) {
            v = recs[i].val;
            if(i - inital_count - bin_end > 64) 
                std::swap(recs[swap_pos++], recs[i]); // bubble the record to the front of unsorted run
            return true;
        }
    }

    return false;
}

bool WOLeaf::Update(const _key_t & k, uint64_t v) {
    auto binary_update = [&v](Record & r) {
        if(r.flag == 1) {
            return false;
        } else {
            r.val = v;
            return true;
        }
    };

    // do binary update in all sorted runs
    if(BinSearch_CallBack(recs, inital_count, k, binary_update)) {
        return true;
    }

    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    for(int i = inital_count; i < inital_count + bin_end; i += PIECE_SIZE) {
        if(BinSearch_CallBack(recs + i, PIECE_SIZE, k, binary_update)) {
            return true;
        }
    }

    // do scan in unsorted runs
    for(int i = inital_count + bin_end; i < inital_count + insert_count; i++) {
        if(recs[i].key == k) {
            recs[i].val = v;
            if(i - inital_count - bin_end > 64) 
                std::swap(recs[swap_pos++], recs[i]); // bubble the record to the front of unsorted run
            return true;
        }
    }

    return false;
}

bool WOLeaf::Remove(const _key_t & k) {
    auto binary_remove = [](Record & r) {
        if(r.flag == 1) {
            return false;
        } else {
            r.flag = 1;
            return true;
        }
    };

    // do binary remove in all sorted runs
    if(BinSearch_CallBack(recs, inital_count, k, binary_remove)) {
        return true;
    }

    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    for(int i = inital_count; i < inital_count + bin_end; i += PIECE_SIZE) {
        if(BinSearch_CallBack(recs + i, PIECE_SIZE, k, binary_remove)) {
            return true;
        }
    }

    // do scan in unsorted runs
    for(int i = inital_count + bin_end; i < inital_count + insert_count; i++) {
        if(recs[i].key == k) {
            // replace the record under removing with last record in node
            std::swap(recs[i], recs[inital_count + insert_count - 1]);
            insert_count -= 1;
            return true;
        }
    }

    return true;
}

int WOLeaf::Scan(const _key_t &startKey, int len, Record *result) {
    static const int MAX_RUN_NUM = GLOBAL_LEAF_SIZE / PIECE_SIZE;
    Record * sort_runs[MAX_RUN_NUM + 1];
    int ends[MAX_RUN_NUM + 1];

    int16_t total_count = inital_count + insert_count;
    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    if(bin_end < insert_count) {
        std::sort(recs + inital_count + bin_end, recs + total_count);
    }

    int run_cnt = 0;
    if(inital_count > 0) {
        sort_runs[0] = recs;
        ends[0] = inital_count;
        run_cnt += 1;
    }
    for(int i = inital_count; i < total_count; i += PIECE_SIZE) {
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

    int16_t total_count = inital_count + insert_count;
    int16_t bin_end = insert_count / PIECE_SIZE * PIECE_SIZE;
    if(bin_end < insert_count) {
        std::sort(recs + inital_count + bin_end, recs + total_count);
    }

    int run_cnt = 0;
    if(inital_count > 0) {
        sort_runs[0] = recs;
        ends[0] = inital_count;
        run_cnt += 1;
    }
    for(int i = inital_count; i < total_count; i += PIECE_SIZE) {
        sort_runs[run_cnt] = recs + i;
        ends[run_cnt] = (i + PIECE_SIZE <= total_count) ? PIECE_SIZE : total_count - i;
        run_cnt += 1;
    }

    KWayMerge(sort_runs, ends, run_cnt, out);
    return ;
}

void WOLeaf::DoSplit(_key_t * split_key, WOLeaf ** split_node) {
    std::vector<Record> data;
    data.reserve(inital_count + insert_count);
    Dump(data);

    int pid = getSubOptimalSplitkey(data.data(), inital_count + insert_count);
    // creat two new nodes
    WOLeaf * left = new WOLeaf(data.data(), pid);
    WOLeaf * right = new WOLeaf(data.data() + pid, data.size() - pid);
    left->sibling = right;
    right->sibling = sibling;

    // update splitting info
    *split_key = data[pid].key;
    *split_node = right;

    SwapNode(this, left);
    delete left;
}

void WOLeaf::Print(string prefix) {
    std::vector<Record> out;
    Dump(out);

    printf("%s(%d, %d)[", prefix.c_str(), node_type, inital_count + insert_count);
    for(int i = 0; i < out.size(); i++) {
        printf("%12.8lf, ", out[i].key);
    }
    printf("]\n");
}

} // namespace morphtree
