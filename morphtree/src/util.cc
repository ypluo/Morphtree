#include <cassert>
#include <cstdio>
#include "../include/util.h"

bool BinSearch(Record * recs, int len, _key_t k, _val_t &v) { // do binary search
    if (len == 0) return false;

    int low = 0;
    int high = len - 1;

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

int BinSearch_Locate(Record * recs, int len, _key_t k) { // do binary search
    assert(len > 0);

    int low = 0;
    int high = len - 1;

    while(low <= high) {
        int mid = low + (high - low) / 2;
        if(recs[mid].key < k) {
            low = mid + 1;
        } else if (recs[mid].key == k){
            return mid;
        } else {
            high = mid - 1;
        }
    }

    return low; 
}

bool BinSearch_CallBack(Record * recs, int len, _key_t k, std::function<bool(Record &)> func) { 
    if (len == 0) return false;

    int low = 0;
    int high = len - 1;

    while(low <= high) {
        int mid = low + (high - low) / 2;
        if(recs[mid].key < k) {
            low = mid + 1;
        } else if (recs[mid].key == k){
            return func(recs[mid]);
        } else {
            high = mid - 1;
        }
    }
    return false; 
}

bool ExpSearch(Record * recs, int len, int predict, _key_t k, _val_t &v) {  
    assert(predict >= 0 && predict <= len - 1);

    int cur = predict;
    if(recs[predict].key > k) { // go left
        int step = 8;
        while(cur >= 0 && recs[cur].key > k) {
            step = (step < 128 ? (step * 2) : step);
            predict = cur;
            cur -= step;
        }
        
        // cur is less than 0 or recs[cur] <= k
        cur = std::max(0, cur);
        if(recs[cur].key <= k)
            return BinSearch(recs + cur, predict - cur + 1, k, v);
        else
            return false;
    } else if(recs[predict].key == k) {
        v = recs[predict].val;
        return true;
    } else { // go right
        int step = 16;
        while(cur < len && recs[cur].key < k) {
            step = (step < 128 ? (step * 2) : step);
            predict = cur;
            cur += step;
        }
        
        // cur is larger than len - 1 or recs[cur] > k
        cur = std::min(len - 1, cur);
        if(recs[cur].key >= k)
            return BinSearch(recs + predict, cur - predict + 1, k, v);
        else
            return false;
    }
}

_key_t GetMedian(std::vector<_key_t> & medians) {
    int num = medians.size();
    std::priority_queue<_key_t, std::vector<_key_t>> q; // max heap

    for(int i = 0; i < num / 2; i++) {
        q.push(medians[i]);
    }

    for(int i = num / 2; i < num; i++) {
        if(medians[i] < q.top()) {
            q.pop();
            q.push(medians[i]);
        }
    }

    return q.top();
}

void TwoWayMerge(Record * a, Record * b, int lena, int lenb, std::vector<Record> & out) {
    int cura = 0;
    int curb = 0;

    while(cura < lena && curb < lenb) {
        if(a[cura].key < b[curb].key) {
            out.push_back(a[cura++]);
        } else if(a[cura].key > b[curb].key) {
            out.push_back(b[curb++]);
        } else {
            out.push_back(a[cura++]);
            out.push_back(b[curb++]);
        }
    }

    while(cura < lena)
        out.push_back(a[cura++]);

    while(curb < lenb)
        out.push_back(b[curb++]);
}

void KWayMerge(Record ** runs, int * run_lens, int k, std::vector<Record> & out) {
    struct HeapEle {
        int run_id;
        Record val;

        bool operator < (const HeapEle & oth) const {
            return val > oth.val; // this will make a min heap
        }
    };

    int * run_idxs = new int[k];
    std::priority_queue<HeapEle, std::vector<HeapEle>> q; // min heap

    for(int i = 0; i < k; i++) {
        if(run_lens[i] > 0)
            q.push({i, runs[i][0]});
        run_idxs[i] = 1;
    }

    while(!q.empty()) {
        HeapEle e = q.top(); q.pop();

        out.push_back(e.val);
        int & cur_pos = run_idxs[e.run_id];
        if (cur_pos < run_lens[e.run_id]) {
            q.push({e.run_id, runs[e.run_id][cur_pos++]});
        }
    }
}

extern int KWayScan(Record ** runs, int * run_lens, int k, _key_t startKey, int len, Record * out) {
    struct HeapEle {
        int run_id;
        Record ele;

        bool operator < (const HeapEle & oth) const {
            return ele > oth.ele; // this will make a min heap
        }
    };

    int * run_idxs = new int[k];
    std::priority_queue<HeapEle, std::vector<HeapEle>> q; // min heap

    // locate the starting point of each scan
    for(int i = 0; i < k; i++) {
        run_idxs[i] = BinSearch_Locate(runs[i], run_lens[i], startKey);
    }

    // start merge scan
    for(int i = 0; i < k; i++) {
        int & run_pos = run_idxs[i];
        if(run_pos < run_lens[i])
            q.push({i, runs[i][run_pos++]});
    }

    int cur = 0;
    while(!q.empty() && cur < len) {
        HeapEle e = q.top(); q.pop();

        out[cur++] = e.ele;
        int & run_pos = run_idxs[e.run_id];
        if (run_pos < run_lens[e.run_id]) {
            q.push({e.run_id, runs[e.run_id][run_pos++]});
        }
    }

    return cur;
}

void KWayMerge_nodup(Record ** runs, int * run_lens, int k, std::vector<Record> & out) {
    struct HeapEle {
        int run_id;
        Record val;

        bool operator < (const HeapEle & oth) const {
            return val > oth.val; // this will make a min heap
        }
    };

    int * run_idxs = new int[k];
    std::priority_queue<HeapEle, std::vector<HeapEle>> q; // min heap

    for(int i = 0; i < k; i++) {
        if(run_lens[i] > 0)
            q.push({i, runs[i][0]});
        run_idxs[i] = 1;
    }

    _key_t last_k = MAX_KEY;
    while(!q.empty()) {
        HeapEle e = q.top(); q.pop();
        if(e.val.key != last_k)
            out.push_back(e.val);
        int & cur_pos = run_idxs[e.run_id];
        if (cur_pos < run_lens[e.run_id]) {
            q.push({e.run_id, runs[e.run_id][cur_pos++]});
        }

        last_k = e.val.key;
    }
}

int getSubOptimalSplitkey(Record * recs, int num) {
    static const int PIVOT_NUM = 32;
    _key_t min_pivot = recs[0].key;
    _key_t max_pivot = recs[num * (PIVOT_NUM - 1) / PIVOT_NUM].key;
    double slope = (PIVOT_NUM - 1) / (max_pivot - min_pivot);
    double intercept = 0 - slope * min_pivot;

    // find two points that are furthest from the interpolated line
    double max_distance = -1;
    int max_i;
    double submax_distance = -1;
    int submax_i;
    for(int i = PIVOT_NUM / 4; i <= PIVOT_NUM * 3 / 4; i++) {
        _key_t cur_pivot = recs[num * i / PIVOT_NUM].key;
        double distance = std::abs(slope * cur_pivot + intercept - i);

        if(distance > max_distance) {
            submax_distance = max_distance; submax_i = max_i;
            max_distance = distance; max_i = i;
        } else if(distance > submax_distance) {
            submax_distance = distance; submax_i = i;
        }
    }

    // use the average subscript as the split point
    return (max_i + submax_i + 1) / 2 * num / PIVOT_NUM;
}