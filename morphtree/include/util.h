#include <queue>
#include <vector>

#include "common.h"

class LinearModelBuilder {
public:
    double a_;
    double b_;
    LinearModelBuilder() { }

    inline void add(_key_t x, int y) {
        count_++;
        x_sum_ += static_cast<long double>(x);
        y_sum_ += static_cast<long double>(y);
        xx_sum_ += static_cast<long double>(x) * x;
        xy_sum_ += static_cast<long double>(x) * y;
        x_min_ = std::min(x, x_min_);
        x_max_ = std::max(x, x_max_);
        y_min_ = std::min<double>(y, y_min_);
        y_max_ = std::max<double>(y, y_max_);
    }

    void build() {
        if (count_ <= 1) {
            a_ = 0;
            b_ = static_cast<double>(y_sum_);
            return;
        }

        if (static_cast<long double>(count_) * xx_sum_ - x_sum_ * x_sum_ == 0) {
            // all values in a bucket have the same key.
            a_ = 0;
            b_ = static_cast<double>(y_sum_) / count_;
            return;
        }

        auto slope = static_cast<double>(
                (static_cast<long double>(count_) * xy_sum_ - x_sum_ * y_sum_) /
                (static_cast<long double>(count_) * xx_sum_ - x_sum_ * x_sum_));
        auto intercept = static_cast<double>(
                (y_sum_ - static_cast<long double>(slope) * x_sum_) / count_);
        a_ = slope;
        b_ = intercept;

        // If floating point precision errors, fit spline
        if (a_ <= 0) {
            a_ = (y_max_ - y_min_) / (x_max_ - x_min_);
            b_ = -static_cast<double>(x_min_) * a_;
        }
	}

    double predict(double x) {
        return x * a_ + b_;
    }

private:
    int count_ = 0;
    long double x_sum_ = 0;
    long double y_sum_ = 0;
    long double xx_sum_ = 0;
    long double xy_sum_ = 0;
    _key_t x_min_ = 0;
    _key_t x_max_ = INT64_MAX;
    double y_min_ = std::numeric_limits<double>::max();
    double y_max_ = std::numeric_limits<double>::lowest();
};

template <typename T>
T GetMedian(std::vector<T> & medians) {
    int num = medians.size();
    std::priority_queue<T, std::vector<T>> q; // max heap

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

template<typename T>
void KWayMerge(T ** runs, int * run_lens, int k, std::vector<T> & out) {
    struct HeapEle {
        int run_id;
        T val;

        bool operator < (const HeapEle & oth) const {
            return val > oth.val; // this will make a min heap
        }
    };

    int * run_idxs = new int[k];
    std::priority_queue<HeapEle, std::vector<HeapEle>> q; // min heap

    for(int i = 0; i < k; i++) {
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