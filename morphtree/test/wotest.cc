#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <algorithm>
#include <omp.h>

#include "../src/node.h"
#include "../src/morphtree_impl.h"

#include "gtest/gtest.h"

using namespace morphtree;

const int TEST_SCALE = GLOBAL_LEAF_SIZE * 400;
const int THREAD_NUM = 8;

class wotest : public testing::Test {
protected:
    MorphtreeImpl<NodeType::WOLEAF, false> * tree;
    std::vector<Record> recs;
    std::vector<Record> recs1;

    virtual void SetUp() {
        uint32_t step = UINT32_MAX / TEST_SCALE;
        std::default_random_engine gen(997);
        std::uniform_int_distribution<uint32_t> dist(0, step);

        recs.resize(TEST_SCALE);
        recs1.resize(TEST_SCALE);

        for(uint64_t i = 0; i < TEST_SCALE; i++) {
            recs[i].key = i * step + dist(gen);
            recs[i].val = uint64_t(recs[i].key + 1);
        }

        for(uint64_t i = TEST_SCALE; i < TEST_SCALE * 2; i++) {
            recs1[i - TEST_SCALE].key = i * step + dist(gen);
            recs1[i - TEST_SCALE].val = uint64_t(recs1[i - TEST_SCALE].key + 1);
        }
        
        std::shuffle(recs.begin(), recs.end(), gen);
        std::shuffle(recs1.begin(), recs1.end(), gen);

        // bulkload a tree
        tree = new MorphtreeImpl<NodeType::WOLEAF, false>();
        for(int i = 0; i < TEST_SCALE; i++) {
            // printf("%lf\n", recs[i].key);
            tree->insert(recs[i].key, recs[i].val);
        }
    }

    virtual void TearDown() {
        delete tree;
    }
};

TEST_F(wotest, insert) {
    // auto s = seconds();
    omp_set_num_threads(THREAD_NUM);
    #pragma omp parallel for schedule(dynamic,32)
    for(int i = 0; i < TEST_SCALE; i++) {
        //printf("%lf\n", recs1[i].key);
        tree->insert(recs1[i].key, recs1[i].val);
    }
    // auto e = seconds();
    // printf("%lf\n", e - s);

    uint64_t v;
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", recs1[i].key);
        tree->lookup(recs[i].key, v);
        ASSERT_EQ(v, recs[i].val);
    }
}

TEST_F(wotest, lookup) { 
    uint16_t notfound = 0;
    uint64_t v;
    omp_set_num_threads(THREAD_NUM);
    #pragma omp parallel for schedule(dynamic,32) shared(notfound)
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", recs[i].key);
        bool found = tree->lookup(recs[i].key, v);
        if(found == false) notfound += 1;
    }
    ASSERT_EQ(notfound, 0);
} 

TEST_F(wotest, update) { 
    omp_set_num_threads(THREAD_NUM);
    #pragma omp parallel for schedule(dynamic,32)
    for(int i = 0; i < TEST_SCALE; i += 2) {
        tree->update(recs[i].key, uint64_t(recs[i].val) * 2);
    }
    
    uint64_t v;
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", recs[i].key);
        ASSERT_TRUE(tree->lookup(recs[i].key, v));
        if(i % 2 == 0)
            ASSERT_EQ(v, uint64_t(recs[i].val) * 2);
        else 
            ASSERT_EQ(v, uint64_t(recs[i].val));
    }
} 

TEST_F(wotest, remove) {
    // remove half of the records
    omp_set_num_threads(THREAD_NUM);
    #pragma omp parallel for schedule(dynamic,32)
    for(int i = 0; i < TEST_SCALE; i += 2) {
        tree->remove(recs[i].key);
    }
    
    uint64_t v;
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", recs[i].key);
        if(i % 2 == 0) {
            ASSERT_FALSE(tree->lookup(recs[i].key, v));
        } else {
            ASSERT_TRUE(tree->lookup(recs[i].key, v));
            ASSERT_EQ(v, uint64_t(recs[i].val));
        }
    }
}

// scan test is predicated on that key/values are sequencial number for 0 to TEST_SCALE - 1
TEST_F(wotest, scan) {
    uint16_t notvalid = 0;
    int max_scan_len = TEST_SCALE / 20;
    Record * buf = new Record[max_scan_len];
    std::sort(recs.begin(), recs.end());

    omp_set_num_threads(THREAD_NUM);
    #pragma omp parallel for shared(notvalid)
    for(int step = 10; step < max_scan_len; step += max_scan_len / 128) {
        // printf("Step: %d\n", step);
        for(int i = 0; i < TEST_SCALE - step; i += TEST_SCALE / 64) {
            // do scan
            int count = tree->scan(recs[i].key, step, buf);
            // do validate
            if(count != step) notvalid += 1;
        }
    }
    ASSERT_LT(notvalid, 3);
    delete [] buf;
}