#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include "../src/node.h"
#include "../src/morphtree_impl.h"

#include "gtest/gtest.h"

using namespace morphtree;

const int TEST_SCALE = 100000;

class wotest : public testing::Test {
protected:
    MorphtreeImpl<NodeType::WOLEAF, false> * tree;
    std::vector<Record> recs;

    virtual void SetUp() {
        recs.resize(TEST_SCALE);
        tree = new MorphtreeImpl<NodeType::WOLEAF, false>();

        for(int i = 0; i < TEST_SCALE; i++) {
            recs[i].key = _key_t(i);
            recs[i].val = uint64_t(i);
        }

        std::default_random_engine gen(997);
        std::shuffle(recs.begin(), recs.end(), gen);

        for(int i = 0; i < TEST_SCALE; i++) {
            // printf("insert %d %lf\n",i, recs[i].key);
            tree->insert(recs[i].key, uint64_t(recs[i].key));
        }
    }

    virtual void TearDown() {
        delete tree;
    }
};

TEST_F(wotest, lookup) {
    uint64_t v;
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", recs[i].key);
        ASSERT_TRUE(tree->lookup(recs[i].key, v));
        ASSERT_EQ(v, uint64_t(recs[i].val));
    }
} 

TEST_F(wotest, update) {
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