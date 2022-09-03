#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include "../src/node.h"
#include "morphtree.h"

#include "gtest/gtest.h"

const int TEST_SCALE = 100000;

class rotest : public testing::Test {
protected:
    Morphtree * tree;
    std::vector<Record> recs;

    virtual void SetUp() {
        recs.reserve(TEST_SCALE);

        for(int i = 0; i < TEST_SCALE; i++) {
            recs[i].key = _key_t(i);
            recs[i].val = uint64_t(i);
        }

        std::sort(recs.begin(), recs.end());
        tree = new Morphtree(recs);
    }

    virtual void TearDown() {
        delete tree;
    }
};

TEST_F(rotest, DISABLED_lookup) { 
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", recs[i].key);
        ASSERT_EQ(tree->lookup(recs[i].key), uint64_t(recs[i].val));
    }
} 