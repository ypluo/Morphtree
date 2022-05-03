#include "../include/util.h"
#include "../src/fixtree.h"
#include "gtest/gtest.h"

TEST(testfix, load) {
    const int TEST_SCALE = 100000;

    Record * tmp = new Record[TEST_SCALE];
    for(int i = 0; i < TEST_SCALE; i++) {
        tmp[i] = {_key_t(i), _val_t(i)};
    }

    Fixtree tree(tmp, TEST_SCALE);

    // tree.printAll(); 
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%5.1lf\n", tmp[i].key);
        ASSERT_EQ(tree.Lookup(tmp[i].key), (void *)(uint64_t)i);
    }

    
    delete tmp;
} 