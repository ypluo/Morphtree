#include <random>

#include "../src/epoch.h"
#include "../src/node.h"
#include "gtest/gtest.h"

using namespace morphtree;

const int SCALE1 = GLOBAL_LEAF_SIZE / 2; // not big enough to trigger a node split

TEST(NodeMorph, DISABLED_wonode) {
    BaseNode * n = new WOLeaf;
    _key_t split_key = 0;
    BaseNode * split_node = nullptr;
    
    Record * tmp = new Record[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i].key = i + 1;
        tmp[i].val = (uint64_t)tmp[i].key;
    }

    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(997));

    // insert data into nodes
    for(uint64_t i = 0; i < SCALE1; i++) {
        n->Store(tmp[i].key, (uint64_t)tmp[i].val, &split_key, &split_node);
    }

    {   
        EpochGuard epoch;
        MorphNode(n, (uint8_t)0, NodeType::ROLEAF);
    }

    // test lookup
    uint64_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        // printf("%lf\n", tmp[i].key);
        ASSERT_TRUE(n->Lookup(tmp[i].key, res));
        ASSERT_EQ(res, uint64_t(tmp[i].val));
    }
    ASSERT_EQ(split_node, nullptr);

    delete tmp;
    delete n;
}

TEST(NodeMorph, ronode) {
    int load_size = SCALE1 * 3 / 5;

    _key_t split_key = UINT64_MAX;
    BaseNode * split_node = nullptr;
    Record * tmp = new Record[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i].key = i + 1;
        tmp[i].val = (uint64_t)tmp[i].key;
    }
    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(997));

    // bulk load
    std::sort(tmp, tmp + load_size);
    BaseNode * n = new ROLeaf(tmp, load_size);

    // test insert
    for(uint64_t i = load_size; i < SCALE1; i++) {
        ASSERT_FALSE(n->Store(tmp[i].key, tmp[i].val, &split_key, &split_node));
    }
    
    {   
        EpochGuard epoch;
        MorphNode(n, (uint8_t)0, NodeType::WOLEAF);
    }

    // test lookup
    uint64_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        // printf("%lf\n", tmp[i].key);
        ASSERT_TRUE(n->Lookup(tmp[i].key, res));
        ASSERT_EQ(res, uint64_t(tmp[i].val));
    }
    ASSERT_EQ(split_node, nullptr);

    delete tmp;
    delete n;
}

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    ebr = EpochBasedMemoryReclamationStrategy::getInstance();
    do_morphing = false;
    return RUN_ALL_TESTS();
}