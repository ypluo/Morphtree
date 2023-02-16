#include <random>

#include "../src/node.h"
#include "gtest/gtest.h"

using namespace morphtree;

const int SCALE1 = GLOBAL_LEAF_SIZE / 2; // not big enough to trigger a node split

TEST(NodeMorph, wonode) {
    BaseNode * n = new WOLeaf;
    _key_t split_key = 0;
    BaseNode * split_node = nullptr;
    
    _key_t * tmp = new _key_t[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i] = i;
    }
    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(getRandom()));

    // insert data into nodes
    for(uint64_t i = 0; i < SCALE1; i++) {
        n->Store(tmp[i], uint64_t((uint64_t)tmp[i]), &split_key, &split_node);
    }

    MorphNode(n, (uint8_t)0, NodeType::ROLEAF);

    // test lookup
    uint64_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        ASSERT_TRUE(n->Lookup(i, res));
        ASSERT_EQ(res, uint64_t(i));
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
        tmp[i].key = i;
        tmp[i].val = (uint64_t)i;
    }
    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(getRandom()));

    // bulk load
    std::sort(tmp, tmp + load_size);
    BaseNode * n = new ROLeaf(tmp, load_size);

    // test insert
    for(uint64_t i = load_size; i < SCALE1; i++) {
        ASSERT_FALSE(n->Store(tmp[i].key, tmp[i].val, &split_key, &split_node));
    }

    MorphNode(n, (uint8_t)0, NodeType::WOLEAF);

    // test lookup
    uint64_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        ASSERT_TRUE(n->Lookup(i, res));
        ASSERT_EQ(res, uint64_t(i));
    }
    ASSERT_EQ(split_node, nullptr);

    delete tmp;
    delete n;
}

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    do_morphing = false;
    return RUN_ALL_TESTS();
}