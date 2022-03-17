#include <random>
#include <algorithm>

#include "../src/node.h"

#include "gtest/gtest.h"

using namespace morphtree;

/* SingleNode Test: all operations are applied to one node */
const int SCALE1 = GLOBAL_LEAF_SIZE / 2; // not big enough to trigger a node split

TEST(SingleNode, woleaf) {
    WOLeaf * n = new WOLeaf;
    _key_t split_key = 0;
    WOLeaf * split_node = nullptr;
    
    _key_t * tmp = new _key_t[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i] = i;
    }
    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(getRandom()));

    // insert data into nodes
    for(uint64_t i = 0; i < SCALE1; i++) {
        n->Store(tmp[i], _val_t(tmp[i]), &split_key, &split_node);
    }

    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        //printf("%d\n", i);
        ASSERT_TRUE(n->Lookup(i, res));
        ASSERT_EQ(res, _val_t(i));
    }
    ASSERT_EQ(split_node, nullptr);

    delete tmp;
    delete n;
}

TEST(SingleNode, rwleaf) {
    RWLeaf * n = new RWLeaf;
    _key_t split_key = 0;
    RWLeaf * split_node = nullptr;
    
    _key_t * tmp = new _key_t[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i] = i;
    }
    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(getRandom()));

    // insert data into nodes
    for(uint64_t i = 0; i < SCALE1; i++) {
        n->Store(tmp[i], _val_t(tmp[i]), &split_key, &split_node);
    }

    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        ASSERT_TRUE(n->Lookup(i, res));
        ASSERT_EQ(res, _val_t(i));
    }
    ASSERT_EQ(split_node, nullptr);

    delete tmp;
    delete n;
}

TEST(SingleNode, roleaf) {
    int load_size = SCALE1 * 3 / 5;

    _key_t split_key = UINT64_MAX;
    ROLeaf * split_node = nullptr;
    Record * tmp = new Record[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i].key = i;
        tmp[i].val = (_val_t)i;
    }
    std::shuffle(tmp, tmp + SCALE1 - 1, std::default_random_engine(getRandom()));

    // bulk load
    std::sort(tmp, tmp + load_size);
    ROLeaf * n = new ROLeaf(tmp, load_size);

    // test insert
    for(uint64_t i = load_size; i < SCALE1; i++) {
        ASSERT_FALSE(n->Store(tmp[i].key, tmp[i].val, &split_key, &split_node));
    }

    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        ASSERT_TRUE(n->Lookup(i, res));
        ASSERT_EQ(res, _val_t(i));
    }

    delete tmp;
    delete n;
}   

TEST(SingleNode, roinner) {
    int load_size = SCALE1;
    std::default_random_engine gen(getRandom());
    std::uniform_int_distribution<uint64_t> dist(0, SCALE1 * 10);


    _key_t split_key = UINT64_MAX;
    ROInner * split_node = nullptr;
    Record * tmp = new Record[SCALE1];
    for(uint64_t i = 0; i < SCALE1; i++) {
        tmp[i].key = dist(gen);
        tmp[i].val = (_val_t)tmp[i].key;
    }

    // bulk load
    std::sort(tmp, tmp + load_size);
    ROInner * n = new ROInner(tmp, load_size);
    
    // n->Print("");
    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        ASSERT_TRUE(n->Lookup(tmp[i].key, res));
        ASSERT_EQ(res, _val_t(tmp[i].key));
    }

    delete tmp;
    // delete n; // problem with cascading delete
}

/* TwoNode Test: store operations may trigger a node split */
const int SCALE2 = GLOBAL_LEAF_SIZE * 5 / 4; // big enough to trigger a node split

TEST(TwoNode, wonode) {
    WOLeaf * n = new WOLeaf;
    _key_t split_key = UINT64_MAX;
    WOLeaf * split_node = nullptr;
    
    _key_t * tmp = new _key_t[SCALE2];
    for(uint64_t i = 0; i < SCALE2; i++) {
        tmp[i] = i;
    }
    std::shuffle(tmp, tmp + SCALE2 - 1, std::default_random_engine(getRandom()));

    // insert data into nodes
    for(int i = 0; i < SCALE2; i++) {
        if(tmp[i] < split_key) {
            n->Store(tmp[i], _val_t(tmp[i]), &split_key, &split_node);
        } else {
            split_node->Store(tmp[i], _val_t(tmp[i]), nullptr, nullptr);
        }
    }

    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        if(i < split_key)
            ASSERT_TRUE(n->Lookup(i, res));
        else
            ASSERT_TRUE(split_node->Lookup(i, res));
        ASSERT_EQ(res, _val_t(i));
    }
    
    delete tmp;
    delete n;
    delete split_node;
}

TEST(TwoNode, rwleaf) {
    RWLeaf * n = new RWLeaf;
    _key_t split_key = UINT64_MAX;
    RWLeaf * split_node = nullptr;
    
    _key_t * tmp = new _key_t[SCALE2];
    for(uint64_t i = 0; i < SCALE2; i++) {
        tmp[i] = i;
    }
    std::shuffle(tmp, tmp + SCALE2 - 1, std::default_random_engine(getRandom()));

    // insert data into nodes
    for(int i = 0; i < SCALE2; i++) {
        if(tmp[i] < split_key) {
            n->Store(tmp[i], _val_t(tmp[i]), &split_key, &split_node);
        } else {
            split_node->Store(tmp[i], _val_t(tmp[i]), nullptr, nullptr);
        }
    }

    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE1; i++) {
        if(i < split_key)
            ASSERT_TRUE(n->Lookup(i, res));
        else
            ASSERT_TRUE(split_node->Lookup(i, res));
        ASSERT_EQ(res, _val_t(i));
    }
    
    delete tmp;
    delete n;
    delete split_node;
}

TEST(TwoNode, roleaf) {
    _key_t split_key = UINT64_MAX;
    ROLeaf * split_node = nullptr;
    
    Record * tmp = new Record[SCALE2];
    for(uint64_t i = 0; i < SCALE2; i++) {
        tmp[i].key = i;
        tmp[i].val = (_val_t)i;
    }
    std::shuffle(tmp, tmp + SCALE2 - 1, std::default_random_engine(getRandom()));
    std::sort(tmp, tmp + SCALE2 / 2);

    // bulk load
    ROLeaf * n = new ROLeaf(tmp, SCALE2 / 2);

    // insert data into nodes
    for(int i = SCALE2 / 2; i < SCALE2; i++) {
        if(tmp[i].key < split_key) {
            n->Store(tmp[i].key, tmp[i].val, &split_key, &split_node);
        } else {
            ASSERT_FALSE(split_node->Store(tmp[i].key, tmp[i].val, nullptr, nullptr));
        }
    }

    // test lookup
    _val_t res;
    for(uint64_t i = 0; i < SCALE2; i++) {
        if(i < split_key)
            ASSERT_TRUE(n->Lookup(i, res));
        else
            ASSERT_TRUE(split_node->Lookup(i, res));
        ASSERT_EQ(res, _val_t(i));
    }
    
    delete tmp;
    delete n;
    delete split_node;
}

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    do_morphing = false;
    return RUN_ALL_TESTS();
}