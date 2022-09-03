#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include "../src/node.h"
#include "morphtree.h"

#include "gtest/gtest.h"

class wotest : public testing::Test {
protected:
    Morphtree * tree;

    virtual void SetUp() {
        tree = new Morphtree;
    }

    virtual void TearDown() {
        delete tree;
    }
};

TEST_F(wotest, DISABLED_lookup) {
    const int TEST_SCALE = 100000;
    uint64_t STEP = 1000;

    _key_t * keys = new _key_t[TEST_SCALE];
    for(int i = 0; i < TEST_SCALE; i++) {
        // keys[i] = _key_t(i);
        keys[i] = _key_t(i) * STEP + ((i | 0x5a5a5a5a) % 0xffff);
    }

    std::default_random_engine gen(997);
    // std::default_random_engine gen(getRandom());
    std::shuffle(keys, &keys[TEST_SCALE - 1], gen);

    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("insert %d %lf\n",i, keys[i]);
        tree->insert(keys[i], uint64_t(keys[i]));
    }
    
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", keys[i]);
        ASSERT_EQ(tree->lookup(keys[i]), uint64_t(keys[i]));
    }

    delete keys;
} 

TEST_F(wotest, DISABLED_update) {
    const int TEST_SCALE = 100000;
    const int step = 1000;

    _key_t * keys = new _key_t[TEST_SCALE];
    for(int i = 0; i < TEST_SCALE; i++) {
        keys[i] = _key_t(i);
        // keys[i] = _key_t(i) * STEP + ((i | 0x5a5a5a5a) % 0xffff);
    }

    for(int i = 0; i < TEST_SCALE; i++) {
        tree->insert(keys[i], uint64_t(keys[i]));
    }

    for(int i = 0; i < TEST_SCALE; i += 2) {
        tree->update(keys[i], uint64_t(keys[i]) * 2);
    }
    
    for(int i = 0; i < TEST_SCALE; i++) {
        // printf("%lf\n", keys[i]);
        if(i % 2 == 0)
            ASSERT_EQ(tree->lookup(keys[i]), uint64_t(keys[i]) * 2);
        else 
            ASSERT_EQ(tree->lookup(keys[i]), uint64_t(keys[i]));
    }

    delete keys;
}

TEST_F(wotest, remove) {
    const int TEST_SCALE = 100000;
    uint64_t STEP = 1000;

    _key_t * keys = new _key_t[TEST_SCALE];
    for(int i = 0; i < TEST_SCALE; i++) {
        keys[i] = _key_t(i);
        // keys[i] = _key_t(i) * STEP + ((i | 0x5a5a5a5a) % 0xffff);
    }

    std::default_random_engine gen(997);
    // std::default_random_engine gen(getRandom());
    // std::shuffle(keys, &keys[TEST_SCALE - 1], gen);

    // remove half of the records
    for(int i = 0; i < TEST_SCALE; i += 2) {
        tree->remove(keys[i]);
    }
    
    for(int i = 0; i < TEST_SCALE; i++) {
        printf("%lf\n", keys[i]);
        if(i % 2 == 0)
            ASSERT_EQ(tree->lookup(keys[i]), 0);
        else 
            ASSERT_EQ(tree->lookup(keys[i]), uint64_t(keys[i]));
    }

    delete keys;
}

TEST_F(wotest, DISABLED_ycsb) {
    std::ifstream infile_load("../../build/dataset.dat");
    std::ifstream infile_txn("../../build/query.dat");
    if(!infile_load || !infile_txn) {
        printf("file open error\n");
        return;
    }

    int count = 0;
    std::string op;
    _key_t key;
    while (true) {
        infile_load >> op >> key;
        // std::cout << op << " " << key << std::endl;
        if(!infile_load.good()) {
            break;
        }
        tree->insert(_key_t(key), uint64_t((uint64_t)key));
        count += 1;
    }

    while (infile_txn.good()) {
        infile_txn >> op >> key;
        // std::cout << op << " " << key << std::endl;

        if(!infile_txn.good()) {
            break;
        }
        if (op.compare("INSERT") == 0) {
            tree->insert(_key_t(key), uint64_t((uint64_t)key));
        }
        else if (op.compare("READ") == 0) {
            ASSERT_EQ(tree->lookup(_key_t(key)), uint64_t((uint64_t)key));
        }
    }

    infile_load.close();
    infile_txn.close();
} 