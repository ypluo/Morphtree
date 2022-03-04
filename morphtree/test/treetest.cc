#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include "../src/node.h"
#include "morphtree.h"

#include "gtest/gtest.h"

class tree_test : public testing::Test {
protected:
    Morphtree * tree;

    virtual void SetUp() {
        tree = new Morphtree;
    }

    virtual void TearDown() {
        delete tree;
    }
};

TEST_F(tree_test, load) {
    const int TEST_SCALE = 1000000;
    uint64_t STEP = UINT64_MAX / TEST_SCALE;

    _key_t * keys = new _key_t[TEST_SCALE];
    for(int i = 0; i < TEST_SCALE; i++) {
        //keys[i] = _key_t(i);
        keys[i] = _key_t(i) * STEP + ((i | 0x5a5a5a5a) % 0xff);
    }

    //std::default_random_engine gen(997);
    std::default_random_engine gen(getRandom());
    std::shuffle(keys, &keys[TEST_SCALE - 1], gen);

    for(int i = 0; i < TEST_SCALE; i++) {
        //printf("%d\n", keys[i]);
        tree->insert(_key_t(keys[i]), _val_t((uint64_t)i));
    }

    for(int i = 0; i < TEST_SCALE; i++) {
        //printf("%d\n", keys[i]);
        ASSERT_EQ(tree->lookup(_key_t(keys[i])), _val_t((uint64_t)i));
    }

    delete keys;
} 

TEST_F(tree_test, ycsb) {
    std::ifstream infile_load("dataset.dat");
    std::ifstream infile_txn("query.dat");
    if(!infile_load || !infile_txn) {
        printf("file open error\n");
        return;
    }

    int count = 0;
    std::string op;
    _key_t key;
    while (true) {
        infile_load >> op >> key;
        if(!infile_load.good()) {
            break;
        }
        tree->insert(_key_t(key), _val_t(key));
        count += 1;
    }

    bool printall = false;
    while (infile_txn.good()) {
        infile_txn >> op >> key;
        //std::cout << op << " " << key << std::endl;

        if(!infile_txn.good()) {
            break;
        }
        if (op.compare("INSERT") == 0) {
            tree->insert(_key_t(key), _val_t(key));
        }
        else if (op.compare("READ") == 0) {
            ASSERT_EQ(tree->lookup(_key_t(key)), _val_t(key));
        }
    }

    infile_load.close();
    infile_txn.close();
} 