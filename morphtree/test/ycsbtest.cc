#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include "../src/node.h"
#include "morphtree.h"

#include "gtest/gtest.h"

class ycsbtest : public testing::Test {
protected:
    morphtree::Morphtree * tree;

    virtual void SetUp() {
        tree = new morphtree::Morphtree;
    }

    virtual void TearDown() {
        delete tree;
    }
};

TEST_F(ycsbtest, ycsb) {
    std::ifstream infile_load("/home/lyp/morphtree/build/dataset.dat");
    std::ifstream infile_txn("/home/lyp/morphtree/build/query.dat");
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
        tree->insert(_key_t(key), _val_t((uint64_t)key));
        count += 1;
    }

    while (infile_txn.good()) {
        infile_txn >> op >> key;
        // std::cout << op << " " << key << std::endl;

        if(!infile_txn.good()) {
            break;
        }
        if (op.compare("INSERT") == 0) {
            tree->insert(_key_t(key), _val_t((uint64_t)key));
        }
        else if (op.compare("READ") == 0) {
            ASSERT_EQ(tree->lookup(_key_t(key)), _val_t((uint64_t)key));
        }
    }

    infile_load.close();
    infile_txn.close();
} 