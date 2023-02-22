#include <random>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <omp.h>

#include "../src/node.h"
#include "morphtree.h"

#include "gtest/gtest.h"

const int THREAD_NUM = 8;

class ycsbtest : public testing::Test {
protected:
    Morphtree * tree;

    virtual void SetUp() {
        tree = new Morphtree;
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

TEST_F(ycsbtest, concurrent_ycsb) {
    std::ifstream infile_load("/home/lyp/morphtree/build/dataset.dat");
    std::ifstream infile_txn("/home/lyp/morphtree/build/query.dat");
    if(!infile_load || !infile_txn) {
        printf("file open error\n");
        return;
    }

    int count = 0;
    std::string op;
    _key_t key;
    int range = 0;
    while (true) {
        infile_load >> op >> key;
        // std::cout << op << " " << key << std::endl;
        if(!infile_load.good()) {
            break;
        }
        tree->insert(_key_t(key), uint64_t(std::abs(key) + 1));
        count += 1;
    }

    std::vector<_key_t> keys;
    std::vector<int> ranges;
    std::vector<int> ops;
    keys.reserve(32000000);
    ranges.reserve(32000000);
    ops.reserve(32000000);

    std::string insert("INSERT");
    std::string read("READ");
    std::string update("UPDATE");
    std::string remove("REMOVE");
    std::string scan("SCAN");

    enum {OP_INSERT, OP_READ, OP_UPDATE, OP_REMOVE, OP_SCAN};
	while (true) {
		infile_txn >> op >> key;
		if(!infile_txn.good()) {
			break;
		}
        ranges.push_back(1);
		if (op.compare(insert) == 0) {
			ops.push_back(OP_INSERT);
			keys.push_back(key);
		}
		else if (op.compare(read) == 0) {
			ops.push_back(OP_READ);
			keys.push_back(key);
		}
		else if (op.compare(update) == 0) {
			ops.push_back(OP_UPDATE);
			keys.push_back(key);
		}
		else if (op.compare(remove) == 0) {
			ops.push_back(OP_REMOVE);
			keys.push_back(key);
		}
		else if (op.compare(scan) == 0) {
			infile_txn >> range;
			ops.push_back(OP_SCAN);
			keys.push_back(key);
			ranges.back() = range;
		}
		else {
			std::cout << "UNRECOGNIZED CMD!\n";
			return;
		}
	}

    Record buf[1000];
    uint64_t v;
    omp_set_num_threads(THREAD_NUM);
    #pragma omp parallel for schedule(dynamic, 32) private(buf, v)
    for(int i = 0; i < keys.size(); i++) {
        int op = ops[i];
        if (op == OP_INSERT) { //INSERT
            tree->insert(keys[i], uint64_t(std::abs(keys[i]) + 1));
        } else if (op == OP_READ) { //READ
            v = tree->lookup(keys[i]);
            assert(v == uint64_t(std::abs(keys[i]) + 1));
        } else if (op == OP_UPDATE) { //UPDATE
            tree->update(keys[i], uint64_t(std::abs(keys[i]) + 2));
        } else if (op == OP_REMOVE) {
            tree->remove(keys[i]);
        } else if (op == OP_SCAN) { //SCAN
            tree->scan(keys[i], ranges[i], buf);
        }
    }

    infile_load.close();
    infile_txn.close();
} 