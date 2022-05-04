//
// Created by tawnysky on 2020/10/28.
//

#pragma once

#include <cstdint>
#include <vector>
#include <stdexcept>
#include <cstring>
#include <limits>
#include <iostream>
#include <cassert>
#include <algorithm>
#include <random>
#include <chrono> 

#define NDEBUG

// Epsilon for double comparison
#define EPS 1e-12

// Epsilon for optimal plr
#define EPSILON_INIT_INNER_NODE 128

#define EPSILON_INIT_LEAF_NODE 256

// Max size of level 1
#define ROOT_LEVEL1_SIZE 8

#define SIMPLE_INNER_NODE_LEVEL1_SIZE 8

#define SIMPLE_LEAF_NODE_LEVEL1_SIZE 12

// Initial filling ratio
#define INNER_NODE_INIT_RATIO 0.6

#define LEAF_NODE_INIT_RATIO 0.6

// Maximum filling ratio
#define INNER_NODE_MAX_RATIO 0.9

#define LEAF_NODE_MAX_RATIO 0.9

// Maximum overflowed data ratio
#define LEAF_NODE_OVERFLOW_MAX_RATIO 0.3

#define INNER_NODE_OVERFLOW_MAX_RATIO 0.3

// Max size of node
#define LEAF_NODE_MAX_SIZE (1 << 20)

#define INNER_NODE_MAX_SIZE (1 << 18)

// Min overflow bound
#define OVERFLOW_MIN_BOUND (1 << 9)
