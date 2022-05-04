cmake_minimum_required(VERSION 3.16)
project(Flat)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "-O3 -march=native")

add_executable(Flat test.cpp include/piecewise_linear_model.h include/flat_nodes.h include/flat_index.h include/parameters.h include/zipfian.h include/zipfian_random.h)
