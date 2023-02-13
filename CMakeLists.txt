cmake_minimum_required(VERSION 3.11)

project(vibench)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -std=c++17 -pthread -fmax-errors=5 -fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")

find_path(GLOG_INCLUDE_DIRS NAMES glog/logging.h) 
find_library(GLOG_LIBRARIES NAMES glog)

#======= include directories =======
include_directories(${CMAKE_SOURCE_DIR}/ALEX/src/core)
include_directories(${CMAKE_SOURCE_DIR}/LIPP/src/core)
include_directories(${CMAKE_SOURCE_DIR}/PGM-index)
include_directories(${CMAKE_SOURCE_DIR}/FITingTree)
include_directories(${CMAKE_SOURCE_DIR}/morphtree)

#==== set compile and link options =====
# link options must be prior to adding sub directories
add_link_options(-pthread -fopenmp)

#======= add sub directories =======
add_subdirectory(morphtree)
add_subdirectory(ycsb)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark morphtree glog)