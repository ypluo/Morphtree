cmake_minimum_required(VERSION 3.11)

project(morphtree)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -std=c++17 -pthread -fmax-errors=5 -fopenmp -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

message("-- CMAKE BUILD TYPE: " ${CMAKE_BUILD_TYPE})

find_library(GLOG_LIBRARIES NAMES glog)
set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")
link_directories(${MKL_LINK_DIRECTORY})

#======= include directories =======
include_directories(${CMAKE_SOURCE_DIR}/ALEX/src/core)
include_directories(${CMAKE_SOURCE_DIR}/LIPP/src/core)
include_directories(${CMAKE_SOURCE_DIR}/PGM-index)
include_directories(${CMAKE_SOURCE_DIR}/FITingTree)
include_directories(${CMAKE_SOURCE_DIR}/morphtree)

include_directories(${MKL_INCLUDE_DIRECTORY})

#==== set compile and link options =====
# link options must be prior to adding sub directories
add_link_options(-pthread -fopenmp)

#======= add sub directories =======
add_subdirectory(morphtree)
add_subdirectory(ycsb)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark morphtree glog mkl_rt)