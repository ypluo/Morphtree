cmake_minimum_required(VERSION 3.11)

project(vibench)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-march=native -O3 -fmax-errors=5")

#======= include directories =======
include_directories(${CMAKE_SOURCE_DIR}/ALEX/src/core)
include_directories(${CMAKE_SOURCE_DIR}/LIPP/src/core)
include_directories(${CMAKE_SOURCE_DIR}/PGM-index)
include_directories(${CMAKE_SOURCE_DIR}/FITingTree)
include_directories(${CMAKE_SOURCE_DIR}/morphtree)

#==== set compile and link options =====
# link options must be prior to adding sub directories
add_link_options(-pthread)

#======= add sub directories =======
add_subdirectory(morphtree)
add_subdirectory(ycsb)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark morphtree)