cmake_minimum_required(VERSION 3.11)

project(vibench)

if(MSVC)
    set(CMAKE_CXX_FLAGS "/O2 /arch:AVX2 /EHsc")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    set(CMAKE_CXX_FLAGS "-O3 -xHost")
else()
    # clang and gcc
    set(CMAKE_CXX_FLAGS "-march=native -O3 -fmax-errors=5")
endif()

#======= include directories =======
include_directories(${CMAKE_SOURCE_DIR}/ALEX/src/core)
include_directories(${CMAKE_SOURCE_DIR}/ARTOLC)
include_directories(${CMAKE_SOURCE_DIR}/stxbtree)
include_directories(${CMAKE_SOURCE_DIR}/morphtree)

#==== set compile and link options =====
# link options must be prior to adding sub directories
add_link_options(-pthread)
link_libraries(/usr/local/lib/libtbb.so)

#======= add sub directories =======
add_subdirectory(ALEX)
add_subdirectory(ARTOLC)
add_subdirectory(morphtree)
add_subdirectory(lipp)

# add YCSB
add_subdirectory(ycsb)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark artolc morphtree)