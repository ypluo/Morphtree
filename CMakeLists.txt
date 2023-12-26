cmake_minimum_required(VERSION 3.11)

project(morphtree)

set(CMAKE_CXX_STANDARD 17)

if(MSVC)
    set(CMAKE_CXX_FLAGS "/O2 /arch:AVX2 /EHsc")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    set(CMAKE_CXX_FLAGS "-O3 -xHost")
else()
    # clang and gcc
    set(CMAKE_CXX_FLAGS "-march=native -O3 -fmax-errors=5")
endif()

set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")

message("CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})

#======= include directories =======
include_directories(${CMAKE_SOURCE_DIR}/ALEX/src/core)
include_directories(${CMAKE_SOURCE_DIR}/LIPP/src/core)
include_directories(${CMAKE_SOURCE_DIR}/morphtree)

#==== set compile and link options =====
# link options must be prior to adding sub directories
add_link_options(-pthread)

#======= add sub directories =======
add_subdirectory(morphtree)

# add YCSB
add_subdirectory(ycsb)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark morphtree)
link_directories(${MKL_LINK_DIRECTORY})