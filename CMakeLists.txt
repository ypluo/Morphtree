cmake_minimum_required(VERSION 3.11)

project(vibench)

#======= include directories =======
include_directories(${CMAKE_SOURCE_DIR}/ALEX/src/core)
include_directories(${CMAKE_SOURCE_DIR}/ARTOLC)
include_directories(${CMAKE_SOURCE_DIR}/BTreeOLC)
include_directories(${CMAKE_SOURCE_DIR}/hydralist/include)
include_directories(${CMAKE_SOURCE_DIR}/hydralist/src)

#==== set compile and link options =====
# link options must be prior to adding sub directories
add_link_options(-pthread)
link_libraries(/usr/local/lib/libtbb.so)

#======= add sub directories =======
#add_subdirectory(hydralist)
add_subdirectory(ALEX)
add_subdirectory(ARTOLC)
add_subdirectory(BTreeOLC)
add_subdirectory(morphtree)

# add YCSB
add_subdirectory(YCSB)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark alex artolc btreeolc morphtree)

add_custom_target(myclean
  COMMAND make clean
  COMMAND cd "${CMAKE_SOURCE_DIR}/ycsb" && make clean
)