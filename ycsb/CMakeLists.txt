project(ycsb)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
add_library(core "core_workload.cc")

add_executable(ycsbc "ycsbc.cc")
target_link_libraries(ycsbc core)

add_executable(rwratio1 "rwratio1.cc")
target_link_libraries(rwratio1 core)

add_executable(rwratio2 "rwratio2.cc")
target_link_libraries(rwratio2 core)

add_executable(keygen "keygen.cc")