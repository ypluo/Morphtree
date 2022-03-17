project(ycsb)

#set(CMAKE_CXX_FLAGS_RELEASE "-O3")
add_library(core "core_workload.cc")

add_executable(ycsbc "ycsbc.cc")
target_link_libraries(ycsbc core)

add_executable(rwratio "rwratio.cc")
target_link_libraries(rwratio core)

add_executable(keygen "keygen.cc")