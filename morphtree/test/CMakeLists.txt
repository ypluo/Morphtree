add_executable(nodetest "nodetest.cc")
target_link_libraries(nodetest gtest_main ${NODE_LIB})

add_executable(nodeperf "nodeperf.cc")
target_link_libraries(nodeperf ${NODE_LIB})

add_executable(nodemorph "nodemorph.cc")
target_link_libraries(nodemorph gtest_main ${NODE_LIB})

add_executable(datagen "datagen.cc")
target_link_libraries(datagen)

add_executable(treetest "treetest.cc")
target_link_libraries(treetest gtest_main ${NODE_LIB})