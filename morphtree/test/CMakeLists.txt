add_executable(nodetest "nodetest.cc")
target_link_libraries(nodetest gtest_main morphtree)

add_executable(nodemorph "nodemorph.cc")
target_link_libraries(nodemorph gtest_main morphtree)

add_executable(treetest "treetest.cc")
target_link_libraries(treetest gtest_main morphtree)