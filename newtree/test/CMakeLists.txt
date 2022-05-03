add_executable(treetest "treetest.cc")
target_link_libraries(treetest gtest_main morphtree)

add_executable(testfix "testfix.cc")
target_link_libraries(testfix gtest_main)