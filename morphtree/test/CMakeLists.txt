add_executable(nodetest "nodetest.cc")
target_link_libraries(nodetest gtest_main morphtree)

# add_executable(nodemorph "nodemorph.cc")
# target_link_libraries(nodemorph gtest_main morphtree)

add_executable(wotest "wotest.cc")
target_link_libraries(wotest gtest_main morphtree)

add_executable(rotest "rotest.cc")
target_link_libraries(rotest gtest_main morphtree)

add_executable(ycsbtest "ycsbtest.cc")
target_link_libraries(ycsbtest gtest_main morphtree)