set(MORPHTREE_SRC basenode.cc roinner.cc roleaf.cc woleaf.cc util.cc)

add_library(morphtree ${MORPHTREE_SRC})
target_link_libraries(morphtree glog)