set(CMAKE_CXX_FLAGS_RELEASE "-O3")
file(GLOB BTREE_SRC "*.cc" "*.h")

add_library(stxbtree STATIC ${BTREE_SRC})

target_include_directories(stxbtree PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)