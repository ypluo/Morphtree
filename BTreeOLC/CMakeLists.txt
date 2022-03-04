set(CMAKE_CXX_FLAGS_RELEASE "-O3")
file(GLOB btreeolc "*.cpp" "*.h")

add_library(btreeolc STATIC ${btreeolc})

target_include_directories(btreeolc PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

#SET(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})