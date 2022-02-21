set(CMAKE_CXX_FLAGS_RELEASE "-O3")
file(GLOB artsrc "*.cpp" "*.h")

add_library(artolc STATIC ${artsrc})

target_include_directories(artolc PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

SET(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_DIR})