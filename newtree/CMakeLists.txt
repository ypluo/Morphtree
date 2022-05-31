# Project properties
project(newtree)

include_directories(include)
add_compile_options(-O3)

# Now add what we want to compile
add_subdirectory(src)