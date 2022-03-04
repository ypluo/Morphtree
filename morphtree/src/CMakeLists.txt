add_library(basenode "basenode.cc")
add_library(roinner "roinner.cc")
add_library(roleaf "roleaf.cc")
add_library(rwleaf "rwleaf.cc")
add_library(woleaf "woleaf.cc")


set(NODE_LIB 
        basenode
        roinner
        roleaf 
        rwleaf
        woleaf
    CACHE STRING "NODE_LIB" FORCE
)

add_library(morphtree "morphtree.cc")