#!/bin/bash

DATASET=$1
CONFIG_FILE=../morphtree/include/config.h

for p in 256 512 768 1024 1536; do
    echo ${p}

    echo "#ifndef __CONFIG__"              > ${CONFIG_FILE}
    echo "#define __CONFIG__"              >> ${CONFIG_FILE}
    echo "const int CONFIG_PROBE = 8;"     >> ${CONFIG_FILE}
    echo "const int CONFIG_PIECE = ${p};"  >> ${CONFIG_FILE}
    echo "const float CONFIG_BULK = 0.25;"  >> ${CONFIG_FILE}
    echo "const float CONFIG_NODESIZE = 10240;"  >> ${CONFIG_FILE}
    echo "#endif // __CONFIG__"            >> ${CONFIG_FILE}
    
    make -j 8
    taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
    taskset -c 21-22 cp ../workloads/${DATASET}_query0.dat query.dat
    taskset -c 21-22 ./benchmark wotree >> ${DATASET}_piece.txt

    taskset -c 21-22 cp ../workloads/${DATASET}_query1.dat query.dat
    taskset -c 21-22 ./benchmark wotree >> ${DATASET}_piece.txt
    echo               >> ${DATASET}_piece.txt
done
