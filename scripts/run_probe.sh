#!/bin/bash

DATASET=$1
CONFIG_FILE=../morphtree/include/config.h

for len in 4 8 16 32 64; do
    echo ${len}

    echo "#ifndef __CONFIG__"              > ${CONFIG_FILE}
    echo "#define __CONFIG__"              >> ${CONFIG_FILE}
    echo "const int CONFIG_PROBE = ${len};">> ${CONFIG_FILE}
    echo "const int CONFIG_PIECE = 1024;"  >> ${CONFIG_FILE}
    echo "const float CONFIG_BULK = 0.25;"  >> ${CONFIG_FILE}
    echo "const float CONFIG_NODESIZE = 10240;"  >> ${CONFIG_FILE}
    echo "#endif // __CONFIG__"          >> ${CONFIG_FILE}
    
    make -j 8
    taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
    taskset -c 21-22 cp ../workloads/${DATASET}_query0.dat query.dat
    taskset -c 21-22 ./benchmark rotree >> ${DATASET}_probe.txt

    taskset -c 21-22 cp ../workloads/${DATASET}_query1.dat query.dat
    taskset -c 21-22 ./benchmark rotree >> ${DATASET}_probe.txt
    echo               >> ${DATASET}_probe.txt
done
