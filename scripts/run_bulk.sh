#!/bin/bash

DATASET=$1
CONFIG_FILE=../morphtree/include/config.h

taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 21-22 cp ../workloads/${DATASET}_query1.dat query.dat

for bulk in 0.0625 0.125 0.25 0.5 1; do
    echo ${bulk}

    echo "#ifndef __CONFIG__"              > ${CONFIG_FILE}
    echo "#define __CONFIG__"              >> ${CONFIG_FILE}
    echo "const int CONFIG_PROBE = 8;"     >> ${CONFIG_FILE}
    echo "const int CONFIG_PIECE = 1024;"  >> ${CONFIG_FILE}
    echo "const float CONFIG_BULK = ${bulk};"  >> ${CONFIG_FILE}
    echo "#endif // __CONFIG__"             >> ${CONFIG_FILE}
    
    make -j 8
    taskset -c 21-22 ./benchmark alex >> bulkload.txt
    taskset -c 21-22 ./benchmark lipp >> bulkload.txt
    taskset -c 21-22 ./benchmark pgm  >> bulkload.txt
    taskset -c 21-22 ./benchmark fiting >> bulkload.txt
    taskset -c 21-22 ./benchmark wotree >> bulkload.txt
    taskset -c 21-22 ./benchmark rotree >> bulkload.txt
    taskset -c 21-22 ./benchmark morphtree >> bulkload.txt
    echo >> bulkload.txt
done
