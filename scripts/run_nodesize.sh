#!/bin/bash

DATASET=$1
CONFIG_FILE=../morphtree/include/config.h

# for ns in 2048 4096 6144 8192 10240 14336 20480; do
for ns in 2048; do
    echo ${ns}

    echo "#ifndef __CONFIG__"              > ${CONFIG_FILE}
    echo "#define __CONFIG__"              >> ${CONFIG_FILE}
    echo "const int CONFIG_PROBE = 8;"     >> ${CONFIG_FILE}
    echo "const int CONFIG_PIECE = 1024;"  >> ${CONFIG_FILE}
    echo "const float CONFIG_BULK = 0.25;" >> ${CONFIG_FILE}
    echo "const float CONFIG_NODESIZE = ${ns};"  >> ${CONFIG_FILE}
    echo "#endif // __CONFIG__"                  >> ${CONFIG_FILE}
    
    make -j 8 > /dev/null
    taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
    taskset -c 21-22 cp ../workloads/${DATASET}_query0.dat query.dat
    taskset -c 21-22 ./benchmark wotree >> ${DATASET}_nodesize.txt

    taskset -c 21-22 cp ../workloads/${DATASET}_query1.dat query.dat
    taskset -c 21-22 ./benchmark rotree >> ${DATASET}_nodesize.txt
    echo               >> ${DATASET}_nodesize.txt
done
