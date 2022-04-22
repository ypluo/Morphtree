#!/bin/bash

# basic setting
DATASET=$1
QUERYONLY=1
DATASIZE=64000000
QUERYSIZE=128000000
READ=1
WRITE=0
KEYSET=/data/lyp/datasets/${DATASET}.dat

# generate dataset.dat and query.dat
if [[ ${QUERYONLY} -eq 0 ]]; then
    echo "generate dataset.dat and query.dat"
    echo "recordcount=${DATASIZE}"      > workload.spec
    echo "operationcount=${QUERYSIZE}"  >> workload.spec
    echo "readproportion=${READ}"       >> workload.spec
    echo "updateproportion=0"           >> workload.spec
    echo "scanproportion=0"             >> workload.spec
    echo "insertproportion=${WRITE}"    >> workload.spec
    echo "requestdistribution=zipfian"  >> workload.spec

    ./ycsb/ycsbc -P workload.spec -F ${KEYSET}
    cp dataset.dat ../workloads/${DATASET}_dataset.dat
    cp query.dat ../workloads/${DATASET}_query1.dat

else
    echo "generate query.dat only"
    echo "recordcount=0"                > workload.spec
    echo "insertstart=${DATASIZE}"      >> workload.spec
    echo "operationcount=${QUERYSIZE}"  >> workload.spec
    echo "readproportion=${READ}"       >> workload.spec
    echo "updateproportion=0"           >> workload.spec
    echo "scanproportion=0"             >> workload.spec
    echo "insertproportion=${WRITE}"    >> workload.spec
    echo "requestdistribution=zipfian"  >> workload.spec

    ./ycsb/ycsbc -P workload.spec -F ${KEYSET} --query_only
    cp query.dat ../workloads/${DATASET}_query1.dat

fi
