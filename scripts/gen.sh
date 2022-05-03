#!/bin/bash

# basic setting
DATASET=$1
TYPE=$2
DATASIZE=64000000
QUERYSIZE=100000000 # 100M for longitude data
KEYSET=/data/lyp/datasets/${DATASET}.dat

case ${TYPE} in 
1): 
    QUERYONLY=0; READ=1; WRITE=0 ;;
2):
    QUERYONLY=1; READ=0.5; WRITE=0.5 ;;
3):
    QUERYONLY=1; READ=0; WRITE=1 ;;
*):
    exit 0 ;;
esac

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
    cp query.dat ../workloads/${DATASET}_query${TYPE}.dat

fi
