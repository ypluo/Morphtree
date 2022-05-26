#!/bin/bash

# basic setting
DATASET=$1
READ=$2
WRITE=$3
DATASIZE=64000000
QUERYSIZE=64000000
KEYSET=/data/lyp/datasets/${DATASET}.dat
DIST=zipfian

echo "recordcount=${DATASIZE}"      > workload.spec
echo "operationcount=${QUERYSIZE}"  >> workload.spec
echo "readproportion=${READ}"       >> workload.spec
echo "insertproportion=${WRITE}"    >> workload.spec
echo "updateproportion=0"           >> workload.spec
echo "scanproportion=0"             >> workload.spec
echo "requestdistribution=${DIST}"  >> workload.spec

if [[ ${READ} == 1 ]]
then
    taskset -c 1-4 ./ycsb/ycsbc -P workload.spec -F ${KEYSET}
    taskset -c 1-4 cp dataset.dat ../workloads/${DATASET}_dataset.dat
    taskset -c 1-4 cp query.dat ../workloads/${DATASET}_query${READ}.dat
else
    taskset -c 1-4 ./ycsb/ycsbc -P workload.spec -F ${KEYSET} --query_only
    taskset -c 1-4 cp query.dat ../workloads/${DATASET}_query${READ}.dat
fi
