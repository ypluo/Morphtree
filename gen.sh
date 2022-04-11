#!/bin/bash

# basic setting
DATASET=$1
DATASIZE=64000000
QUERYSIZE=96000000
READ=1
WRITE=0
KEYSET=../datasets/${DATASET}_u64.dat

# generate dataset.dat and query.dat
echo "recordcount=${DATASIZE}"      > workload.spec
echo "operationcount=${QUERYSIZE}"   >> workload.spec
echo "readproportion=${READ}"       >> workload.spec
echo "updateproportion=0"           >> workload.spec
echo "scanproportion=0"             >> workload.spec
echo "insertproportion=${WRITE}"    >> workload.spec
./ycsb/ycsbc -P workload.spec -F ${KEYSET}
cp dataset.dat ../workloads/${DATASET}_dataset.dat
cp query.dat ../workloads/${DATASET}_query.dat