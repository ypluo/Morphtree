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
echo "readproportion=0"             >> workload.spec
echo "insertproportion=0"           >> workload.spec
echo "updateproportion=1"           >> workload.spec
echo "scanproportion=0"             >> workload.spec
echo "requestdistribution=${DIST}"  >> workload.spec
# echo "maxscanlength=100"            >> workload.spec

taskset -c 21-22 ./ycsb/ycsbc -P workload.spec -F ${KEYSET} --query_only
taskset -c 21-22 cp query.dat ../workloads/${DATASET}_update.dat