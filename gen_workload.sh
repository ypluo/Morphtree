#!/bin/bash

DATASET=$1

# workload specifications
RECORD=0
START=64000000
OPERATION=64000000
READ=1
WRITE=0
DIST=zipfian
KEYSET=../datasets/${DATASET}_u64.dat

# generate workload specific file
echo "recordcount=${RECORD}"         > workload.spec
echo "insertstart=${START}"          >> workload.spec
echo "operationcount=${OPERATION}"   >> workload.spec
echo "readproportion=${READ}"        >> workload.spec
echo "updateproportion=0"            >> workload.spec
echo "scanproportion=0"              >> workload.spec
echo "insertproportion=${WRITE}"     >> workload.spec
echo "requestdistribution=${DIST}"   >> workload.spec

# generate workload
./ycsb/ycsbc -P workload.spec -F ${KEYSET} --query_only
mv query.dat ../workloads/${DATASET}_query3.dat