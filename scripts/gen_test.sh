#!/bin/bash

DATASET=$1

# workload specifications
RECORD=16000000
OPERATION=4000000
READ=0
WRITE=1
DIST=zipfian
KEYSET=/data/lyp/datasets/${DATASET}.dat

# generate workload specific file
echo "recordcount=${RECORD}"         > workload.spec
echo "insertstart=0"                 >> workload.spec
echo "operationcount=${OPERATION}"   >> workload.spec
echo "readproportion=${READ}"        >> workload.spec
echo "updateproportion=0"            >> workload.spec
echo "scanproportion=0"              >> workload.spec
echo "insertproportion=${WRITE}"     >> workload.spec
echo "requestdistribution=${DIST}"   >> workload.spec

# generate workload
taskset -c 21-22 ./ycsb/ycsbc -P workload.spec -F ${KEYSET}