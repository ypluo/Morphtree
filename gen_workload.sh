#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Using default dataset and workload files"
    DATASET_FNAME=lognormal.dat
    WORKLOAD_FNAME=workload1.spec
else
    echo "using dataset ${1} and workload ${2}"
    DATASET_FNAME=$1
    WORKLOAD_FNAME=$2
fi

BINARY_DIR=.
YCSB_BIN=${BINARY_DIR}/ycsb/ycsbc
DATASET=${BINARY_DIR}/../datasets/${DATASET_FNAME}
WORKLOAD=${BINARY_DIR}/../workloads/${WORKLOAD_FNAME}

${YCSB_BIN} load -P ${WORKLOAD} -F ${DATASET} > ${BINARY_DIR}/dataset.dat
${YCSB_BIN} run  -P ${WORKLOAD} -F ${DATASET} > ${BINARY_DIR}/query.dat