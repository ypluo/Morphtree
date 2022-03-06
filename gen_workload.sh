#!/bin/bash

BINARY_DIR=.
BIN=${BINARY_DIR}/YCSB/ycsbc
DATASET=${BINARY_DIR}/keyset.dat
WORKLOAD=${BINARY_DIR}/../YCSB/workloads/workloada.spec

${BIN} load -P ${WORKLOAD} -F ${DATASET} > ${BINARY_DIR}/dataset.dat
${BIN} run -P ${WORKLOAD} -F ${DATASET} > ${BINARY_DIR}/query.dat