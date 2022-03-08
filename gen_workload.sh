#!/bin/bash

BINARY_DIR=.
BIN=${BINARY_DIR}/ycsb/ycsbc
DATASET=${BINARY_DIR}/keyset.dat
WORKLOAD=${BINARY_DIR}/../ycsb/workloads/workloada.spec

${BIN} load -P ${WORKLOAD} > ${BINARY_DIR}/dataset.dat
${BIN} run -P ${WORKLOAD} > ${BINARY_DIR}/query.dat