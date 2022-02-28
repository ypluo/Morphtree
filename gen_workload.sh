#!/bin/bash

BIN=./ycsb/ycsbc
BINARY_DIR=build
WORKLOAD=./ycsb/workloads/workloada.spec

${BIN} load -P ${WORKLOAD} > ${BINARY_DIR}/dataset.dat
${BIN} run -P ${WORKLOAD} > ${BINARY_DIR}/query.dat