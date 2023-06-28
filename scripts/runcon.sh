#!/bin/bash

DATASET=$1
READ=$2
THREAD_NUM=$3

taskset -c 20-39,60-79 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 20-39,60-79 cp ../workloads/${DATASET}_query${READ}.dat query.dat

taskset -c 20-39,60-79 ./benchmark btree     ${THREAD_NUM}
taskset -c 20-39,60-79 ./benchmark alex      ${THREAD_NUM}
taskset -c 20-39,60-79 ./benchmark lipp      ${THREAD_NUM}
taskset -c 20-39,60-79 ./benchmark finedex   ${THREAD_NUM}
# taskset -c 20-39,60-79 ./benchmark xindex    ${THREAD_NUM}
taskset -c 20-39,60-79 ./benchmark wotree    ${THREAD_NUM}
taskset -c 20-39,60-79 ./benchmark rotree    ${THREAD_NUM}
taskset -c 20-39,60-79 ./benchmark morphtree ${THREAD_NUM}
echo " "