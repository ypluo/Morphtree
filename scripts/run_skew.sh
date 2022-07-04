#!/bin/bash

DATASET=$1
SKEW=$2

taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 21-22 cp ../workloads/${DATASET}_query_skew${SKEW}.dat query.dat

taskset -c 21-22 ./benchmark alex         >> ${DATASET}.txt
taskset -c 21-22 ./benchmark lipp         >> ${DATASET}.txt
taskset -c 21-22 ./benchmark pgm          >> ${DATASET}.txt
taskset -c 21-22 ./benchmark fiting       >> ${DATASET}.txt
taskset -c 21-22 ./benchmark wotree       >> ${DATASET}.txt
taskset -c 21-22 ./benchmark rotree       >> ${DATASET}.txt
taskset -c 21-22 ./benchmark morphtree    >> ${DATASET}.txt
echo " "  >> ${DATASET}.txt
