#!/bin/bash

DATASET=$1
TYPE=$2

KEYSET=/data/lyp/datasets/${DATASET}.dat

taskset -c 21-22 ./ycsb/rwratio${TYPE} -F ${KEYSET}
taskset -c 21-22 cp dataset.dat ../workloads/${DATASET}_dynamic_dataset.dat
taskset -c 21-22 cp query.dat ../workloads/${DATASET}_dynamic_query${TYPE}.dat