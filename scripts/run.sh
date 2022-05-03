#!/bin/bash

DATASET=$1
TYPE=$2

cp ../workloads/${DATASET}_dataset.dat dataset.dat
cp ../workloads/${DATASET}_query${TYPE}.dat query.dat

# taskset -c 1-4 ./benchmark alex
# taskset -c 1-4 ./benchmark rotree
# taskset -c 1-4 ./benchmark morphtree
taskset -c 1-4 ./benchmark lipp
taskset -c 1-4 ./benchmark wotree