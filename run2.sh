#!/bin/bash

DATASET=$1

cp ../workloads/${DATASET}_dynamic_dataset.dat dataset.dat
cp ../workloads/${DATASET}_dynamic_query.dat query.dat

taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark lipp
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree
# taskset -c 1-4 ./benchmark rwtree