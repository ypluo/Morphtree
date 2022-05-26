#!/bin/bash

DATASET=$1
READ=$2

taskset -c 1-4 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 1-4 cp ../workloads/${DATASET}_query${READ}.dat query.dat

taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark lipp
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree
echo " "