#!/bin/bash

DATASET=$1
TYPE=$2

taskset -c 21-22 cp ../workloads/${DATASET}_dynamic_dataset.dat dataset.dat
taskset -c 21-22 cp ../workloads/${DATASET}_dynamic_query${TYPE}.dat query.dat

taskset -c 21-22 ./benchmark alex
taskset -c 21-22 ./benchmark lipp
taskset -c 21-22 ./benchmark wotree
taskset -c 21-22 ./benchmark rotree
taskset -c 21-22 ./benchmark morphtree