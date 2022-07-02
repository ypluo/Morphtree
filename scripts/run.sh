#!/bin/bash

DATASET=$1
READ=$2

taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 21-22 cp ../workloads/${DATASET}_query${READ}.dat query.dat

# taskset -c 21-22 ./benchmark alex
# taskset -c 21-22 ./benchmark lipp
taskset -c 21-22 ./benchmark pgm
#taskset -c 21-22 ./benchmark fiting
# taskset -c 21-22 ./benchmark wotree
# taskset -c 21-22 ./benchmark rotree
# taskset -c 21-22 ./benchmark morphtree
echo " "
