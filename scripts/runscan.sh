#!/bin/bash

DATASET=$1

taskset -c 21-22 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 21-22 cp ../workloads/${DATASET}_update.dat query.dat

# taskset -c 21-22 ./benchmark lipp         >> ${DATASET}.txt
# taskset -c 21-22 ./benchmark alex         >> ${DATASET}.txt
# taskset -c 21-22 ./benchmark pgm          >> ${DATASET}.txt
# taskset -c 21-22 ./benchmark fiting       >> ${DATASET}.txt
# taskset -c 21-22 ./benchmark btree        >> ${DATASET}.txt
taskset -c 21-22 ./benchmark morphtree    >> ${DATASET}.txt

# taskset -c 21-22 ./benchmark wotree       >> ${DATASET}.txt
# taskset -c 21-22 ./benchmark rotree       >> ${DATASET}.txt

echo " "  >> ${DATASET}.txt
