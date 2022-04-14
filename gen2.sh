#!/bin/bash

DATASET=$1
KEYSET=/data/lyp/datasets/${DATASET}.dat

./ycsb/rwratio -F ${KEYSET}
cp dataset.dat ../workloads/${DATASET}_dynamic_dataset.dat
cp query.dat ../workloads/${DATASET}_dynamic_query.dat