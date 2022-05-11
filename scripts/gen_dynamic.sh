#!/bin/bash

DATASET=$1
TYPE=$2

KEYSET=/data/lyp/datasets/${DATASET}.dat

./ycsb/rwratio${TYPE} -F ${KEYSET}
cp dataset.dat ../workloads/${DATASET}_dynamic_dataset.dat
cp query.dat ../workloads/${DATASET}_dynamic_query${TYPE}.dat