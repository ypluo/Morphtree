#!/bin/bash

DATASET=$1
READ=$2
threadnum="2 4 8 16 24 32 40"

for c in ${threadnum}; do
    echo ${c}
    ../scripts/runcon.sh ${DATASET} ${READ} ${c} 
done