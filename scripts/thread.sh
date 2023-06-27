#!/bin/bash

DATASET=$1
READ=$2
threadnum="1 5 10 20 30 40"

for c in ${threadnum}; do
    echo ${c}
    ../scripts/runcon.sh ${DATASET} ${READ} ${c} 
done