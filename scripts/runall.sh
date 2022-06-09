#!/bin/bash

DATASET=$1

../scripts/run.sh ${DATASET} 0    > ${DATASET}.txt
../scripts/run.sh ${DATASET} 0.2  >> ${DATASET}.txt
../scripts/run.sh ${DATASET} 0.4  >> ${DATASET}.txt
../scripts/run.sh ${DATASET} 0.6  >> ${DATASET}.txt
../scripts/run.sh ${DATASET} 0.8  >> ${DATASET}.txt
../scripts/run.sh ${DATASET} 1    >> ${DATASET}.txt