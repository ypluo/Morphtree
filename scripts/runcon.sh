#!/bin/bash

DATASET=$1
READ=$2
THREAD_NUM=$3

taskset -c 20-39,60-79 cp ../workloads/${DATASET}_dataset.dat dataset.dat
taskset -c 20-39,60-79 cp ../workloads/${DATASET}_query${READ}.dat query.dat

taskset -c 20-39,60-79 ./benchmark btree
taskset -c 20-39,60-79 ./benchmark alex
taskset -c 20-39,60-79 ./benchmark lipp
taskset -c 20-39,60-79 ./benchmark finedex
taskset -c 20-39,60-79 ./benchmark xindex
taskset -c 20-39,60-79 ./benchmark wotree
taskset -c 20-39,60-79 ./benchmark rotree
taskset -c 20-39,60-79 ./benchmark morphtree
echo " "