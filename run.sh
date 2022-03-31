#!/bin/bash

echo -e "\nUsing dataset file: ../datasets/lognormal_u64.dat"
cp ../workloads/lognormal_dataset.dat dataset.dat
cp ../workloads/lognormal_query.dat query.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


echo -e "\nUsing dataset file: ../datasets/normal_u64.dat"
cp ../workloads/normal_dataset.dat dataset.dat
cp ../workloads/normal_query.dat query.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


echo -e "\nUsing dataset file: ../datasets/lon_u64.dat"
cp ../workloads/lon_dataset.dat dataset.dat
cp ../workloads/lon_query.dat query.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree