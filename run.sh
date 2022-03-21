#!/bin/bash

echo -e "Using dataset file: ../datasets/keyset_u64.dat"
cp ../workloads/keyset_dataset.dat dataset.dat
cp ../workloads/keyset_query.dat query.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


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


echo -e "\nUsing dataset file: ../datasets/lonlat_u64.dat"
cp ../workloads/lonlat_dataset.dat dataset.dat
cp ../workloads/lonlat_query.dat query.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree