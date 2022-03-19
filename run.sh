#!/bin/bash

echo -e "Using dataset file: ../datasets/keyset_u64.dat"
../gen_workload.sh -k ../datasets/keyset_u64.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


echo -e "\nUsing dataset file: ../datasets/lognormal_u64.dat"
../gen_workload.sh -k ../datasets/lognormal_u64.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


echo -e "\nUsing dataset file: ../datasets/normal_u64.dat"
../gen_workload.sh -k ../datasets/normal_u64.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


echo -e "\nUsing dataset file: ../datasets/lon_u64.dat"
../gen_workload.sh -k ../datasets/lon_u64.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree


echo -e "\nUsing dataset file: ../datasets/lonlat_u64.dat"
../gen_workload.sh -k ../datasets/lonlat_u64.dat
taskset -c 1-4 ./benchmark artolc 
taskset -c 1-4 ./benchmark alex
taskset -c 1-4 ./benchmark btree
taskset -c 1-4 ./benchmark wotree
taskset -c 1-4 ./benchmark rotree
taskset -c 1-4 ./benchmark morphtree