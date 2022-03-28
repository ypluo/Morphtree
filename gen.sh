#!/bin/bash

../gen_workload.sh -k ../datasets/lognormal_u64.dat
mv dataset.dat ../workloads/lognormal_dataset.dat
mv query.dat ../workloads/lognormal_query.dat

../gen_workload.sh -k ../datasets/normal_u64.dat
mv dataset.dat ../workloads/normal_dataset.dat
mv query.dat ../workloads/normal_query.dat

../gen_workload.sh -k ../datasets/lon_u64.dat
mv dataset.dat ../workloads/lon_dataset.dat
mv query.dat ../workloads/lon_query.dat