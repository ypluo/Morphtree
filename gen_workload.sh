#!/bin/bash

WORKLOAD=./ycsb/workloads/workloada.spec

./ycsb/ycsbc load -P ${WORKLOAD} > dataset.dat
./ycsb/ycsbc run -P ${WORKLOAD} > query.dat