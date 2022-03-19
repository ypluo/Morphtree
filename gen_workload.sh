#!/bin/bash

# getopt
ARGS=`getopt -o R:O:r:w:d:k: \
             --long record_count:,operation_count:,read_ratio:,write_ratio:,distribution:,keyset: \
             -n "run.sh"\
             -- "$@"`
if [ $? != 0 ] ; then 
    echo "Terminating..." >&2 ; exit 1 ;
fi
eval set -- "$ARGS"

# default workload specifications
RECORD=4000000
OPERATION=4000000
READ=1
WRITE=0
DIST=uniform
KEYSET=../datasets/normal_u64.dat

# parse options
while true ; do
    case "$1" in
        -R|--record_count)      RECORD=$2;    shift 2 ;;
        -O|--operation_count)   OPERATION=$2; shift 2 ;;
        -r|--read_ratio)        READ=$2;      shift 2 ;;
        -w|--write_ratio)       WRITE=$2;     shift 2 ;;
        -d|--distribution)      DIST=$2;      shift 2 ;;
        -k|--keyset)            KEYSET=$2;     shift 2 ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

BINARY_DIR=.
YCSB_BIN=${BINARY_DIR}/ycsb/ycsbc

# generate datatset
echo "recordcount=0"                 > workload.spec
echo "operationcount=${RECORD}"      >> workload.spec
echo "readproportion=0"              >> workload.spec
echo "updateproportion=0"            >> workload.spec
echo "scanproportion=0"              >> workload.spec
echo "insertproportion=1"            >> workload.spec
echo "requestdistribution=${DIST}"   >> workload.spec
echo "skewness=0.9"                  >> workload.spec
${YCSB_BIN} -P workload.spec -F ${KEYSET} > ${BINARY_DIR}/dataset.dat

# generate workload
echo "recordcount=${RECORD}"         > workload.spec
echo "operationcount=${OPERATION}"   >> workload.spec
echo "readproportion=${READ}"        >> workload.spec
echo "updateproportion=0"            >> workload.spec
echo "scanproportion=0"              >> workload.spec
echo "insertproportion=${WRITE}"     >> workload.spec
echo "requestdistribution=${DIST}"   >> workload.spec
echo "skewness=0.9"                  >> workload.spec
${YCSB_BIN} -P workload.spec -F ${KEYSET} > ${BINARY_DIR}/query.dat