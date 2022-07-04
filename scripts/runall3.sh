#!/bin/bash

../scripts/run_skew.sh fb 0.4    > fb.txt
../scripts/run_skew.sh fb 0.6   >> fb.txt
../scripts/run_skew.sh fb 0.8   >> fb.txt
../scripts/run_skew.sh fb 0.9   >> fb.txt
../scripts/run_skew.sh fb 0.99  >> fb.txt

../scripts/run_skew.sh lon 0.4    > lon.txt
../scripts/run_skew.sh lon 0.6   >> lon.txt
../scripts/run_skew.sh lon 0.8   >> lon.txt
../scripts/run_skew.sh lon 0.9   >> lon.txt
../scripts/run_skew.sh lon 0.99  >> lon.txt