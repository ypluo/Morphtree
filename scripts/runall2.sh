#!/bin/bash

../scripts/run_dynamic.sh fb 1 > fb.txt
../scripts/run_dynamic.sh fb 2 >> fb.txt

../scripts/run_dynamic.sh lon 1 > lon.txt
../scripts/run_dynamic.sh lon 2 >> lon.txt

../scripts/run_dynamic.sh lonlat 1 > lonlat.txt
../scripts/run_dynamic.sh lonlat 2 >> lonlat.txt

../scripts/run_dynamic.sh lognormal 1 > lognormal.txt
../scripts/run_dynamic.sh lognormal 2 >> lognormal.txt