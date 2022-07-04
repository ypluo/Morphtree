#!/bin/bash

../scripts/run_ratio.sh fb 0    > fb.txt
../scripts/run_ratio.sh fb 0.2  >> fb.txt
../scripts/run_ratio.sh fb 0.4  >> fb.txt
../scripts/run_ratio.sh fb 0.6  >> fb.txt
../scripts/run_ratio.sh fb 0.8  >> fb.txt
../scripts/run_ratio.sh fb 1    >> fb.txt

../scripts/run_ratio.sh lon 0    > lon.txt
../scripts/run_ratio.sh lon 0.2  >> lon.txt
../scripts/run_ratio.sh lon 0.4  >> lon.txt
../scripts/run_ratio.sh lon 0.6  >> lon.txt
../scripts/run_ratio.sh lon 0.8  >> lon.txt
../scripts/run_ratio.sh lon 1    >> lon.txt

../scripts/run_ratio.sh lonlat 0    > lonlat.txt
../scripts/run_ratio.sh lonlat 0.2  >> lonlat.txt
../scripts/run_ratio.sh lonlat 0.4  >> lonlat.txt
../scripts/run_ratio.sh lonlat 0.6  >> lonlat.txt
../scripts/run_ratio.sh lonlat 0.8  >> lonlat.txt
../scripts/run_ratio.sh lonlat 1    >> lonlat.txt

../scripts/run_ratio.sh lognormal 0    > lognormal.txt
../scripts/run_ratio.sh lognormal 0.2  >> lognormal.txt
../scripts/run_ratio.sh lognormal 0.4  >> lognormal.txt
../scripts/run_ratio.sh lognormal 0.6  >> lognormal.txt
../scripts/run_ratio.sh lognormal 0.8  >> lognormal.txt
../scripts/run_ratio.sh lognormal 1    >> lognormal.txt