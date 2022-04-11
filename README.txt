部分运行命令记载:

../gen_dataset.sh lognormal;../gen_dataset.sh normal;../gen_dataset.sh lon;

../gen_workload.sh lognormal;../gen_workload.sh normal;../gen_workload.sh lon

../run.sh lognormal | tee lognormal.txt

../run.sh normal | tee normal.txt

../run.sh lon | tee lon.txt