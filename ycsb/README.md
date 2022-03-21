### YCSB-Gen

This is a YCSB workload generator. It is modified from project [YCSB-C](https://github.com/basicthinker/YCSB-C). 

We strip functionalities other than generating dataset and query workload from default key space. Besides, we add support for user-defined key space, which is very helpful in testing dataset-sensitive indices or key value stores.

#### Usage

To build YCSB-Gen on ubuntu, for example:

```sh
mkdir build
cd build & make
```

To generate a dataset and workload file from default key space 

```
./ycsbc -P workloads/workloada.spec
```

To generate a dataset and workload file from given key space

```
./ycsbc -P workloads/workloada.spec -F <your keyset file>
```