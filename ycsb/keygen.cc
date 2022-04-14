/*  
    Copyright(c) 2022 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#include <iostream>
#include <fstream>
#include <cstring>
#include <random>
#include <algorithm>
#include <cassert>

#include "utils.h"
#include "core_workload.h"

using std::cout;
using std::endl;
using std::ofstream;

using _key_t = double;

string WorkloadName[] = {"random", "lognormal", "normal"};
enum WorkloadDist {RANDOM, LOGNORMAL, NORMAL};

void generate(_key_t *arr, uint64_t scale, WorkloadDist d) {
    std::mt19937 gen(utils::rand());
    if(d == WorkloadDist::RANDOM) {
        std::uniform_real_distribution<double> dist(0, 64000000);
        for(uint64_t i = 0; i < scale; i++) {
            arr[i] = utils::FNVHash64(i + 1);
        }
    } else if (d == WorkloadDist::LOGNORMAL) {
        std::lognormal_distribution<double> dist(22.123456789, 5.123456789);
        uint64_t i = 0;
        while (i < scale) {
            arr[i++] = (uint64_t)dist(gen);
        }
    } else {
        std::normal_distribution<double> dist(0, 1000000);
        uint64_t i = 0;
        while (i < scale) {
            arr[i++] = (uint64_t)dist(gen);
        }
    }
    return ;
}

int main(int argc, char ** argv) {
    uint64_t scale = ycsbc::KEYSET_SCALE_DEFAULT;
    WorkloadDist dist;
    if(argc < 2) {
        cout << "please give a dataset distribution: (random, lognormal, normal)" << endl;
        exit(-1);
    }
    if(strcmp(argv[1], "RANDOM") == 0) 
        dist = WorkloadDist::RANDOM;
    else if(strcmp(argv[1], "lognormal") == 0)
        dist = WorkloadDist::LOGNORMAL;
    else if(strcmp(argv[1], "normal") == 0)
        dist = WorkloadDist::NORMAL;
    else 
        dist = WorkloadDist::RANDOM;
    
    _key_t * arr = new _key_t[scale];
    string filename = string("/data/lyp/datasets/") + WorkloadName[dist] + ".dat";
    if(!utils::file_exist(filename.c_str())) {
        remove(filename.c_str());
    }

    // generate dataset file
    generate(arr, scale, dist);

    // output the keyset into ascii-encoded file
    ofstream fout(filename);
    for(int i = 0; i < scale; i++) {
        fout << arr[i] << endl;
    }
    cout << "Generate a keyset file " << filename << "| scale:" << scale << endl;

    delete [] arr;
    fout.close();
    return 0;
}