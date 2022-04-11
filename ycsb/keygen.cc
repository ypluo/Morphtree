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

string WorkloadName[] = {"random", "lognormal", "normal"};
enum WorkloadDist {RANDOM, LOGNORMAL, NORMAL};

void generate(uint64_t *arr, uint64_t scale, WorkloadDist d) {
    std::mt19937 gen(utils::rand());
    if(d == WorkloadDist::RANDOM) {
        for(uint64_t i = 0; i < scale; i++) {
            arr[i] = utils::FNVHash64(i + 1);
        }
    } else if (d == WorkloadDist::LOGNORMAL) {
        std::lognormal_distribution<double> dist(22.123456789, 5.123456789);
        uint64_t i = 0;
        while (i < scale) {
            double val = (uint64_t)dist(gen);
            if(val <= 0 || val > (double) UINT64_MAX) {
                continue;
            } else {
                arr[i++] = (uint64_t)std::round(val);
            }
        }
    } else {
        std::normal_distribution<double> dist(INT64_MAX / 2, INT64_MAX / 8);
        uint64_t i = 0;
        while (i < scale) {
            double val = (uint64_t)dist(gen);
            if(val < 0 || val > (double) UINT64_MAX) {
                continue;
            } else {
                arr[i++] = (uint64_t)std::round(val);
            }
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
    
    uint64_t * arr = new uint64_t[scale];
    string filename = string("../datasets/") + WorkloadName[dist] + ".dat";
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