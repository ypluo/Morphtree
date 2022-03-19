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

void gen_keyset(uint64_t *arr, uint64_t scale, bool uniform) {
    if(uniform) {
        uint64_t step = UINT64_MAX / scale / 2;
        for(uint64_t i = 0; i < scale; i++) {
            arr[i] = i * step + 1;
        }
    } else {
        std::mt19937 gen(utils::rand());
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
        std::sort(arr, arr + scale);
    }
    return ;
}

int main(int argc, char ** argv) {
    uint64_t scale = ycsbc::KEYSET_SCALE_DEFAULT;
    bool opt_uniform = true;

    if(argc > 1 && strcmp(argv[1], "real") == 0) {
        opt_uniform = false;
    }

    uint64_t * arr = new uint64_t[scale];
    if(!utils::file_exist("../datasets/keyset.dat")) {
        remove("../datasets/keyset.dat");
    }
    // generate a ordered keyset
    gen_keyset(arr, scale, opt_uniform);
    // output the keyset into ascii-encoded file
    ofstream fout("../datasets/keyset.dat");
    for(int i = 0; i < scale; i++) {
        fout << arr[i] << endl;
    }
    cout << "Generate a keyset file | scale:" << scale << endl;

    delete [] arr;
    fout.close();
    return 0;
}