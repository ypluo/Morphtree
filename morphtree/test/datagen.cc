/*  
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#include <iostream>
#include <fstream>
#include <cstring>
#include <random>
#include <algorithm>

#include "common.h"

using std::cout;
using std::endl;
using std::ofstream;

void gen_keyset(int64_t *arr, int64_t scale, bool random) {
    std::mt19937 gen(getRandom());
    if(random) {
        uint64_t step = INT64_MAX / scale;
        std::uniform_int_distribution<int64_t> dist(1, INT64_MAX);
        int64_t seed = getRandom();
        for(int64_t i = 0; i < scale; i++) {
            #ifdef DEBUG
                arr[i] = i + 1;
            #else  
                arr[i] = dist(gen);
            #endif
        }
    } else {
        std::normal_distribution<double> dist(INT64_MAX / 2, INT64_MAX / 8);
        int64_t i = 0;
        while (i < scale) {
            double val = (int64_t)dist(gen);
            if(val < 0 || val > (double) INT64_MAX) {
                continue;
            } else {
                arr[i++] = (int64_t)std::round(val);
            }
        }
    }
    return ;
}

int main(int argc, char ** argv) {
    uint64_t scale = 64 * MILLION;

    bool opt_random = true;
    if(argc > 1 && strcmp(argv[1], "-s") == 0) {
        opt_random = false;
    }

    int64_t * arr = new int64_t[scale];
    if(!fileExist("keyset.dat")) {
        remove("keyset.dat");
    }

    gen_keyset(arr, scale, opt_random);
    ofstream fout("keyset.dat", std::ios::binary);
    fout.write((char *) arr, sizeof(int64_t) * scale);
    cout << "generate a keyset file successfully" << endl;

    delete [] arr;
    fout.close();

    return 0;
}