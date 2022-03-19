#include <iostream>
#include <fstream>
#include <cstdint>
#include <sys/stat.h>
#include <cmath>

using namespace std;
const uint64_t LIMIT = 128 * 1024 * 1024;

inline bool file_exist(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
}

int main(int argc, char ** argv) {
    char * opt_filename;
    uint64_t ratio = 10000000;

    if(argc < 2 || !file_exist(argv[1])) {
        printf("Specify a valid filename\n");
        exit(-1);
    } else {
        opt_filename = argv[1];
    }
    if(argc == 3) {
        ratio = atoi(argv[2]);
    }

    string filename_out = opt_filename;
    filename_out = filename_out.substr(0, filename_out.rfind(".")) + "_u64.dat";

    // open files
    ifstream fin(opt_filename);
    ofstream fout(filename_out.c_str());
    double key;
    uint64_t count = 0;
    while(true) {
        fin >> key;
        if(!fin.good() || count == LIMIT) break;
        fout << uint64_t(std::abs(key) * ratio) << endl;
        count += 1;
    }

    fin.close();
    fout.close();
    return 0;
}