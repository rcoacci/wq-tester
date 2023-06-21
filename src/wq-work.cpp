#include <cstdio>
#include <cstring>
#include <random>
#include <unistd.h>
#include <fstream>
using namespace std;

int main(int argc, char *argv[]) {

    char* infile = strdup(argv[1]);
    long in_size = atol(argv[2]);
    char* outfile = strdup(argv[3]);
    long out_size = atol(argv[4]);
    double runtime = atoi(argv[5]);
    double chance = atof(argv[6]);
    if(argc<7) {
        printf("Wrong number of arguments!\n");
        exit(-1);
    }
    if (out_size>in_size) out_size=in_size;
    default_random_engine e1(random_device{}());
    uniform_real_distribution<> dist(0., 1.);
    if(dist(e1) <= chance){
        runtime *=2.5;
    }
    char* buffer = new char[in_size * 1024 * 1024];
    fstream in (infile, ios::binary|ios::in);
    printf("[Worker] Reading %ld MB from %s\n", in_size, infile);
    in.read(buffer, in_size*1024*1024);
    if(in.gcount() != in_size*1024*1024) printf("Error reading input! Bytes read: %ld\n", in.gcount());
    printf("[Worker] Processing for %.2f min\n", runtime);
    fflush(stdout);
    sleep(int(runtime*60));
    printf("[Worker] Writing %ld MB to %s\n", out_size, outfile);
    fstream out (outfile, ios::binary|ios::out);
    out.write(buffer, out_size*1024*1024);
    delete[] buffer;
    printf("[Worker] Done.\n");
    return 0;
}
