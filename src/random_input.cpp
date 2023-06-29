#include <random>
#include <limits>
#include <fstream>
#include <string>
#include <iostream>
#include <cmath>

using namespace std;

int main(int argc, char *argv[]) {

    const auto mega = 1024*1024L;
    int num_files = strtol(argv[1], nullptr, 10);
    double file_size = strtod(argv[2], nullptr);

    default_random_engine e1(random_device{}());
    uniform_int_distribution<unsigned int> dist{};
    auto num_values = file_size*mega/(sizeof(char)*numeric_limits<unsigned int>::digits10+1);
    for(int i=0; i < num_files; ++i){
        auto outfile = string("input")+to_string(i);
        ofstream out (outfile, ios::binary);
        cout << "Generating "<< int(num_values) <<" random integers in input"<< i <<"\n";
        for (int v = 0; v < num_values; v++){
            auto val = dist(e1);
            out << val << "\n";
        }
        out.close();
    }

    return 0;
}
