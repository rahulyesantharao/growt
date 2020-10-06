#include <iostream>
#include "flat_hash_map.hpp"

int main(){

    ska::flat_hash_map<size_t, size_t> m(1000 );
    for(int i = 0; i <  100000; i++){
        m.insert(i, i+1);
    }
    m.find(99);
    std::cout << "what is up bois?" << std::endl;
}