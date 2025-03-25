#include "turtle_kv/turtle_kv.hpp"

#include <iostream>

int main()
{
    if (turtle_kv::entry_point()) {
        std::cout << "Test package is working!" << std::endl;
    }
    return 0;
}
