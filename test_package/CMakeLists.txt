cmake_minimum_required(VERSION 3.20)
project(PackageTest CXX)

find_package(turtle_kv CONFIG REQUIRED)


add_executable(example src/example.cpp)
target_link_libraries(example turtle_kv::turtle_kv)
