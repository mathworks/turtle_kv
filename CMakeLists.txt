cmake_minimum_required(VERSION 3.20)
project(turtle_kv CXX)

include(${COR_CMAKE_INCLUDE_DIR}/common.cmake)
include(${CMAKE_BINARY_DIR}/conan_find_requirements.cmake)

add_subdirectory(src)
