cmake_minimum_required(VERSION 3.20)
project(turtle_kv CXX)

include(${COR_CMAKE_INCLUDE_DIR}/common.cmake)
include(${CMAKE_BINARY_DIR}/conan_find_requirements.cmake)

# Enable link-time optimization. NOTE: currently breaks arm64 Linux (M1)
#
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)

# The src dir should appear in the include path.
#
set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_INCLUDE_CURRENT_DIR_IN_INTERFACE ON)

# Generate compile_commands.json.
#
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Do not attempt to set/fix rpath for binaries.
#
set(CMAKE_SKIP_INSTALL_RPATH TRUE)

# Turn off terminal color codes in gcc.
#
add_definitions("-fno-diagnostics-color")

# Set options needed for stack tracing.
#
add_compile_options(-D_GNU_SOURCE -fno-omit-frame-pointer)

# Set warning/error level for the compilation.
#
add_compile_options(-Wall -Wextra -Werror -Werror=switch-enum -Wswitch-enum -Wsuggest-override -Woverloaded-virtual)

# Disable -Werror=array-bounds for upgrading to GCC 12, this can be removed
# when the warning is fixed in llfs. See https://github.com/mathworks/llfs/issues/161
#
add_compile_options(-Wno-array-bounds)

add_compile_definitions(BOOST_ERROR_CODE_HEADER_ONLY)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer")

set(CMAKE_CXX_FLAGS_RELEASE  "-O3 -DNDEBUG -march=native")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -march=native -mbmi2 -mavx2 -msse2 -msse4.2 -Wno-address-of-packed-member")
set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer")

add_subdirectory(src)
add_subdirectory(bench)
