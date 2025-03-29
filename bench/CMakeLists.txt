cmake_minimum_required(VERSION 3.20)

file(GLOB Sources
  ./*.cpp
  ./*/*.cpp
  ./*/*/*.cpp
  ./*/*/*/*.cpp
  )

add_executable(turtle_kv_bench ${Sources})

target_link_libraries(
  turtle_kv_bench
  PUBLIC
  turtle_kv
  gtest::gtest
  )
