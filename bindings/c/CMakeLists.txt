cmake_minimum_required(VERSION 3.20)

file(GLOB Sources
  ./turtlekv.cpp
  ./turtlekv_c.c
)

add_library(turtlekv ${Sources})

target_link_libraries(
  turtlekv
  PUBLIC
  turtle_kv
)

file(GLOB TestSources
  turtlekv.test.cpp
)

add_executable(libturtlekv_c_test ${TestSources})

target_link_libraries(
  libturtlekv_c_test
  PRIVATE
  turtlekv
  GTest::gtest_main
  GTest::gtest
  GTest::gmock
  Boost::stacktrace
  backtrace
  dl
)
