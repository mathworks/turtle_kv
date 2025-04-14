cmake_minimum_required(VERSION 3.20)

cxx_library(turtle_kv)

target_link_libraries(
  turtle_kv
  PUBLIC
  batteries::batteries
  boost::boost
  llfs::llfs
  vqf::vqf
  gperftools::gperftools
  )

target_link_libraries(
  turtle_kv_Test
  PRIVATE
  GTest::gtest_main
  GTest::gtest
  GTest::gmock
  turtle_kv
  batteries::batteries
  Boost::context
  Boost::stacktrace
  backtrace
  dl
  stdc++fs
  gperftools::gperftools
  )
