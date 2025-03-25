cmake_minimum_required(VERSION 3.20)

cxx_library(turtle_kv)

target_link_libraries(
  turtle_kv
  PUBLIC
  batteries::batteries
  boost::boost
  llfs::llfs
  #
  # Add other library -> library dependencies here.
  #
  )

target_link_libraries(
  turtle_kv_Test
  PRIVATE gtest::gtest
  #
  # Add other test -> library dependencies here
  #
  )
