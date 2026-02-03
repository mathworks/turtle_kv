cmake_minimum_required(VERSION 3.20)

cxx_library(turtle_kv)

set(PLATFORM_DEPS )

if (LINUX)
  set(PLATFORM_DEPS ${PLATFORM_DEPS}
    keyvcr::keyvcr
    vqf::vqf
  )
endif()

target_link_libraries(
  turtle_kv
  PUBLIC
  abseil::abseil
  batteries::batteries
  boost::boost
  llfs::llfs
  ${PLATFORM_DEPS}
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
  )
