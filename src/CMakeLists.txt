cmake_minimum_required(VERSION 3.20)

cxx_library(turtle_kv)

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

macro (SetFlagsForOptionBool name)
  string(TOUPPER ${name} _name_uppercase)
  if ("${TURTLE_KV_OPTION_${_name_uppercase}}" STREQUAL "True")
    target_compile_definitions(turtle_kv PUBLIC -DTURTLE_KV_${_name_uppercase}=1)
  else ()
    target_compile_definitions(turtle_kv PUBLIC -DTURTLE_KV_${_name_uppercase}=0)
  endif ()
endmacro()

SetFlagsForOptionBool(with_keyvcr)
SetFlagsForOptionBool(use_bloom_filter)
SetFlagsForOptionBool(use_quotient_filter)
SetFlagsForOptionBool(enable_leaf_filters)
SetFlagsForOptionBool(enable_metrics)
SetFlagsForOptionBool(profile_updates)
SetFlagsForOptionBool(profile_queries)

if (("${TURTLE_KV_OPTION_WITH_KEYVCR}" STREQUAL "True") AND (LINUX))
  set(TURTLE_KV_KEYVCR_DEPS keyvcr::keyvcr)
endif ()

if (("${TURTLE_KV_OPTION_USE_QUOTIENT_FILTER}" STREQUAL "True") AND (LINUX))
  set(TURTLE_KV_VQF_DEPS vqf::vqf)
endif ()

#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

target_link_libraries(
  turtle_kv
  PUBLIC
  abseil::abseil
  batteries::batteries
  boost::boost
  llfs::llfs
  yaml-cpp::yaml-cpp
  ${TURTLE_KV_KEYVCR_DEPS}
  ${TURTLE_KV_VQF_DEPS}
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
