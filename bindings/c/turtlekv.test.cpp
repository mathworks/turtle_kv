//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include "turtlekv.h"
//
#include "turtlekv.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <batteries/finally.hpp>
#include <batteries/segv.hpp>
#include <batteries/stream_util.hpp>

namespace {

using namespace batt::int_types;

class TurtleKVCBindingEnvironment : public ::testing::Environment
{
 public:
  ~TurtleKVCBindingEnvironment() override
  {
  }

  void SetUp() override
  {
    turtlekv_StatusOrRuntime init = turtlekv_runtime_initialize();
    ASSERT_TRUE(turtlekv_status_is_ok(init.status));

    this->runtime = init.runtime;
  }

  void TearDown() override
  {
    if (this->runtime) {
      turtlekv_runtime_shutdown(this->runtime);
      this->runtime = nullptr;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  turtlekv_Runtime* runtime = nullptr;
};

TurtleKVCBindingEnvironment* const test_env = static_cast<TurtleKVCBindingEnvironment*>(
    testing::AddGlobalTestEnvironment(new TurtleKVCBindingEnvironment));

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
TEST(TurtlekvTest, StatusGetMessage)
{
  turtlekv_Status status;

  //----- --- -- -  -  -   -
  const auto verify_case = [&](turtlekv_StatusCode code, const char* expect_message) {
    status.code = code;
    turtlekv_StrView actual_message = turtlekv_status_get_message(status);
    EXPECT_THAT((std::string_view{
                    actual_message.data,
                    actual_message.size,
                }),
                ::testing::StrEq(expect_message));
  };
  //----- --- -- -  -  -   -

  verify_case(TURTLEKV_STATUS_CODE_OK, "Ok");
  verify_case(TURTLEKV_STATUS_CODE_CANCELLED, "Cancelled");
  verify_case(TURTLEKV_STATUS_CODE_UNKNOWN, "Unknown");
  verify_case(TURTLEKV_STATUS_CODE_INVALID_ARGUMENT, "Invalid Argument");
  verify_case(TURTLEKV_STATUS_CODE_DEADLINE_EXCEEDED, "Deadline Exceeded");
  verify_case(TURTLEKV_STATUS_CODE_NOT_FOUND, "Not Found");
  verify_case(TURTLEKV_STATUS_CODE_ALREADY_EXISTS, "Already Exists");
  verify_case(TURTLEKV_STATUS_CODE_PERMISSION_DENIED, "Permission Denied");
  verify_case(TURTLEKV_STATUS_CODE_RESOURCE_EXHAUSTED, "Resource Exhausted");
  verify_case(TURTLEKV_STATUS_CODE_FAILED_PRECONDITION, "Failed Precondition");
  verify_case(TURTLEKV_STATUS_CODE_ABORTED, "Aborted");
  verify_case(TURTLEKV_STATUS_CODE_OUT_OF_RANGE, "Out of Range");
  verify_case(TURTLEKV_STATUS_CODE_UNIMPLEMENTED, "Unimplemented");
  verify_case(TURTLEKV_STATUS_CODE_INTERNAL, "Internal");
  verify_case(TURTLEKV_STATUS_CODE_UNAVAILABLE, "Unavailable");
  verify_case(TURTLEKV_STATUS_CODE_DATA_LOSS, "Data Loss");
  verify_case(TURTLEKV_STATUS_CODE_UNAUTHENTICATED, "Unauthenticated");
  verify_case(TURTLEKV_STATUS_CODE_CLOSED, "Closed");
  verify_case(TURTLEKV_STATUS_CODE_GRANT_UNAVAILABLE,
              "The requested grant count exceeds available count");
  verify_case(TURTLEKV_STATUS_CODE_LOOP_BREAK, "Loop break");
  verify_case(TURTLEKV_STATUS_CODE_END_OF_STREAM, "End of stream");
  verify_case(TURTLEKV_STATUS_CODE_CLOSED_BEFORE_END_OF_STREAM,
              "The stream was closed before the end of data");
  verify_case(TURTLEKV_STATUS_CODE_GRANT_REVOKED, "The Grant was revoked");
  verify_case(TURTLEKV_STATUS_CODE_POKE, "The observed object was poked");
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
TEST(TurtlekvTest, CreateDestroyOptions)
{
  turtlekv_KVStoreOptions* options = turtlekv_kv_store_options_create_default();

  ASSERT_NE(options, nullptr);

  turtlekv_kv_store_options_destroy(options);
}

extern "C" void test_log_handler(void* self, turtlekv_LogEvent event)
{
  BATT_CHECK_EQ(self, &test_log_handler);

  std::cerr << BATT_INSPECT(event.level) << std::endl
            << BATT_INSPECT(event.filename) << std::endl
            << BATT_INSPECT(event.basename) << std::endl
            << BATT_INSPECT(event.line) << std::endl
            << BATT_INSPECT(event.timestamp) << std::endl
            << BATT_INSPECT(event.time_usec) << std::endl
            << BATT_INSPECT_STR((std::string_view{event.message, event.message_size})) << std::endl
      //
      ;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
TEST(TurtlekvTest, GetSetOptions)
{
  turtlekv_KVStoreOptions* options = turtlekv_kv_store_options_create_default();
  auto destroy_options_on_scope_exit = batt::finally([options] {
    turtlekv_kv_store_options_destroy(options);
  });

  const size_t n_options = turtlekv_kv_store_get_num_options();
  EXPECT_EQ(n_options, 22);

  const auto verify_name = [](size_t i, const char* name) {
    EXPECT_THAT(turtlekv_kv_store_option_name_for_index(i), ::testing::StrEq(name))
        << BATT_INSPECT(i) << BATT_INSPECT_STR(name);

    turtlekv_StatusOrUInt64 result = turtlekv_kv_store_option_index_for_name(name);
    ASSERT_TRUE(turtlekv_status_is_ok(result.status));
    EXPECT_EQ(i, result.n) << BATT_INSPECT(i) << BATT_INSPECT_STR(name);
  };

  const auto verify_type = [](const char* name, turtlekv_ConfigOptionType t) {
    turtlekv_StatusOrConfigOptionType result = turtlekv_get_config_option_type(name);
    ASSERT_TRUE(turtlekv_status_is_ok(result.status));
    EXPECT_EQ(t, result.type_) << BATT_INSPECT_STR(name);
  };

  [[maybe_unused]] const auto expect_is_str = [&verify_type](const char* name) {
    verify_type(name, TURTLEKV_CONFIG_OPTION_STRING);
  };

  [[maybe_unused]] const auto expect_is_u64 = [&verify_type](const char* name) {
    verify_type(name, TURTLEKV_CONFIG_OPTION_UINT64);
  };

  [[maybe_unused]] const auto expect_is_i64 = [&verify_type](const char* name) {
    verify_type(name, TURTLEKV_CONFIG_OPTION_INT64);
  };

  [[maybe_unused]] const auto expect_is_double = [&verify_type](const char* name) {
    verify_type(name, TURTLEKV_CONFIG_OPTION_DOUBLE);
  };

  [[maybe_unused]] const auto expect_is_bool = [&verify_type](const char* name) {
    verify_type(name, TURTLEKV_CONFIG_OPTION_BOOL);
  };

  [[maybe_unused]] const auto verify_i64 = [options](const char* name, i64 expected_value) {
    turtlekv_StatusOrInt64 result = turtlekv_kv_store_options_get_int64(options, name);
    ASSERT_TRUE(turtlekv_status_is_ok(result.status));
    EXPECT_EQ(result.i, expected_value);
  };

  [[maybe_unused]] const auto verify_u64 = [options](const char* name, i64 expected_value) {
    turtlekv_StatusOrUInt64 result = turtlekv_kv_store_options_get_uint64(options, name);
    ASSERT_TRUE(turtlekv_status_is_ok(result.status));
    EXPECT_EQ(result.n, expected_value);
  };

  [[maybe_unused]] const auto verify_bool = [options](const char* name, bool expected_value) {
    turtlekv_StatusOrBool result = turtlekv_kv_store_options_get_bool(options, name);
    ASSERT_TRUE(turtlekv_status_is_ok(result.status));
    EXPECT_EQ(result.b, expected_value);
  };

  const auto verify_parse_format =
      [options](const char* name, bool new_value, const char* value_str) {
        turtlekv_StatusOrUInt64 index = turtlekv_kv_store_option_index_for_name(name);
        ASSERT_TRUE(turtlekv_status_is_ok(index.status));

        turtlekv_Status status = turtlekv_kv_store_options_parse(options, name, value_str);
        ASSERT_TRUE(turtlekv_status_is_ok(status))
            << BATT_INSPECT_STR(name) << BATT_INSPECT_STR(value_str);

        turtlekv_StatusOrBool result_of_get = turtlekv_kv_store_options_get_bool(options, name);
        ASSERT_TRUE(turtlekv_status_is_ok(result_of_get.status));
        EXPECT_EQ(result_of_get.b, new_value);

        char buffer[256];
        turtlekv_StatusOrUInt64 result_of_format =
            turtlekv_kv_store_options_format(options, index.n, buffer, sizeof(buffer));

        ASSERT_TRUE(turtlekv_status_is_ok(result_of_format.status));
        EXPECT_THAT((std::string_view{buffer, result_of_format.n}), ::testing::StrEq(value_str));
      };

  verify_name(0, "b_tree_mode");
  expect_is_bool("b_tree_mode");
  verify_bool("b_tree_mode", false);
  turtlekv_kv_store_options_set_bool(options, "b_tree_mode", true);
  verify_bool("b_tree_mode", true);
  verify_parse_format("b_tree_mode", true, "1");

  verify_name(1, "buffer_level_trim");
  expect_is_u64("buffer_level_trim");
  turtlekv_kv_store_options_set_uint64(options, "buffer_level_trim", 3);
  verify_u64("buffer_level_trim", 3);
  turtlekv_kv_store_options_set_uint64(options, "buffer_level_trim", 1);
  verify_u64("buffer_level_trim", 1);

  verify_name(2, "cache_size_bytes");
  expect_is_u64("cache_size_bytes");

  verify_name(3, "cache_size_mb");
  expect_is_u64("cache_size_mb");

  verify_name(4, "checkpoint_pipeline");
  expect_is_bool("checkpoint_pipeline");

  verify_name(5, "chi");
  expect_is_u64("chi");

  verify_name(6, "filter_bits");
  expect_is_u64("filter_bits");

  verify_name(7, "initial_capacity_bytes");
  expect_is_u64("initial_capacity_bytes");

  verify_name(8, "initial_capacity_gb");
  expect_is_u64("initial_capacity_gb");

  verify_name(10, "leaf_size");
  expect_is_u64("leaf_size");

  verify_name(11, "leaf_size_kb");
  expect_is_u64("leaf_size_kb");

  verify_name(12, "max_capacity_bytes");
  expect_is_u64("max_capacity_bytes");

  verify_name(13, "max_capacity_gb");
  expect_is_u64("max_capacity_gb");

  verify_name(14, "max_flush");
  expect_is_double("max_flush");

  verify_name(15, "min_flush");
  expect_is_double("min_flush");

  verify_name(16, "node_size");
  expect_is_u64("node_size");

  verify_name(17, "node_size_kb");
  expect_is_u64("node_size_kb");

  verify_name(18, "size_tiered");
  expect_is_bool("size_tiered");

  verify_name(19, "value_size_hint");
  expect_is_u64("value_size_hint");

  verify_name(20, "wal_size_bytes");
  expect_is_u64("wal_size_bytes");

  verify_name(21, "wal_size_mb");
  expect_is_u64("wal_size_mb");
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
TEST(TurtlekvTest, CreateAndOpenKVStore)
{
  turtlekv_LogHandlerAttachment attach =
      turtlekv_runtime_attach_log_handler(test_env->runtime,
                                          &test_log_handler,
                                          (void*)&test_log_handler);

  auto on_scope_exit0 = batt::finally([&] {
    turtlekv_runtime_detach_log_handler(test_env->runtime, attach);
  });

  turtlekv_KVStoreOptions* options = turtlekv_kv_store_options_create_default();
  auto on_scope_exit = batt::finally([&] {
    turtlekv_kv_store_options_destroy(options);
  });

  turtlekv_Status create_status = turtlekv_kv_store_create(test_env->runtime,
                                                           "/tmp/libturtlekv_c_test_kv_store",
                                                           options,
                                                           /*remove_existing=*/true);

  ASSERT_TRUE(turtlekv_status_is_ok(create_status));

  turtlekv_StatusOrKVStore open_result =
      turtlekv_kv_store_open(test_env->runtime, "/tmp/libturtlekv_c_test_kv_store", options);

  ASSERT_TRUE(turtlekv_status_is_ok(open_result.status));
  ASSERT_NE(open_result.kv_store, nullptr);

  turtlekv_KVStore* kv_store = open_result.kv_store;
  auto on_scope_exit2 = batt::finally([&] {
    turtlekv_kv_store_close(kv_store);
  });

  turtlekv_KVStoreContext* kv_context = turtlekv_kv_store_create_context(kv_store);
  auto on_scope_exit3 = batt::finally([&] {
    turtlekv_kv_store_destroy_context(kv_context);
  });

  EXPECT_EQ(turtlekv_kv_store_get_simple(kv_context, "hello"), nullptr);
  EXPECT_TRUE(turtlekv_status_is_ok(turtlekv_kv_store_put_simple(kv_context, "hello", "world")));
  EXPECT_THAT(turtlekv_kv_store_get_simple(kv_context, "hello"), ::testing::StrEq("world"));
}

}  // namespace
