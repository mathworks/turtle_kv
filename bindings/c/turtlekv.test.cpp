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
  char buffer[TURTLEKV_STATUS_MESSAGE_MAX_SIZE];
  turtlekv_StatusOrUInt64 result;
  turtlekv_Status status;

  //----- --- -- -  -  -   -
  const auto verify_case = [&](turtlekv_StatusCode code, const char* expect_message) {
    memset(buffer, 0, sizeof(buffer));
    status.code = code;
    result = turtlekv_status_get_message(status, buffer, sizeof(buffer));
    EXPECT_THAT((const char*)buffer, ::testing::StrEq(expect_message));
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
}

}  // namespace
