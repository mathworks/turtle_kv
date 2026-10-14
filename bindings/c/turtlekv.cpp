//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include "turtlekv.h"
//

#include <turtle_kv/kv_store.hpp>
#include <turtle_kv/kv_store_config.hpp>

#include <batteries/status.hpp>

#include <glog/logging.h>

#include <stdlib.h>

#include <atomic>
#include <memory>
#include <unordered_map>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using turtle_kv::KVStoreConfig;
using turtle_kv::KVStoreRuntimeOptions;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

namespace {

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_status_from(turtlekv_StatusCode code)
{
  turtlekv_Status status{.code = code};
  return status;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_ok_status()
{
  return turtlekv_status_from(TURTLEKV_STATUS_CODE_OK);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_status_from(batt::Status status)
{
  static const batt::Status::CodeGroup* batt_code_group =
      std::addressof(batt::Status::code_group_for_type<batt::StatusCode>());

  if (status.group_pointer() != batt_code_group) {
    if (status.ok()) {
      return turtlekv_ok_status();
    }
    return turtlekv_status_from(TURTLEKV_STATUS_CODE_INTERNAL);
  }

  return turtlekv_status_from(static_cast<turtlekv_StatusCode>(status.code()));
}

}  // namespace

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_status_get_message(turtlekv_Status status,
                                                    char* buffer,
                                                    size_t buffer_size)
{
  turtlekv_StatusOrUInt64 result{
      .status = turtlekv_ok_status(),
      .n = 0,
  };

  BATT_CHECK_EQ(batt::OkStatus().code(), 0);

  const std::string_view message =
      batt::Status{static_cast<batt::StatusCode>(status.code)}.message();

  result.n = message.size() + 1;

  if (message.size() + 1 > buffer_size) {
    result.status.code = TURTLEKV_STATUS_CODE_OUT_OF_RANGE;
    return result;
  }

  memcpy(buffer, message.data(), message.size());
  buffer[message.size()] = '\0';

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
bool turtlekv_status_is_ok(turtlekv_Status status)
{
  return status.code == TURTLEKV_STATUS_CODE_OK;
}

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 */
class CustomLogHandler : public google::LogSink
{
 public:
  explicit CustomLogHandler(turtlekv_LogHandler handler_fn, void* handler_data) noexcept
      : handler_fn_{handler_fn}
      , handler_data_{handler_data}
  {
  }

  void send(google::LogSeverity severity,
            const char* full_filename,
            const char* base_filename,
            int line,
            const google::LogMessageTime& t,
            const char* message,
            std::size_t message_len) override
  {
    static const std::unordered_map<int, const char*> kSeverityName = {
        {google::GLOG_INFO, "info"},
        {google::GLOG_WARNING, "warning"},
        {google::GLOG_ERROR, "error"},
        {google::GLOG_FATAL, "fatal"},
    };

    auto it = kSeverityName.find(static_cast<int>(severity));

    turtlekv_LogEvent event;

    event.level = (it == kSeverityName.end()) ? "??" : it->second;
    event.filename = full_filename;
    event.basename = base_filename;
    event.line = line;
    event.timestamp = ::mktime((struct tm*)std::addressof(t.tm()));
    event.time_usec = t.usec();
    event.message = message;
    event.message_size = message_len;

    this->handler_fn_(this->handler_data_, event);
  }

 private:
  turtlekv_LogHandler handler_fn_;
  void* handler_data_;
};

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 */
extern "C" {
struct turtlekv_Runtime_ {
  batt::Status status;
  std::atomic<size_t> next_id;
  std::unordered_map<size_t, std::unique_ptr<CustomLogHandler>> attached_log_handlers;
};
}

namespace {
/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Runtime* global_runtime()
{
  static turtlekv_Runtime instance_{
      .status = batt::StatusCode::kUnknown,
      .next_id = {0},
      .attached_log_handlers = {},
  };
  return &instance_;
}
}  // namespace

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrRuntime turtlekv_runtime_initialize()
{
  google::InitGoogleLogging("turtlekv");

  turtlekv_StatusOrRuntime result{
      .status = turtlekv_ok_status(),
      .runtime = global_runtime(),
  };

  result.runtime->status = batt::OkStatus();

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_runtime_shutdown(turtlekv_Runtime* runtime)
{
  BATT_CHECK_EQ(runtime, global_runtime());
  runtime->status = batt::StatusCode::kClosed;

  for (const auto& [id, p_sink] : runtime->attached_log_handlers) {
    google::RemoveLogSink(p_sink.get());
  }

  runtime->attached_log_handlers.clear();
}

#define TURTLEKV_CHECK_RUNTIME(runtime)                                                            \
  BATT_CHECK_EQ(runtime, global_runtime());                                                        \
  BATT_CHECK(runtime->status.ok())

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_LogHandlerAttachment turtlekv_runtime_attach_log_handler(turtlekv_Runtime* runtime,
                                                                  turtlekv_LogHandler handler,
                                                                  void* handler_data)
{
  TURTLEKV_CHECK_RUNTIME(runtime);

  turtlekv_LogHandlerAttachment attachment{
      .id = runtime->next_id.fetch_add(1),
  };

  auto iter = runtime->attached_log_handlers
                  .emplace(attachment.id, std::make_unique<CustomLogHandler>(handler, handler_data))
                  .first;

  google::AddLogSink(iter->second.get());

  return attachment;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_runtime_detach_log_handler(turtlekv_Runtime* runtime,
                                         turtlekv_LogHandlerAttachment attachment)
{
  TURTLEKV_CHECK_RUNTIME(runtime);

  auto iter = runtime->attached_log_handlers.find(attachment.id);
  if (iter == runtime->attached_log_handlers.end()) {
    return;
  }

  google::RemoveLogSink(iter->second.get());

  runtime->attached_log_handlers.erase(iter);
}

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 */
extern "C" {
struct turtlekv_KVStoreOptions_ {
  KVStoreConfig config;
  KVStoreRuntimeOptions runtime_options;
};
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_KVStoreOptions* turtlekv_kv_store_options_create_default()
{
  auto* options = new turtlekv_KVStoreOptions{
      .config = KVStoreConfig::with_default_values(),
      .runtime_options = KVStoreRuntimeOptions::with_default_values(),
  };

  return options;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_kv_store_options_destroy(turtlekv_KVStoreOptions* options)
{
  delete options;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_create(turtlekv_Runtime* runtime,
                                         const char* file_path,
                                         const turtlekv_KVStoreOptions* options,
                                         bool remove_existing)
{
  TURTLEKV_CHECK_RUNTIME(runtime);

  if (file_path == nullptr || options == nullptr) {
    return turtlekv_status_from(TURTLEKV_STATUS_CODE_INVALID_ARGUMENT);
  }

  batt::Status create_status =
      turtle_kv::KVStore::create(std::filesystem::path{file_path},
                                 options->config,
                                 turtle_kv::RemoveExisting{remove_existing});

  return turtlekv_status_from(create_status);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrKVStore turtlekv_kv_store_open(turtlekv_Runtime* runtime,
                                                const char* file_path,
                                                const turtlekv_KVStoreOptions* options)
{
  TURTLEKV_CHECK_RUNTIME(runtime);

  turtlekv_StatusOrKVStore result{
      .status = turtlekv_status_from(TURTLEKV_STATUS_CODE_UNKNOWN),
      .kv_store = nullptr,
  };

  if (file_path == nullptr || options == nullptr) {
    result.status = turtlekv_status_from(TURTLEKV_STATUS_CODE_INVALID_ARGUMENT);
    return result;
  }

  batt::StatusOr<std::unique_ptr<turtle_kv::KVStore>> status_or_kv =
      turtle_kv::KVStore::open(std::filesystem::path{file_path},
                               options->config.tree_options,
                               options->runtime_options);

  if (!status_or_kv.ok()) {
    result.status = turtlekv_status_from(status_or_kv.status());
    return result;
  }

  BATT_CHECK_NOT_NULLPTR(*status_or_kv);

  result.status = turtlekv_ok_status();
  result.kv_store = reinterpret_cast<turtlekv_KVStore*>(status_or_kv->release());

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_kv_store_close(turtlekv_KVStore* kv_store)
{
  // Create a local unique_ptr and just let it go out of scope.
  //
  std::unique_ptr<turtle_kv::KVStore> local_kv_store{
      reinterpret_cast<turtle_kv::KVStore*>(kv_store)};
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
