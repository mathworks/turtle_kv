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
#include <turtle_kv/kv_store_scanner.hpp>

#include <batteries/status.hpp>

#include <glog/logging.h>

#include <stdlib.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

using namespace batt::int_types;

using batt::None;
using batt::Optional;

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
turtlekv_StrView turtlekv_status_get_message(turtlekv_Status status)
{
  BATT_CHECK_EQ(batt::OkStatus().code(), 0);

  const std::string_view message =
      batt::Status{static_cast<batt::StatusCode>(status.code)}.message();

  return turtlekv_StrView{
      .data = message.data(),
      .size = message.size(),
  };
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

  CustomLogHandler(const CustomLogHandler&) = delete;
  CustomLogHandler& operator=(const CustomLogHandler&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  turtlekv_LogHandler handler_fn_;
  void* handler_data_;
};

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 */
extern "C" {
struct turtlekv_Runtime_ {
  batt::Status status;
  std::mutex mutex_;
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
      .mutex_ = {},
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
  turtlekv_StatusOrRuntime result{
      .status = turtlekv_ok_status(),
      .runtime = global_runtime(),
  };

  // If this is the first time initialize has been called, initialize GLOG.
  //
  if (result.runtime->status == batt::StatusCode::kUnknown) {
    google::InitGoogleLogging("turtlekv");
  }

  result.runtime->status = batt::OkStatus();

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_runtime_shutdown(turtlekv_Runtime* runtime)
{
  BATT_CHECK_EQ(runtime, global_runtime());
  runtime->status = batt::StatusCode::kClosed;

  std::unordered_map<size_t, std::unique_ptr<CustomLogHandler>> local_log_handlers;
  {
    std::scoped_lock<std::mutex> lock{runtime->mutex_};

    local_log_handlers = std::move(runtime->attached_log_handlers);
    runtime->attached_log_handlers.clear();
  }

  for (const auto& [id, p_sink] : local_log_handlers) {
    google::RemoveLogSink(p_sink.get());
  }
}

/** \brief Panics if the passed runtime is not the global singleton, or if it is not in an 'ok'
 * state.
 */
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

  auto unique_sink = std::make_unique<CustomLogHandler>(handler, handler_data);
  CustomLogHandler* sink = unique_sink.get();
  {
    std::scoped_lock<std::mutex> lock{runtime->mutex_};

    runtime->attached_log_handlers.emplace(attachment.id, std::move(unique_sink));
  }

  google::AddLogSink(sink);

  return attachment;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_runtime_detach_log_handler(turtlekv_Runtime* runtime,
                                         turtlekv_LogHandlerAttachment attachment)
{
  TURTLEKV_CHECK_RUNTIME(runtime);

  CustomLogHandler* sink = nullptr;
  {
    std::scoped_lock<std::mutex> lock{runtime->mutex_};

    auto iter = runtime->attached_log_handlers.find(attachment.id);
    if (iter != runtime->attached_log_handlers.end()) {
      sink = iter->second.get();
      runtime->attached_log_handlers.erase(iter);
    }
  }

  google::RemoveLogSink(sink);
}

/*=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 */
extern "C" {
// This struct must be defined privately since it contains C++-only types.
//
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
size_t turtlekv_kv_store_get_num_options()
{
  return turtle_kv::num_config_params();
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
const char* turtlekv_kv_store_option_name_for_index(size_t index)
{
  const turtle_kv::ConfigParam* param = turtle_kv::get_config_param_for_index(index);
  if (param == nullptr) {
    return nullptr;
  }

  return param->name().c_str();
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_kv_store_option_index_for_name(const char* name)
{
  turtlekv_StatusOrUInt64 result;

  const auto status = [&]() -> batt::Status {
    BATT_ASSIGN_OK_RESULT(result.n, turtle_kv::get_index_for_config_param_name(name));
    return batt::OkStatus();
  }();

  result.status = turtlekv_status_from(status);

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_kv_store_options_format(const turtlekv_KVStoreOptions* options,
                                                         size_t index,
                                                         char* value_buffer,
                                                         size_t value_buffer_size)
{
  turtlekv_StatusOrUInt64 result;

  const turtle_kv::ConfigParam* param = turtle_kv::get_config_param_for_index(index);

  if (param == nullptr) {
    result.status = turtlekv_status_from(batt::Status{batt::StatusCode::kNotFound});
    return result;
  }

  batt::StatusOr<std::string> formatted_str =
      param->string_value(&options->config, &options->runtime_options);

  result.status = turtlekv_status_from(formatted_str.status());

  if (!formatted_str.ok()) {
    return result;
  }

  result.n = std::min<usize>(value_buffer_size, formatted_str->size());
  std::memcpy(value_buffer, formatted_str->data(), result.n);

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_parse(turtlekv_KVStoreOptions* options,
                                                const char* name,
                                                const char* value)
{
  batt::StatusOr<usize> index = turtle_kv::get_index_for_config_param_name(name);
  if (!index.ok()) {
    return turtlekv_status_from(index.status());
  }

  const turtle_kv::ConfigParam* param = turtle_kv::get_config_param_for_index(*index);
  if (param == nullptr) {
    return turtlekv_status_from(batt::Status{batt::StatusCode::kNotFound});
  }

  return turtlekv_status_from(
      param->parse_and_set(std::string_view{value}, &options->config, &options->runtime_options));
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrConfigOptionType turtlekv_get_config_option_type(const char* name)
{
  turtlekv_StatusOrConfigOptionType result;

  const auto status = [&]() -> batt::Status {
    BATT_ASSIGN_OK_RESULT(usize index, turtle_kv::get_index_for_config_param_name(name));

    const turtle_kv::ConfigParam* param = turtle_kv::get_config_param_for_index(index);
    BATT_CHECK_NOT_NULLPTR(param);

    result.type_ = batt::case_of(
        param->value_type_id(),
        [](batt::StaticType<std::string>) {
          return TURTLEKV_CONFIG_OPTION_STRING;
        },
        [](batt::StaticType<i64>) {
          return TURTLEKV_CONFIG_OPTION_INT64;
        },
        [](batt::StaticType<u64>) {
          return TURTLEKV_CONFIG_OPTION_UINT64;
        },
        [](batt::StaticType<double>) {
          return TURTLEKV_CONFIG_OPTION_DOUBLE;
        },
        [](batt::StaticType<bool>) {
          return TURTLEKV_CONFIG_OPTION_BOOL;
        });

    return batt::OkStatus();
  }();

  result.status = turtlekv_status_from(status);

  return result;
}

namespace {

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void set_result(turtlekv_StatusOrStrView& dst, const std::string_view& src)
{
  dst.str.data = src.data();
  dst.str.size = src.size();
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void set_result(turtlekv_StatusOrInt64& dst, i64 i)
{
  dst.i = i;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void set_result(turtlekv_StatusOrUInt64& dst, u64 n)
{
  dst.n = n;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void set_result(turtlekv_StatusOrDouble& dst, double d)
{
  dst.d = d;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void set_result(turtlekv_StatusOrBool& dst, bool b)
{
  dst.b = b;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
template <typename ReturnT, typename ValueT>
ReturnT get_option_impl(const turtlekv_KVStoreOptions* options,
                        const char* name,
                        ValueT default_value)
{
  ReturnT result;

  const auto status = [&]() -> batt::Status {
    BATT_ASSIGN_OK_RESULT(usize index, turtle_kv::get_index_for_config_param_name(name));

    const turtle_kv::ConfigParam* param = turtle_kv::get_config_param_for_index(index);
    BATT_CHECK_NOT_NULLPTR(param);

    batt::Status local_status = batt::OkStatus();

    set_result(result,
               batt::case_of(
                   param->typed_value(&options->config, &options->runtime_options),
                   [](ValueT b) {
                     return b;
                   },
                   [&local_status, &default_value](auto) {
                     local_status = batt::StatusCode::kInvalidArgument;
                     return default_value;
                   }));

    return local_status;
  }();

  result.status = turtlekv_status_from(status);

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
template <typename ValueT>
turtlekv_Status set_option_impl(turtlekv_KVStoreOptions* options, const char* name, ValueT value)
{
  const auto status = [&]() -> batt::Status {
    BATT_ASSIGN_OK_RESULT(usize index, turtle_kv::get_index_for_config_param_name(name));

    const turtle_kv::ConfigParam* param = turtle_kv::get_config_param_for_index(index);
    BATT_CHECK_NOT_NULLPTR(param);

    return param->set(value, &options->config, &options->runtime_options);
  }();

  return turtlekv_status_from(status);
}

}  // namespace

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrInt64 turtlekv_kv_store_options_get_int64(const turtlekv_KVStoreOptions* options,
                                                           const char* name)
{
  return get_option_impl<turtlekv_StatusOrInt64, i64>(options, name, 0);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_int64(turtlekv_KVStoreOptions* options,
                                                    const char* name,
                                                    i64 value)
{
  return set_option_impl<i64>(options, name, value);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_kv_store_options_get_uint64(const turtlekv_KVStoreOptions* options,
                                                             const char* name)
{
  return get_option_impl<turtlekv_StatusOrUInt64, u64>(options, name, 0);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_uint64(turtlekv_KVStoreOptions* options,
                                                     const char* name,
                                                     u64 value)
{
  return set_option_impl<u64>(options, name, value);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrBool turtlekv_kv_store_options_get_bool(const turtlekv_KVStoreOptions* options,
                                                         const char* name)
{
  return get_option_impl<turtlekv_StatusOrBool, bool>(options, name, false);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_bool(turtlekv_KVStoreOptions* options,
                                                   const char* name,
                                                   bool value)
{
  return set_option_impl<bool>(options, name, value);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrDouble turtlekv_kv_store_options_get_double(const turtlekv_KVStoreOptions* options,
                                                             const char* name)
{
  return get_option_impl<turtlekv_StatusOrDouble, double>(options, name, 0);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_double(turtlekv_KVStoreOptions* options,
                                                     const char* name,
                                                     double value)
{
  return set_option_impl<double>(options, name, value);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_str(turtlekv_KVStoreOptions* options,
                                                  const char* name,
                                                  const char* value)
{
  const std::string_view view{value};

  const turtlekv_StrView str{
      .data = view.data(),
      .size = view.size(),
  };

  return turtlekv_kv_store_options_set_str_view(options, name, str);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_options_set_str_view(turtlekv_KVStoreOptions* options,
                                                       const char* name,
                                                       turtlekv_StrView value)
{
  return set_option_impl<std::string>(options, name, std::string{value.data, value.size});
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrStrView turtlekv_kv_store_options_get_str_view(
    const turtlekv_KVStoreOptions* options,
    const char* name)
{
  return get_option_impl<turtlekv_StatusOrStrView, std::string>(options, name, "");
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

namespace {

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtle_kv::KVStore* to_kv_store(turtlekv_KVStore* ptr) noexcept
{
  return reinterpret_cast<turtle_kv::KVStore*>(ptr);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
const turtle_kv::KVStore* to_kv_store(const turtlekv_KVStore* ptr) noexcept
{
  return reinterpret_cast<const turtle_kv::KVStore*>(ptr);
}

}  // namespace

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
  std::unique_ptr<turtle_kv::KVStore> local_kv_store{to_kv_store(kv_store)};
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
size_t turtlekv_kv_store_get_checkpoint_distance(const turtlekv_KVStore* kv_store)
{
  return to_kv_store(kv_store)->get_checkpoint_distance();
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_kv_store_set_checkpoint_distance(turtlekv_KVStore* kv_store,
                                               size_t checkpoint_distance)
{
  to_kv_store(kv_store)->set_checkpoint_distance(checkpoint_distance);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
extern "C" {
struct turtlekv_KVStoreContext_ {
  turtle_kv::KVStore* kv_store;
  turtle_kv::KVStore::ThreadContext thread_context;
  Optional<turtle_kv::ValueView> last_value;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit turtlekv_KVStoreContext_(turtlekv_KVStore* ptr) noexcept
      : kv_store{to_kv_store(ptr)}
      , thread_context{this->kv_store}
      , last_value{None}
  {
  }

  turtlekv_KVStoreContext_(const turtlekv_KVStoreContext_&) = delete;
  turtlekv_KVStoreContext_& operator=(const turtlekv_KVStoreContext_&) = delete;
};
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_KVStoreContext* turtlekv_kv_store_create_context(turtlekv_KVStore* kv_store)
{
  return new turtlekv_KVStoreContext{kv_store};
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_kv_store_destroy_context(turtlekv_KVStoreContext* context)
{
  BATT_CHECK_NOT_NULLPTR(context);

  delete context;
}

namespace {

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtle_kv::KVStore::WriteOptions to_write_options(const turtlekv_WriteOptions& from)
{
  return turtle_kv::KVStore::WriteOptions{
      .sync = from.sync,
      .urgent_sync = from.urgent,
  };
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtle_kv::KeyView to_key(const turtlekv_KeyView& from)
{
  return {from.data, from.size};
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtle_kv::ValueView to_put_value(const turtlekv_ValueView& from)
{
  switch (from.kind) {
    case TURTLEKV_VALUE_KIND_PUT:
      return turtle_kv::ValueView::from_str(std::string_view{from.data, from.size});

    case TURTLEKV_VALUE_KIND_DELETE:
      return turtle_kv::ValueView::deleted();

    default:
      break;
  }

  BATT_PANIC() << "unhandled case!" << (int)from.kind;
  BATT_UNREACHABLE();
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrValueView from_value(const turtle_kv::ValueView& from)
{
  turtlekv_StatusOrValueView result;

  result.status = turtlekv_ok_status();

  switch (from.op()) {
    case turtle_kv::ValueView::OpCode::OP_DELETE:
      result.value.kind = TURTLEKV_VALUE_KIND_DELETE;
      result.value.data = nullptr;
      result.value.size = 0;
      break;

    case turtle_kv::ValueView::OpCode::OP_WRITE:
      result.value.kind = TURTLEKV_VALUE_KIND_PUT;
      result.value.data = from.data();
      result.value.size = from.size();
      break;

    case turtle_kv::ValueView::OpCode::OP_NOOP:
    case turtle_kv::ValueView::OpCode::OP_ADD_I32:
    case turtle_kv::ValueView::OpCode::OP_PAGE_SLICE:
    case turtle_kv::ValueView::OpCode::BEGIN_OP_UNDEFINED:
    case turtle_kv::ValueView::OpCode::END_OP_UNDEFINED:
    default:
      result.status = turtlekv_status_from(TURTLEKV_STATUS_CODE_UNIMPLEMENTED);
      break;
  }

  return result;
}

}  // namespace

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_sync(turtlekv_KVStoreContext* context,
                                       i64 edit_offset,
                                       const turtlekv_WriteOptions* opt)
{
  Optional<turtle_kv::KVStore::WriteOptions> write_options;
  if (opt) {
    write_options.emplace(to_write_options(*opt));
  }

  return turtlekv_status_from(
      context->kv_store->sync(turtle_kv::EditOffset{edit_offset}, write_options));
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_sync_all(turtlekv_KVStoreContext* context, bool urgent)
{
  return turtlekv_status_from(context->kv_store->sync(/*upper_bound=*/None,
                                                      turtle_kv::KVStore::WriteOptions{
                                                          .sync = true,
                                                          .urgent_sync = urgent,
                                                      }));
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_put(turtlekv_KVStoreContext* context,
                                      turtlekv_KeyView key,
                                      turtlekv_ValueView value)
{
  return turtlekv_status_from(
      context->kv_store->put(context->thread_context, to_key(key), to_put_value(value)));
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrInt64 turtlekv_kv_store_put_opt(turtlekv_KVStoreContext* context,
                                                 turtlekv_KeyView key,
                                                 turtlekv_ValueView value,
                                                 const turtlekv_WriteOptions* opt)
{
  turtlekv_StatusOrInt64 result;

  if (opt == nullptr) {
    result.status = turtlekv_status_from(TURTLEKV_STATUS_CODE_INVALID_ARGUMENT);
    return result;
  }

  batt::StatusOr<turtle_kv::EditOffset> put_result = context->kv_store->put(context->thread_context,
                                                                            to_key(key),
                                                                            to_put_value(value),
                                                                            to_write_options(*opt));

  result.status = turtlekv_status_from(put_result.status());

  if (!put_result.ok()) {
    return result;
  }

  result.i = put_result->value();

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_remove(turtlekv_KVStoreContext* context, turtlekv_KeyView key)
{
  return turtlekv_status_from(context->kv_store->remove(context->thread_context, to_key(key)));
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrInt64 turtlekv_kv_store_remove_opt(turtlekv_KVStoreContext* context,
                                                    turtlekv_KeyView key,
                                                    const turtlekv_WriteOptions* opt)
{
  static const turtlekv_ValueView tombstone{
      .kind = TURTLEKV_VALUE_KIND_DELETE,
      .data = nullptr,
      .size = 0,
  };

  return turtlekv_kv_store_put_opt(context, key, tombstone, opt);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrValueView turtlekv_kv_store_get(turtlekv_KVStoreContext* context,
                                                 turtlekv_KeyView key)
{
  turtlekv_StatusOrValueView result;

  batt::StatusOr<turtle_kv::ValueView> value =
      context->kv_store->get(context->thread_context, to_key(key));

  result.status = turtlekv_status_from(value.status());
  if (value.ok()) {
    context->last_value.emplace(*value);
    result = from_value(*context->last_value);
  }

  return result;
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_put_simple(turtlekv_KVStoreContext* context,
                                             const char* key,
                                             const char* value)
{
  return turtlekv_kv_store_put(context,
                               turtlekv_KeyView{
                                   .data = key,
                                   .size = strlen(key),
                               },
                               turtlekv_ValueView{
                                   .kind = TURTLEKV_VALUE_KIND_PUT,
                                   .data = value,
                                   .size = strlen(value),
                               });
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_remove_simple(turtlekv_KVStoreContext* context, const char* key)
{
  return turtlekv_kv_store_remove(context,
                                  turtlekv_KeyView{
                                      .data = key,
                                      .size = strlen(key),
                                  });
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
const char* turtlekv_kv_store_get_simple(turtlekv_KVStoreContext* context, const char* key)
{
  turtlekv_StatusOrValueView result = turtlekv_kv_store_get(context,
                                                            turtlekv_KeyView{
                                                                .data = key,
                                                                .size = strlen(key),
                                                            });

  if (!turtlekv_status_is_ok(result.status) || result.value.kind != TURTLEKV_VALUE_KIND_PUT) {
    return nullptr;
  }

  thread_local batt::SmallVec<char, 64> value_str_buffer;

  if (value_str_buffer.size() < result.value.size + 1) {
    value_str_buffer.resize(result.value.size + 1);
  }

  memcpy(value_str_buffer.data(), result.value.data, result.value.size);
  value_str_buffer[result.value.size] = '\0';

  return value_str_buffer.data();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
extern "C" {
struct turtlekv_KVStoreScanner_ {
  turtlekv_KVStoreContext* context;
  turtle_kv::KVStoreScanner scanner;
};
}

namespace {

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
bool from_item(turtlekv_KVStoreContext* context,
               const Optional<turtle_kv::EditView>& edit,
               turtlekv_ItemView* item_out)
{
  if (!edit) {
    return false;
  }

  context->last_value = edit->value;
  turtlekv_StatusOrValueView value_out = from_value(*context->last_value);
  if (!turtlekv_status_is_ok(value_out.status)) {
    return false;
  }

  item_out->key.data = edit->key.data();
  item_out->key.size = edit->key.size();
  item_out->value = value_out.value;

  return true;
}

}  // namespace

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_KVStoreScanner* turtlekv_kv_store_create_scanner(turtlekv_KVStoreContext* context,
                                                          turtlekv_KeyView min_key)
{
  return new turtlekv_KVStoreScanner{
      .context = context,
      .scanner =
          turtle_kv::KVStoreScanner{
              *context->kv_store,
              context->thread_context,
              to_key(min_key),
          },
  };
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
void turtlekv_kv_store_destroy_scanner(turtlekv_KVStoreScanner* ptr)
{
  if (ptr) {
    delete ptr;
  }
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_scanner_start(turtlekv_KVStoreScanner* ptr)
{
  return turtlekv_status_from(ptr->scanner.start());
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
bool turtlekv_kv_store_scanner_peek(turtlekv_KVStoreScanner* ptr, turtlekv_ItemView* item_out)
{
  return from_item(ptr->context, ptr->scanner.peek(), item_out);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
bool turtlekv_kv_store_scanner_next(turtlekv_KVStoreScanner* ptr, turtlekv_ItemView* item_out)
{
  return from_item(ptr->context, ptr->scanner.next(), item_out);
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_Status turtlekv_kv_store_scanner_status(turtlekv_KVStoreScanner* ptr)
{
  return turtlekv_status_from(ptr->scanner.status());
}

/*==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 */
turtlekv_StatusOrUInt64 turtlekv_kv_store_scanner_read(turtlekv_KVStoreScanner* ptr,
                                                       turtlekv_ItemView* item_buffer,
                                                       size_t max_items)
{
  turtlekv_StatusOrUInt64 result{
      .status = turtlekv_status_from(ptr->scanner.status()),
      .n = 0,
  };

  if (!turtlekv_status_is_ok(result.status)) {
    return result;
  }

  while (max_items != 0) {
    if (!from_item(ptr->context, ptr->scanner.next(), item_buffer)) {
      result.status = turtlekv_status_from(ptr->scanner.status());
      break;
    }

    ++item_buffer;
    ++result.n;
    --max_items;
  }

  return result;
}
