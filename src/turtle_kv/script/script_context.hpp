//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_SCRIPT_CONTEXT_HPP

#include <turtle_kv/script/execution_strategy.hpp>
#include <turtle_kv/script/key_distribution.hpp>
#include <turtle_kv/script/key_set.hpp>

#include <turtle_kv/kv_store.hpp>
#include <turtle_kv/kv_store_config.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/slice.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/stream_util.hpp>

#include <yaml-cpp/yaml.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <vector>

namespace turtle_kv {

class ScriptContext
{
 public:
  static constexpr usize kDefaultStackSize = 16;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::filesystem::path kv_store_dir;

  std::filesystem::path script_yml;

  KVStoreConfig config = KVStoreConfig::with_default_values();

  KVStoreRuntimeOptions runtime_options = KVStoreRuntimeOptions::with_default_values();

  std::unique_ptr<KVStore> kv_store;

  KeySet key_set;

  SmallVec<std::string_view, kDefaultStackSize> command_stack;

  SmallVec<script::ExecutionStrategy*, kDefaultStackSize> exec_stack;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status run(script::ExecutionStrategy* exec, const YAML::Node& steps) noexcept;

  template <typename T>
  StatusOr<T> parse_param(const YAML::Node& params, const char* name, T default_value) noexcept;

  ValueView format_value(usize index, const Slice<char>& dst_buffer) noexcept;

  ValueView get_value(usize index, usize value_size) noexcept
  {
    thread_local SmallVec<char, 256> value_buffer;
    value_buffer.reserve(value_size);
    return this->format_value(index, as_slice(value_buffer.data(), value_size));
  }

  StatusOr<usize> schedule(std::vector<script::Operation>&& ops) noexcept
  {
    return this->exec_stack.back()->schedule(std::move(ops));
  }

  StatusOr<usize> schedule(script::Operation&& single_op) noexcept
  {
    std::vector<script::Operation> vec;
    vec.emplace_back(std::move(single_op));
    return this->exec_stack.back()->schedule(std::move(vec));
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline StatusOr<T> ScriptContext::parse_param(const YAML::Node& params,
                                              const char* name,
                                              T default_value) noexcept
{
  const YAML::Node param_value = params[name];
  if (!param_value.IsDefined()) {
    return {std::move(default_value)};
  }

  auto param_value_str = param_value.as<std::string>();

  if constexpr (std::is_same_v<T, std::unique_ptr<KeyDistribution>>) {
    return KeyDistribution::from_param(*this, param_value_str, params);
  } else {
    std::optional<T> parsed = batt::from_string<T>(param_value_str);
    if (!parsed) {
      LOG(ERROR) << name << " param must be of type `" << batt::name_of<T>() << "`";
      return {batt::StatusCode::kInvalidArgument};
    }
    return {*parsed};
  }
}

}  // namespace turtle_kv
