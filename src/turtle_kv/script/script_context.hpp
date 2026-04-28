//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_SCRIPT_CONTEXT_HPP

#include <turtle_kv/script/key_distribution.hpp>
#include <turtle_kv/script/key_set.hpp>

#include <turtle_kv/kv_store.hpp>
#include <turtle_kv/kv_store_config.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <batteries/stream_util.hpp>

#include <yaml-cpp/yaml.h>

#include <atomic>
#include <filesystem>
#include <memory>

namespace turtle_kv {

struct ScriptContext {
  std::filesystem::path kv_store_dir;

  std::filesystem::path script_yml;

  KVStoreConfig config = KVStoreConfig::with_default_values();

  KVStoreRuntimeOptions runtime_options = KVStoreRuntimeOptions::with_default_values();

  std::unique_ptr<KVStore> kv_store;

  KeySet inserted_keys;

  SmallVec<std::string_view, 16> command_stack;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  StatusOr<T> parse_param(const YAML::Node& params, const char* name, T default_value);
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline StatusOr<T> ScriptContext::parse_param(const YAML::Node& params,
                                              const char* name,
                                              T default_value)
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
