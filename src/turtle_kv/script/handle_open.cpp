//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/handle_open.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status handle_open(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  for (const auto& param_pair : params) {
    const std::string param_name = param_pair.first.as<std::string>();

    if (param_name == "config") {
      for (const auto& config_pair : param_pair.second) {
        const std::string config_name = config_pair.first.as<std::string>();
        const std::string config_value = config_pair.second.as<std::string>();

        BATT_REQUIRE_OK(
            parse_config(config_name, config_value, &context.config, &context.runtime_options));
      }

    } else {
      LOG(ERROR) << "bad param:" << BATT_INSPECT(param_name) << BATT_INSPECT(param_pair.second);
      return batt::StatusCode::kInvalidArgument;
    }
  }

  LOG(INFO) << "open(" << context.kv_store_dir << ", " << context.config.tree_options << ", "
            << context.runtime_options << ")";

  BATT_ASSIGN_OK_RESULT(
      context.kv_store,
      KVStore::open(context.kv_store_dir, context.config.tree_options, context.runtime_options));

  return OkStatus();
}

}  // namespace turtle_kv
