//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/handle_config.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status handle_config(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  RemoveExisting remove_existing{false};

  for (const auto& param_pair : params) {
    const std::string config_name = param_pair.first.as<std::string>();
    const std::string config_value = param_pair.second.as<std::string>();

    BATT_REQUIRE_OK(
        parse_config(config_name, config_value, &context.config, &context.runtime_options));
  }

  LOG(INFO) << "config:" << BATT_INSPECT(context.config) << BATT_INSPECT(context.runtime_options);

  return OkStatus();
}

}  // namespace turtle_kv
