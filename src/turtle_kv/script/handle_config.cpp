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
Status handle_config(ScriptContext& context, const YAML::Node& params)
{
  std::vector<std::pair<std::string, std::string>> config_pairs;

  for (const auto& param_pair : params) {
    config_pairs.emplace_back(param_pair.first.as<std::string>(),  //
                              param_pair.second.as<std::string>());
  }

  LOG(INFO) << "config:" << BATT_INSPECT_RANGE(config_pairs);

  BATT_REQUIRE_OK(context.schedule(script::Config{
      .params = std::make_unique<decltype(config_pairs)>(std::move(config_pairs)),
  }));

  return OkStatus();
}

}  // namespace turtle_kv
