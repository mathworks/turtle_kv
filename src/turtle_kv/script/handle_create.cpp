//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/handle_create.hpp>
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status handle_create(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  RemoveExisting remove_existing{false};

  for (const auto& param_pair : params) {
    const std::string param_name = param_pair.first.as<std::string>();

    if (param_name == "remove_existing") {
      remove_existing = RemoveExisting{param_pair.second.as<bool>()};
    } else {
      LOG(ERROR) << "bad param:" << BATT_INSPECT(param_pair.first)
                 << BATT_INSPECT(param_pair.second);
      return batt::StatusCode::kInvalidArgument;
    }
  }

  LOG(INFO) << "create(" << context.kv_store_dir << ", " << context.config
            << ", remove_existing=" << remove_existing << ")";

  BATT_REQUIRE_OK(KVStore::create(context.kv_store_dir, context.config, remove_existing));

  return OkStatus();
}

}  // namespace turtle_kv
