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
  bool invalid_params = false;

  for (const auto& param_pair : params) {
    LOG(ERROR) << "bad param:" << BATT_INSPECT(param_pair.first) << BATT_INSPECT(param_pair.second);
    invalid_params = true;
  }
  if (invalid_params) {
    return batt::StatusCode::kInvalidArgument;
  }

  LOG(INFO) << "open(" << context.kv_store_dir << ", " << context.config.tree_options << ", "
            << context.runtime_options << ")";

  BATT_ASSIGN_OK_RESULT(
      context.kv_store,
      KVStore::open(context.kv_store_dir, context.config.tree_options, context.runtime_options));

  return OkStatus();
}

}  // namespace turtle_kv
