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
Status handle_create(ScriptContext& context, const YAML::Node& params)
{
  BATT_ASSIGN_OK_RESULT(bool remove_existing,
                        context.parse_param<bool>(params, "remove_existing", /*default=*/false));

  LOG(INFO) << "create(remove_existing=" << remove_existing << ")";

  BATT_REQUIRE_OK(context.schedule(script::Create{
      .remove_existing = RemoveExisting{remove_existing},
  }));

  return OkStatus();
}

}  // namespace turtle_kv
