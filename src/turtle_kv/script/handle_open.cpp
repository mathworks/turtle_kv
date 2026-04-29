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
  LOG(INFO) << "open()";

  BATT_REQUIRE_OK(context.schedule(script::Open{}));

  return OkStatus();
}

}  // namespace turtle_kv
