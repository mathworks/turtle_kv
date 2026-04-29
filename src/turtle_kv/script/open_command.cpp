//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/open_command.hpp>
//

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status open_command(ScriptContext& context [[maybe_unused]], const YAML::Node& params)
{
  LOG(INFO) << "open()";

  BATT_REQUIRE_OK(context.schedule(script::Open{}));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
