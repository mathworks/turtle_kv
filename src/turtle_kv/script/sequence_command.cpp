//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/sequence_command.hpp>
//

#include <turtle_kv/script/execution_strategy.hpp>

#include <thread>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status sequence_command(ScriptContext& context, const YAML::Node& params)
{
  BATT_REQUIRE_OK(context.run(nullptr, params));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
