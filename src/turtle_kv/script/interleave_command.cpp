//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/interleave_command.hpp>
//

#include <turtle_kv/script/execution_strategy.hpp>

#include <thread>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status interleave_command(ScriptContext& context, const YAML::Node& params)
{
  Interleave interleave_exec;

  BATT_REQUIRE_OK(context.run(&interleave_exec, params));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
