//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/concurrent_command.hpp>
//

#include <turtle_kv/script/execution_strategy.hpp>

#include <thread>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status concurrent_command(ScriptContext& context, const YAML::Node& params)
{
  const YAML::Node tasks = params["tasks"];

  if (!tasks.IsDefined()) {
    LOG(ERROR) << "'concurrent' command missing required property: 'tasks': " << params;
    return batt::StatusCode::kInternal;
  }

  Concurrent concurrent_exec{context};
  ExecutionTimer all_tasks_timer{context, concurrent_exec};

  BATT_REQUIRE_OK(context.run(&all_tasks_timer, tasks));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
