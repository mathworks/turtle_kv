//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/parallel_command.hpp>
//

#include <turtle_kv/script/execution_strategy.hpp>

#include <thread>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status parallel_command(ScriptContext& context, const YAML::Node& params)
{
  BATT_ASSIGN_OK_RESULT(i32 n_threads, context.parse_param<usize>(params, "n_threads", -1));

  if (n_threads == -1) {
    n_threads = std::thread::hardware_concurrency();
  }

  const YAML::Node stages = params["stages"];

  if (!stages.IsDefined()) {
    LOG(ERROR) << "'parallel' command missing required property: 'stages': " << params;
    return batt::StatusCode::kInternal;
  }

  Parallel parallel_exec{context, n_threads};
  ExecutionTimer stage_timer{context, parallel_exec};

  BATT_REQUIRE_OK(context.run(&stage_timer, stages));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
