//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/run_script.hpp>
//

#include <turtle_kv/script/execution_strategy.hpp>
#include <turtle_kv/script/script_context.hpp>

#include <yaml-cpp/yaml.h>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status run_script(const std::filesystem::path& kv_store_dir [[maybe_unused]],
                  const std::filesystem::path& script_yml [[maybe_unused]])
{
  YAML::Node script = YAML::LoadFile(script_yml)["script"];

  BATT_REQUIRE_EQ(script.IsSequence(), true);

  ScriptContext context;

  context.kv_store_dir = kv_store_dir;
  context.script_yml = script_yml;

  script::ExecuteImmediately default_exec{context};
  script::ExecutionTimer timed_exec{context, default_exec};

  BATT_REQUIRE_OK(context.run(&timed_exec, script));

  LOG(INFO) << "(done)";

  return OkStatus();
}

}  // namespace turtle_kv
