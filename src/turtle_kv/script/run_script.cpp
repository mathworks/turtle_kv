//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/run_script.hpp>
//
#include <turtle_kv/script/command_handlers.hpp>
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

  auto& command_handlers = get_command_handlers();

  for (YAML::Node command : script) {
    BATT_REQUIRE_EQ(command.IsMap(), true);
    BATT_REQUIRE_EQ(command.size(), 1);

    for (const auto& pair : command) {
      const std::string name = pair.first.as<std::string>();
      const YAML::Node& params = pair.second;

      BATT_REQUIRE_EQ(command_handlers.count(name), 1);

      Status status = command_handlers[name](context, params);
      BATT_REQUIRE_OK(status);
    }
  }

  LOG(INFO) << "(done)";

  return OkStatus();
}

}  // namespace turtle_kv
