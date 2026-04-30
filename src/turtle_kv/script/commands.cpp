//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/commands.hpp>
//

#include <turtle_kv/script/concurrent_command.hpp>
#include <turtle_kv/script/config_command.hpp>
#include <turtle_kv/script/create_command.hpp>
#include <turtle_kv/script/insert_command.hpp>
#include <turtle_kv/script/interleave_command.hpp>
#include <turtle_kv/script/open_command.hpp>
#include <turtle_kv/script/parallel_command.hpp>
#include <turtle_kv/script/point_query_command.hpp>
#include <turtle_kv/script/sequence_command.hpp>
#include <turtle_kv/script/update_command.hpp>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unordered_map<std::string, CommandFn>& get_commands()
{
  thread_local std::unordered_map<std::string, CommandFn> commands_;

  thread_local bool initialized = [] {
    commands_["concurrent"] = &concurrent_command;
    commands_["config"] = &config_command;
    commands_["create"] = &create_command;
    commands_["insert"] = &insert_command;
    commands_["interleave"] = &interleave_command;
    commands_["open"] = &open_command;
    commands_["parallel"] = &parallel_command;
    commands_["point_query"] = &point_query_command;
    commands_["sequence"] = &sequence_command;
    commands_["update"] = &update_command;

    return true;
  }();

  BATT_CHECK(initialized);

  return commands_;
}

}  // namespace script
}  // namespace turtle_kv
