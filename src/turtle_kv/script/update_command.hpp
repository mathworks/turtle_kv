//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_UPDATE_COMMAND_HPP

#include <turtle_kv/script/commands.hpp>

namespace turtle_kv {
namespace script {

Status update_command(ScriptContext& context, const YAML::Node& params);

static_assert(std::is_constructible_v<CommandFn, decltype(&update_command)>);

}  // namespace script
}  // namespace turtle_kv
