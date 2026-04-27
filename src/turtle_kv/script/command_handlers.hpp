//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_COMMAND_HANDLERS_HPP

#include <turtle_kv/script/script_context.hpp>

#include <turtle_kv/import/status.hpp>

#include <yaml-cpp/yaml.h>

#include <functional>
#include <string>
#include <unordered_map>

namespace turtle_kv {

/** \brief A type-erased command handler function.
 */
using CommandHandlerFn = std::function<Status(ScriptContext&, const YAML::Node&)>;

/** \brief Returns a lookup-table of handler functions for all registered commands.
 */
std::unordered_map<std::string, CommandHandlerFn>& get_command_handlers();

}  // namespace turtle_kv
