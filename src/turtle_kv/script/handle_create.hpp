//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_HANDLE_CREATE_HPP

#include <turtle_kv/script/command_handlers.hpp>

namespace turtle_kv {

Status handle_create(ScriptContext& context, const YAML::Node& params);

static_assert(std::is_constructible_v<CommandHandlerFn, decltype(&handle_create)>);

}  // namespace turtle_kv
