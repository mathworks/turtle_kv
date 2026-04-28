//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/command_handlers.hpp>
//

#include <turtle_kv/script/handle_config.hpp>
#include <turtle_kv/script/handle_create.hpp>
#include <turtle_kv/script/handle_insert.hpp>
#include <turtle_kv/script/handle_open.hpp>
#include <turtle_kv/script/handle_point_query.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unordered_map<std::string, CommandHandlerFn>& get_command_handlers()
{
  thread_local std::unordered_map<std::string, CommandHandlerFn> handlers_;

  thread_local bool initialized = [] {
    handlers_["config"] = &handle_config;
    handlers_["create"] = &handle_create;
    handlers_["insert"] = &handle_insert;
    handlers_["open"] = &handle_open;
    handlers_["point_query"] = &handle_point_query;

    return true;
  }();

  BATT_CHECK(initialized);

  return handlers_;
}

}  // namespace turtle_kv
