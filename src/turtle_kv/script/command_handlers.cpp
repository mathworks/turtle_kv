//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/command_handlers.hpp>
//

#include <turtle_kv/script/handle_create.hpp>
#include <turtle_kv/script/handle_insert.hpp>
#include <turtle_kv/script/handle_open.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unordered_map<std::string, CommandHandlerFn>& get_command_handlers()
{
  thread_local std::unordered_map<std::string, CommandHandlerFn> handlers_;

  thread_local bool initialized = [] {
    handlers_["create"] = &handle_create;
    handlers_["open"] = &handle_open;
    handlers_["insert"] = &handle_insert;

    return true;
  }();

  BATT_CHECK(initialized);

  return handlers_;
}

}  // namespace turtle_kv
