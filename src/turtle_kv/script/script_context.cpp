//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/script_context.hpp>
//

#include <turtle_kv/script/commands.hpp>

#include <cstring>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status ScriptContext::run(script::ExecutionStrategy* exec, const YAML::Node& script) noexcept
{
  if (exec) {
    BATT_REQUIRE_OK(exec->activate(this->exec_stack.empty() ? nullptr : this->exec_stack.back()));
    this->exec_stack.push_back(exec);
  }
  auto on_scope_exit = batt::finally([&] {
    if (exec) {
      BATT_CHECK_EQ(exec, this->exec_stack.back());
      this->exec_stack.pop_back();
      exec->retire(this->exec_stack.empty() ? nullptr : this->exec_stack.back()).IgnoreError();
    }
  });

  BATT_CHECK(!this->exec_stack.empty());

  auto& commands = script::get_commands();

  for (const YAML::Node& command : script) {
    BATT_REQUIRE_EQ(command.IsMap(), true);
    BATT_REQUIRE_EQ(command.size(), 1);

    for (const auto& pair : command) {
      const std::string name = pair.first.as<std::string>();
      const YAML::Node& params = pair.second;

      if (commands.count(name) == 0) {
        LOG(ERROR) << "command '" << name << "' not found";
        return batt::StatusCode::kInternal;
      }

      this->command_stack.push_back(name);
      auto on_scope_exit = batt::finally([&] {
        this->command_stack.pop_back();
      });

      Status status = commands[name](*this, params);
      BATT_REQUIRE_OK(status);
      break;
    }

    BATT_REQUIRE_OK(this->exec_stack.back()->step());
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ValueView ScriptContext::format_value(usize index, const Slice<char>& dst_buffer) noexcept
{
  usize n = 0;  // std::snprintf(dst_buffer.begin(), dst_buffer.size(), "%uz", index);

  if (n < dst_buffer.size()) {
    std::memset(dst_buffer.begin() + n, '_', dst_buffer.size() - n);
  }

  return ValueView::from_str(std::string_view{dst_buffer.begin(), dst_buffer.size()});
}

}  // namespace turtle_kv
