//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <turtle_kv/script/operations.hpp>
//

#include <turtle_kv/script/script_context.hpp>

#include <batteries/case_of.hpp>

namespace turtle_kv {
namespace script {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op(ScriptContext& context, Operation& op)
{
  return batt::case_of(op, [&context](auto& impl) {
    return execute_op_impl(context, impl);
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, Config& op)
{
  for (const auto& [config_name, config_value] : *op.params) {
    BATT_REQUIRE_OK(
        parse_config(config_name, config_value, &context.config, &context.runtime_options));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, Create& op)
{
  BATT_REQUIRE_OK(KVStore::create(context.kv_store_dir, context.config, op.remove_existing));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, Insert& op)
{
  // Insert!
  //
  BATT_REQUIRE_OK(context.kv_store->put(op.key, context.get_value(op.index, op.value_size)));

  // Mark the key as inserted (for future/concurrent non-empty point queries).
  //
  context.key_set.set_key_inserted(op.index, true);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, Open& op [[maybe_unused]])
{
  BATT_ASSIGN_OK_RESULT(
      context.kv_store,
      KVStore::open(context.kv_store_dir, context.config.tree_options, context.runtime_options));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, PointQuery& op)
{
  KeyView key = context.key_set.wait_for_key_inserted(op.index);

  return context.kv_store->get(key).status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, Update& op)
{
  KeyView key = context.key_set.wait_for_key_inserted(op.index);
  ValueView value = context.get_value(op.index, op.value_size);

  BATT_REQUIRE_OK(context.kv_store->put(key, value));

  return OkStatus();
}

}  // namespace script
}  // namespace turtle_kv
