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
  thread_local SmallVec<char, 256> value_buffer;

  // Retrieve the key by index from the key set.
  //
  KeyView key = op.key;  // context.key_set.get_key_by_index(op.index).value_or_panic();

  // Format a unique value.
  //
  value_buffer.reserve(op.value_size);
  ValueView value = context.next_value(as_slice(value_buffer.data(), op.value_size));

  // Insert!
  //
  BATT_REQUIRE_OK(context.kv_store->put(key, value));

  // Mark the key as inserted (for future/concurrent non-empty point queries).
  //
  context.inserted_keys.insert_key_view_at(op.index, key);

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
  KeyView key = context.inserted_keys.get_key_by_index(op.index).value_or_panic();
  return context.kv_store->get(key).status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status execute_op_impl(ScriptContext& context, Update& op)
{
  return batt::StatusCode::kUnimplemented;
}

}  // namespace script
}  // namespace turtle_kv
