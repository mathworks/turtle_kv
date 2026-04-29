//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_OPERATION_HPP

#include <turtle_kv/api_types.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/status.hpp>

#include <memory>
#include <utility>
#include <variant>
#include <vector>

namespace turtle_kv {

class ScriptContext;

namespace script {

/** \brief Updates the configuration.
 */
struct Config {
  std::unique_ptr<std::vector<std::pair<std::string, std::string>>> params;
};

/** \brief Creates a new KVStore on disk.
 */
struct Create {
  RemoveExisting remove_existing;
};

/** \brief Insertion of a new key.
 */
struct Insert {
  KeyView key;
  usize index;
  usize value_size;
};

/** \brief Opens the KVStore.
 */
struct Open {
};

/** \brief A query on an individual key; non-empty query, expected to succeed.
 */
struct PointQuery {
  usize index;
};

/** \brief An update to an already inserted key.
 */
struct Update {
  usize index;
  usize value_size;
};

using Operation = std::variant<Config, Create, Insert, Open, PointQuery, Update>;

Status execute_op(ScriptContext& context, Operation& op);

Status execute_op_impl(ScriptContext& context, Config& op);
Status execute_op_impl(ScriptContext& context, Create& op);
Status execute_op_impl(ScriptContext& context, Insert& op);
Status execute_op_impl(ScriptContext& context, Open& op);
Status execute_op_impl(ScriptContext& context, PointQuery& op);
Status execute_op_impl(ScriptContext& context, Update& op);

}  // namespace script
}  // namespace turtle_kv
