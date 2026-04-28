//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_KEY_DISTRIBUTION_HPP

#include <turtle_kv/script/key_set.hpp>

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>
#include <turtle_kv/import/status.hpp>

#include <yaml-cpp/yaml.h>

namespace turtle_kv {

struct ScriptContext;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A distribution of keys for operations.
 */
class KeyDistribution
{
 public:
  static StatusOr<std::unique_ptr<KeyDistribution>> from_param(ScriptContext& context,
                                                               const std::string& param_value,
                                                               const YAML::Node& params) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  KeyDistribution(const KeyDistribution&) = delete;
  KeyDistribution& operator=(const KeyDistribution&) = delete;

  virtual ~KeyDistribution() = default;

  virtual KeyView get_next(KeySet& inserted_keys) = 0;

 protected:
  KeyDistribution() = default;
};

}  // namespace turtle_kv
