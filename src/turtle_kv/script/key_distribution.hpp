//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_SCRIPT_KEY_DISTRIBUTION_HPP

#include <turtle_kv/core/key_view.hpp>

#include <turtle_kv/import/int_types.hpp>
#include <turtle_kv/import/small_vec.hpp>

namespace turtle_kv {

class KeyDistribution
{
 public:
  KeyDistribution(const KeyDistribution&) = delete;
  KeyDistribution& operator=(const KeyDistribution&) = delete;

  virtual ~KeyDistribution() = default;

  virtual KeyView get_key(usize ordinal, SmallVecBase<char>& key_buffer, usize key_size) = 0;

 protected:
  KeyDistribution() = default;
};

}  // namespace turtle_kv
