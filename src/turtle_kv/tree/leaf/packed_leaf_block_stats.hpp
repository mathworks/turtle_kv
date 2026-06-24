//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_LEAF_PACKED_LEAF_BLOCK_STATS_HPP

#include <turtle_kv/import/int_types.hpp>

#include <batteries/operators.hpp>

#include <ranges>

namespace turtle_kv {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedLeafBlockStats {
  usize block_size;
  usize item_count;
  usize item_slot_bytes;
  usize item_ptr_bytes;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <std::ranges::range RangeT>
  static PackedLeafBlockStats from(const RangeT& src, usize block_size) noexcept;
};

BATT_OBJECT_PRINT_IMPL((inline),
                       PackedLeafBlockStats,
                       (block_size, item_count, item_slot_bytes, item_ptr_bytes))

}  // namespace turtle_kv
