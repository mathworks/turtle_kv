//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_LEAF_PACKED_LEAF_BLOCK_STATS_IPP

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::ranges::range RangeT>
inline /*static*/ PackedLeafBlockStats PackedLeafBlockStats::from(const RangeT& src,
                                                                  usize dst_size) noexcept
{
  PackedLeafBlockStats stats{
      .block_size = 0,
      .item_count = 0,
      .item_slot_bytes = 0,
      .item_ptr_bytes = 0,
  };

  if (dst_size < sizeof(PackedLeafBlock)) {
    return stats;
  }
  stats.block_size = dst_size;
  usize offset = 0;
  dst_size -= sizeof(PackedLeafBlock);
  offset += sizeof(PackedLeafBlock);

  for (const auto& src_item : src) {
    const usize slot_size = packed_key_value_slot_size(src_item);
    const usize total_item_size = slot_size + sizeof(PackedKeyValueSlotPtr);
    if (dst_size < total_item_size) {
      break;
    }
    stats.item_count += 1;
    stats.item_slot_bytes += slot_size;
    stats.item_ptr_bytes += sizeof(PackedKeyValueSlotPtr);
    dst_size -= total_item_size;
    offset += total_item_size;

    if constexpr (false) {
      LOG(INFO) << BATT_INSPECT(offset) << BATT_INSPECT_STR(get_key(src_item))
                << BATT_INSPECT(stats.item_count);
    }
  }

  return stats;
}

}  // namespace turtle_kv
