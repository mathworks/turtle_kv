//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_LEAF_BLOCK_IPP

#include "packed_leaf_block.hpp"

#include <turtle_kv/import/buffer.hpp>

#include <batteries/status.hpp>

#include <glog/logging.h>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::ranges::range RangeT>
/*static*/ PackedLeafBlockStats PackedLeafBlockStats::from(const RangeT& src,
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::ranges::range RangeT, typename IterT>
StatusOr<IterT> pack_leaf_block(const RangeT& src, MutableBuffer dst) noexcept
{
  if (dst.size() < sizeof(PackedLeafBlock)) {
    return {batt::StatusCode::kResourceExhausted};
  }

  auto stats = PackedLeafBlockStats::from(src, dst.size());
  if (stats.block_size != dst.size()) {
    return {batt::StatusCode::kResourceExhausted};
  }

  PackedLeafBlock* block = static_cast<PackedLeafBlock*>(dst.data());
  {
    block->magic = PackedLeafBlock::kMagic;
    block->items_[0].offset =
        byte_distance(block->items_, advance_pointer(&block->items_[1], stats.item_ptr_bytes));
  }

  PackedKeyValueSlotPtr* pp_slot = block->items_;
  void* p_slot = const_cast<PackedKeyValueSlot*>(pp_slot->get());

  IterT src_iter = std::begin(src);
  const IterT src_end = std::next(src_iter, stats.item_count);
  for (; src_iter != src_end; ++src_iter) {
    const usize slot_size = pack_key_value_slot(*src_iter, p_slot);
    p_slot = advance_pointer(p_slot, slot_size);
    ++pp_slot;
    pp_slot->offset = byte_distance(pp_slot, p_slot);
  }

  return {src_iter};
}

}  // namespace turtle_kv
