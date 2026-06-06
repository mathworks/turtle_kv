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

#include <turtle_kv/core/key_range.hpp>

#include <turtle_kv/import/buffer.hpp>
#include <turtle_kv/import/interval.hpp>

#include <llfs/strings.hpp>

#include <batteries/checked_cast.hpp>
#include <batteries/status.hpp>

#include <glog/logging.h>

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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <std::ranges::range RangeT, typename IterT>
inline StatusOr<IterT> pack_leaf_block(const RangeT& src,
                                       MutableBuffer dst,
                                       const Optional<PackedLeafBlockStats>& opt_stats) noexcept
{
  if (dst.size() < sizeof(PackedLeafBlock)) {
    return {batt::StatusCode::kResourceExhausted};
  }

  PackedLeafBlockStats stats = opt_stats.or_else([&] {
    return PackedLeafBlockStats::from(src, dst.size());
  });

  if (stats.block_size != dst.size()) {
    return {batt::StatusCode::kResourceExhausted};
  }

  PackedLeafBlock* block = static_cast<PackedLeafBlock*>(dst.data());
  {
    block->magic = PackedLeafBlock::kMagic;
    block->items_[0].offset =
        byte_distance(block->items_, advance_pointer(&block->items_[1], stats.item_ptr_bytes));
  }

  //----- --- -- -  -  -   -
  // Pack all slot data.
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

  //----- --- -- -  -  -   -
  // Set the common prefix.
  //
  block->shared_prefix_size =
      BATT_CHECKED_CAST(u16,
                        llfs::find_common_prefix(0, block->min_key(), block->max_key()).size());

  return {src_iter};
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// struct PackedLeafBlock

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline /*static*/ const PackedLeafBlock& PackedLeafBlock::view_of(
    const ConstBuffer& buffer) noexcept
{
  BATT_CHECK_GE(buffer.size(), sizeof(PackedLeafBlock));

  const auto* block = static_cast<const PackedLeafBlock*>(buffer.data());

  BATT_CHECK_EQ(block->magic, PackedLeafBlock::kMagic);

  return *block;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const PackedKeyValueSlotPtr* PackedLeafBlock::find_key(const KeyView& key) const noexcept
{
  const auto convert_result = [](auto&& first_last_pair) -> const PackedKeyValueSlotPtr* {
    if (first_last_pair.first == first_last_pair.second) {
      return nullptr;
    }
    return first_last_pair.first;
  };

  if (this->shared_prefix_size > 0) {
    const usize prefix_size = this->shared_prefix_size;
    if (key.size() < prefix_size) {
      return nullptr;
    }
    auto order = batt::compare(key.substr(0, prefix_size), this->shared_key_prefix());
    if (order != batt::Order::Equal) {
      return nullptr;
    }

    return convert_result(
        std::equal_range(this->items_begin(), this->items_end(), key, KeySuffixOrder{prefix_size}));
  }

  return convert_result(std::equal_range(this->items_begin(),
                                         this->items_end(),
                                         key,
                                         [](const auto& l, const auto& r) {
                                           return batt::compare(get_key(l), get_key(r)) ==
                                                  batt::Order::Less;
                                         }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline const PackedKeyValueSlotPtr* PackedLeafBlock::lower_bound(const KeyView& key) const noexcept
{
  return std::lower_bound(this->items_begin(),
                          this->items_end(),
                          key,
                          [](const auto& l, const auto& r) {
                            return batt::compare(get_key(l), get_key(r)) == batt::Order::Less;
                          });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline Slice<const PackedKeyValueSlotPtr> PackedLeafBlock::items_slice(
    Optional<KeyView> key_lower_bound,
    Optional<KeyView> key_upper_bound) const noexcept
{
  auto [first, last] = std::equal_range(this->items_begin(),
                                        this->items_end(),
                                        Interval<KeyView>{key_lower_bound.or_else(global_min_key),
                                                          key_upper_bound.or_else(global_max_key)},
                                        ExtendedKeyRangeOrder{});
  return as_slice(first, last);
}

}  // namespace turtle_kv
