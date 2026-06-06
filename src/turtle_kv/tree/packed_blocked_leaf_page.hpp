//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_BLOCKED_LEAF_PAGE_HPP

#include <turtle_kv/tree/packed_leaf_block.hpp>

#include <turtle_kv/core/packed_key_value_slot.hpp>

#include <turtle_kv/import/int_types.hpp>

#include <llfs/packed_array.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/packed_pointer.hpp>

#include <artc/packed/node_base.hpp>
#include <artc/packed/query.hpp>

#include <batteries/bit_ops/bit_count.hpp>

namespace turtle_kv {

struct PackedBlockedLeafPage {
  static constexpr u64 kMagic = 0x6456beb7f9558445ull;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename EditT>
  static usize packed_edit_size(const EditT& edit) noexcept
  {
    return PackedLeafBlock::packed_edit_size(edit);
  }

  static usize estimate_capacity(usize leaf_size,
                                 usize block_size,
                                 usize max_key_size,
                                 usize max_edit_size) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  big_u64 magic;                                // +8 -> 8
  little_u32 item_count;                        // +4 -> 12
  little_u32 total_packed_size;                 // +4 -> 16
  little_u32 blocks_per_art_key;                // +4 -> 20
  little_u32 block_size_bytes;                  // +4 -> 24
  little_u32 block_count;                       // +4 -> 28
  llfs::PackedPointer<PackedLeafBlock> block0;  // +4 -> 32

  /** \brief Pointer to array that stores, for each block, the starting item index relative to the
   * entire leaf.
   */
  llfs::PackedPointer<llfs::PackedArray<little_u32>> block_starting_item;  // +4 -> 36

  /** \brief Pointer to packed ART index.
   */
  llfs::PackedPointer<const artc::packed::NodeBase> art_block_index;  // +4 -> 40

  u8 pad_[24];

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

static_assert(sizeof(PackedBlockedLeafPage) == 64);

template <typename ItemRangeT>
StatusOr<PackedBlockedLeafPage*> pack_blocked_leaf_page(const usize block_size,
                                                        const ItemRangeT& src_items,
                                                        const MutableBuffer& dst_buffer) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline /*static*/ usize PackedBlockedLeafPage::estimate_capacity(usize leaf_size,
                                                                 usize block_size,
                                                                 usize max_key_size,
                                                                 usize max_edit_size) noexcept
{
  const usize space_after_header =
      leaf_size - (sizeof(llfs::PackedPageHeader) + sizeof(PackedBlockedLeafPage));

  const usize max_block_count = space_after_header / block_size;

  const usize block_starts_size =
      sizeof(llfs::PackedArray<little_u32>) + sizeof(little_u32) * max_block_count;

  const usize space_after_block_starts = space_after_header - block_starts_size;

  const usize max_art_size = max_key_size * max_block_count * 2;

  const usize space_after_art = space_after_block_starts - max_art_size;

  BATT_CHECK_EQ(batt::bit_count(block_size), 1) << "Leaf block_size must be a power of 2";
  const usize space_for_blocks = space_after_art & ~(block_size - 1);
  const usize block_count = space_for_blocks / block_size;

  const usize max_wasted_per_block = max_edit_size - 1;
  const usize min_block_capacity = PackedLeafBlock::capacity(block_size) - max_wasted_per_block;

  const usize final_estimate = block_count * min_block_capacity;

  BATT_CHECK_GT(leaf_size, final_estimate);

  return final_estimate;
}

}  // namespace turtle_kv

#include "packed_blocked_leaf_page.ipp"
