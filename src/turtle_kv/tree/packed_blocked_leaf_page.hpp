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
#include <llfs/packed_pointer.hpp>

#include <artc/packed/node_base.hpp>
#include <artc/packed/query.hpp>

namespace turtle_kv {

struct PackedBlockedLeafPage {
  static constexpr u64 kMagic = 0x6456beb7f9558445ull;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  big_u64 magic;
  little_u32 item_count;
  little_u32 total_packed_size;
  little_u32 blocks_per_art_key;
  little_u32 block_size_bytes;
  little_u32 block_count;
  little_u32 block0_byte_offset_in_page;

  /** \brief Pointer to array that stores, for each block, the starting item index relative to the
   * entire leaf.
   */
  llfs::PackedPointer<llfs::PackedArray<little_u32>> block_starting_item;

  /** \brief Pointer to packed ART index.
   */
  llfs::PackedPointer<artc::packed::NodeBase> art_block_index;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

template <typename ItemRangeT>
StatusOr<PackedBlockedLeafPage*> pack_blocked_leaf_page(const ItemRangeT& src_items,
                                                        MutableBuffer dst_buffer) noexcept;

}  // namespace turtle_kv

#include "packed_blocked_leaf_page.ipp"
