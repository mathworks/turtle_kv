//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define TURTLE_KV_TREE_PACKED_BLOCKED_LEAF_PAGE_HPP

#include "packed_blocked_leaf_page.hpp"

#include <turtle_kv/import/small_vec.hpp>

#include <llfs/packed_page_header.hpp>

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemRangeT>
StatusOr<PackedBlockedLeafPage*> pack_blocked_leaf_page(const ItemRangeT& src_items,
                                                        MutableBuffer dst_buffer) noexcept
{
  const usize block_size = 8192;
  const usize item_count = std::size(src_items);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Calculate the number of blocks needed and how many items in each one.
  //
  SmallVec<PackedLeafBlockStats, 256> block_stats;
  {
    auto src_iter = std::begin(src_items);
    const auto src_end = std::end(src_items);
    usize blocks_size_remaining = dst_buffer.size() - block_size;
    for (;;) {
      if (src_iter == src_end) {
        break;
      }
      BATT_CHECK_LT(src_iter, src_end);

      BATT_ASSIGN_OK_RESULT(auto stats,
                            PackedLeafBlockStats::from(std::ranges::subrange(src_iter, src_end),
                                                       blocks_size_remaining));

      blocks_size_remaining -= block_size;
      src_iter = std::next(src_iter, stats.item_count);
    }
  }
  const usize block_count = block_stats.size();
  const usize block_starting_item_array_size =
      sizeof(llfs::PackedArray<little_u32>) + sizeof(little_u32) * block_count;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Initialize the leaf header.
  //
  MutableBuffer dst_remaining = dst_buffer;
  dst_remaining += sizeof(llfs::PackedPageHeader);

  auto* leaf_header = static_cast<PackedBlockedLeafPage*>(dst_remaining.data());
  {
    leaf_header->magic = PackedBlockedLeafPage::kMagic;
    leaf_header->item_count = BATT_CHECKED_CAST(u32, item_count);
    leaf_header->total_packed_size = 0;    // TODO [tastolfi 2026-06-01]
    leaf_header->blocks_per_trie_key = 0;  // TODO [tastolfi 2026-06-01]
    leaf_header->block_size_bytes = BATT_CHECKED_CAST(u32, block_size);
    leaf_header->block_count = BATT_CHECKED_CAST(u32, block_count);
    leaf_header->block0_byte_offset_in_page = 0;  // TODO [tastolfi 2026-06-01]
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Pack `block_starting_item` array.
  //

  {
    auto* block_starting_item = static_cast<llfs::PackedArray<little_u32>*>(dst_remaining.data());
    dst_remaining += block_starting_item_array_size;

    block_starting_item->initialize(block_stats.size());

    little_u32* block_start = block_starting_item->data();
    u32 item_i = 0;
    for (const PackedLeafBlockStats& stats : block_stats) {
      *block_start = item_i;
      item_i += stats.item_count;
      ++block_start;
    }

    leaf_header->block_starting_item.reset_unsafe(block_starting_item);
  }
}

}  // namespace turtle_kv
