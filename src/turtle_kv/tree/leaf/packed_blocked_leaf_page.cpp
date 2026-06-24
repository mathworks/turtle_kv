//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the TurtleKV Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include "packed_blocked_leaf_page.hpp"
//

namespace turtle_kv {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ usize PackedBlockedLeafPage::estimate_capacity(usize leaf_size,
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
